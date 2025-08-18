package com.wepay.kafka.connect.bigquery.write.storage;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriterBuilder;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

public class AsyncStorageWriteApiWriter {
    private final Executor executor;
    private final Executor callbackExec;
    private final ConcurrentLinkedQueue<AppendRowsAttempt> appendAttempts = new ConcurrentLinkedQueue<>();
//    private final ArrayDeque<ApiFuture<Void>> futuresQueue = new ArrayDeque<>();
    private final AtomicReference<Throwable> fatal = new AtomicReference<>();
    private final StorageWriteApiBase streamWriter;

    public AsyncStorageWriteApiWriter(StorageWriteApiBase streamWriter,
                                      Executor appendRowsExecutor,
                                      Executor callbackExec) {
        this.executor = appendRowsExecutor;
        this.streamWriter = streamWriter;
        this.callbackExec = callbackExec;
    }

    void sendAppendRowsRequest(List<ConvertedRecord> batch, TableName tableName) {
        executor.execute(() -> {
            harvestNonBlocking();

            try {
                ApiFuture<AppendRowsResponse> appendRowsResponseApiFuture =
                        streamWriter.asyncInitializeAndWriteRecords(tableName, batch, "default", callbackExec);
                Map<TopicPartition, Long> maxOffsets = computeMaxOffsets(batch);
                appendAttempts.add(new AppendRowsAttempt(appendRowsResponseApiFuture, maxOffsets));

//                f.addListener(() -> {
//                    try {
//                        f.get(); // non-blocking since done
//                    } catch (Exception e) {
//                        fatal.compareAndSet(null, e);
//                    }
//                }, callbackExec);

            } catch (Throwable t) {
                fatal.compareAndSet(null, t);
            }
        });
    }

    public void maybeThrowFatal() {
        Throwable t = fatal.get();
        if (t != null) {
            if (t instanceof RuntimeException) throw (RuntimeException) t;
            throw new org.apache.kafka.connect.errors.ConnectException(t);
        }
    }

//    public void awaitCurrentAppends() throws InterruptedException {
//        CountDownLatch done = new CountDownLatch(1);
//        executor.execute(() -> {
//            try {
//                for (AppendRowsAttempt a : appendAttempts) {
//                    try {
//                        a.future.get();
//                    } catch (Exception e) {
//                        fatal.compareAndSet(null, e);
//                    }
//                }
//            } finally {
//                done.countDown();
//            }
//        });
//        done.await();
//        maybeThrowFatal();
//    }

//    private void checkForFailedResponses(boolean wait) {
//        while (!futuresQueue.isEmpty()) {
//            ApiFuture<AppendRowsResponse> head = futuresQueue.peekFirst();
//            if (!wait && !head.isDone()) break;
//            try {
//                head.get();
//            } catch (Exception e) {
//                fatal.compareAndSet(null, e);
//            } finally {
//                futuresQueue.removeFirst();
//            }
//        }
//    }

    public void harvestNonBlocking() {
        for (AppendRowsAttempt a : appendAttempts) {
            if (a.future.isDone()) {
                try {
                    a.future.get();
                } catch (Exception e) {
                    fatal.compareAndSet(null, e);
                }
            }
        }
    }

    private Map<TopicPartition, Long> computeMaxOffsets(List<ConvertedRecord> batch) {
        Map<TopicPartition, Long> result = new HashMap<>();
        for (ConvertedRecord record : batch) {
            SinkRecord original = record.original();
            TopicPartition topicPartition = new TopicPartition(original.topic(), original.kafkaPartition());
            result.merge(topicPartition, original.kafkaOffset(), Math::max);
        }
        return result;
    }

    public Map<TopicPartition, OffsetAndMetadata> computeSafeCommits(
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
        maybeThrowFatal();      // surface any failures before trying to commit

        Map<TopicPartition, Long> safeMaxOffsetPerTp = new HashMap<>();
        for (TopicPartition tp : offsetsToCommit.keySet()) safeMaxOffsetPerTp.put(tp, -1L);

        for (AppendRowsAttempt attempt : appendAttempts) {
            boolean isDone = attempt.future.isDone();

            // if is done, verify success before using
            if (isDone) {
                try {
                    attempt.future.get();
                } catch (Exception ex) {
                    fatal.compareAndSet(null, ex);
                }
                maybeThrowFatal();
            }

            boolean blocks = false;
            for (Map.Entry<TopicPartition, Long> e : attempt.maxOffsetInThisAttempt.entrySet()) {
                final TopicPartition tp = e.getKey();
                final OffsetAndMetadata om = offsetsToCommit.get(tp);
                if (om == null) continue; // Kafka is not trying to commit for this TP during current cycle

                final long boundary = om.offset() - 1L;
                final long maxInAttempt = e.getValue();
                if (maxInAttempt > boundary) continue;  // irrelevant for this round

                if (isDone) {
                    safeMaxOffsetPerTp.merge(tp, maxInAttempt, Math::max);
                } else {
                    blocks = true;
                    break;
                }

            }
            if (blocks) break;
        }

        final Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> e : safeMaxOffsetPerTp.entrySet()) {
            long last = e.getValue();
            if (last >= 0) result.put(e.getKey(), new OffsetAndMetadata(last + 1L));
        }

        gcAttempts(result);
        return result;
        }

        private void gcAttempts(Map<TopicPartition, OffsetAndMetadata> committedThisRound) {
            synchronized (appendAttempts) {
                while (!appendAttempts.isEmpty()) {
                    AppendRowsAttempt head = appendAttempts.peek();

                    // Keep pending head; we still need it to compute contiguity next round
                    if (!head.future.isDone()) break;

                    boolean fullyBehind = true;
                    for (Map.Entry<TopicPartition, Long> e : head.maxOffsetInThisAttempt.entrySet()) {
                        TopicPartition tp = e.getKey();
                        OffsetAndMetadata om = committedThisRound.get(tp);
                        if (om == null) {                 // we didn't advance this TP this round → keep it
                            fullyBehind = false;
                            break;
                        }
                        long committedUpTo = om.offset() - 1L;
                        if (e.getValue() > committedUpTo) {
                            fullyBehind = false;            // head still straddles beyond what we just committed
                            break;
                        }
                    }

                    if (fullyBehind) {
                        appendAttempts.poll();
                    } else {
                        break;
                    }
                }
            }
        }

//        private void gcAttempts (Map < TopicPartition, OffsetAndMetadata > committed){
//            while (!appendAttempts.isEmpty()) {
//                AppendRowsAttempt head = appendAttempts.peekFirst();
//                boolean fullyBehind = true;
//                for (Map.Entry<TopicPartition, Long> e : head.maxOffsetInThisAttempt.entrySet()) {
//                    OffsetAndMetadata om = committed.get(e.getKey());
//                    if (om == null) continue; // not committing this TP now; keep it
//                    long committedUpTo = om.offset() - 1;
//                    if (e.getValue() > committedUpTo) {
//                        fullyBehind = false;
//                        break;
//                    }
//                }
//                if (fullyBehind) appendAttempts.removeFirst();
//                else break;
//            }
//        }

        public static class Builder implements TableWriterBuilder {
            private final List<ConvertedRecord> records = new ArrayList<>();
            private final SinkRecordConverter recordConverter;
            private final TableName tableName;
            private final AsyncStorageWriteApiWriter asyncWriter;

            public Builder(TableName tableName,
                           SinkRecordConverter recordConverter,
                           AsyncStorageWriteApiWriter asyncWriter) {
                this.tableName = tableName;
                this.recordConverter = recordConverter;
                this.asyncWriter = asyncWriter;
            }

            /**
             * Captures actual record and corresponding JSONObject converted record
             *
             * @param sinkRecord The actual records
             */
            @Override
            public void addRow(SinkRecord sinkRecord, TableId tableId) {
                records.add(new ConvertedRecord(sinkRecord, convertRecord(sinkRecord)));
            }

            /**
             * Converts SinkRecord to JSONObject to be sent to BQ Streams
             *
             * @param record which is to be converted
             * @return converted record as JSONObject
             */
            private JSONObject convertRecord(SinkRecord record) {
                Map<String, Object> convertedRecord = recordConverter.getRegularRow(record);
                return getJsonFromMap(convertedRecord);
            }

            /**
             * @return Builds Storage write API writer which would do actual data ingestion using streams
             */
            @Override
            public Runnable build() {
                // snapshot the list so the builder can be reused next poll if desired
                final List<ConvertedRecord> batch = new ArrayList<>(records);
                return () -> asyncWriter.sendAppendRowsRequest(batch, tableName);
            }

            private JSONObject getJsonFromMap(Map<String, Object> map) {
                JSONObject jsonObject = new JSONObject();
                map.forEach((key, value) -> {
                    if (value instanceof Map<?, ?>) {
                        value = getJsonFromMap((Map<String, Object>) value);
                    } else if (value instanceof List<?>) {
                        JSONArray items = new JSONArray();
                        ((List<?>) value).forEach(v -> {
                            if (v instanceof Map<?, ?>) {
                                items.put(getJsonFromMap((Map<String, Object>) v));
                            } else {
                                items.put(v);
                            }
                        });
                        value = items;
                    }
                    jsonObject.put(key, value);
                });
                return jsonObject;
            }
        }

    private static class AppendRowsAttempt {
        final ApiFuture<AppendRowsResponse> future;
        final Map<TopicPartition, Long> maxOffsetInThisAttempt;

        AppendRowsAttempt(ApiFuture<AppendRowsResponse> future, Map<TopicPartition, Long> maxOffsetInThisAttempt) {
            this.future = future;
            this.maxOffsetInThisAttempt = maxOffsetInThisAttempt;
        }
    }
}
