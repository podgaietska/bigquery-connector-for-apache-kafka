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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

public class AsyncStorageWriteApiWriter {
    private final Executor executor;
    private final Executor callbackExec;
    private final ConcurrentLinkedQueue<AppendRowsAttempt> appendAttempts = new ConcurrentLinkedQueue<>();
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
            try {
                ApiFuture<AppendRowsResponse> appendRowsResponseApiFuture =
                        streamWriter.asyncInitializeAndWriteRecords(tableName, batch, "default", callbackExec);
                Map<TopicPartition, Long> minOffsetsByTp = computeMinOffsetsPerTp(batch);
                appendAttempts.add(new AppendRowsAttempt(appendRowsResponseApiFuture, minOffsetsByTp));

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

    private void drainCompletedAppends(boolean wait) {
        while (!appendAttempts.isEmpty()) {
            AppendRowsAttempt head = appendAttempts.peek();
            if (!wait && !head.future.isDone()) break;
            try {
                head.future.get();
            } catch (Exception e) {
                fatal.compareAndSet(null, e);
            } finally {
                appendAttempts.poll();
            }
        }
    }

    private Map<TopicPartition, Long> computeMinOffsetsPerTp(List<ConvertedRecord> batch) {
        Map<TopicPartition, Long> minOffsetForTp = new HashMap<>();
        for (ConvertedRecord record : batch) {
            SinkRecord originalRecord = record.original();
            TopicPartition tp = new TopicPartition(originalRecord.topic(), originalRecord.kafkaPartition());
            long offset = originalRecord.kafkaOffset();
            minOffsetForTp.merge(tp, offset, Math::min);
        }
        return minOffsetForTp;
    }

    // VERSION 2
    public Map<TopicPartition, OffsetAndMetadata> getSafeToCommitOffsets(
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
        // trim completed responses from head and surface any failures
        drainCompletedAppends(/* wait */ false);
        maybeThrowFatal();

        final Map<TopicPartition, Long> minPendingOffset = new HashMap<>(offsetsToCommit.size());
        final Set<TopicPartition> unresolved = new HashSet<>(offsetsToCommit.keySet());
        final Map<TopicPartition, Long> tpBoundary = new HashMap<>(offsetsToCommit.size());
        offsetsToCommit.forEach((tp, om) -> tpBoundary.put(tp, om.offset() - 1L));

        for (AppendRowsAttempt attempt: appendAttempts) {
            final boolean done = attempt.future.isDone();
            if (done) {
                try {
                    attempt.future.get();
                } catch (Exception ex) {
                    fatal.compareAndSet(null, ex);
                }
                maybeThrowFatal();
            }

            for (Map.Entry<TopicPartition, Long> e : attempt.minOffsetByTp.entrySet()) {
                final TopicPartition tp = e.getKey();
                if (!unresolved.contains(tp)) continue;

                final Long minOffsetInAttempt = e.getValue();
                final long boundary = tpBoundary.get(tp);

                if (minOffsetInAttempt > boundary) {
                    // first attempt we see for this TP starts beyond boundary so (regardless of done/pending)
                    unresolved.remove(tp);
                } else if (!done) {
                    // pending overlap: block this TP, commit value will be minInAttempt
                    minPendingOffset.put(tp, minOffsetInAttempt);
                    unresolved.remove(tp);
                }
                // else if done & min ≤ b: already written; keep TP unresolved and keep scanning
            }

            if (unresolved.isEmpty()) break; // early exit cuz all commitable offsets for TPs have been decided
        }

        // Build the result: if TP has a pending overlap, commit just before it (commit value = min);
        // otherwise, commit exactly what Kafka asked for.
        Map<TopicPartition, OffsetAndMetadata> safeToCommit = new HashMap<>(offsetsToCommit.size());

        for (Map.Entry<TopicPartition, OffsetAndMetadata> requested : offsetsToCommit.entrySet()) {
            TopicPartition tp = requested.getKey();
            final Long pendingMin = minPendingOffset.get(tp);

            if (pendingMin != null) {
                // Safe last-written is pendingMin - 1, so commit value (next offset) is pendingMin
                safeToCommit.put(tp, new OffsetAndMetadata(pendingMin));
            } else {
                // No pending overlap at/below boundary so we can grant Kafka’s requested commit
                safeToCommit.put(tp, requested.getValue());
            }
        }

        return safeToCommit;
    }

    // VERSION 1
//    public Map<TopicPartition, OffsetAndMetadata> getSafeToCommitOffsets(
//            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
//        // trim completed responses from head and surface any failures
//        drainCompletedAppends(/* wait */ false);
//        maybeThrowFatal();
//
//        final Map<TopicPartition, Long> minPendingOffset = new HashMap<>(offsetsToCommit.size());
//        final Set<TopicPartition> unresolved = new HashSet<>(offsetsToCommit.keySet());
//        final Map<TopicPartition, Long> tpBoundary = new HashMap<>(offsetsToCommit.size());
//        offsetsToCommit.forEach((tp, om) -> tpBoundary.put(tp, om.offset() - 1L));
//
//        for (AppendRowsAttempt attempt: appendAttempts) {
//            if (attempt.future.isDone()) {
//                try {
//                    attempt.future.get();
//                } catch (Exception ex) {
//                    fatal.compareAndSet(null, ex);
//                }
//                maybeThrowFatal();
//                continue;
//            }
//
//            for (Map.Entry<TopicPartition, Long> e : attempt.minOffsetByTp.entrySet()) {
//                final TopicPartition tp = e.getKey();
//                if (!unresolved.contains(tp)) continue;
//
//                final Long minOffsetInAttempt = e.getValue();
//                final long boundary = tpBoundary.get(tp);
//
//                if (minOffsetInAttempt > boundary) {
//                    // First time we've seen this TP and min offset in this attempt is greater than one kafka
//                    // wants to commit. Means kafka is safe to commit offset it wants
//                    unresolved.remove(tp);
//                } else {
//                    minPendingOffset.put(tp, minOffsetInAttempt);
//                    unresolved.remove(tp);
//                }
//            }
//
//            if (unresolved.isEmpty()) break; // early exit: all TPs decided
//        }
//
//        // Build the result: if TP has a pending overlap, commit just before it (i.e., commit value = min);
//        // otherwise, commit exactly what Kafka asked for.
//        Map<TopicPartition, OffsetAndMetadata> safeToCommit = new HashMap<>(offsetsToCommit.size());
//
//        for (Map.Entry<TopicPartition, OffsetAndMetadata> requested : offsetsToCommit.entrySet()) {
//            TopicPartition tp = requested.getKey();
//            final Long pendingMin = minPendingOffset.get(tp);
//
//            if (pendingMin != null) {
//                // Safe last-written is pendingMin - 1, so commit value (next offset) is pendingMin
//                safeToCommit.put(tp, new OffsetAndMetadata(pendingMin));
//            } else {
//                // No pending overlap at/below boundary so we can grant Kafka’s requested commit
//                safeToCommit.put(tp, requested.getValue());
//            }
//        }
//
//        return safeToCommit;
//    }

//    public Map<TopicPartition, OffsetAndMetadata> getSafeToCommitOffsets(
//            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
//        // trim completed responses from head and surface any failures
//        checkForFailedResponses();
//        maybeThrowFatal();
//
//        Map<TopicPartition, Long> minPendingOffset = new HashMap<>();
//
//        for (AppendRowsAttempt attempt : appendAttempts) {
//            if (attempt.future.isDone()) {
//                // if completed, check if completed without errors
//                try {
//                    attempt.future.get();
//                } catch (Exception ex) {
//                    fatal.compareAndSet(null, ex);
//                }
//                maybeThrowFatal();
//            } else {
//                // if not completed
//                for (Map.Entry<TopicPartition, AppendRowsAttempt.OffsetRange> e : attempt.offsetRange.entrySet()) {
//                    TopicPartition topicPartition = e.getKey();
//                    AppendRowsAttempt.OffsetRange offsetRange = e.getValue();
//
//                    OffsetAndMetadata offsetAndMetadataToCommit = offsetsToCommit.get(topicPartition);
//                    if (offsetAndMetadataToCommit == null) continue; // not committing this TP now
//
//                    long offsetToCommit = offsetAndMetadataToCommit.offset() - 1L;
//
//                    if (offsetToCommit >= offsetRange.min) { // attempt's offsets overlap with what kafka is trying to commit
//                        minPendingOffset.merge(topicPartition, offsetRange.min, Math::min);
//                    }
//                }
//            }
//        }
//
//        // Build the result: if TP has a pending overlap, commit just before it (i.e., commit value = min);
//        // otherwise, commit exactly what Kafka asked for.
//        Map<TopicPartition, OffsetAndMetadata> safeToCommit = new HashMap<>();
//
//        for (Map.Entry<TopicPartition, OffsetAndMetadata> e : offsetsToCommit.entrySet()) {
//            TopicPartition requestedTopicPartition = e.getKey();
//            final OffsetAndMetadata requestedOffset = e.getValue();
//
//            final Long pendingMin = minPendingOffset.get(requestedTopicPartition);
//            if (pendingMin != null) {
//                // Safe last-written is pendingMin - 1, so commit value (next offset) is pendingMin
//                safeToCommit.put(requestedTopicPartition, new OffsetAndMetadata(pendingMin));
//            } else {
//                // No pending overlap at/below boundary so we can grant Kafka’s requested commit
//                safeToCommit.put(requestedTopicPartition, requestedOffset);
//            }
//        }
//
//        return safeToCommit;
//    }

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
        final Map<TopicPartition, Long> minOffsetByTp;

        AppendRowsAttempt(ApiFuture<AppendRowsResponse> future, Map<TopicPartition, Long> minOffsetByTp) {
            this.future = future;
            this.minOffsetByTp = minOffsetByTp;
        }
    }
}
