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
            checkForFailedResponses();

            try {
                ApiFuture<AppendRowsResponse> appendRowsResponseApiFuture =
                        streamWriter.asyncInitializeAndWriteRecords(tableName, batch, "default", callbackExec);
                Map<TopicPartition, AppendRowsAttempt.OffsetRange> maxOffsets = computeOffsetRange(batch);
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

    private void checkForFailedResponses() {
        while (!appendAttempts.isEmpty()) {
            AppendRowsAttempt head = appendAttempts.peek();
            if (!head.future.isDone()) break;
            try {
                head.future.get();
            } catch (Exception e) {
                fatal.compareAndSet(null, e);
            } finally {
                appendAttempts.poll();
            }
        }
    }

    private Map<TopicPartition,  AppendRowsAttempt.OffsetRange> computeOffsetRange(List<ConvertedRecord> batch) {
        Map<TopicPartition,  AppendRowsAttempt.OffsetRange> result = new HashMap<>();
        for (ConvertedRecord record : batch) {
            SinkRecord originalRecord = record.original();
            TopicPartition topicPartition = new TopicPartition(originalRecord.topic(), originalRecord.kafkaPartition());
            long currentOffset = originalRecord.kafkaOffset();
            AppendRowsAttempt.OffsetRange currentOffsetRange = result.get(topicPartition);

            if (currentOffsetRange == null) {
                result.put(topicPartition, new  AppendRowsAttempt.OffsetRange(currentOffset, currentOffset));
            } else {
                result.put(topicPartition, new  AppendRowsAttempt.OffsetRange(
                        Math.min(currentOffsetRange.min, currentOffset), Math.max(currentOffsetRange.max, currentOffset))
                );
            }
        }
        return result;
    }

    public Map<TopicPartition, OffsetAndMetadata> getSafeToCommitOffsets(
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
        // check all done responses and surface any failures before proceeding with committing.
        checkForFailedResponses();
        maybeThrowFatal();

        Map<TopicPartition, Long> minPendingOffset = new HashMap<>();

        for (AppendRowsAttempt attempt : appendAttempts) {
            if (attempt.future.isDone()) {
                // if completed, check if completed without errors
                try {
                    attempt.future.get();
                } catch (Exception ex) {
                    fatal.compareAndSet(null, ex);
                }
                maybeThrowFatal();
            } else {
                // if not completed
                for (Map.Entry<TopicPartition, AppendRowsAttempt.OffsetRange> e : attempt.offsetRange.entrySet()) {
                    TopicPartition topicPartition = e.getKey();
                    AppendRowsAttempt.OffsetRange offsetRange = e.getValue();

                    OffsetAndMetadata offsetAndMetadataToCommit = offsetsToCommit.get(topicPartition);
                    if (offsetAndMetadataToCommit == null) continue; // not committing this TP now

                    long offsetToCommit = offsetAndMetadataToCommit.offset() - 1L;

                    if (offsetToCommit >= offsetRange.min) { // attempt's offsets overlap with what kafka is trying to commit
                        minPendingOffset.merge(topicPartition, offsetRange.min, Math::min);
                    }
                }
            }
        }

        // Build the result: if TP has a pending overlap, commit just before it (i.e., commit value = min);
        // otherwise, commit exactly what Kafka asked for.
        Map<TopicPartition, OffsetAndMetadata> safeToCommit = new HashMap<>();

        for (Map.Entry<TopicPartition, OffsetAndMetadata> e : offsetsToCommit.entrySet()) {
            TopicPartition requestedTopicPartition = e.getKey();
            final OffsetAndMetadata requestedOffset = e.getValue();

            final Long pendingMin = minPendingOffset.get(requestedTopicPartition);
            if (pendingMin != null) {
                // Safe last-written is pendingMin - 1, so commit value (next offset) is pendingMin
                safeToCommit.put(requestedTopicPartition, new OffsetAndMetadata(pendingMin));
            } else {
                // No pending overlap at/below boundary so we can grant Kafka’s requested commit
                safeToCommit.put(requestedTopicPartition, requestedOffset);
            }
        }

        return safeToCommit;
    }

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
        final Map<TopicPartition, OffsetRange> offsetRange;

        AppendRowsAttempt(ApiFuture<AppendRowsResponse> future, Map<TopicPartition, OffsetRange> offsetRange) {
            this.future = future;
            this.offsetRange = offsetRange;
        }

        public static final class OffsetRange {
            final long min;
            final long max;
            OffsetRange(long min, long max) { this.min = min; this.max = max; }
        }
    }
}
