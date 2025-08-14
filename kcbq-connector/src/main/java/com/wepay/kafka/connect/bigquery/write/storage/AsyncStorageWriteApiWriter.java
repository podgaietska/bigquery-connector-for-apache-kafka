package com.wepay.kafka.connect.bigquery.write.storage;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriterBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class AsyncStorageWriteApiWriter {
    private final ExecutorService appendRowsExecutor = Executors.newSingleThreadExecutor();
    private final Executor callbackExec;
    private final ArrayDeque<ApiFuture<Void>> futuresQueue = new ArrayDeque<>();
    private final AtomicReference<Throwable> fatal = new AtomicReference<>();
    private final StorageWriteApiBase streamWriter;

    public AsyncStorageWriteApiWriter(StorageWriteApiBase streamWriter,
                            Executor callbackExec) {
        this.streamWriter = streamWriter;
        this.callbackExec = callbackExec;
    }

    void submit(List<ConvertedRecord> batch, TableName tableName) {
        appendRowsExecutor.execute(() -> {
            checkForFailedResponses(false);
            try {
                ApiFuture<Void> appendRowsResponseApiFuture =
                        streamWriter.asyncInitializeAndWriteRecords(tableName, batch, "default", callbackExec);
                futuresQueue.addLast(appendRowsResponseApiFuture);
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

    public void fenceAndDrain() throws InterruptedException {
        CountDownLatch done = new CountDownLatch(1);
        appendRowsExecutor.execute(() -> {
            try {
                checkForFailedResponses(true);
            } finally {
                done.countDown();
            }
        });
        done.await();
        if (fatal.get() != null) throw new RuntimeException(fatal.get());
    }

    private void checkForFailedResponses(boolean wait) {
        while (!futuresQueue.isEmpty()) {
            ApiFuture<Void> head = futuresQueue.peekFirst();
            if (!wait && !head.isDone()) break;
            try {
                head.get();
            } catch (Exception e) {
                fatal.compareAndSet(null, e);
            } finally {
                futuresQueue.removeFirst();
            }
        }
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
            return () -> asyncWriter.submit(batch, tableName);
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
}
