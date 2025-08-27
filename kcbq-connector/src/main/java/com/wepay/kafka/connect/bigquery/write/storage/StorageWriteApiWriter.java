/*
 * Copyright 2024 Copyright 2022 Aiven Oy and
 * bigquery-connector-for-apache-kafka project contributors
 *
 * This software contains code derived from the Confluent BigQuery
 * Kafka Connector, Copyright Confluent, Inc, which in turn
 * contains code derived from the WePay BigQuery Kafka Connector,
 * Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.write.storage;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import com.wepay.kafka.connect.bigquery.write.batch.KcbqThreadPoolExecutor;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriterBuilder;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.annotation.Nullable;

/**
 * Storage Write API writer that attempts to write all the rows it is given at once
 */
public class StorageWriteApiWriter {

  private final KcbqThreadPoolExecutor executor;
  private final Thread drainer = new Thread(this::drainHeadLoop);
  private volatile boolean running = true;
  private final ConcurrentLinkedQueue<AppendRowsAttempt> appendAttempts = new ConcurrentLinkedQueue<>();
  private final StorageWriteApiBase streamWriter;

  /**
   * @param streamWriter The stream writer to use - Default, Batch etc
   * @param executor     The executor
   */
  public StorageWriteApiWriter(StorageWriteApiBase streamWriter,
                               KcbqThreadPoolExecutor executor) {
    this.streamWriter = streamWriter;
    this.executor = executor;
    startDrainer();
  }

  void sendAppendRowsRequest(List<ConvertedRecord> batch, TableName tableName, String streamName) {
    try {
      ApiFuture<Void> appendRowsResponseApiFuture =
              streamWriter.initializeAndWriteRecords(tableName, batch, streamName);
      Map<TopicPartition, Long> minOffsetsByTp = computeMinOffsetsPerTp(batch);
      appendAttempts.add(new AppendRowsAttempt(appendRowsResponseApiFuture, minOffsetsByTp));
    } catch (Throwable t) {
      reportTerminal(t);
    }
  }

  public void startDrainer() {
    drainer.setDaemon(true);
    drainer.start();
  }

  public void stopDrainer() throws InterruptedException {
    running = false;
    drainer.join(2000);
  }

  private void drainHeadLoop() {
    final long SLEEP_NANOS = TimeUnit.MICROSECONDS.toNanos(200);
    try {
      while (running) {
        AppendRowsAttempt head = appendAttempts.peek();
        if (head == null) {
          LockSupport.parkNanos(SLEEP_NANOS);
          continue;
        }
        if (!head.future.isDone()) {
          LockSupport.parkNanos(SLEEP_NANOS);
          continue;
        }
        try {
          head.future.get();
        } catch (Exception e) {
          reportTerminal(e);
        }
        appendAttempts.poll(); // remove head
      }
    } catch (Throwable t) {
      reportTerminal(t);
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

  public Map<TopicPartition, OffsetAndMetadata> getCommittableOffsets(
          Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
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
          reportTerminal(ex);
        }
        executor.maybeThrowEncounteredError();
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

      if (unresolved.isEmpty()) break; // early exit cuz all committable offsets for TPs have been decided
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

  void reportTerminal(Throwable throwable) {
    executor.reportError(throwable);
  }

  public static class Builder implements TableWriterBuilder {
    private final List<ConvertedRecord> records = new ArrayList<>();
    private final SinkRecordConverter recordConverter;
    private final TableName tableName;
    private final StorageWriteApiWriter storageWriteApiWriter;
    private final StorageApiBatchModeHandler batchModeHandler;

    public Builder(StorageWriteApiWriter storageWriteApiWriter,
                   TableName tableName,
                   SinkRecordConverter recordConverter,
                   @Nullable StorageApiBatchModeHandler batchModeHandler) {
      this.storageWriteApiWriter = storageWriteApiWriter;
      this.tableName = tableName;
      this.recordConverter = recordConverter;
      this.batchModeHandler = batchModeHandler;
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
      final String streamName = (batchModeHandler != null && !records.isEmpty())
              ? batchModeHandler.updateOffsetsOnStream(tableName.toString(), records)
              : "default";
      return () -> storageWriteApiWriter.sendAppendRowsRequest(records, tableName, streamName);
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
    final ApiFuture<Void> future;
    final Map<TopicPartition, Long> minOffsetByTp;

    AppendRowsAttempt(ApiFuture<Void> future, Map<TopicPartition, Long> minOffsetByTp) {
      this.future = future;
      this.minOffsetByTp = minOffsetByTp;
    }
  }
}
