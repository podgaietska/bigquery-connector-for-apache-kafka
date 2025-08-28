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
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import com.wepay.kafka.connect.bigquery.write.batch.KcbqThreadPoolExecutor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Storage Write API writer that attempts to write all the rows it is given at once
 */
public class StorageWriteApiWriter {
  private static final Logger logger = LoggerFactory.getLogger(StorageWriteApiWriter.class);
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

  public void sendAppendRowsRequest(Map<PartitionedTableId, List<ConvertedRecord>> batches, String streamName) {
    List<ConvertedRecord> allRows = flatten(batches.values());
    Map<TopicPartition, Long> minOffsetsByTp = computeMinOffsetsPerTp(allRows);

    AppendRowsAttempt.Builder attemptBuilder = new AppendRowsAttempt.Builder(minOffsetsByTp);

    for (Map.Entry<PartitionedTableId, List<ConvertedRecord>> e : batches.entrySet()) {
      PartitionedTableId dest = e.getKey();
      List<ConvertedRecord> rows = e.getValue();
      ApiFuture<Void> f = streamWriter.initializeAndWriteRecords(dest, rows, streamName);
      attemptBuilder.add(f);
    }

    appendAttempts.add(attemptBuilder.build());
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
    final long sleepNanos = TimeUnit.MICROSECONDS.toNanos(200);
    try {
      while (running) {
        AppendRowsAttempt head = appendAttempts.peek();
        if (head == null) {
          LockSupport.parkNanos(sleepNanos);
          continue;
        }
        if (!head.future.isDone()) {
          LockSupport.parkNanos(sleepNanos);
          continue;
        }
        try {
          head.future.get();
        } catch (Exception e) {
          reportTerminal(e);
        }
        appendAttempts.poll();
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

    for (AppendRowsAttempt attempt : appendAttempts) {
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
        if (!unresolved.contains(tp)) {
          continue;
        }

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

      if (unresolved.isEmpty()) {
        break; // early exit cuz all committable offsets for TPs have been decided
      }
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

    logger.info("safe to commit map: {}", safeToCommit);

    return safeToCommit;
  }

  void reportTerminal(Throwable throwable) {
    executor.reportError(throwable);
  }

  private static List<ConvertedRecord> flatten(Collection<List<ConvertedRecord>> lists) {
    List<ConvertedRecord> out = new ArrayList<>();
    for (List<ConvertedRecord> l : lists) {
      out.addAll(l);
    }
    return out;
  }

  public static class Builder implements PartitionedTableWriterBuilder {
    private final Map<PartitionedTableId, List<ConvertedRecord>> records = new LinkedHashMap<>();
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
      // records.add(new ConvertedRecord(sinkRecord, convertRecord(sinkRecord)));
    }

    @Override
    public void addRow(SinkRecord sinkRecord, PartitionedTableId table) {
      records.computeIfAbsent(table, ignored -> new ArrayList<>())
              .add(new ConvertedRecord(sinkRecord, convertRecord(sinkRecord)));
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
              ? batchModeHandler.updateOffsetsOnStream(tableName.toString(), flatten(records.values()))
              : "default";
      return () -> storageWriteApiWriter.sendAppendRowsRequest(records, streamName);
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

    static final class Builder {
      private final Map<TopicPartition, Long> minOffsetsByTp;
      private final List<ApiFuture<Void>> subFutures = new ArrayList<>();

      Builder(Map<TopicPartition, Long> minOffsetsByTp) {
        this.minOffsetsByTp = Objects.requireNonNull(minOffsetsByTp);
      }

      void add(ApiFuture<Void> f) {
        subFutures.add(Objects.requireNonNull(f));
      }

      AppendRowsAttempt build() {
        ApiFuture<List<Void>> all = ApiFutures.allAsList(subFutures);
        ApiFuture<Void> combined = ApiFutures.transform(all, ignored -> null, Runnable::run);
        return new AppendRowsAttempt(combined, minOffsetsByTp);
      }
    }
  }
}
