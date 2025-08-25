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
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.common.annotations.VisibleForTesting;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiErrorResponses;
import com.wepay.kafka.connect.bigquery.utils.Time;
import com.wepay.kafka.connect.bigquery.write.RecordBatches;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/**
 * Base class which handles data ingestion to bigquery tables using different kind of streams
 */
public abstract class StorageWriteApiBase {

  private static final Logger logger = LoggerFactory.getLogger(StorageWriteApiBase.class);
  private static final double RETRY_DELAY_MULTIPLIER = 1.1;
  private static final int MAX_RETRY_DELAY_MINUTES = 1;
  protected final JsonStreamWriterFactory jsonWriterFactory;
  protected final int retry;
  protected final long retryWait;
  private final boolean autoCreateTables;
  private final BigQueryWriteSettings writeSettings;
  private final boolean attemptSchemaUpdate;
  protected SchemaManager schemaManager;
  @VisibleForTesting
  protected Time time;
  ErrantRecordHandler errantRecordHandler;
  private BigQueryWriteClient writeClient;

  /**
   * @param retry               How many retries to make in the event of a retriable error.
   * @param retryWait           How long to wait in between retries.
   * @param writeSettings       Write Settings for stream which carry authentication and other header information
   * @param autoCreateTables    boolean flag set if table should be created automatically
   * @param errantRecordHandler Used to handle errant records
   */
  protected StorageWriteApiBase(int retry,
                                long retryWait,
                                BigQueryWriteSettings writeSettings,
                                boolean autoCreateTables,
                                ErrantRecordHandler errantRecordHandler,
                                SchemaManager schemaManager,
                                boolean attemptSchemaUpdate) {
    this.retry = retry;
    this.retryWait = retryWait;
    this.autoCreateTables = autoCreateTables;
    this.writeSettings = writeSettings;
    this.errantRecordHandler = errantRecordHandler;
    this.schemaManager = schemaManager;
    this.attemptSchemaUpdate = attemptSchemaUpdate;
    try {
      this.writeClient = getWriteClient();
    } catch (IOException e) {
      logger.error("Failed to create Big Query Storage Write API write client due to {}", e.getMessage());
      throw new BigQueryStorageWriteApiConnectException("Failed to create Big Query Storage Write API write client", e);
    }
    this.jsonWriterFactory = getJsonWriterFactory();
    this.time = Time.SYSTEM;
  }

  public abstract void preShutdown();

  protected abstract StreamWriter streamWriter(
      TableName tableName,
      String streamName,
      List<ConvertedRecord> records
  );

  /**
   * Gets called on task.stop() and should have resource cleanup logic.
   */
  public void shutdown() {
    preShutdown();
    this.writeClient.close();
  }

  protected ApiFuture<AppendRowsResponse> initializeAndWriteRecords(
          TableName tableName,
          List<ConvertedRecord> rows,
          String streamName,
          Executor callbackExec
  ) {
    logger.debug("Sending {} records to Storage Write Api stream {}", rows.size(), streamName);
    StorageWriteApiRetryHandler retryHandler = new StorageWriteApiRetryHandler(tableName, getSinkRecords(rows), retry, retryWait, time);
    StreamWriter writer = streamWriter(tableName, streamName, rows);
    RecordBatches<ConvertedRecord> batches = new RecordBatches<>(rows);
    List<ConvertedRecord> batch = batches.currentBatch();

    return writeBatch(writer, batch, retryHandler, tableName, callbackExec);
  }

  protected ApiFuture<AppendRowsResponse> writeBatch(
          StreamWriter writer,
          List<ConvertedRecord> batch,
          StorageWriteApiRetryHandler retryHandler,
          TableName tableName,
          Executor callbackExec
  ) {
    JSONArray jsonRecords = getJsonRecords(batch);
    ApiFuture<AppendRowsResponse> appendRowsResponseApiFuture;
    try {
      appendRowsResponseApiFuture = writer.appendRows(jsonRecords);
    } catch (Exception e) {
      if (shouldHandleSchemaMismatch(e)) {
        return ApiFutures.transformAsync(
                runAsync(() -> retryHandler.attemptTableOperation(schemaManager::updateSchema), callbackExec),
                ignored -> {
                  retryHandler.maybeRetry("schema mismatch (sync)");
                  return writeBatch(writer, batch, retryHandler, tableName, callbackExec);
                },
                callbackExec
        );
      }
      return ApiFutures.immediateFailedFuture(e);
    }

    // 1) Handle *successful RPC completion* (response came back)
    ApiFuture<AppendRowsResponse> validatedAppendRowsFuture =
            ApiFutures.transformAsync(
                    appendRowsResponseApiFuture,
                    appendRowsResponse -> {
                      if (appendRowsResponse.hasUpdatedSchema()) {
                        if (!canAttemptSchemaUpdate()) {
                          return ApiFutures.immediateFailedFuture(
                                  new BigQueryStorageWriteApiConnectException(
                                          "Connector is not configured to perform schema updates."));
                        }
                        retryHandler.attemptTableOperation(schemaManager::updateSchema);
                        retryHandler.maybeRetry("write to table " + tableName);
                        return writeBatch(writer, batch, retryHandler, tableName, callbackExec);
                      }
                      if (appendRowsResponse.hasError()) {
                        String msg = appendRowsResponse.getError().getMessage();
                        retryHandler.setMostRecentException(
                                new BigQueryStorageWriteApiConnectException(
                                        "Failed to write rows on table " + tableName + " due to " + msg));

                        if (BigQueryStorageWriteApiErrorResponses.isMalformedRequest(msg)) {
                          Map<Integer, String> rowErrors = convertToMap(appendRowsResponse.getRowErrorsList());
                          List<ConvertedRecord> filtered =
                                  maybeHandleDlqRoutingAndFilterRecords(batch, rowErrors, tableName.getTable());
                          if (filtered.isEmpty()) {
                            return ApiFutures.immediateFuture(appendRowsResponse);
                          }
                          retryHandler.maybeRetry("write to table " + tableName);
                          return writeBatch(writer, filtered, retryHandler, tableName, callbackExec);
                        }

                        if (!BigQueryStorageWriteApiErrorResponses.isRetriableError(msg)) {
                          failTask(retryHandler.getMostRecentException());
                          return ApiFutures.immediateFailedFuture(retryHandler.getMostRecentException());
                        }
                        retryHandler.maybeRetry("write to table " + tableName);
                        return writeBatch(writer, batch, retryHandler, tableName, callbackExec);
                      }
                      return ApiFutures.immediateFuture(appendRowsResponse);
                    },
                    callbackExec
            );

//     2) Handle *RPC failure path* (exceptions, message too large, stream closed, table missing, schema mismatch)
    return ApiFutures.catchingAsync(
            validatedAppendRowsFuture,
            Throwable.class,
            t -> {
              Throwable e = (t instanceof java.util.concurrent.ExecutionException && t.getCause() != null) ? t.getCause() : t;
              String msg = e.getMessage() == null ? e.toString() : e.getMessage();
              retryHandler.setMostRecentException(
                      new BigQueryStorageWriteApiConnectException(
                              "Failed to write rows on table " + tableName + " due to " + msg, e));

              List<ConvertedRecord> batchToRetry = batch;
              if (BigQueryStorageWriteApiErrorResponses.isMessageTooLargeError(msg)) {
                if (batchToRetry.size() <= 1) {
                  Map<Integer, String> rowErrors = java.util.Collections.singletonMap(0, msg);
                  batchToRetry =
                          maybeHandleDlqRoutingAndFilterRecords(batch, rowErrors, tableName.getTable());
                  if (batchToRetry.isEmpty()) {
                    return ApiFutures.immediateFuture(null);
                  }
                } else {
                  int mid = batchToRetry.size() / 2;
                  List<ConvertedRecord> left = batchToRetry.subList(0, mid);
                  List<ConvertedRecord> right = batchToRetry.subList(mid, batchToRetry.size());

                  return ApiFutures.transformAsync(
                          writeBatch(writer, left, retryHandler, tableName, callbackExec),
                          ignored -> writeBatch(writer, right, retryHandler, tableName, callbackExec),
                          callbackExec
                  );
                }
              } else if (BigQueryStorageWriteApiErrorResponses.isMalformedRequest(msg)) {
                Map<Integer, String> rowErrors = getRowErrorMapping(new RuntimeException(msg));
                batchToRetry =
                        maybeHandleDlqRoutingAndFilterRecords(batch, rowErrors, tableName.getTable());
                if (batchToRetry.isEmpty()) {
                  return ApiFutures.immediateFuture(null);
                }
              } else if (BigQueryStorageWriteApiErrorResponses.isStreamClosed(msg)) {
                writer.refresh();
              } else if (shouldHandleSchemaMismatch(new RuntimeException(msg))) {
                retryHandler.attemptTableOperation(schemaManager::updateSchema);
              } else if (BigQueryStorageWriteApiErrorResponses.isTableMissing(msg) && getAutoCreateTables()) {
                retryHandler.attemptTableOperation(schemaManager::createTable);
              } else if (isNonRetriable(new RuntimeException(msg))) {
                failTask(retryHandler.getMostRecentException());
                return ApiFutures.immediateFailedFuture(retryHandler.getMostRecentException());
              }

              retryHandler.maybeRetry("write to table " + tableName);
              return writeBatch(writer, batchToRetry, retryHandler, tableName, callbackExec);
            },
            callbackExec
    );
  }

  private static ApiFuture<Void> runAsync(Runnable r, Executor exec) {
    SettableApiFuture<Void> f = SettableApiFuture.create();
    exec.execute(() -> {
      try {
        r.run();
        f.set(null);
      } catch (Throwable t) {
        f.setException(t);
      }
    });
    return f;
  }

  private abstract static class BatchWriteException extends Exception {

    protected BatchWriteException() {
      super();
    }

    protected BatchWriteException(String message) {
      super(message);
    }

  }

  private static class BatchTooLargeException extends BatchWriteException {

    public BatchTooLargeException(String message) {
      super(message);
    }

  }

  private static class MalformedRowsException extends BatchWriteException {

    private final Map<Integer, String> rowErrorMapping;

    public MalformedRowsException(Map<Integer, String> rowErrorMapping) {
      this.rowErrorMapping = rowErrorMapping;
    }

    public Map<Integer, String> getRowErrorMapping() {
      return rowErrorMapping;
    }

  }

  private static class RetryException extends BatchWriteException {

    public RetryException() {
      super();
    }

    public RetryException(String message) {
      super(message);
    }
  }

  /**
   * Creates Storage Api write client which carries all write settings information
   *
   * @return Returns BigQueryWriteClient object
   * @throws IOException
   */
  protected BigQueryWriteClient getWriteClient() throws IOException {
    if (this.writeClient == null) {
      this.writeClient = BigQueryWriteClient.create(writeSettings);
    }
    return this.writeClient;
  }

  /**
   * Returns a {@link JsonStreamWriterFactory} for creating configured {@link JsonStreamWriter} instances
   *
   * @return a {@link JsonStreamWriterFactory}
   */
  protected JsonStreamWriterFactory getJsonWriterFactory() {
    RetrySettings retrySettings = RetrySettings.newBuilder()
            .setMaxAttempts(retry)
            .setInitialRetryDelay(Duration.ofMillis(retryWait))
            .setRetryDelayMultiplier(RETRY_DELAY_MULTIPLIER)
            .setMaxRetryDelay(Duration.ofMinutes(MAX_RETRY_DELAY_MINUTES))
            .build();
    return streamOrTableName -> JsonStreamWriter.newBuilder(streamOrTableName, writeClient)
            .setRetrySettings(retrySettings)
            .build();
  }

  /**
   * Verifies the exception object and returns row-wise error map
   *
   * @param exception if the exception is not of expected type
   * @return Map of row index to error message detail
   */
  protected Map<Integer, String> getRowErrorMapping(Exception exception) {
    if (exception.getCause() instanceof Exceptions.AppendSerializtionError) {
      exception = (Exceptions.AppendSerializtionError) exception.getCause();
    }
    if (exception instanceof Exceptions.AppendSerializtionError) {
      return ((Exceptions.AppendSerializtionError) exception).getRowIndexToErrorMessage();
    } else {
      throw new BigQueryStorageWriteApiConnectException(
          "Exception is not an instance of Exceptions.AppendSerializtionError", exception);
    }
  }

  protected boolean getAutoCreateTables() {
    return this.autoCreateTables;
  }

  protected boolean canAttemptSchemaUpdate() {
    return this.attemptSchemaUpdate;
  }

  /**
   * @param rows List of pre- and post-conversion records
   * @return Returns list of all pre-conversion records
   */
  protected List<SinkRecord> getSinkRecords(List<ConvertedRecord> rows) {
    return rows.stream()
        .map(ConvertedRecord::original)
        .collect(Collectors.toList());
  }

  /**
   * Sends errant records to configured DLQ and returns remaining
   *
   * @param input           List of pre- and post-conversion records
   * @param indexToErrorMap Map of record index to error received from api call
   * @return Returns list of good records filtered from input which needs to be retried. Append row does
   * not write partially even if there is a single failure, good data has to be retried
   */
  protected List<ConvertedRecord> sendErrantRecordsToDlqAndFilterValidRecords(
      List<ConvertedRecord> input,
      Map<Integer, String> indexToErrorMap) {
    List<ConvertedRecord> filteredRecords = new ArrayList<>();
    Map<SinkRecord, Throwable> recordsToDlq = new LinkedHashMap<>();

    for (int i = 0; i < input.size(); i++) {
      if (indexToErrorMap.containsKey(i)) {
        SinkRecord inputRecord = input.get(i).original();
        Throwable error = new Throwable(indexToErrorMap.get(i));
        recordsToDlq.put(inputRecord, error);
      } else {
        filteredRecords.add(input.get(i));
      }
    }

    if (errantRecordHandler.getErrantRecordReporter() != null) {
      errantRecordHandler.reportErrantRecords(recordsToDlq);
    }

    return filteredRecords;
  }

  /**
   * Converts Row Error to Map
   *
   * @param rowErrors List of row errors
   * @return Returns Map with key as Row index and value as the Row Error Message
   */
  protected Map<Integer, String> convertToMap(List<RowError> rowErrors) {
    Map<Integer, String> errorMap = new HashMap<>();

    rowErrors.forEach(rowError -> errorMap.put((int) rowError.getIndex(), rowError.getMessage()));

    return errorMap;
  }

  protected List<ConvertedRecord> maybeHandleDlqRoutingAndFilterRecords(
      List<ConvertedRecord> rows,
      Map<Integer, String> errorMap,
      String tableName
  ) {
    if (errantRecordHandler.getErrantRecordReporter() != null) {
      //Routes to DLQ
      return sendErrantRecordsToDlqAndFilterValidRecords(rows, errorMap);
    } else {
      // Fail if no DLQ
      logger.warn("DLQ is not configured!");
      throw new BigQueryStorageWriteApiConnectException(tableName, errorMap);
    }
  }

  private JSONArray getJsonRecords(List<ConvertedRecord> rows) {
    JSONArray jsonRecords = new JSONArray();
    for (ConvertedRecord item : rows) {
      jsonRecords.put(item.converted());
    }
    return jsonRecords;
  }

  protected boolean shouldHandleSchemaMismatch(Exception e) {
    if (!canAttemptSchemaUpdate()) {
      return false;
    }

    if (BigQueryStorageWriteApiErrorResponses.hasInvalidSchema(Collections.singletonList(e.getMessage()))) {
      return true;
    }

    if (BigQueryStorageWriteApiErrorResponses.isMalformedRequest(e.getMessage())
        && BigQueryStorageWriteApiErrorResponses.hasInvalidSchema(getRowErrorMapping(e).values())) {
      return true;
    }

    return false;
  }

  protected boolean shouldHandleTableCreation(String errorMessage) {
    return BigQueryStorageWriteApiErrorResponses.isTableMissing(errorMessage) && getAutoCreateTables();
  }

  protected boolean isNonRetriable(Exception e) {
    return !BigQueryStorageWriteApiErrorResponses.isRetriableError(e.getMessage())
        && BigQueryStorageWriteApiErrorResponses.isNonRetriableStorageError(e);
  }

  protected void failTask(RuntimeException failure) {
    // Fail on non-retriable error
    logger.error("Encountered unrecoverable failure", failure);
    throw failure;
  }

}
