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
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import com.wepay.kafka.connect.bigquery.utils.Time;
import com.wepay.kafka.connect.bigquery.write.RecordBatches;
import com.wepay.kafka.connect.bigquery.write.batch.KcbqThreadPoolExecutor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;
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
  KcbqThreadPoolExecutor executor;
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
                                boolean attemptSchemaUpdate,
                                KcbqThreadPoolExecutor executor) {
    this.retry = retry;
    this.retryWait = retryWait;
    this.autoCreateTables = autoCreateTables;
    this.writeSettings = writeSettings;
    this.errantRecordHandler = errantRecordHandler;
    this.schemaManager = schemaManager;
    this.attemptSchemaUpdate = attemptSchemaUpdate;
    this.executor = executor;
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
      PartitionedTableId table,
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

  /**
   * Handles required initialization steps and goes to append records to table
   *
   * @param table      The table to write data to
   * @param rows       List of pre- and post-conversion records.
   *                   Converted JSONObjects would be sent to api.
   *                   Pre-conversion sink records are required for DLQ routing
   * @param streamName The stream to use to write table to table.
   */
  public ApiFuture<Void> initializeAndWriteRecords(
          PartitionedTableId table,
          List<ConvertedRecord> rows,
          String streamName
  ) {
    logger.debug("Sending {} records to Storage Write Api stream {}", rows.size(), streamName);
    StorageWriteApiRetryHandler retryHandler =
            new StorageWriteApiRetryHandler(table.getBaseTableId(), getSinkRecords(rows), retry, retryWait, time);
    logger.info("getting writer");
    StreamWriter writer = streamWriter(table, streamName, rows);
    ApiFuture<Void> writeBatchesApiFuture = writeBatches(writer, new RecordBatches<>(rows), retryHandler, table);

    return ApiFutures.transform(
            writeBatchesApiFuture,
            ignored -> {
              logger.info("about to call on success");
              writer.onSuccess();
              return null;
            },
            executor
    );
  }

  private ApiFuture<Void> writeBatches(
          StreamWriter writer,
          RecordBatches<ConvertedRecord> batches,
          StorageWriteApiRetryHandler retryHandler,
          PartitionedTableId table
  ) {
    TableName tableName = TableNameUtils.tableName(table.getFullTableId());

    if (batches.completed()) {
      return ApiFutures.immediateFuture(null);
    }

    final List<ConvertedRecord> batch = batches.currentBatch();
    ApiFuture<Void> appendRowsApiFuture = sendAppendRowsRequest(writer, batch, retryHandler, tableName);

    ApiFuture<Void> handled = ApiFutures.catchingAsync(
            appendRowsApiFuture,
            Exception.class,
            e -> {
              Function<Map<Integer, String>, ApiFuture<Void>> handleBatchFiltering = rowErrors -> {
                List<ConvertedRecord> remaining =
                        maybeHandleDlqRoutingAndFilterRecords(batch, rowErrors, table.getBaseTableId().getTable());
                if (remaining.isEmpty()) {
                  return ApiFutures.immediateFuture(null);
                }
                retryHandler.maybeRetry("filtered remainder");
                return writeBatches(writer, new RecordBatches<>(remaining), retryHandler, table);
              };
              if (e instanceof RetryException) {
                retryHandler.maybeRetry("write to table " + tableName);
                if (e.getMessage() != null) {
                  logger.warn(e.getMessage() + " Retry appendRowsApiFuture " + retryHandler.getAttempt());
                }
                return writeBatches(writer, batches, retryHandler, table);
              } else if (e instanceof BatchTooLargeException) {
                if (batch.size() <= 1) {
                  return handleBatchFiltering.apply(Collections.singletonMap(0, e.getMessage()));
                } else {
                  int prev = batch.size();
                  batches.reduceBatchSize();
                  logger.debug("Reducing batch size for {} from {} to {}", tableName, prev, batches.currentBatch().size());
                  return writeBatches(writer, batches, retryHandler, table);
                }
              } else if (e instanceof MalformedRowsException) {
                return handleBatchFiltering.apply(((MalformedRowsException) e).getRowErrorMapping());
              }

              return ApiFutures.immediateFailedFuture(e);
            },
            executor
    );

    return ApiFutures.transformAsync(
            handled,
            ignored -> {
              batches.advanceToNextBatch();
              if (batches.completed()) {
                return ApiFutures.immediateFuture(null);
              }
              return writeBatches(writer, batches, retryHandler, table);
            },
            executor
    );
  }

  private ApiFuture<Void> sendAppendRowsRequest(
          StreamWriter writer,
          List<ConvertedRecord> batch,
          StorageWriteApiRetryHandler retryHandler,
          TableName tableName
  ) {
    JSONArray json = getJsonRecords(batch);
    ApiFuture<AppendRowsResponse> appendRowsApiFuture;

    try {
      appendRowsApiFuture = writer.appendRows(json);
    } catch (Exception e) {
      if (shouldHandleSchemaMismatch(e)) {
        return ApiFutures.transformAsync(
                runAsync(() -> retryHandler.attemptTableOperation(schemaManager::updateSchema), executor),
                ignored -> ApiFutures.immediateFailedFuture(new RetryException()),
                executor
        );
      }
      return ApiFutures.immediateFailedFuture(e);
    }

    ApiFuture<AppendRowsResponse> validatedAppendRowsFuture = ApiFutures.transformAsync(
            appendRowsApiFuture,
            appendRowsResponse -> validateAppendRowsResponse(appendRowsResponse, retryHandler, tableName),
            executor
    );
    ApiFuture<AppendRowsResponse> handled = ApiFutures.catchingAsync(
            validatedAppendRowsFuture,
            Exception.class,
            exception -> handleAppendRowsExceptions(exception, writer, retryHandler, tableName),
            executor
    );
    return ApiFutures.transform(handled, r -> null, executor);
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

  private ApiFuture<AppendRowsResponse> validateAppendRowsResponse(
          AppendRowsResponse appendRowsResponse, StorageWriteApiRetryHandler retryHandler, TableName tableName) {
    if (appendRowsResponse.hasUpdatedSchema()) {

      if (!canAttemptSchemaUpdate()) {
        return ApiFutures.immediateFailedFuture(new BigQueryStorageWriteApiConnectException(
                "Connector is not configured to perform schema updates."));
      }
      retryHandler.attemptTableOperation(schemaManager::updateSchema);
      return ApiFutures.immediateFailedFuture(new RetryException());
    }
    if (appendRowsResponse.hasError()) {
      String errorMessage = String.format("Failed to write rows on table %s due to %s", tableName, appendRowsResponse.getError().getMessage());
      retryHandler.setMostRecentException(new BigQueryStorageWriteApiConnectException(errorMessage));
      if (BigQueryStorageWriteApiErrorResponses.isMalformedRequest(errorMessage)) {
        return ApiFutures.immediateFailedFuture(
                new MalformedRowsException(convertToMap(appendRowsResponse.getRowErrorsList())));
      }
      return ApiFutures.immediateFailedFuture(retryHandler.getMostRecentException());
    }
    if (!appendRowsResponse.hasAppendResult()) {
      logger.warn("Write result had no appendResult; treating as success, but please investigate.");
    }
    return ApiFutures.immediateFuture(appendRowsResponse);
  }

  private ApiFuture<AppendRowsResponse> handleAppendRowsExceptions(Exception e, StreamWriter writer,
                                                                   StorageWriteApiRetryHandler retryHandler, TableName tableName) {
    if (e instanceof BigQueryStorageWriteApiConnectException || e instanceof BatchWriteException) {
      return ApiFutures.immediateFailedFuture(e);
    }

    String message = e.getMessage();
    String errorMessage = String.format("Failed to write rows on table %s due to %s", tableName, message);
    retryHandler.setMostRecentException(new BigQueryStorageWriteApiConnectException(errorMessage, e));

    if (BigQueryStorageWriteApiErrorResponses.isStreamClosed(message)) {
      writer.refresh();
    } else if (shouldHandleSchemaMismatch(e)) {
      logger.warn("Schema mismatch; attempting update");
      retryHandler.attemptTableOperation(schemaManager::updateSchema);
    } else if (BigQueryStorageWriteApiErrorResponses.isMessageTooLargeError(message)) {
      return ApiFutures.immediateFailedFuture(new BatchTooLargeException(errorMessage));
    } else if (BigQueryStorageWriteApiErrorResponses.isMalformedRequest(message)) {
      return ApiFutures.immediateFailedFuture(new MalformedRowsException(getRowErrorMapping(e)));
    } else if (BigQueryStorageWriteApiErrorResponses.isTableMissing(message) && getAutoCreateTables()) {
      retryHandler.attemptTableOperation(schemaManager::createTable);
    } else {
      failTask(retryHandler.getMostRecentException());
      return ApiFutures.immediateFailedFuture(retryHandler.getMostRecentException());
    }
    return ApiFutures.immediateFailedFuture(new RetryException(errorMessage));
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
