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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.rpc.Status;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import com.wepay.kafka.connect.bigquery.utils.MockTime;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import com.wepay.kafka.connect.bigquery.write.batch.KcbqThreadPoolExecutor;
import io.grpc.StatusRuntimeException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

public class StorageWriteApiDefaultStreamTest {

  private final PartitionedTableId mockedPartitionedTableId = new PartitionedTableId.Builder("dummyDataset", "dummyTable").setProject("dummyProject").build();
  private final String mockedTableName = TableNameUtils.tableName(mockedPartitionedTableId.getFullTableId()).toString();
  private final JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
  private final SinkRecord mockedSinkRecord = new SinkRecord(
      "abc",
      0,
      Schema.BOOLEAN_SCHEMA,
      null,
      Schema.BOOLEAN_SCHEMA,
      null,
      0);
  private final List<ConvertedRecord> testRows = Collections.singletonList(new ConvertedRecord(mockedSinkRecord, new JSONObject()));
  private final List<ConvertedRecord> testMultiRows = Arrays.asList(
      new ConvertedRecord(mockedSinkRecord, new JSONObject()),
      new ConvertedRecord(mockedSinkRecord, new JSONObject()));
  private final StorageWriteApiDefaultStream defaultStream = mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);
  private final String baseErrorMessage = "Failed to write rows on table "
      + mockedTableName;
  private final String retriableExpectedException = "Exceeded 0 attempts to write to table "
          + mockedTableName + " ";
  private final String malformedrequestExpectedException = "Insertion failed at table dummyTable for following rows:" +
          " \n [row index 0] (Failure reason : f0 field is unknown) ";
  ErrantRecordHandler mockedErrantRecordHandler = mock(ErrantRecordHandler.class);
  ErrantRecordReporter mockedErrantReporter = mock(ErrantRecordReporter.class);
  KcbqThreadPoolExecutor mockedExecutor = mock(KcbqThreadPoolExecutor.class);
  AppendRowsResponse malformedError = AppendRowsResponse.newBuilder()
      .setError(
          Status.newBuilder()
              .setCode(3)
              .setMessage("I am an INVALID_ARGUMENT error")
              .build()
      ).addRowErrors(
          RowError.newBuilder()
              .setIndex(0)
              .setMessage("f0 field is unknown")
              .build()
      ).build();
  AppendRowsResponse successResponse = AppendRowsResponse.newBuilder()
      .setAppendResult(AppendRowsResponse.AppendResult.newBuilder().getDefaultInstanceForType()).build();
  Map<Integer, String> errorMapping = new HashMap<>();
  Exceptions.AppendSerializtionError appendSerializationException = new Exceptions.AppendSerializtionError(
      3,
      "INVALID_ARGUMENT",
      "DEFAULT",
      errorMapping);
  AppendRowsResponse schemaError = AppendRowsResponse.newBuilder()
      .setUpdatedSchema(TableSchema.newBuilder().build())
      .build();
  ExecutionException tableMissingException = new ExecutionException(new StatusRuntimeException(
      io.grpc.Status
          .fromCode(io.grpc.Status.Code.NOT_FOUND)
          .withDescription("Not found: table. Table is deleted")
  ));
  SchemaManager mockedSchemaManager = mock(SchemaManager.class);
  SettableApiFuture<AppendRowsResponse> responseFuture;
  MockTime time = new MockTime();

  @BeforeEach
  public void setUp() throws Exception {
    errorMapping.put(0, "f0 field is unknown");
    defaultStream.tableToStream = new ConcurrentHashMap<>();
    defaultStream.tableToStream.put("testTable", mockedStreamWriter);
    defaultStream.schemaManager = mockedSchemaManager;
    defaultStream.time = time;
    defaultStream.errantRecordHandler = mockedErrantRecordHandler;
    defaultStream.executor = mockedExecutor;
    doAnswer(inv -> { ((Runnable) inv.getArgument(0)).run(); return null; })
            .when(mockedExecutor).execute(any(Runnable.class));
    doNothing().when(mockedExecutor).maybeThrowEncounteredError();
    doNothing().when(mockedExecutor).reportError(any());
    responseFuture = SettableApiFuture.create();
    when(mockedStreamWriter.append(any())).thenReturn(responseFuture);
    doReturn(mockedStreamWriter).when(defaultStream).getDefaultStream(any(), any());

    doReturn(true).when(mockedSchemaManager).createTable(any(), any());
    doNothing().when(mockedSchemaManager).updateSchema(any(), any());
    when(mockedErrantRecordHandler.getErrantRecordReporter()).thenReturn(mockedErrantReporter);
    when(defaultStream.getAutoCreateTables()).thenReturn(true);
    when(defaultStream.canAttemptSchemaUpdate()).thenReturn(true);
  }

  @Test
  public void testDefaultStreamNoExceptions() throws Exception {
    responseFuture.set(successResponse);
    defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testRows, null).get();
  }

  @ParameterizedTest(name = "{index} – {0}")
  @MethodSource("terminalClientErrors")
  public void testDefaultStreamTerminalClientErrors(String testCase, String errorMessage) throws Exception {
    AppendRowsResponse clientError = AppendRowsResponse.newBuilder()
            .setError(
                    Status.newBuilder()
                            .setCode(0)
                            .setMessage(errorMessage)
                            .build()
            ).build();

    responseFuture.set(clientError);

    verifyTerminalException(errorMessage);
  }

  public static Stream<Arguments> terminalClientErrors() {
    return Stream.of(
            Arguments.of("Non-retriable errors", "I am non-retriable error"),
            Arguments.of("Request-level errors", "I am an INTERNAL error")
    );
  }

  @Test
  public void testDefaultStreamMalformedRequestErrorAllToDLQ() throws Exception {
    responseFuture.set(malformedError);
    defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testRows, null).get();
    verifyDLQ();
  }

  @Test
  public void testDefaultStreamMalformedRequestErrorSomeToDLQ() throws Exception {
    responseFuture.set(malformedError);

    ExecutionException e = assertThrows(
            ExecutionException.class,
            () -> defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testMultiRows, null).get()
    );
    assertTrue(e.getCause() instanceof BigQueryStorageWriteApiConnectException);
    verifyDLQ();
  }

  @Test
  public void testHasSchemaUpdates() throws Exception {
    SettableApiFuture<AppendRowsResponse> second = SettableApiFuture.create();
    doReturn(responseFuture)
            .doReturn(second)
            .when(mockedStreamWriter)
            .append(any());

    responseFuture.set(schemaError);
    second.set(successResponse);

    defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testRows, null).get();

    verify(mockedSchemaManager, times(1)).updateSchema(any(), any());
  }

  @Test
  public void testHasSchemaUpdatesNotConfigured() {
    responseFuture.set(schemaError);
    when(defaultStream.canAttemptSchemaUpdate()).thenReturn(false);

    ExecutionException e = assertThrows(
            ExecutionException.class,
            () -> defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testRows, null).get()
    );
    assertTrue(e.getCause() instanceof BigQueryStorageWriteApiConnectException);

    verifyNoInteractions(mockedSchemaManager);
  }

  @ParameterizedTest(name = "{index} – {0}")
  @MethodSource("terminalClientExceptions")
  public void testDefaultStreamTerminalClientException(String testCase, Exception exception) {
    responseFuture.setException(exception);

    verifyTerminalException(exception.getMessage());
  }

  public static Stream<Arguments> terminalClientExceptions() {
    return Stream.of(
            Arguments.of("Non-retriable errors", new InterruptedException("I am non-retriable error")),
            Arguments.of("Request-level errors", new ExecutionException(new StatusRuntimeException(
                    io.grpc.Status.fromCode(io.grpc.Status.Code.INTERNAL).withDescription("I am an INTERNAL error"))))
    );
  }

  @Test
  public void testDefaultStreamMalformedRequestExceptionAllToDLQ() throws Exception {
    responseFuture.setException(appendSerializationException);
    defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testRows, null).get();
    verifyDLQ();
  }

  @Test
  public void testDefaultStreamMalformedRequestExceptionSomeToDLQ() {
    responseFuture.setException(appendSerializationException);
    ExecutionException e = assertThrows(
            ExecutionException.class,
            () -> defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testMultiRows, null).get()
    );
    assertTrue(e.getCause() instanceof BigQueryStorageWriteApiConnectException);
    verifyDLQ();
  }

  @Test
  public void testDefaultStreamTableMissingException() throws Exception {
    SettableApiFuture<AppendRowsResponse> second = SettableApiFuture.create();

    doReturn(responseFuture)
            .doReturn(second)
            .when(mockedStreamWriter)
            .append(any());

    responseFuture.setException(tableMissingException);
    second.set(successResponse);

    when(defaultStream.getAutoCreateTables()).thenReturn(true);

    defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testRows, null).get();
    verify(mockedSchemaManager, times(1)).createTable(any(), any());
  }

  @Test
  public void testHasSchemaUpdatesException() throws Exception {
    errorMapping.put(0, "JSONObject does not have the required field f1");
    SettableApiFuture<AppendRowsResponse> second = SettableApiFuture.create();
    doReturn(responseFuture)
            .doReturn(second)
            .when(mockedStreamWriter)
            .append(any());

    responseFuture.setException(appendSerializationException);
    second.set(successResponse);

    defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testRows, null).get();
    verify(mockedSchemaManager, times(1)).updateSchema(any(), any());

  }

  @Test
  public void testDefaultStreamClosedException() {
    responseFuture.setException(
            new StatusRuntimeException(
                    io.grpc.Status.FAILED_PRECONDITION.withDescription("Exceptions$StreamWriterClosedException"))
    );

    ExecutionException e = assertThrows(
            ExecutionException.class,
            () -> defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testRows, null).get()
    );

    Throwable cause = e.getCause();
    assertTrue(cause instanceof BigQueryStorageWriteApiConnectException);
  }

  @Test
  public void testShutdown() {
    defaultStream.tableToStream = new ConcurrentHashMap<>();
    defaultStream.tableToStream.put("testTable", mockedStreamWriter);
    defaultStream.preShutdown();
    verify(mockedStreamWriter, times(1)).close();
  }

  private void verifyTerminalException(String expectedException) {
    ExecutionException e = assertThrows(
            ExecutionException.class,
            () -> defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testRows, null).get()
    );
    Throwable cause = e.getCause();
    assertAll(
            () -> assertTrue(cause instanceof BigQueryStorageWriteApiConnectException),
            () -> assertTrue(cause.getMessage().startsWith(baseErrorMessage), "Should fail task with base message"),
            () -> assertTrue(cause.getMessage().contains(expectedException),"Should include cause of failure"),
            () -> assertFalse(cause.getMessage().contains(retriableExpectedException),
                    "Should not use connector retry path")
    );
  }

  private void verifyDLQ() {
    ArgumentCaptor<Map<SinkRecord, Throwable>> captorRecord = ArgumentCaptor.forClass(Map.class);

    verify(mockedErrantRecordHandler, times(1))
            .reportErrantRecords(captorRecord.capture());
    assertTrue(captorRecord.getValue().containsKey(mockedSinkRecord));
    assertEquals("f0 field is unknown", captorRecord.getValue().get(mockedSinkRecord).getMessage());
    assertEquals(1, captorRecord.getValue().size());
  }
}
