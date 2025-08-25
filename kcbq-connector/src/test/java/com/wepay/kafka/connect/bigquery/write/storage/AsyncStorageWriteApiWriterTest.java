package com.wepay.kafka.connect.bigquery.write.storage;

import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import com.wepay.kafka.connect.bigquery.write.batch.KcbqThreadPoolExecutor;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class AsyncStorageWriteApiWriterTest {
    private StorageWriteApiBase streamWriter;
    private KcbqThreadPoolExecutor executor;
    private AsyncStorageWriteApiWriter writer;
    private AsyncStorageWriteApiWriter quietWriter;
    private TableName table = TableName.of("p", "d", "t");

    @BeforeEach
    void setup() {
        streamWriter = mock(StorageWriteApiBase.class);
        executor = mock(KcbqThreadPoolExecutor.class);

        writer = new AsyncStorageWriteApiWriter(streamWriter, executor);

        quietWriter = spy(new AsyncStorageWriteApiWriter(streamWriter, executor) {
            @Override public void startDrainer() { /* don't start thread */ }
        });
    }

    @AfterEach
    void teardown() throws Exception {
        writer.stopDrainer();
    }

    private List<ConvertedRecord> batch(long... offsets) {
        List<ConvertedRecord> list = new ArrayList<>();
        for (long off : offsets) {
            SinkRecord r = new SinkRecord("topicA", 0, Schema.STRING_SCHEMA, "k",
                    Schema.STRING_SCHEMA, "v", off);
            list.add(new ConvertedRecord(r, new JSONObject()));
        }
        return list;
    }

    @Test
    void builder_defaultStream_callsSendAppendRowsRequest() {
        AsyncStorageWriteApiWriter spyWriter = spy(quietWriter); // no drainer
        SinkRecordConverter dummy = mock(SinkRecordConverter.class);
        when(dummy.getRegularRow(any())).thenReturn(Collections.emptyMap());

        AsyncStorageWriteApiWriter.Builder b = new AsyncStorageWriteApiWriter.Builder(
                table, dummy, spyWriter, /*batchModeHandler*/ null);

        // one row
        b.addRow(new SinkRecord("t", 0, Schema.STRING_SCHEMA, "k",
                Schema.STRING_SCHEMA, "v", 5L), null);

        Runnable r = b.build();
        r.run();

        ArgumentCaptor<List<ConvertedRecord>> cap = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<String> stream = ArgumentCaptor.forClass(String.class);
        verify(spyWriter).sendAppendRowsRequest(cap.capture(), eq(table), stream.capture());
        assertEquals(1, cap.getValue().size());
        assertEquals("default", stream.getValue());
    }

    @Test
    void builder_batchStream_callsWithHandlerStreamName() {
        AsyncStorageWriteApiWriter spyWriter = spy(quietWriter);
        SinkRecordConverter dummy = mock(SinkRecordConverter.class);
        when(dummy.getRegularRow(any())).thenReturn(Collections.emptyMap());
        StorageApiBatchModeHandler handler = mock(StorageApiBatchModeHandler.class);
        when(handler.updateOffsetsOnStream(eq(table.toString()), anyList())).thenReturn("table_s1");

        AsyncStorageWriteApiWriter.Builder b = new AsyncStorageWriteApiWriter.Builder(
                table, dummy, spyWriter, handler);

        b.addRow(new SinkRecord("t", 0, Schema.STRING_SCHEMA, "k",
                Schema.STRING_SCHEMA, "v", 9L), null);

        Runnable r = b.build();
        r.run();

        ArgumentCaptor<String> stream = ArgumentCaptor.forClass(String.class);
        verify(spyWriter).sendAppendRowsRequest(anyList(), eq(table), stream.capture());
        assertEquals("table_s1", stream.getValue());
    }

    // 2) Drainer: success → calls maybeThrowEncounteredError and removes attempt
    @Test
    void drainer_success_callsMaybeThrowEncounteredError() throws Exception {
        SettableApiFuture<Void> f = SettableApiFuture.create();
        when(streamWriter.initializeAndWriteRecords(eq(table), anyList(), eq("default")))
                .thenReturn(f);

        writer.sendAppendRowsRequest(batch(1L), table, "default");
        f.set(null); // complete successfully

        verify(executor, timeout(1000)).maybeThrowEncounteredError();
        verify(executor, never()).reportError(any());
    }

    @Test
    void drainer_failure_callsReportError() throws Exception {
        SettableApiFuture<Void> f = SettableApiFuture.create();
        when(streamWriter.initializeAndWriteRecords(eq(table), anyList(), eq("default")))
                .thenReturn(f);

        writer.sendAppendRowsRequest(batch(2L), table, "default");
        f.setException(new RuntimeException("boom"));

        verify(executor, timeout(1000)).reportError(argThat(t -> t.getMessage().contains("boom")));
    }

    // 4) getCommittableOffsets: pending overlap => block (commit = minPending)
    @Test
    void committableOffsets_pendingOverlap_blocks() {
        // Prepare one pending attempt: min offset 95 on TP(“topicA”,0)
        SettableApiFuture<Void> pending = SettableApiFuture.create(); // never completed
        when(streamWriter.initializeAndWriteRecords(eq(table), anyList(), anyString()))
                .thenReturn(pending);

        List<ConvertedRecord> batch = batch(95L, 110L); // min per TP = 95
        quietWriter.sendAppendRowsRequest(batch, table, "default");

        TopicPartition tp = new TopicPartition("topicA", 0);
        Map<TopicPartition, OffsetAndMetadata> requested = new HashMap<>();
        requested.put(tp, new OffsetAndMetadata(100L)); // boundary=99

        Map<TopicPartition, OffsetAndMetadata> out = quietWriter.getCommittableOffsets(requested);
        assertEquals(95L, out.get(tp).offset()); // commit = minPending
        verify(executor, never()).maybeThrowEncounteredError();
    }

    // 4b) getCommittableOffsets: pending non-overlap => allow requested
    @Test
    void committableOffsets_pendingNonOverlap_allows() {
        SettableApiFuture<Void> pending = SettableApiFuture.create();
        when(streamWriter.initializeAndWriteRecords(eq(table), anyList(), anyString()))
                .thenReturn(pending);

        // min = 150 > boundary(99)
        quietWriter.sendAppendRowsRequest(batch(150L), table, "default");

        TopicPartition tp = new TopicPartition("topicA", 0);
        Map<TopicPartition, OffsetAndMetadata> requested = Map.of(tp, new OffsetAndMetadata(100L));
        Map<TopicPartition, OffsetAndMetadata> out = quietWriter.getCommittableOffsets(requested);

        assertEquals(100L, out.get(tp).offset()); // unchanged
    }

    // 4c) getCommittableOffsets: completed attempt => call maybeThrowEncounteredError and allow requested
    @Test
    void committableOffsets_doneAttempt_callsMaybeThrowError_andAllowsRequested() throws Exception {
        SettableApiFuture<Void> done = SettableApiFuture.create();
        when(streamWriter.initializeAndWriteRecords(eq(table), anyList(), anyString()))
                .thenReturn(done);

        quietWriter.sendAppendRowsRequest(batch(10L), table, "default");
        done.set(null); // complete

        TopicPartition tp = new TopicPartition("topicA", 0);
        Map<TopicPartition, OffsetAndMetadata> requested = Map.of(tp, new OffsetAndMetadata(100L));
        Map<TopicPartition, OffsetAndMetadata> out = quietWriter.getCommittableOffsets(requested);

        assertEquals(100L, out.get(tp).offset());
        verify(executor).maybeThrowEncounteredError();
    }
}
