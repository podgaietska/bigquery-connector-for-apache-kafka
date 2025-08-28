package com.wepay.kafka.connect.bigquery.write.storage;

import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriter;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriterBuilder;
import org.apache.kafka.connect.sink.SinkRecord;

public interface PartitionedTableWriterBuilder extends TableWriterBuilder {
  /**
   * Add a record to the builder.
   *
   * @param sinkRecord the row to add.
   * @param table      the table the row will be written to.
   */
  void addRow(SinkRecord sinkRecord, PartitionedTableId table);

  /**
   * Create a {@link TableWriter} from this builder.
   *
   * @return a TableWriter containing the given writer, table, topic, and all added rows.
   */
  Runnable build();
}
