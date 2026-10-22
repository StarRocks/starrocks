// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.iceberg.procedure;

import com.google.common.collect.Iterables;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AddEqualityDeleteFilesProcedureTest {
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "p", Types.StringType.get()),
            Types.NestedField.optional(3, "v", Types.StringType.get()));
    // Files written the way a hive table writes them: no Iceberg field ids, partition values in the path.
    private static final MessageType ID_FILE_SCHEMA = MessageTypeParser.parseMessageType(
            "message delete { optional int32 id; }");
    private static final MessageType V_FILE_SCHEMA = MessageTypeParser.parseMessageType(
            "message delete { optional binary v (UTF8); }");

    @TempDir
    File tempDir;

    @Test
    public void testPartitionedDeleteIsScopedToItsPartition() throws Exception {
        Table table = createTable(PartitionSpec.builderFor(SCHEMA).identity("p").build(), "2");
        appendDataFile(table, "p=a");
        appendDataFile(table, "p=b");
        long dataSequenceNumber = table.currentSnapshot().sequenceNumber();
        writeIdFile("deletes/p=a/delete.parquet", 1);

        execute(table, args("deletes", "parquet", "ID"));

        List<DeleteFile> added = new ArrayList<>();
        table.currentSnapshot().addedDeleteFiles(table.io()).forEach(added::add);
        assertEquals(1, added.size());
        DeleteFile deleteFile = added.get(0);
        assertEquals(FileContent.EQUALITY_DELETES, deleteFile.content());
        assertEquals(List.of(1), deleteFile.equalityFieldIds());
        assertEquals("a", deleteFile.partition().get(0, String.class));
        assertEquals(1, deleteFile.recordCount());
        assertTrue(deleteFile.dataSequenceNumber() > dataSequenceNumber);
        // no column stats, so the delete file is never pruned by them
        assertNull(deleteFile.valueCounts());
        assertNull(deleteFile.nullValueCounts());
        assertNull(deleteFile.lowerBounds());
        assertNull(deleteFile.upperBounds());

        Map<String, Integer> deletesPerPartition = deletesPerDataFile(table);
        assertEquals(1, deletesPerPartition.get("a"));
        assertEquals(0, deletesPerPartition.get("b"));

        // a file outside any p=<value> directory would land in the null partition and delete nothing
        writeIdFile("flat_deletes/delete.parquet", 1);
        StarRocksConnectorException e = assertThrows(StarRocksConnectorException.class,
                () -> execute(table, args("flat_deletes", "parquet", "id")));
        assertTrue(e.getMessage().contains("no p=<value> directory"), e.getMessage());
    }

    @Test
    public void testUnpartitionedDeleteAppliesToAllDataFiles() throws Exception {
        Table table = createTable(PartitionSpec.unpartitioned(), "2");
        appendDataFile(table, null);
        writeIdFile("deletes/delete.parquet", 1);

        execute(table, args("deletes", "parquet", "id"));

        assertEquals(List.of(1), deletesPerDataFile(table).values().stream().toList());
    }

    @Test
    public void testChecksPhysicalFieldIds() throws Exception {
        writeParquet("parquet_id_1/delete.parquet", "message delete { optional int32 id = 1; }", 1);
        writeParquet("parquet_id_7/delete.parquet", "message delete { optional int32 id = 7; }", 1);
        writeOrc("orc_no_id/delete.orc", null);
        writeOrc("orc_id_7/delete.orc", "7");

        Table table = createTable(PartitionSpec.unpartitioned(), "2");
        appendDataFile(table, null);
        // a reader matches these columns by field id, so a different id would never match the table's id column
        assertThrows(StarRocksConnectorException.class, () -> execute(table, args("parquet_id_7", "parquet", "id")));
        assertThrows(StarRocksConnectorException.class, () -> execute(table, args("orc_id_7", "orc", "id")));

        // a file with any field id is read by field id, so an id column without one would be read as NULL
        writeTwoIntColumns("first_has_id/delete.parquet", "message delete { optional int32 k = 5; optional int32 id; }");
        writeTwoIntColumns("later_has_id/delete.parquet", "message delete { optional int32 id; optional int32 k = 5; }");
        assertThrows(StarRocksConnectorException.class, () -> execute(table, args("first_has_id", "parquet", "id")));
        assertThrows(StarRocksConnectorException.class, () -> execute(table, args("later_has_id", "parquet", "id")));
        writeOrcFile("orc_mixed_id/delete.orc", "struct<k:int,id:int>", "5");
        assertThrows(StarRocksConnectorException.class, () -> execute(table, args("orc_mixed_id", "orc", "id")));
        // the reader takes the first column with a field id, here k rather than id
        writeTwoIntColumns("duplicate_id/delete.parquet", "message delete { optional int32 k = 1; optional int32 id = 1; }");
        StarRocksConnectorException e = assertThrows(StarRocksConnectorException.class,
                () -> execute(table, args("duplicate_id", "parquet", "id")));
        assertTrue(e.getMessage().contains("duplicate field ids"), e.getMessage());

        execute(table, args("parquet_id_1", "parquet", "id"));
        execute(table, args("orc_no_id", "orc", "id"));
        assertEquals(List.of(2), deletesPerDataFile(table).values().stream().toList());
    }

    @Test
    public void testRejectsLiveDataUnderOlderSpec() throws Exception {
        writeIdFile("deletes/p=a/delete.parquet", 1);

        // partitioned by p, then by p and v: the p=a data file stays in spec 0 and a spec 1 delete would miss it
        Table evolved = createTable(PartitionSpec.builderFor(SCHEMA).identity("p").build(), "2");
        appendDataFile(evolved, "p=a");
        evolved.updateSpec().addField("v").commit();
        assertThrows(StarRocksConnectorException.class, () -> execute(evolved, args("deletes", "parquet", "id")));
        assertTrue(evolved.currentSnapshot().deleteManifests(evolved.io()).isEmpty());

        // evolved to unpartitioned: the deletes are global and reach the old spec's data files
        Table unpartitioned = createTable(PartitionSpec.builderFor(SCHEMA).identity("p").build(), "2");
        appendDataFile(unpartitioned, "p=a");
        unpartitioned.updateSpec().removeField("p").commit();
        writeIdFile("global_deletes/delete.parquet", 1);
        execute(unpartitioned, args("global_deletes", "parquet", "id"));
        assertEquals(List.of(1), deletesPerDataFile(unpartitioned).values().stream().toList());
    }

    @Test
    public void testRejectsEqualityDeletesUnderOlderSpec() throws Exception {
        writeIdFile("old_deletes/p=a/delete.parquet", 1);
        writeIdFile("new_deletes/delete.parquet", 2);

        // partitioned by p with a live p=a delete, then unpartitioned: a spec 1 delete would mix delete specs
        Table table = createTable(PartitionSpec.builderFor(SCHEMA).identity("p").build(), "2");
        appendDataFile(table, "p=a");
        execute(table, args("old_deletes", "parquet", "id"));
        table.updateSpec().removeField("p").commit();
        long snapshotId = table.currentSnapshot().snapshotId();
        StarRocksConnectorException e = assertThrows(StarRocksConnectorException.class,
                () -> execute(table, args("new_deletes", "parquet", "id")));
        assertTrue(e.getMessage().contains("equality delete files written under partition spec 0"), e.getMessage());
        assertEquals(snapshotId, table.currentSnapshot().snapshotId());

        // allowed once the session reads equality deletes across partition specs
        ConnectContext connectContext = new ConnectContext();
        connectContext.getSessionVariable().setEnableReadIcebergEqDeleteWithPartitionEvolution(true);
        execute(table, args("new_deletes", "parquet", "id"), connectContext);
        assertEquals(List.of(2), deletesPerDataFile(table).values().stream().toList());
    }

    @Test
    public void testChecksPhysicalTypes() throws Exception {
        writeParquet("parquet_string/delete.parquet", "message delete { optional binary id (UTF8); }", "1");
        writeParquet("parquet_int64/delete.parquet", "message delete { optional int64 id; }", 1L);
        writeOrcString("orc_string/delete.orc");

        Table table = createTable(PartitionSpec.unpartitioned(), "2");
        appendDataFile(table, null);
        // id is INT in the table; the reader cannot convert a string column to it
        writeParquet("parquet_date/delete.parquet", "message delete { optional int32 id (DATE); }", 1);
        for (List<String> input : List.of(List.of("parquet_string", "parquet"), List.of("orc_string", "orc"),
                List.of("parquet_int64", "parquet"), List.of("parquet_date", "parquet"))) {
            StarRocksConnectorException e = assertThrows(StarRocksConnectorException.class,
                    () -> execute(table, args(input.get(0), input.get(1), "id")));
            assertTrue(e.getMessage().contains("cannot be read as the table's type"), e.getMessage());
        }
        assertEquals(1, snapshotCount(table));

        // the reader reads a fixed-length byte array without the UUID annotation as a string
        MessageType fixedSchema = MessageTypeParser.parseMessageType(
                "message delete { optional fixed_len_byte_array(3) v; }");
        try (ParquetWriter<Group> writer = newWriter("fixed_v/delete.parquet", fixedSchema)) {
            writer.write(new SimpleGroupFactory(fixedSchema).newGroup().append("v", Binary.fromString("abc")));
        }
        execute(table, args("fixed_v", "parquet", "v"));
        assertEquals(2, snapshotCount(table));

        // a LONG table column also accepts an INT32 file column
        Schema longSchema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "d", Types.DecimalType.of(9, 2)));
        Table longTable = new HadoopTables(new Configuration()).create(longSchema, PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"), new File(tempDir, "long_table").toURI().toString());
        writeIdFile("int32/delete.parquet", 1);
        execute(longTable, args("int32", "parquet", "id"));
        execute(longTable, args("parquet_int64", "parquet", "id"));
        assertEquals(2, snapshotCount(longTable));

        // Iceberg writes TIME as an ORC LONG tagged iceberg.long-type=TIME, which the reader converts
        TypeDescription timeSchema = TypeDescription.fromString("struct<id:bigint>");
        timeSchema.getChildren().get(0).setAttribute("iceberg.long-type", "TIME");
        File timeFile = new File(tempDir, "orc_time/delete.orc");
        timeFile.getParentFile().mkdirs();
        try (Writer writer = OrcFile.createWriter(new Path(timeFile.toURI()),
                OrcFile.writerOptions(new Configuration()).setSchema(timeSchema))) {
            VectorizedRowBatch batch = timeSchema.createRowBatch();
            ((LongColumnVector) batch.cols[0]).vector[batch.size++] = 3_600_000_000L;
            writer.addRowBatch(batch);
        }
        assertThrows(StarRocksConnectorException.class, () -> execute(longTable, args("orc_time", "orc", "id")));
        assertEquals(2, snapshotCount(longTable));

        // decimal and other types with scale or unit annotations are not supported as equality columns
        StarRocksConnectorException e = assertThrows(StarRocksConnectorException.class,
                () -> execute(longTable, args("int32", "parquet", "d")));
        assertTrue(e.getMessage().contains("only INT, BIGINT, STRING and DATE"), e.getMessage());
    }

    @Test
    public void testDateAndUnsupportedEqualityColumns() throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "d", Types.DateType.get()),
                Types.NestedField.optional(2, "ts", Types.TimestampType.withoutZone()));
        Table table = new HadoopTables(new Configuration()).create(schema, PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"), new File(tempDir, "date_table").toURI().toString());
        appendDataFile(table, null);

        MessageType dateSchema = MessageTypeParser.parseMessageType("message delete { optional int32 d (DATE); }");
        try (ParquetWriter<Group> writer = newWriter("parquet_date/delete.parquet", dateSchema)) {
            writer.write(new SimpleGroupFactory(dateSchema).newGroup().append("d", 19000));
        }
        writeOrcFile("orc_date/delete.orc", "struct<d:date>", null);
        execute(table, args("parquet_date", "parquet", "d"));
        execute(table, args("orc_date", "orc", "d"));
        assertEquals(List.of(2), deletesPerDataFile(table).values().stream().toList());

        // an INT32 without the DATE annotation holds a plain integer, not days since the epoch
        MessageType intSchema = MessageTypeParser.parseMessageType("message delete { optional int32 d; }");
        try (ParquetWriter<Group> writer = newWriter("parquet_int/delete.parquet", intSchema)) {
            writer.write(new SimpleGroupFactory(intSchema).newGroup().append("d", 19000));
        }
        assertThrows(StarRocksConnectorException.class, () -> execute(table, args("parquet_int", "parquet", "d")));

        StarRocksConnectorException e = assertThrows(StarRocksConnectorException.class,
                () -> execute(table, args("parquet_date", "parquet", "ts")));
        assertTrue(e.getMessage().contains("only INT, BIGINT, STRING and DATE"), e.getMessage());
        assertEquals(3, snapshotCount(table)); // the append and the two deletes
    }

    @Test
    public void testRejectsInvalidInput() throws Exception {
        writeIdFile("deletes/delete.parquet", 1);
        writeVFile("v_only/delete.parquet");

        Table v1Table = createTable(PartitionSpec.unpartitioned(), "1");
        assertThrows(StarRocksConnectorException.class, () -> execute(v1Table, args("deletes", "parquet", "id")));

        Table table = createTable(PartitionSpec.unpartitioned(), "2");
        assertThrows(StarRocksConnectorException.class, () -> execute(table, args("deletes", "parquet", "no_such")));
        assertThrows(StarRocksConnectorException.class, () -> execute(table, args("deletes", "parquet", " , ")));
        assertThrows(StarRocksConnectorException.class, () -> execute(table, args("deletes", "avro", "id")));
        // the file has no id column, so it cannot delete by id
        assertThrows(StarRocksConnectorException.class, () -> execute(table, args("v_only", "parquet", "id")));
        assertEquals(null, table.currentSnapshot());
    }

    private Table createTable(PartitionSpec spec, String formatVersion) {
        String location = new File(tempDir, "table_v" + formatVersion + "_" + spec.specId() + "_" +
                System.nanoTime()).toURI().toString();
        return new HadoopTables(new Configuration())
                .create(SCHEMA, spec, Map.of("format-version", formatVersion), location);
    }

    private void appendDataFile(Table table, String partitionPath) {
        DataFiles.Builder builder = DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/" + System.nanoTime() + ".parquet")
                .withFormat("PARQUET")
                .withFileSizeInBytes(10)
                .withRecordCount(1);
        if (partitionPath != null) {
            builder.withPartitionPath(partitionPath);
        }
        table.newFastAppend().appendFile(builder.build()).commit();
    }

    private void execute(Table table, Map<String, ConstantOperator> args) {
        execute(table, args, null);
    }

    private void execute(Table table, Map<String, ConstantOperator> args, ConnectContext connectContext) {
        Transaction transaction = table.newTransaction();
        IcebergTableProcedureContext context = new IcebergTableProcedureContext(
                null, table, connectContext, transaction, new HdfsEnvironment(), null, null);
        AddEqualityDeleteFilesProcedure.getInstance().execute(context, args);
        transaction.commitTransaction();
        table.refresh();
    }

    private Map<String, ConstantOperator> args(String location, String fileFormat, String equalityColumns) {
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put(AddEqualityDeleteFilesProcedure.LOCATION,
                ConstantOperator.createVarchar(new File(tempDir, location).toURI().toString()));
        args.put(AddEqualityDeleteFilesProcedure.FILE_FORMAT, ConstantOperator.createVarchar(fileFormat));
        args.put(AddEqualityDeleteFilesProcedure.EQUALITY_COLUMNS, ConstantOperator.createVarchar(equalityColumns));
        return args;
    }

    // Number of delete files the scan attaches to each data file, keyed by the data file's partition value.
    private static Map<String, Integer> deletesPerDataFile(Table table) throws Exception {
        Map<String, Integer> result = new HashMap<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile file = task.file();
                String key = table.spec().isPartitioned() ? file.partition().get(0, String.class) : file.location();
                result.put(key, task.deletes().size());
            }
        }
        return result;
    }

    private void writeIdFile(String relativePath, int id) throws Exception {
        try (ParquetWriter<Group> writer = newWriter(relativePath, ID_FILE_SCHEMA)) {
            writer.write(new SimpleGroupFactory(ID_FILE_SCHEMA).newGroup().append("id", id));
        }
    }

    private void writeVFile(String relativePath) throws Exception {
        try (ParquetWriter<Group> writer = newWriter(relativePath, V_FILE_SCHEMA)) {
            writer.write(new SimpleGroupFactory(V_FILE_SCHEMA).newGroup().append("v", "x"));
        }
    }

    private void writeParquet(String relativePath, String schemaText, int id) throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType(schemaText);
        try (ParquetWriter<Group> writer = newWriter(relativePath, schema)) {
            writer.write(new SimpleGroupFactory(schema).newGroup().append("id", id));
        }
    }

    private void writeParquet(String relativePath, String schemaText, long id) throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType(schemaText);
        try (ParquetWriter<Group> writer = newWriter(relativePath, schema)) {
            writer.write(new SimpleGroupFactory(schema).newGroup().append("id", id));
        }
    }

    private void writeParquet(String relativePath, String schemaText, String id) throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType(schemaText);
        try (ParquetWriter<Group> writer = newWriter(relativePath, schema)) {
            writer.write(new SimpleGroupFactory(schema).newGroup().append("id", id));
        }
    }

    private void writeTwoIntColumns(String relativePath, String schemaText) throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType(schemaText);
        try (ParquetWriter<Group> writer = newWriter(relativePath, schema)) {
            writer.write(new SimpleGroupFactory(schema).newGroup().append("k", 1).append("id", 1));
        }
    }

    private static int snapshotCount(Table table) {
        return Iterables.size(table.snapshots());
    }

    // A one-row ORC file whose columns all hold 1 (int or date); firstColumnIcebergId, when set, is written as the
    // first column's field id the way Iceberg writes it.
    private void writeOrcFile(String relativePath, String typeText, String firstColumnIcebergId) throws Exception {
        TypeDescription schema = TypeDescription.fromString(typeText);
        if (firstColumnIcebergId != null) {
            schema.getChildren().get(0).setAttribute("iceberg.id", firstColumnIcebergId);
        }
        File file = new File(tempDir, relativePath);
        file.getParentFile().mkdirs();
        try (Writer writer = OrcFile.createWriter(new Path(file.toURI()),
                OrcFile.writerOptions(new Configuration()).setSchema(schema))) {
            VectorizedRowBatch batch = schema.createRowBatch();
            for (int i = 0; i < batch.cols.length; i++) {
                ((LongColumnVector) batch.cols[i]).vector[0] = 1;
            }
            batch.size = 1;
            writer.addRowBatch(batch);
        }
    }

    private void writeOrcString(String relativePath) throws Exception {
        TypeDescription schema = TypeDescription.fromString("struct<id:string>");
        File file = new File(tempDir, relativePath);
        file.getParentFile().mkdirs();
        try (Writer writer = OrcFile.createWriter(new Path(file.toURI()),
                OrcFile.writerOptions(new Configuration()).setSchema(schema))) {
            VectorizedRowBatch batch = schema.createRowBatch();
            ((BytesColumnVector) batch.cols[0]).setVal(batch.size++, "1".getBytes(StandardCharsets.UTF_8));
            writer.addRowBatch(batch);
        }
    }

    // A one-row ORC file with an int column id; icebergId, when set, is written the way Iceberg writes field ids.
    private void writeOrc(String relativePath, String icebergId) throws Exception {
        TypeDescription schema = TypeDescription.fromString("struct<id:int>");
        if (icebergId != null) {
            schema.getChildren().get(0).setAttribute("iceberg.id", icebergId);
        }
        File file = new File(tempDir, relativePath);
        file.getParentFile().mkdirs();
        try (Writer writer = OrcFile.createWriter(new Path(file.toURI()),
                OrcFile.writerOptions(new Configuration()).setSchema(schema))) {
            VectorizedRowBatch batch = schema.createRowBatch();
            ((LongColumnVector) batch.cols[0]).vector[batch.size++] = 1;
            writer.addRowBatch(batch);
        }
    }

    private ParquetWriter<Group> newWriter(String relativePath, MessageType schema) throws Exception {
        File file = new File(tempDir, relativePath);
        file.getParentFile().mkdirs();
        return ExampleParquetWriter.builder(new Path(file.toURI())).withType(schema).build();
    }
}
