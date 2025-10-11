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

package com.starrocks.connector.paimon;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorTableVersion;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.PointerType;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.ConnectorTableMetadataProcessor;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.ColWithComment;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.FunctionName;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.ExternalScanPartitionPruneRule;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.system.ManifestsTable;
import org.apache.paimon.table.system.SnapshotsTable;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.SerializationUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.catalog.Type.INT;
import static com.starrocks.catalog.Type.VARCHAR;
import static org.apache.paimon.io.DataFileMeta.DUMMY_LEVEL;
import static org.apache.paimon.io.DataFileMeta.EMPTY_MAX_KEY;
import static org.apache.paimon.io.DataFileMeta.EMPTY_MIN_KEY;
import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PaimonMetadataTest {
    @Mocked
    Catalog paimonNativeCatalog;
    @Mocked
    org.apache.paimon.table.Table nativeTable;
    private PaimonMetadata metadata;
    private final List<DataSplit> splits = new ArrayList<>();

    private static ConnectContext connectContext;
    private static OptimizerContext optimizerContext;
    private static ColumnRefFactory columnRefFactory;

    @BeforeEach
    public void setUp() {
        this.metadata = new PaimonMetadata("paimon_catalog", new HdfsEnvironment(), paimonNativeCatalog,
                new ConnectorProperties(ConnectorType.PAIMON));

        BinaryRow row1 = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row1, 10);
        writer.writeInt(0, 2000);
        writer.writeInt(1, 4444);
        writer.complete();

        BinaryRow row2 = new BinaryRow(2);
        writer = new BinaryRowWriter(row2, 10);
        writer.writeInt(0, 3000);
        writer.writeInt(1, 5555);
        writer.complete();

        List<DataFileMeta> meta1 = new ArrayList<>();
        meta1.add(new DataFileMeta("file1", 100, 200, EMPTY_MIN_KEY, EMPTY_MAX_KEY, EMPTY_STATS, EMPTY_STATS,
                1, 1, 1, DUMMY_LEVEL, 0L, null, null, null));
        meta1.add(new DataFileMeta("file2", 100, 300, EMPTY_MIN_KEY, EMPTY_MAX_KEY, EMPTY_STATS, EMPTY_STATS,
                1, 1, 1, DUMMY_LEVEL, 0L, null, null, null));

        List<DataFileMeta> meta2 = new ArrayList<>();
        meta2.add(new DataFileMeta("file3", 100, 400, EMPTY_MIN_KEY, EMPTY_MAX_KEY, EMPTY_STATS, EMPTY_STATS,
                1, 1, 1, DUMMY_LEVEL, 0L, null, null, null));
        this.splits.add(DataSplit.builder().withSnapshot(1L).withPartition(row1).withBucket(1)
                .withBucketPath("not used").withDataFiles(meta1).isStreaming(false).build());
        this.splits.add(DataSplit.builder().withSnapshot(1L).withPartition(row2).withBucket(1)
                .withBucketPath("not used").withDataFiles(meta2).isStreaming(false).build());

        connectContext = UtFrameUtils.createDefaultCtx();
        columnRefFactory = new ColumnRefFactory();
        optimizerContext = OptimizerFactory.mockContext(connectContext, columnRefFactory);
    }

    @Test
    public void testRowCount() {
        long rowCount = metadata.getRowCount(splits);
        assertEquals(900, rowCount);
    }

    @Test
    public void testGetTable(@Mocked FileStoreTable paimonNativeTable) throws Catalog.TableNotExistException {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(1, "col2", new IntType(true)));
        fields.add(new DataField(2, "col3", new DoubleType(false)));
        new Expectations() {
            {
                paimonNativeCatalog.getTable((Identifier) any);
                result = paimonNativeTable;
                paimonNativeTable.rowType().getFields();
                result = fields;
                paimonNativeTable.partitionKeys();
                result = new ArrayList<>(Collections.singleton("col1"));
                paimonNativeTable.location().toString();
                result = "hdfs://127.0.0.1:10000/paimon";
                paimonNativeTable.primaryKeys();
                result = List.of("col2");
            }
        };
        com.starrocks.catalog.Table table = metadata.getTable(connectContext, "db1", "tbl1");
        PaimonTable paimonTable = (PaimonTable) table;
        org.junit.jupiter.api.Assertions.assertTrue(metadata.tableExists(connectContext, "db1", "tbl1"));
        assertEquals("db1", paimonTable.getCatalogDBName());
        assertEquals("tbl1", paimonTable.getCatalogTableName());
        assertEquals("CREATE TABLE `tbl1` (\n" +
                        "  `col2` int(11) DEFAULT NULL,\n" +
                        "  `col3` double DEFAULT NULL\n" +
                        ")\n" +
                        "PARTITION BY (col1)\n" +
                        "PROPERTIES (\"primary-key\" = \"col2\");",
                AstToStringBuilder.getExternalCatalogTableDdlStmt(paimonTable));
        assertEquals(Lists.newArrayList("col1"), paimonTable.getPartitionColumnNames());
        assertEquals("hdfs://127.0.0.1:10000/paimon", paimonTable.getTableLocation());
        assertEquals(ScalarType.INT, paimonTable.getBaseSchema().get(0).getType());
        org.junit.jupiter.api.Assertions.assertTrue(paimonTable.getBaseSchema().get(0).isAllowNull());
        assertEquals(ScalarType.DOUBLE, paimonTable.getBaseSchema().get(1).getType());
        org.junit.jupiter.api.Assertions.assertTrue(paimonTable.getBaseSchema().get(1).isAllowNull());
        assertEquals("paimon_catalog", paimonTable.getCatalogName());
        assertEquals("paimon_catalog.db1.tbl1.null", paimonTable.getUUID());
    }

    @Test
    public void testGetDatabaseDoesNotExist() throws Exception {
        String dbName = "nonexistentDb";
        new Expectations() {
            {
                paimonNativeCatalog.getDatabase(dbName);
                result = new Catalog.DatabaseNotExistException("Database does not exist");
            }
        };
        org.junit.jupiter.api.Assertions.assertNull(metadata.getDb(connectContext, "nonexistentDb"));
    }

    @Test
    public void testGetTableDoesNotExist() throws Exception {
        Identifier identifier = new Identifier("nonexistentDb", "nonexistentTbl");
        new Expectations() {
            {
                paimonNativeCatalog.getTable(identifier);
                result = new Catalog.TableNotExistException(identifier);
            }
        };
        org.junit.jupiter.api.Assertions.assertFalse(metadata.tableExists(connectContext, "nonexistentDb", "nonexistentTbl"));
        org.junit.jupiter.api.Assertions.assertNull(metadata.getTable(connectContext, "nonexistentDb", "nonexistentTbl"));
    }

    @Test
    public void testGetSystemTable(@Mocked ManifestsTable paimonSystemTable,
                                   @Mocked ReadBuilder readBuilder,
                                   @Mocked InnerTableScan scan) throws Exception {
        new Expectations() {
            {
                paimonNativeCatalog.getTable((Identifier) any);
                result = paimonSystemTable;
                paimonSystemTable.latestSnapshotId();
                result = new Exception("Readonly Table tbl1$manifests does not support currentSnapshot.");
                // accoding to PaimonMetadata::getTableVersionRange，the snapshot id of system table is -1L,
                // so the above paimonSystemTable.latestSnapshotId() would not be called.
                minTimes = 0;
                paimonSystemTable.newReadBuilder();
                result = readBuilder;
                readBuilder.withFilter((List<Predicate>) any).withProjection((int[]) any).newScan();
                result = scan;
            }
        };
        PaimonTable paimonTable = (PaimonTable) metadata.getTable(connectContext, "db1", "tbl1$manifests");
        List<String> requiredNames = Lists.newArrayList("file_name", "file_size");
        List<RemoteFileInfo> result =
                metadata.getRemoteFiles(paimonTable, GetRemoteFilesParams.newBuilder().setFieldNames(requiredNames)
                        .setTableVersionRange(TvrTableSnapshot.of(Optional.of(-1L))).build());
        assertEquals(1, result.size());
    }

    @Test
    public void testListPartitionNames(@Mocked FileStoreTable mockPaimonTable)
            throws Catalog.TableNotExistException {
        List<String> partitionNames = Lists.newArrayList("year", "month");
        RowType partitionRowType = new RowType(
                Arrays.asList(
                        new DataField(0, "year", SerializationUtils.newStringType(false)),
                        new DataField(1, "month", SerializationUtils.newStringType(false))
                ));
        Identifier tblIdentifier = new Identifier("db1", "tbl1");
        org.apache.paimon.partition.Partition partition1 = new Partition(Map.of("year", "2020", "month", "1"),
                100L, 1L, 1L, 1741327322000L);
        org.apache.paimon.partition.Partition partition2 = new Partition(Map.of("year", "2020", "month", "2"),
                100L, 1L, 1L, 1741327322000L);

        new Expectations() {
            {
                paimonNativeCatalog.getTable(tblIdentifier);
                result = mockPaimonTable;
                mockPaimonTable.partitionKeys();
                result = partitionNames;
                mockPaimonTable.rowType();
                result = partitionRowType;
                paimonNativeCatalog.listPartitions(tblIdentifier);
                result = Arrays.asList(partition1, partition2);
            }
        };
        List<String> result = metadata.listPartitionNames("db1", "tbl1", ConnectorMetadatRequestContext.DEFAULT);
        assertEquals(2, result.size());
        List<String> expectations = Lists.newArrayList("year=2020/month=1", "year=2020/month=2");
        Assertions.assertThat(result).hasSameElementsAs(expectations);
        Config.enable_paimon_refresh_manifest_files = true;
        metadata.refreshTable("db1", metadata.getTable(connectContext, "db1", "tbl1"), new ArrayList<>(), false);
        metadata.refreshTable("db1", metadata.getTable(connectContext, "db1", "tbl1"), expectations, false);

    }

    @Test
    public void testRefreshPaimonMetadata() throws Catalog.DatabaseNotExistException {
        new Expectations() {
            {
                paimonNativeCatalog.listDatabases();
                result = ImmutableList.of("db");
                paimonNativeCatalog.listTables((String) any);
                result = ImmutableList.of("tbl");
            }
        };
        ConnectorTableMetadataProcessor connectorTableMetadataProcessor = new ConnectorTableMetadataProcessor();
        connectorTableMetadataProcessor.registerPaimonCatalog("paimon_catalog", paimonNativeCatalog);
        connectorTableMetadataProcessor.refreshPaimonCatalog();
    }

    @Test
    public void testGetRemoteFiles(@Mocked FileStoreTable paimonNativeTable,
                                   @Mocked ReadBuilder readBuilder,
                                   @Mocked InnerTableScan scan)
            throws Catalog.TableNotExistException {
        new Expectations() {
            {
                paimonNativeCatalog.getTable((Identifier) any);
                result = paimonNativeTable;
                paimonNativeTable.newReadBuilder();
                result = readBuilder;
                readBuilder.withFilter((List<Predicate>) any).withProjection((int[]) any).newScan().plan().splits();
                result = splits;
                readBuilder.newScan();
                result = scan;
            }
        };
        PaimonTable paimonTable = (PaimonTable) metadata.getTable(connectContext, "db1", "tbl1");
        List<String> requiredNames = Lists.newArrayList("f2", "dt");
        // accoding to PaimonMetadata::getTableVersionRange, empty table's snapshot id is -1L.
        List<RemoteFileInfo> result =
                metadata.getRemoteFiles(paimonTable, GetRemoteFilesParams.newBuilder().setFieldNames(requiredNames)
                        .setTableVersionRange(TvrTableSnapshot.of(Optional.of(-1L))).build());
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getFiles().size());
        PaimonRemoteFileDesc desc = (PaimonRemoteFileDesc) result.get(0).getFiles().get(0);
        assertEquals(2, desc.getPaimonSplitsInfo().getPaimonSplits().size());
    }

    @Test
    public void testGetRemoteFileInfosWithLimit() throws Exception {

        java.nio.file.Path tmpDir = Files.createTempDirectory("tmp_");

        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tmpDir.toString())));

        catalog.createDatabase("test_db", true);

        // create schema
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.partitionKeys("create_date");
        schemaBuilder.column("create_date", DataTypes.STRING());
        schemaBuilder.column("user", DataTypes.STRING());
        schemaBuilder.column("record_time", DataTypes.STRING());

        Options options = new Options();
        options.set(CoreOptions.BUCKET, 2);
        options.set(CoreOptions.BUCKET_KEY, "user");
        schemaBuilder.options(options.toMap());

        Schema schema = schemaBuilder.build();

        // create table
        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(identifier, schema, true);

        // insert data
        org.apache.paimon.table.Table table = catalog.getTable(identifier);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder().withOverwrite();
        BatchTableWrite write = writeBuilder.newWrite();

        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        LocalDateTime now = LocalDateTime.now();
        GenericRow record1 = GenericRow.of(BinaryString.fromString(dateFormatter.format(now)),
                BinaryString.fromString("user_1"),
                BinaryString.fromString(dateTimeFormatter.format(now)));
        GenericRow record2 = GenericRow.of(BinaryString.fromString(dateFormatter.format(now)),
                BinaryString.fromString("user_2"),
                BinaryString.fromString(dateTimeFormatter.format(now)));

        now = now.minusDays(1);
        GenericRow record3 = GenericRow.of(BinaryString.fromString(dateFormatter.format(now)),
                BinaryString.fromString("user_1"),
                BinaryString.fromString(dateTimeFormatter.format(now)));
        GenericRow record4 = GenericRow.of(BinaryString.fromString(dateFormatter.format(now)),
                BinaryString.fromString("user_2"),
                BinaryString.fromString(dateTimeFormatter.format(now)));

        now = now.minusDays(1);
        GenericRow record5 = GenericRow.of(BinaryString.fromString(dateFormatter.format(now)),
                BinaryString.fromString("user_1"),
                BinaryString.fromString(dateTimeFormatter.format(now)));
        GenericRow record6 = GenericRow.of(BinaryString.fromString(dateFormatter.format(now)),
                BinaryString.fromString("user_2"),
                BinaryString.fromString(dateTimeFormatter.format(now)));

        write.write(record1);
        write.write(record2);
        write.write(record3);
        write.write(record4);
        write.write(record5);
        write.write(record6);

        List<CommitMessage> messages = write.prepareCommit();

        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(messages);

        List<String> fieldNames = Lists.newArrayList("create_date", "user", "record_time");

        HdfsEnvironment environment = new HdfsEnvironment();
        ConnectorProperties properties = new ConnectorProperties(ConnectorType.PAIMON);

        // no predicate, limit 1
        PaimonMetadata metadata = new PaimonMetadata("paimon", environment, catalog, properties);
        // commit one batch data, so the latest snapshot id is 1L.
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().setFieldNames(fieldNames).setLimit(1)
                .setTableVersionRange(TvrTableSnapshot.of(Optional.of(1L))).build();
        List<RemoteFileInfo> result = metadata.getRemoteFiles(metadata.getTable(connectContext, "test_db", "test_table"), params);
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getFiles().size());
        assertEquals(1, ((PaimonRemoteFileDesc) result.get(0).getFiles().get(0))
                .getPaimonSplitsInfo().getPaimonSplits().size());

        // no predicate, no limit
        metadata = new PaimonMetadata("paimon", environment, catalog, properties);
        params = GetRemoteFilesParams.newBuilder().setFieldNames(fieldNames).setLimit(-1)
                .setTableVersionRange(TvrTableSnapshot.of(Optional.of(1L))).build();
        result = metadata.getRemoteFiles(metadata.getTable(connectContext, "test_db", "test_table"), params);
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getFiles().size());
        assertEquals(6, ((PaimonRemoteFileDesc) result.get(0).getFiles().get(0))
                .getPaimonSplitsInfo().getPaimonSplits().size());

        ColumnRefOperator createDateColumn = new ColumnRefOperator(1, Type.STRING, "create_date", false);
        ScalarOperator createDateEqualPredicate = new BinaryPredicateOperator(BinaryType.EQ, createDateColumn,
                ConstantOperator.createVarchar(dateFormatter.format(now)));

        // partition predicate, limit 1
        metadata = new PaimonMetadata("paimon", environment, catalog, properties);
        params = GetRemoteFilesParams.newBuilder().setFieldNames(fieldNames).setPredicate(createDateEqualPredicate)
                .setLimit(1).setTableVersionRange(TvrTableSnapshot.of(Optional.of(1L))).build();
        result = metadata.getRemoteFiles(metadata.getTable(connectContext, "test_db", "test_table"), params);
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getFiles().size());
        assertEquals(1, ((PaimonRemoteFileDesc) result.get(0).getFiles().get(0))
                .getPaimonSplitsInfo().getPaimonSplits().size());

        // partition predicate, no limit
        metadata = new PaimonMetadata("paimon", environment, catalog, properties);
        params = GetRemoteFilesParams.newBuilder().setFieldNames(fieldNames).setPredicate(createDateEqualPredicate)
                .setLimit(-1).setTableVersionRange(TvrTableSnapshot.of(Optional.of(1L))).build();
        result = metadata.getRemoteFiles(metadata.getTable(connectContext, "test_db", "test_table"), params);
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getFiles().size());
        assertEquals(2, ((PaimonRemoteFileDesc) result.get(0).getFiles().get(0))
                .getPaimonSplitsInfo().getPaimonSplits().size());

        ColumnRefOperator userColumn = new ColumnRefOperator(2, Type.STRING, "user", false);
        ScalarOperator userEqualPredicate = new BinaryPredicateOperator(BinaryType.EQ, userColumn,
                ConstantOperator.createVarchar("user_1"));

        // none partition predicate, limit 1
        metadata = new PaimonMetadata("paimon", environment, catalog, properties);
        params = GetRemoteFilesParams.newBuilder().setFieldNames(fieldNames).setPredicate(userEqualPredicate)
                .setLimit(1).setTableVersionRange(TvrTableSnapshot.of(Optional.of(1L))).build();
        result = metadata.getRemoteFiles(metadata.getTable(connectContext, "test_db", "test_table"), params);
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getFiles().size());
        assertEquals(3, ((PaimonRemoteFileDesc) result.get(0).getFiles().get(0))
                .getPaimonSplitsInfo().getPaimonSplits().size());

        ScalarOperator createDateGreaterPredicate = new BinaryPredicateOperator(BinaryType.GT, createDateColumn,
                ConstantOperator.createVarchar(dateFormatter.format(now)));

        // partition and none partition predicate, limit 1
        metadata = new PaimonMetadata("paimon", environment, catalog, properties);
        params = GetRemoteFilesParams.newBuilder().setFieldNames(fieldNames)
                .setPredicate(Utils.compoundAnd(createDateGreaterPredicate, userEqualPredicate))
                .setLimit(1).setTableVersionRange(TvrTableSnapshot.of(Optional.of(1L))).build();
        result = metadata.getRemoteFiles(metadata.getTable(connectContext, "test_db", "test_table"), params);
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getFiles().size());
        assertEquals(2, ((PaimonRemoteFileDesc) result.get(0).getFiles().get(0))
                .getPaimonSplitsInfo().getPaimonSplits().size());

        Function coalesce = GlobalStateMgr.getCurrentState().getFunction(
                new Function(new FunctionName(FunctionSet.COALESCE), Lists.newArrayList(Type.VARCHAR, Type.VARCHAR),
                        Type.VARCHAR, false),
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

        CallOperator createDateCoalesce = new CallOperator("coalesce", Type.VARCHAR,
                List.of(createDateColumn, ConstantOperator.createVarchar("unknown")), coalesce);

        ScalarOperator createDateCoalescePredicate = new BinaryPredicateOperator(BinaryType.EQ, createDateCoalesce,
                ConstantOperator.createVarchar(dateFormatter.format(now)));

        // partition with function predicate, limit 1
        metadata = new PaimonMetadata("paimon", environment, catalog, properties);
        params = GetRemoteFilesParams.newBuilder().setFieldNames(fieldNames).setPredicate(createDateCoalescePredicate)
                .setLimit(1).setTableVersionRange(TvrTableSnapshot.of(Optional.of(1L))).build();
        result = metadata.getRemoteFiles(metadata.getTable(connectContext, "test_db", "test_table"), params);
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getFiles().size());
        assertEquals(6, ((PaimonRemoteFileDesc) result.get(0).getFiles().get(0))
                .getPaimonSplitsInfo().getPaimonSplits().size());

        catalog.dropTable(identifier, true);
        catalog.dropDatabase("test_db", true, true);
        Files.delete(tmpDir);
    }

    @Test
    public void testGetCloudConfiguration() {
        CloudConfiguration cc = metadata.getCloudConfiguration();
        assertEquals(cc.getCloudType(), CloudType.DEFAULT);
    }

    @Test
    public void testGetUpdateTime(@Mocked SnapshotsTable snapshotsTable,
                                  @Mocked RecordReader<InternalRow> recordReader) throws Exception {
        RowType rowType = new RowType(Arrays.asList(
                new DataField(0, "snapshot_id", new BigIntType(false)),
                new DataField(1, "schema_id", new BigIntType(false)),
                new DataField(2, "commit_user", SerializationUtils.newStringType(false)),
                new DataField(3, "commit_identifier", new BigIntType(false)),
                new DataField(4, "commit_kind", SerializationUtils.newStringType(false)),
                new DataField(5, "commit_time", new TimestampType(false, 3)),
                new DataField(6, "base_manifest_list", SerializationUtils.newStringType(false))));

        GenericRow row1 = new GenericRow(1);
        row1.setField(0, Timestamp.fromLocalDateTime(LocalDateTime.of(2023, 1, 1, 0, 0, 0, 0)));

        GenericRow row2 = new GenericRow(1);
        row2.setField(0, Timestamp.fromLocalDateTime(LocalDateTime.of(2023, 2, 1, 0, 0, 0, 0)));

        new MockUp<RecordReaderIterator>() {
            private int callCount;
            private final GenericRow[] elements = {row1, row2};
            private final boolean[] hasNextOutputs = {true, true, false};

            @Mock
            public boolean hasNext() {
                if (callCount < hasNextOutputs.length) {
                    return hasNextOutputs[callCount];
                }
                return false;
            }

            @Mock
            public InternalRow next() {
                if (callCount < elements.length) {
                    return elements[callCount++];
                }
                return null;
            }
        };
        new Expectations() {
            {
                paimonNativeCatalog.getTable((Identifier) any);
                result = snapshotsTable;
                snapshotsTable.rowType();
                result = rowType;
                snapshotsTable.newReadBuilder().withProjection((int[]) any).newRead().createReader((TableScan.Plan) any);
                result = recordReader;
            }
        };

        long updateTime = metadata.getTableUpdateTime("db1", "tbl1");
        assertEquals(1675180800000L, updateTime);
    }

    @Test
    public void testPrunePaimonPartition() {
        new MockUp<MetadataMgr>() {
            @Mock
            public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
                return Lists.newArrayList(RemoteFileInfo.builder()
                        .setFiles(Lists.newArrayList(PaimonRemoteFileDesc.createPaimonRemoteFileDesc(
                                new PaimonSplitsInfo(null, Lists.newArrayList((Split) splits.get(0))))))
                        .build());
            }
        };

        PaimonTable paimonTable = (PaimonTable) metadata.getTable(connectContext, "db1", "tbl1");

        ExternalScanPartitionPruneRule rule0 = new ExternalScanPartitionPruneRule();

        ColumnRefOperator colRef1 = new ColumnRefOperator(1, Type.INT, "f2", true);
        Column col1 = new Column("f2", Type.INT, true);
        ColumnRefOperator colRef2 = new ColumnRefOperator(2, Type.STRING, "dt", true);
        Column col2 = new Column("dt", Type.STRING, true);

        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<>();
        Map<Column, ColumnRefOperator> columnMetaToColRefMap = new HashMap<>();
        colRefToColumnMetaMap.put(colRef1, col1);
        colRefToColumnMetaMap.put(colRef1, col1);
        columnMetaToColRefMap.put(col2, colRef2);
        columnMetaToColRefMap.put(col2, colRef2);
        OptExpression scan =
                new OptExpression(new LogicalPaimonScanOperator(paimonTable, colRefToColumnMetaMap, columnMetaToColRefMap,
                        -1, null));
        rule0.transform(scan, OptimizerFactory.mockContext(new ColumnRefFactory()));
        assertEquals(1, ((LogicalPaimonScanOperator) scan.getOp()).getScanOperatorPredicates()
                .getSelectedPartitionIds().size());
    }

    @Test
    public void testCreatePaimonView() {
        org.junit.jupiter.api.Assertions.assertThrows(StarRocksConnectorException.class,
                () -> metadata.createView(connectContext,
                        new CreateViewStmt(false, false,
                                new TableName("catalog", "db", "table"),
                    Lists.newArrayList(new ColWithComment("k1", "",
                            NodePosition.ZERO)),
                                "",
                                false,
                                null,
                                NodePosition.ZERO)));
    }

    @Test
    public void testGetTableStatistics() {
        String stats = "{\n" +
                "  \"snapshotId\" : 2,\n" +
                "  \"schemaId\" : 0,\n" +
                "  \"mergedRecordCount\" : 7,\n" +
                "  \"mergedRecordSize\" : 12807,\n" +
                "  \"colStats\" : {\n" +
                "    \"dt\" : {\n" +
                "      \"colId\" : 4,\n" +
                "      \"distinctCount\" : 3,\n" +
                "      \"nullCount\" : 0,\n" +
                "      \"avgLen\" : 8,\n" +
                "      \"maxLen\" : 8\n" +
                "    },\n" +
                "    \"acount\" : {\n" +
                "      \"colId\" : 2,\n" +
                "      \"distinctCount\" : 4,\n" +
                "      \"min\" : \"0.0\",\n" +
                "      \"max\" : \"5.3\",\n" +
                "      \"nullCount\" : 1,\n" +
                "      \"avgLen\" : 8,\n" +
                "      \"maxLen\" : 8\n" +
                "    },\n" +
                "    \"flag\" : {\n" +
                "      \"colId\" : 3,\n" +
                "      \"distinctCount\" : 2,\n" +
                "      \"min\" : \"false\",\n" +
                "      \"max\" : \"true\",\n" +
                "      \"nullCount\" : 1,\n" +
                "      \"avgLen\" : 1,\n" +
                "      \"maxLen\" : 1\n" +
                "    },\n" +
                "    \"create_time\" : {\n" +
                "      \"colId\" : 5,\n" +
                "      \"distinctCount\" : 2,\n" +
                "      \"min\" : \"2024-08-07 17:06:00\",\n" +
                "      \"max\" : \"2024-08-08 17:06:00\",\n" +
                "      \"nullCount\" : 1,\n" +
                "      \"avgLen\" : 8,\n" +
                "      \"maxLen\" : 8\n" +
                "    },\n" +
                "    \"user_id\" : {\n" +
                "      \"colId\" : 0,\n" +
                "      \"distinctCount\" : 3,\n" +
                "      \"min\" : \"1\",\n" +
                "      \"max\" : \"3\",\n" +
                "      \"nullCount\" : 1,\n" +
                "      \"avgLen\" : 4,\n" +
                "      \"maxLen\" : 4\n" +
                "    },\n" +
                "    \"behavior\" : {\n" +
                "      \"colId\" : 1,\n" +
                "      \"distinctCount\" : 4,\n" +
                "      \"nullCount\" : 1,\n" +
                "      \"avgLen\" : 2,\n" +
                "      \"maxLen\" : 2\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Statistics statistics = JsonSerdeUtil.fromJson(stats, Statistics.class);
        Map<String, ColStats<?>> colStatsMap = statistics.colStats();
        for (String col : colStatsMap.keySet()) {
            DataType dataType = null;
            if (col.equals("user_id")) {
                dataType = new IntType(true);
            }
            if (col.equals("behavior") || col.equals("dt")) {
                dataType = new CharType(20);
            }
            if (col.equals("acount")) {
                dataType = new DoubleType(true);
            }
            if (col.equals("create_time")) {
                dataType = new LocalZonedTimestampType(6);
            }
            if (col.equals("flag")) {
                dataType = new BooleanType(true);
            }
            colStatsMap.get(col).deserializeFieldsFromString(dataType);

        }
        new Expectations() {
            {
                nativeTable.statistics();
                result = Optional.of(statistics);
            }
        };
        PaimonTable paimonTable =
                new PaimonTable("paimon", "db1", "tbl1", Lists.newArrayList(), nativeTable);
        optimizerContext.getSessionVariable().setEnablePaimonColumnStatistics(true);

        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, Type.INT, "user_id", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(4, Type.STRING, "behavior", true);
        ColumnRefOperator columnRefOperator3 = new ColumnRefOperator(5, Type.DOUBLE, "acount", true);
        ColumnRefOperator columnRefOperator4 = new ColumnRefOperator(6, Type.BOOLEAN, "flag", true);
        ColumnRefOperator columnRefOperator5 = new ColumnRefOperator(7, Type.STRING, "dt", true);
        ColumnRefOperator columnRefOperator6 = new ColumnRefOperator(8, Type.DATETIME, "create_time", true);
        ColumnRefOperator columnRefOperator7 = new ColumnRefOperator(9, Type.ARRAY_BIGINT, "list", true);

        colRefToColumnMetaMap.put(columnRefOperator1, new Column("user_id", Type.INT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("behavior", Type.STRING));
        colRefToColumnMetaMap.put(columnRefOperator3, new Column("acount", Type.DOUBLE));
        colRefToColumnMetaMap.put(columnRefOperator4, new Column("flag", Type.BOOLEAN));
        colRefToColumnMetaMap.put(columnRefOperator5, new Column("dt", Type.STRING));
        colRefToColumnMetaMap.put(columnRefOperator6, new Column("create_time", Type.DATETIME));
        colRefToColumnMetaMap.put(columnRefOperator7, new Column("list", Type.ARRAY_BIGINT));

        com.starrocks.sql.optimizer.statistics.Statistics tableStatistics =
                metadata.getTableStatistics(optimizerContext, paimonTable, colRefToColumnMetaMap,
                        null, null, -1, null);
        assertEquals(tableStatistics.getColumnStatistics().size(), colRefToColumnMetaMap.size());
    }

    @Test
    public void testGetSnapshotIdFromVersion() throws Exception {

        //1.initialize env、db、table、load data
        java.nio.file.Path tmpDir = Files.createTempDirectory("tmp_");

        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tmpDir.toString())));

        catalog.createDatabase("test_db", true);

        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("order_id", DataTypes.STRING());
        schemaBuilder.column("order_date", DataTypes.STRING());

        Options options = new Options();
        options.set(CoreOptions.BUCKET, 2);
        options.set(CoreOptions.BUCKET_KEY, "order_id");
        schemaBuilder.options(options.toMap());

        Schema schema = schemaBuilder.build();

        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(identifier, schema, true);

        org.apache.paimon.table.Table table = catalog.getTable(identifier);
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = writeBuilder.newWrite();

        List<CommitMessage> messages;
        StreamTableCommit commit = writeBuilder.newCommit();

        //load first batch data, generate snapshot 1
        messages = write.prepareCommit(false, 0);
        GenericRow record1 = GenericRow.of(BinaryString.fromString("1001"), BinaryString.fromString("2025-09-01"));
        GenericRow record2 = GenericRow.of(BinaryString.fromString("1002"), BinaryString.fromString("2025-09-02"));
        write.write(record1);
        write.write(record2);
        commit.commit(0, messages);

        //load second batch data, generate snapshot 2
        messages = write.prepareCommit(false, 1);
        GenericRow record3 = GenericRow.of(BinaryString.fromString("1003"), BinaryString.fromString("2025-09-03"));
        GenericRow record4 = GenericRow.of(BinaryString.fromString("1004"), BinaryString.fromString("2025-09-04"));
        write.write(record3);
        write.write(record4);
        commit.commit(1, messages);

        HdfsEnvironment environment = new HdfsEnvironment();
        ConnectorProperties properties = new ConnectorProperties(ConnectorType.PAIMON);
        PaimonMetadata metadata = new PaimonMetadata("paimon", environment, catalog, properties);
        long snapshotId;
        ConstantOperator constantOperator;
        ConnectorTableVersion tableVersion;

        //2 check
        //2.1 check specify timestamp
        constantOperator = new ConstantOperator("9999-12-31", VARCHAR);
        tableVersion = new ConnectorTableVersion(PointerType.TEMPORAL, constantOperator);
        snapshotId = metadata.getSnapshotIdFromVersion(table, tableVersion);
        assertEquals(snapshotId, 2L);

        constantOperator = new ConstantOperator("9999-12-31 12:00:00", VARCHAR);
        tableVersion = new ConnectorTableVersion(PointerType.TEMPORAL, constantOperator);
        snapshotId = metadata.getSnapshotIdFromVersion(table, tableVersion);
        assertEquals(snapshotId, 2L);

        constantOperator = new ConstantOperator("9999-12-31 12:00:00.100", VARCHAR);
        tableVersion = new ConnectorTableVersion(PointerType.TEMPORAL, constantOperator);
        snapshotId = metadata.getSnapshotIdFromVersion(table, tableVersion);
        assertEquals(snapshotId, 2L);

        constantOperator = new ConstantOperator("9999-12-31aaa", VARCHAR);
        tableVersion = new ConnectorTableVersion(PointerType.TEMPORAL, constantOperator);
        ConnectorTableVersion finalTableVersion1 = tableVersion;
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Invalid temporal version [9999-12-31aaa]",
                () -> metadata.getSnapshotIdFromVersion(table, finalTableVersion1));

        //2.2 check specify snapshot id
        constantOperator = new ConstantOperator(1, INT);
        tableVersion = new ConnectorTableVersion(PointerType.VERSION, constantOperator);
        snapshotId = metadata.getSnapshotIdFromVersion(table, tableVersion);
        assertEquals(snapshotId, 1L);

        constantOperator = new ConstantOperator("1234", INT);
        tableVersion = new ConnectorTableVersion(PointerType.VERSION, constantOperator);
        ConnectorTableVersion finalTableVersion2 = tableVersion;
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "test_db.test_table does not include snapshot: 1234",
                () -> metadata.getSnapshotIdFromVersion(table, finalTableVersion2));

        //2.3 check specify tag
        ((FileStoreTable) table).createTag("t_1", 1L); //create tag base snapshot 1
        constantOperator = new ConstantOperator("tag:t_1", VARCHAR);
        tableVersion = new ConnectorTableVersion(PointerType.VERSION, constantOperator);
        snapshotId = metadata.getSnapshotIdFromVersion(table, tableVersion);
        assertEquals(snapshotId, 1L);

        ((FileStoreTable) table).createTag("t_2"); //create tag base latest snapshot
        constantOperator = new ConstantOperator("tag:t_2", VARCHAR);
        tableVersion = new ConnectorTableVersion(PointerType.VERSION, constantOperator);
        snapshotId = metadata.getSnapshotIdFromVersion(table, tableVersion);
        assertEquals(snapshotId, 2L);

        constantOperator = new ConstantOperator("tag:t_not_exist", VARCHAR);
        tableVersion = new ConnectorTableVersion(PointerType.VERSION, constantOperator);
        ConnectorTableVersion finalTableVersion3 = tableVersion;
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "test_db.test_table does not include tag: t_not_exist",
                () -> metadata.getSnapshotIdFromVersion(table, finalTableVersion3));

        //2.4 check sepcify branch
        //2.4.1 check branch base exist tag
        ((FileStoreTable) table).createBranch("b_1", "t_1"); //create branch base tag t_1
        constantOperator = new ConstantOperator("branch:b_1", VARCHAR);
        tableVersion = new ConnectorTableVersion(PointerType.VERSION, constantOperator);
        snapshotId = metadata.getSnapshotIdFromVersion(table, tableVersion);
        assertEquals(snapshotId, 1L);

        //2.4.2 check empty branch
        ((FileStoreTable) table).createBranch("b_2"); //create empty  branch
        FileStoreTable tableBranch = ((FileStoreTable) table).switchToBranch("b_2");
        //load first batch data to b_2 branch, generate snapshot 1
        GenericRow record5 = GenericRow.of(BinaryString.fromString("3001"), BinaryString.fromString("2025-09-05"));
        GenericRow record6 = GenericRow.of(BinaryString.fromString("3002"), BinaryString.fromString("2025-09-06"));
        write = tableBranch.newWrite("root");
        commit = tableBranch.newCommit("root");
        write.write(record5);
        write.write(record6);
        messages = write.prepareCommit(false, 0);
        commit.commit(0, messages);
        //load second batch data to b_2 branch, generate snapshot 2
        GenericRow record7 = GenericRow.of(BinaryString.fromString("4001"), BinaryString.fromString("2025-09-07"));
        GenericRow record8 = GenericRow.of(BinaryString.fromString("4002"), BinaryString.fromString("2025-09-08"));
        write = tableBranch.newWrite("root");
        commit = tableBranch.newCommit("root");
        write.write(record7);
        write.write(record8);
        messages = write.prepareCommit(false, 1);
        commit.commit(1, messages);

        constantOperator = new ConstantOperator("branch:b_2", VARCHAR);
        tableVersion = new ConnectorTableVersion(PointerType.VERSION, constantOperator);
        snapshotId = metadata.getSnapshotIdFromVersion(table, tableVersion);
        assertEquals(snapshotId, 2L);

        //2.4.3 check not exist branch
        constantOperator = new ConstantOperator("branch:b_not_exist", VARCHAR);
        tableVersion = new ConnectorTableVersion(PointerType.VERSION, constantOperator);
        ConnectorTableVersion finalTableVersion4 = tableVersion;
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "test_db.test_table does not include branch: b_not_exist",
                () -> metadata.getSnapshotIdFromVersion(table, finalTableVersion4));

        //2.5 check other error format specify
        constantOperator = new ConstantOperator("branchabcd:b_1", VARCHAR);
        tableVersion = new ConnectorTableVersion(PointerType.VERSION, constantOperator);
        ConnectorTableVersion finalTableVersion5 = tableVersion;
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Please input corrent format like branch:branch_name or tag:tag_name",
                () -> metadata.getSnapshotIdFromVersion(table, finalTableVersion5));

        //3 clean env
        catalog.dropTable(identifier, true);
        catalog.dropDatabase("test_db", true, true);
        Files.delete(tmpDir);
    }

    @Test
    public void testGetTableVersionRange() throws Exception {
        //1.initialize env、db、table、load data
        java.nio.file.Path tmpDir = Files.createTempDirectory("tmp_");

        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tmpDir.toString())));

        catalog.createDatabase("test_db", true);

        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("order_id", DataTypes.STRING());
        schemaBuilder.column("order_date", DataTypes.STRING());

        Options options = new Options();
        options.set(CoreOptions.BUCKET, 2);
        options.set(CoreOptions.BUCKET_KEY, "order_id");
        schemaBuilder.options(options.toMap());

        Schema schema = schemaBuilder.build();

        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(identifier, schema, true);

        com.starrocks.catalog.Table table = metadata.getTable(connectContext, "test_db", "test_table");
        PaimonTable paimonTable = (PaimonTable) table;
        org.apache.paimon.table.Table nativeTable = catalog.getTable(identifier);
        StreamWriteBuilder writeBuilder = nativeTable.newStreamWriteBuilder();
        StreamTableWrite write = writeBuilder.newWrite();

        List<CommitMessage> messages;
        StreamTableCommit commit = writeBuilder.newCommit();

        //load first batch data, genarate snapshot 1
        messages = write.prepareCommit(false, 0);
        GenericRow record1 = GenericRow.of(BinaryString.fromString("1001"), BinaryString.fromString("2025-09-01"));
        GenericRow record2 = GenericRow.of(BinaryString.fromString("1002"), BinaryString.fromString("2025-09-02"));
        write.write(record1);
        write.write(record2);
        commit.commit(0, messages);

        //load second batch data, generate snapshot 2
        messages = write.prepareCommit(false, 1);
        GenericRow record3 = GenericRow.of(BinaryString.fromString("1003"), BinaryString.fromString("2025-09-03"));
        GenericRow record4 = GenericRow.of(BinaryString.fromString("1004"), BinaryString.fromString("2025-09-04"));
        write.write(record3);
        write.write(record4);
        commit.commit(1, messages);
        paimonTable.setPaimonNativeTable(nativeTable);

        HdfsEnvironment environment = new HdfsEnvironment();
        ConnectorProperties properties = new ConnectorProperties(ConnectorType.PAIMON);
        PaimonMetadata metadata = new PaimonMetadata("paimon", environment, catalog, properties);
        long snapshotId;
        ConstantOperator constantOperator;
        ConnectorTableVersion tableVersion;

        //2 check
        //2.1 check startVersion and endVersion are empty
        Optional<ConnectorTableVersion> startVersion = Optional.empty();
        Optional<ConnectorTableVersion> endVersion = Optional.empty();
        TvrVersionRange tvrVersionRange = metadata.getTableVersionRange("test_db", table, startVersion, endVersion);
        assertEquals(tvrVersionRange.toString(), "Snapshot@(2)");

        //2.2 check startVersion is empty, endVersion is not empty
        startVersion = Optional.empty();
        constantOperator = new ConstantOperator(1, INT);
        endVersion = Optional.of(new ConnectorTableVersion(PointerType.VERSION, constantOperator));
        tvrVersionRange = metadata.getTableVersionRange("test_db", table, startVersion, endVersion);
        assertEquals(tvrVersionRange.toString(), "Delta@[MIN,1]");

        //3 clean env 
        catalog.dropTable(identifier, true);
        catalog.dropDatabase("test_db", true, true);
        Files.delete(tmpDir);
    }
}
