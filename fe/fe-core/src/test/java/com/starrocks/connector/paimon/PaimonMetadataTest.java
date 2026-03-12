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
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PaimonView;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
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
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.ColWithComment;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.KeyPartitionRef;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.TruncateTablePartitionStmt;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.StarRocksPlannerException;
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
import com.starrocks.system.Frontend;
import com.starrocks.type.ArrayType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CachingCatalog;
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
import org.apache.paimon.options.CatalogOptions;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.catalog.Table.TableType.PAIMON_VIEW;
import static com.starrocks.type.IntegerType.INT;
import static com.starrocks.type.VarcharType.VARCHAR;
import static org.apache.paimon.io.DataFileMeta.DUMMY_LEVEL;
import static org.apache.paimon.io.DataFileMeta.EMPTY_MAX_KEY;
import static org.apache.paimon.io.DataFileMeta.EMPTY_MIN_KEY;
import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        meta1.add(DataFileMeta.create("file1", 100L, 200L, EMPTY_MIN_KEY, EMPTY_MAX_KEY,
                EMPTY_STATS, EMPTY_STATS, 100L, 200L, 1L, DUMMY_LEVEL, 0L, null, null, null, null, null));
        meta1.add(DataFileMeta.create("file2", 100L, 300L, EMPTY_MIN_KEY, EMPTY_MAX_KEY,
                EMPTY_STATS, EMPTY_STATS, 100L, 300L, 1L, DUMMY_LEVEL, 0L, null, null, null, null, null));

        List<DataFileMeta> meta2 = new ArrayList<>();
        meta2.add(DataFileMeta.create("file3", 100L, 400L, EMPTY_MIN_KEY, EMPTY_MAX_KEY,
                EMPTY_STATS, EMPTY_STATS, 100L, 400L, 1L, DUMMY_LEVEL, 0L, null, null, null, null, null));
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
                paimonNativeTable.uuid();
                result = "fake_uuid";
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
        assertEquals(IntegerType.INT, paimonTable.getBaseSchema().get(0).getType());
        org.junit.jupiter.api.Assertions.assertTrue(paimonTable.getBaseSchema().get(0).isAllowNull());
        assertEquals(FloatType.DOUBLE, paimonTable.getBaseSchema().get(1).getType());
        org.junit.jupiter.api.Assertions.assertTrue(paimonTable.getBaseSchema().get(1).isAllowNull());
        assertEquals("paimon_catalog", paimonTable.getCatalogName());
        assertEquals("paimon_catalog.db1.tbl1.fake_uuid", paimonTable.getUUID());
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
                paimonNativeCatalog.getView(identifier);
                result = new Catalog.ViewNotExistException(identifier);
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
                paimonSystemTable.latestSnapshot();
                result = new Exception("Readonly Table tbl1$manifests does not support currentSnapshot.");
                // according to PaimonMetadata::getTableVersionRange，the snapshot id of system table is -1L,
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
                100L, 1L, 1L, 1741327322000L, true);
        org.apache.paimon.partition.Partition partition2 = new Partition(Map.of("year", "2020", "month", "2"),
                100L, 1L, 1L, 1741327322000L, true);

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
        // according to PaimonMetadata::getTableVersionRange, empty table's snapshot id is -1L.
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

        ColumnRefOperator createDateColumn = new ColumnRefOperator(1, StringType.STRING, "create_date", false);
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

        ColumnRefOperator userColumn = new ColumnRefOperator(2, StringType.STRING, "user", false);
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
                new Function(new FunctionName(FunctionSet.COALESCE),
                        Lists.newArrayList(VarcharType.VARCHAR, VarcharType.VARCHAR),
                        VarcharType.VARCHAR, false),
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

        CallOperator createDateCoalesce = new CallOperator("coalesce", VarcharType.VARCHAR,
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

        ColumnRefOperator colRef1 = new ColumnRefOperator(1, IntegerType.INT, "f2", true);
        Column col1 = new Column("f2", IntegerType.INT, true);
        ColumnRefOperator colRef2 = new ColumnRefOperator(2, StringType.STRING, "dt", true);
        Column col2 = new Column("dt", StringType.STRING, true);

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
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, IntegerType.INT, "user_id", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(4, StringType.STRING, "behavior", true);
        ColumnRefOperator columnRefOperator3 = new ColumnRefOperator(5, FloatType.DOUBLE, "acount", true);
        ColumnRefOperator columnRefOperator4 = new ColumnRefOperator(6, com.starrocks.type.BooleanType.BOOLEAN, "flag", true);
        ColumnRefOperator columnRefOperator5 = new ColumnRefOperator(7, StringType.STRING, "dt", true);
        ColumnRefOperator columnRefOperator6 = new ColumnRefOperator(8, DateType.DATETIME, "create_time", true);
        ColumnRefOperator columnRefOperator7 = new ColumnRefOperator(9, ArrayType.ARRAY_BIGINT, "list", true);

        colRefToColumnMetaMap.put(columnRefOperator1, new Column("user_id", IntegerType.INT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("behavior", StringType.STRING));
        colRefToColumnMetaMap.put(columnRefOperator3, new Column("acount", FloatType.DOUBLE));
        colRefToColumnMetaMap.put(columnRefOperator4, new Column("flag", com.starrocks.type.BooleanType.BOOLEAN));
        colRefToColumnMetaMap.put(columnRefOperator5, new Column("dt", StringType.STRING));
        colRefToColumnMetaMap.put(columnRefOperator6, new Column("create_time", DateType.DATETIME));
        colRefToColumnMetaMap.put(columnRefOperator7, new Column("list", ArrayType.ARRAY_BIGINT));

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

        //2.4 check specify branch
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
                "Please input correct format like branch:branch_name or tag:tag_name",
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

    @Test
    public void testTruncateTable() throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        NodeMgr nodeMgr = new NodeMgr();
        Frontend frontend = new Frontend(0, FrontendNodeType.LEADER, "", "localhost", 0);
        frontend.setAlive(true);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getNodeMgr();
                minTimes = 0;
                result = nodeMgr;
            }
        };

        new Expectations(nodeMgr) {
            {
                nodeMgr.getMySelf();
                minTimes = 0;
                result = frontend;
            }
        };

        new Expectations(frontend) {
            {
                frontend.getFeVersion();
                minTimes = 0;
                result = "test-version";
            }
        };

        // 1 create catalog and table
        java.nio.file.Path tmpDir = Files.createTempDirectory("paimon_truncate_test");
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tmpDir.toString())));

        catalog.createDatabase("test_db", true);
        Schema schema = Schema.newBuilder()
                .column("id", org.apache.paimon.types.DataTypes.STRING())
                .column("name", org.apache.paimon.types.DataTypes.STRING())
                .build();

        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(identifier, schema, true);

        // insert data
        org.apache.paimon.table.Table nativeTable = catalog.getTable(identifier);
        BatchWriteBuilder writeBuilder = nativeTable.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        BatchTableCommit commit = writeBuilder.newCommit();

        GenericRow record1 = GenericRow.of(BinaryString.fromString("1"), BinaryString.fromString("test1"));
        GenericRow record2 = GenericRow.of(BinaryString.fromString("2"), BinaryString.fromString("test2"));
        write.write(record1);
        write.write(record2);
        List<CommitMessage> messages = write.prepareCommit();
        commit.commit(messages);

        // verify data exists before truncate
        long rowCountBefore = getRowCountFromTable(nativeTable);
        assertTrue(rowCountBefore > 0);

        // create PaimonMetadata and truncate the table
        HdfsEnvironment environment = new HdfsEnvironment();
        ConnectorProperties properties = new ConnectorProperties(ConnectorType.PAIMON);
        PaimonMetadata metadata = new PaimonMetadata("paimon", environment, catalog, properties);

        TruncateTableStmt truncateTableStmt = new TruncateTableStmt(
                new TableRef(QualifiedName.of(List.of("paimon", "test_db", "test_table")), null, NodePosition.ZERO));
        metadata.truncateTable(truncateTableStmt, new ConnectContext());

        // verify data is deleted after truncate
        nativeTable = catalog.getTable(identifier);
        long rowCountAfter = getRowCountFromTable(nativeTable);
        assertEquals(0, rowCountAfter);

        // clean env
        catalog.dropTable(identifier, true);
        catalog.dropDatabase("test_db", true, true);
        Files.delete(tmpDir);
    }

    @Test
    public void testTruncatePartitionedTable() throws Exception {
        // Create catalog and partitioned table
        java.nio.file.Path tmpDir = Files.createTempDirectory("paimon_truncate_partition_test");
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tmpDir.toString())));

        catalog.createDatabase("test_db", true);
        Schema schema = Schema.newBuilder()
                .column("id", org.apache.paimon.types.DataTypes.STRING())
                .column("dt", org.apache.paimon.types.DataTypes.STRING())
                .column("name", org.apache.paimon.types.DataTypes.STRING())
                .partitionKeys("dt")
                .build();

        Identifier identifier = Identifier.create("test_db", "test_partitioned_table");
        catalog.createTable(identifier, schema, true);

        // Insert data into different partitions
        org.apache.paimon.table.Table nativeTable = catalog.getTable(identifier);
        BatchWriteBuilder writeBuilder = nativeTable.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        BatchTableCommit commit = writeBuilder.newCommit();

        // Insert data for partition dt='2023-11-20'
        GenericRow record1 = GenericRow.of(
                BinaryString.fromString("1"),
                BinaryString.fromString("2023-11-20"),
                BinaryString.fromString("test1"));
        // Insert data for partition dt='2023-11-21'
        GenericRow record2 = GenericRow.of(
                BinaryString.fromString("2"),
                BinaryString.fromString("2023-11-21"),
                BinaryString.fromString("test2"));
        // Insert data for partition dt='2023-11-20' again
        GenericRow record3 = GenericRow.of(
                BinaryString.fromString("3"),
                BinaryString.fromString("2023-11-20"),
                BinaryString.fromString("test3"));

        write.write(record1);
        write.write(record2);
        write.write(record3);
        List<CommitMessage> messages = write.prepareCommit();
        commit.commit(messages);

        // Verify data exists before truncate
        long rowCountBefore = getRowCountFromTable(nativeTable);
        assertEquals(3, rowCountBefore);

        // Create PaimonMetadata and truncate partition dt='2023-11-20'
        HdfsEnvironment environment = new HdfsEnvironment();
        ConnectorProperties properties = new ConnectorProperties(ConnectorType.PAIMON);
        PaimonMetadata metadata = new PaimonMetadata("paimon", environment, catalog, properties);

        // TRUNCATE TABLE paimon.test_db.test_partitioned_table PARTITION (dt='2023-11-20')
        KeyPartitionRef keyPartitionRef = new KeyPartitionRef(
                Lists.newArrayList("dt"),
                Lists.newArrayList(new StringLiteral("2023-11-20")),
                NodePosition.ZERO);
        TableRef tableRef = new TableRef(
                QualifiedName.of(List.of("paimon", "test_db", "test_partitioned_table")),
                null,
                NodePosition.ZERO);
        TruncateTablePartitionStmt truncateTableStmt = new TruncateTablePartitionStmt(tableRef, keyPartitionRef);
        metadata.truncateTable(truncateTableStmt, new ConnectContext());

        // Verify only partition dt='2023-11-20' is truncated, dt='2023-11-21' remains
        nativeTable = catalog.getTable(identifier);
        long rowCountAfter = getRowCountFromTable(nativeTable);
        assertEquals(1, rowCountAfter); // Only record2 (dt='2023-11-21') remains

        // Clean env
        catalog.dropTable(identifier, true);
        catalog.dropDatabase("test_db", true, true);
        Files.delete(tmpDir);
    }

    @Test
    public void testTruncatePartitionedTableWithMultiplePartitionColumns() throws Exception {
        // Create catalog and table with multiple partition columns
        java.nio.file.Path tmpDir = Files.createTempDirectory("paimon_truncate_multi_partition_test");
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tmpDir.toString())));

        catalog.createDatabase("test_db", true);
        Schema schema = Schema.newBuilder()
                .column("id", org.apache.paimon.types.DataTypes.STRING())
                .column("dt", org.apache.paimon.types.DataTypes.STRING())
                .column("region", org.apache.paimon.types.DataTypes.STRING())
                .column("name", org.apache.paimon.types.DataTypes.STRING())
                .partitionKeys("dt", "region")
                .build();

        Identifier identifier = Identifier.create("test_db", "test_multi_partition_table");
        catalog.createTable(identifier, schema, true);

        // Insert data
        org.apache.paimon.table.Table nativeTable = catalog.getTable(identifier);
        BatchWriteBuilder writeBuilder = nativeTable.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        BatchTableCommit commit = writeBuilder.newCommit();

        // Insert data for (dt='2023-11-20', region='us')
        GenericRow record1 = GenericRow.of(
                BinaryString.fromString("1"),
                BinaryString.fromString("2023-11-20"),
                BinaryString.fromString("us"),
                BinaryString.fromString("test1"));
        // Insert data for (dt='2023-11-20', region='cn')
        GenericRow record2 = GenericRow.of(
                BinaryString.fromString("2"),
                BinaryString.fromString("2023-11-20"),
                BinaryString.fromString("cn"),
                BinaryString.fromString("test2"));
        // Insert data for (dt='2023-11-21', region='us')
        GenericRow record3 = GenericRow.of(
                BinaryString.fromString("3"),
                BinaryString.fromString("2023-11-21"),
                BinaryString.fromString("us"),
                BinaryString.fromString("test3"));

        write.write(record1);
        write.write(record2);
        write.write(record3);
        List<CommitMessage> messages = write.prepareCommit();
        commit.commit(messages);

        // Verify data exists
        long rowCountBefore = getRowCountFromTable(nativeTable);
        assertEquals(3, rowCountBefore);

        // Create PaimonMetadata and truncate partition (dt='2023-11-20', region='us')
        HdfsEnvironment environment = new HdfsEnvironment();
        ConnectorProperties properties = new ConnectorProperties(ConnectorType.PAIMON);
        PaimonMetadata metadata = new PaimonMetadata("paimon", environment, catalog, properties);

        // TRUNCATE TABLE paimon.test_db.test_multi_partition_table PARTITION (dt='2023-11-20', region='us')
        KeyPartitionRef keyPartitionRef = new KeyPartitionRef(
                Lists.newArrayList("dt", "region"),
                Lists.newArrayList(
                        new StringLiteral("2023-11-20"),
                        new StringLiteral("us")),
                NodePosition.ZERO);
        TableRef tableRef = new TableRef(
                QualifiedName.of(List.of("paimon", "test_db", "test_multi_partition_table")),
                null,
                NodePosition.ZERO);
        TruncateTablePartitionStmt truncateTableStmt = new TruncateTablePartitionStmt(tableRef, keyPartitionRef);
        metadata.truncateTable(truncateTableStmt, new ConnectContext());

        // Verify only (dt='2023-11-20', region='us') is truncated
        nativeTable = catalog.getTable(identifier);
        long rowCountAfter = getRowCountFromTable(nativeTable);
        assertEquals(2, rowCountAfter); // record2 and record3 remain

        // Clean env
        catalog.dropTable(identifier, true);
        catalog.dropDatabase("test_db", true, true);
        Files.delete(tmpDir);
    }

    @Test
    public void testTruncatePartitionedTableWithPartialPartitionSpec() throws Exception {
        // Test truncating with only one partition column when table has multiple partition columns
        java.nio.file.Path tmpDir = Files.createTempDirectory("paimon_truncate_partial_partition_test");
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tmpDir.toString())));

        catalog.createDatabase("test_db", true);
        Schema schema = Schema.newBuilder()
                .column("id", org.apache.paimon.types.DataTypes.STRING())
                .column("dt", org.apache.paimon.types.DataTypes.STRING())
                .column("region", org.apache.paimon.types.DataTypes.STRING())
                .column("name", org.apache.paimon.types.DataTypes.STRING())
                .partitionKeys("dt", "region")
                .build();

        Identifier identifier = Identifier.create("test_db", "test_partial_partition_table");
        catalog.createTable(identifier, schema, true);

        // Insert data
        org.apache.paimon.table.Table nativeTable = catalog.getTable(identifier);
        BatchWriteBuilder writeBuilder = nativeTable.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        BatchTableCommit commit = writeBuilder.newCommit();

        // Insert data for (dt='2023-11-20', region='us')
        GenericRow record1 = GenericRow.of(
                BinaryString.fromString("1"),
                BinaryString.fromString("2023-11-20"),
                BinaryString.fromString("us"),
                BinaryString.fromString("test1"));
        // Insert data for (dt='2023-11-20', region='cn')
        GenericRow record2 = GenericRow.of(
                BinaryString.fromString("2"),
                BinaryString.fromString("2023-11-20"),
                BinaryString.fromString("cn"),
                BinaryString.fromString("test2"));
        // Insert data for (dt='2023-11-21', region='us')
        GenericRow record3 = GenericRow.of(
                BinaryString.fromString("3"),
                BinaryString.fromString("2023-11-21"),
                BinaryString.fromString("us"),
                BinaryString.fromString("test3"));

        write.write(record1);
        write.write(record2);
        write.write(record3);
        List<CommitMessage> messages = write.prepareCommit();
        commit.commit(messages);

        // Verify data exists
        long rowCountBefore = getRowCountFromTable(nativeTable);
        assertEquals(3, rowCountBefore);

        // Create PaimonMetadata and truncate with partial partition spec (only dt='2023-11-20')
        // This should truncate all partitions where dt='2023-11-20' (both region='us' and region='cn')
        HdfsEnvironment environment = new HdfsEnvironment();
        ConnectorProperties properties = new ConnectorProperties(ConnectorType.PAIMON);
        PaimonMetadata metadata = new PaimonMetadata("paimon", environment, catalog, properties);

        // TRUNCATE TABLE paimon.test_db.test_partial_partition_table PARTITION (dt='2023-11-20')
        KeyPartitionRef keyPartitionRef = new KeyPartitionRef(
                Lists.newArrayList("dt"),
                Lists.newArrayList(new StringLiteral("2023-11-20")),
                NodePosition.ZERO);
        TableRef tableRef = new TableRef(
                QualifiedName.of(List.of("paimon", "test_db", "test_partial_partition_table")),
                null,
                NodePosition.ZERO);
        TruncateTablePartitionStmt truncateTableStmt = new TruncateTablePartitionStmt(tableRef, keyPartitionRef);
        metadata.truncateTable(truncateTableStmt, new ConnectContext());

        // Verify all partitions with dt='2023-11-20' are truncated
        nativeTable = catalog.getTable(identifier);
        long rowCountAfter = getRowCountFromTable(nativeTable);
        assertEquals(1, rowCountAfter); // Only record3 (dt='2023-11-21', region='us') remains

        // Clean env
        catalog.dropTable(identifier, true);
        catalog.dropDatabase("test_db", true, true);
        Files.delete(tmpDir);
    }

    @Test
    public void testTruncateTableNotExist() {
        // Test truncate on non-existent table
        java.nio.file.Path tmpDir = null;
        try {
            tmpDir = Files.createTempDirectory("paimon_truncate_not_exist_test");
            Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tmpDir.toString())));

            catalog.createDatabase("test_db", true);

            HdfsEnvironment environment = new HdfsEnvironment();
            ConnectorProperties properties = new ConnectorProperties(ConnectorType.PAIMON);
            PaimonMetadata metadata = new PaimonMetadata("paimon", environment, catalog, properties);

            TableRef tableRef = new TableRef(
                    QualifiedName.of(List.of("paimon", "test_db", "non_existent_table")),
                    null,
                    NodePosition.ZERO);
            TruncateTableStmt truncateTableStmt = new TruncateTableStmt(tableRef);

            ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                    "Failed to truncate paimon table: test_db.non_existent_table, table does not exist",
                    () -> metadata.truncateTable(truncateTableStmt, new ConnectContext()));

            catalog.dropDatabase("test_db", true, true);
            Files.delete(tmpDir);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testTruncateTableWithException(@Mocked org.apache.paimon.table.Table mockTable,
                                               @Mocked BatchWriteBuilder mockWriteBuilder,
                                               @Mocked BatchTableCommit mockCommit) throws Exception {
        // Test exception handling in truncateTable
        java.nio.file.Path tmpDir = Files.createTempDirectory("paimon_truncate_exception_test");
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tmpDir.toString())));

        catalog.createDatabase("test_db", true);
        Schema schema = Schema.newBuilder()
                .column("id", org.apache.paimon.types.DataTypes.STRING())
                .column("name", org.apache.paimon.types.DataTypes.STRING())
                .build();

        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(identifier, schema, true);

        HdfsEnvironment environment = new HdfsEnvironment();
        ConnectorProperties properties = new ConnectorProperties(ConnectorType.PAIMON);
        PaimonMetadata metadata = new PaimonMetadata("paimon", environment, catalog, properties);

        // Mock Catalog.getTable() to return mockTable
        new Expectations(catalog) {
            {
                catalog.getTable(identifier);
                result = mockTable;
            }
        };

        // Mock Table.newBatchWriteBuilder() to return mockWriteBuilder
        // Note: For interfaces, use Expectations() without parameter
        new Expectations() {
            {
                mockTable.newBatchWriteBuilder();
                result = mockWriteBuilder;
            }
        };

        // Mock BatchWriteBuilder.newCommit() to return mockCommit
        // Note: For interfaces, use Expectations() without parameter
        new Expectations() {
            {
                mockWriteBuilder.newCommit();
                result = mockCommit;
            }
        };

        // Mock BatchTableCommit.truncateTable() to throw exception
        // Note: For interfaces, use Expectations() without parameter
        new Expectations() {
            {
                mockCommit.truncateTable();
                result = new RuntimeException("Mock exception for testing");
            }
        };

        TruncateTableStmt truncateTableStmt = new TruncateTableStmt(
                new TableRef(QualifiedName.of(List.of("paimon", "test_db", "test_table")), null, NodePosition.ZERO));

        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed to truncate paimon table: test_db.test_table, error: Mock exception for testing",
                () -> metadata.truncateTable(truncateTableStmt, new ConnectContext()));

        catalog.dropTable(identifier, true);
        catalog.dropDatabase("test_db", true, true);
        Files.delete(tmpDir);
    }

    @Test
    public void testTruncatePartitionWithNonLiteralExpr() throws Exception {
        // Test buildPartitionMap with non-LiteralExpr
        java.nio.file.Path tmpDir = Files.createTempDirectory("paimon_truncate_non_literal_test");
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tmpDir.toString())));

        catalog.createDatabase("test_db", true);
        Schema schema = Schema.newBuilder()
                .column("id", org.apache.paimon.types.DataTypes.STRING())
                .column("dt", org.apache.paimon.types.DataTypes.STRING())
                .partitionKeys("dt")
                .build();

        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(identifier, schema, true);

        HdfsEnvironment environment = new HdfsEnvironment();
        ConnectorProperties properties = new ConnectorProperties(ConnectorType.PAIMON);
        PaimonMetadata metadata = new PaimonMetadata("paimon", environment, catalog, properties);

        // Create KeyPartitionRef with non-LiteralExpr (use SlotRef as an example)
        SlotRef slotRef = new SlotRef(null, "dt");
        KeyPartitionRef keyPartitionRef = new KeyPartitionRef(
                Lists.newArrayList("dt"),
                Lists.newArrayList(slotRef),
                NodePosition.ZERO);
        TableRef tableRef = new TableRef(
                QualifiedName.of(List.of("paimon", "test_db", "test_table")),
                null,
                NodePosition.ZERO);
        TruncateTablePartitionStmt truncateTableStmt = new TruncateTablePartitionStmt(tableRef, keyPartitionRef);

        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Partition value must be a literal expression, got: SlotRef",
                () -> metadata.truncateTable(truncateTableStmt, new ConnectContext()));

        catalog.dropTable(identifier, true);
        catalog.dropDatabase("test_db", true, true);
        Files.delete(tmpDir);
    }

    @Test
    public void testTruncatePartitionOnUnpartitionedTable() throws Exception {
        // Test truncate partition on unpartitioned table should throw error
        java.nio.file.Path tmpDir = Files.createTempDirectory("paimon_truncate_unpartitioned_test");
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tmpDir.toString())));

        catalog.createDatabase("test_db", true);
        Schema schema = Schema.newBuilder()
                .column("id", org.apache.paimon.types.DataTypes.STRING())
                .column("name", org.apache.paimon.types.DataTypes.STRING())
                .build();

        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(identifier, schema, true);

        HdfsEnvironment environment = new HdfsEnvironment();
        ConnectorProperties properties = new ConnectorProperties(ConnectorType.PAIMON);
        PaimonMetadata metadata = new PaimonMetadata("paimon", environment, catalog, properties);

        // Try to truncate partition on unpartitioned table
        KeyPartitionRef keyPartitionRef = new KeyPartitionRef(
                Lists.newArrayList("dt"),
                Lists.newArrayList(new StringLiteral("2023-11-20")),
                NodePosition.ZERO);
        TableRef tableRef = new TableRef(
                QualifiedName.of(List.of("paimon", "test_db", "test_table")),
                null,
                NodePosition.ZERO);
        TruncateTablePartitionStmt truncateTableStmt = new TruncateTablePartitionStmt(tableRef, keyPartitionRef);

        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Table [test_db.test_table] is not partitioned, cannot truncate partitions",
                () -> metadata.truncateTable(truncateTableStmt, new ConnectContext()));

        catalog.dropTable(identifier, true);
        catalog.dropDatabase("test_db", true, true);
        Files.delete(tmpDir);
    }

    @Test
    public void testTruncatePartitionWithInvalidColumnName() throws Exception {
        // Test truncate partition with invalid partition column name
        java.nio.file.Path tmpDir = Files.createTempDirectory("paimon_truncate_invalid_column_test");
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tmpDir.toString())));

        catalog.createDatabase("test_db", true);
        Schema schema = Schema.newBuilder()
                .column("id", org.apache.paimon.types.DataTypes.STRING())
                .column("dt", org.apache.paimon.types.DataTypes.STRING())
                .column("name", org.apache.paimon.types.DataTypes.STRING())
                .partitionKeys("dt")
                .build();

        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(identifier, schema, true);

        HdfsEnvironment environment = new HdfsEnvironment();
        ConnectorProperties properties = new ConnectorProperties(ConnectorType.PAIMON);
        PaimonMetadata metadata = new PaimonMetadata("paimon", environment, catalog, properties);

        // Try to truncate partition with invalid column name (invalid_col instead of dt)
        KeyPartitionRef keyPartitionRef = new KeyPartitionRef(
                Lists.newArrayList("invalid_col"),
                Lists.newArrayList(new StringLiteral("2023-11-20")),
                NodePosition.ZERO);
        TableRef tableRef = new TableRef(
                QualifiedName.of(List.of("paimon", "test_db", "test_table")),
                null,
                NodePosition.ZERO);
        TruncateTablePartitionStmt truncateTableStmt = new TruncateTablePartitionStmt(tableRef, keyPartitionRef);

        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Partition names in partition spec do not match table partition columns for table [test_db.test_table]",
                () -> metadata.truncateTable(truncateTableStmt, new ConnectContext()));

        catalog.dropTable(identifier, true);
        catalog.dropDatabase("test_db", true, true);
        Files.delete(tmpDir);
    }

    @Test
    public void testPaimonTruncatePartitionsDirectBehavior() throws Exception {
        // Test Paimon's truncatePartitions API behavior directly when partition doesn't exist
        // This test bypasses StarRocks validation to see what Paimon API does
        java.nio.file.Path tmpDir = Files.createTempDirectory("paimon_direct_truncate_test");
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tmpDir.toString())));

        catalog.createDatabase("test_db", true);
        Schema schema = Schema.newBuilder()
                .column("id", org.apache.paimon.types.DataTypes.STRING())
                .column("dt", org.apache.paimon.types.DataTypes.STRING())
                .column("name", org.apache.paimon.types.DataTypes.STRING())
                .partitionKeys("dt")
                .build();

        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(identifier, schema, true);

        // Insert data only for partition dt='2023-11-20'
        org.apache.paimon.table.Table nativeTable = catalog.getTable(identifier);
        BatchWriteBuilder writeBuilder = nativeTable.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        BatchTableCommit commit = writeBuilder.newCommit();

        GenericRow record1 = GenericRow.of(
                BinaryString.fromString("1"),
                BinaryString.fromString("2023-11-20"),
                BinaryString.fromString("test1"));
        write.write(record1);
        List<CommitMessage> messages = write.prepareCommit();
        commit.commit(messages);
        commit.close();

        // Verify data exists
        long rowCountBefore = getRowCountFromTable(nativeTable);
        assertEquals(1, rowCountBefore);

        // Directly call Paimon's truncatePartitions API with non-existent partition
        // This bypasses StarRocks validation to see Paimon's actual behavior
        BatchTableCommit truncateCommit = nativeTable.newBatchWriteBuilder().newCommit();
        Map<String, String> nonExistentPartition = new HashMap<>();
        nonExistentPartition.put("dt", "2023-12-31"); // This partition doesn't exist

        try {
            // Call Paimon API directly - this will show us what Paimon does
            truncateCommit.truncatePartitions(Collections.singletonList(nonExistentPartition));
            truncateCommit.close();

            // Check if Paimon silently did nothing (no-op) or threw an exception
            // If we reach here, Paimon did not throw an exception
            long rowCountAfter = getRowCountFromTable(nativeTable);
            assertEquals(1, rowCountAfter); // Data should still exist if Paimon did nothing

            // Log the behavior for documentation
            System.out.println("Paimon truncatePartitions with non-existent partition: " +
                    "No exception thrown, operation appears to be a no-op");
        } catch (Exception e) {
            // If Paimon throws an exception, log it
            System.out.println("Paimon truncatePartitions with non-existent partition threw exception: " +
                    e.getClass().getSimpleName() + ": " + e.getMessage());
            truncateCommit.close();
            throw e; // Re-throw to see what exception Paimon throws
        }

        catalog.dropTable(identifier, true);
        catalog.dropDatabase("test_db", true, true);
        Files.delete(tmpDir);
    }

    private long getRowCountFromTable(org.apache.paimon.table.Table table) throws Exception {
        List<org.apache.paimon.table.source.Split> splits = table.newReadBuilder().newScan().plan().splits();
        return PaimonMetadata.getRowCount(splits);
    }

    public void testListPartitionNamesIsolationAcrossTables(@Mocked FileStoreTable mockPaimonTable1,
                                                            @Mocked FileStoreTable mockPaimonTable2)
            throws Catalog.TableNotExistException {

        Options options = new Options();
        options.set(CatalogOptions.CACHE_ENABLED, true);
        Catalog cachingCatalog = CachingCatalog.tryToCreate(paimonNativeCatalog, options);
        PaimonMetadata newMetadata = new PaimonMetadata("test_catalog", new HdfsEnvironment(), cachingCatalog,
                new ConnectorProperties(ConnectorType.PAIMON));

        Identifier tblIdentifier1 = new Identifier("db1", "tbl1");
        List<String> partitionKeys1 = Lists.newArrayList("year", "month");
        Identifier tblIdentifier2 = new Identifier("db2", "tbl2");
        List<String> partitionKeys2 = Lists.newArrayList("year", "month");

        RowType tblRowType1 = RowType.of(new DataType[] {new IntType(true), new IntType(true)}, new String[] {"year", "month"});
        RowType tblRowType2 = RowType.of(new DataType[] {new IntType(true), new IntType(true)}, new String[] {"year", "month"});

        Map<String, String> spec1 = new LinkedHashMap<>();
        spec1.put("year", "2020");
        spec1.put("month", "1");
        org.apache.paimon.partition.Partition db1PaimonPartition1 =
                new org.apache.paimon.partition.Partition(spec1, 100L, 2048L, 2L, System.currentTimeMillis(), false);

        Map<String, String> spec2 = new LinkedHashMap<>();
        spec2.put("year", "2020");
        spec2.put("month", "1");
        org.apache.paimon.partition.Partition db2PaimonPartition1 =
                new org.apache.paimon.partition.Partition(spec2, 100L, 2048L, 2L, System.currentTimeMillis(), false);

        Map<String, String> spec3 = new LinkedHashMap<>();
        spec3.put("year", "2022");
        spec3.put("month", "1");
        org.apache.paimon.partition.Partition db2PaimonPartition2 =
                new org.apache.paimon.partition.Partition(spec3, 100L, 2048L, 2L, System.currentTimeMillis(), false);

        new Expectations() {
            {
                // Table 1
                paimonNativeCatalog.getTable(tblIdentifier1);
                result = mockPaimonTable1;
                paimonNativeCatalog.listPartitions(tblIdentifier1);
                result = Lists.newArrayList(db1PaimonPartition1);
                mockPaimonTable1.partitionKeys();
                result = partitionKeys1;
                mockPaimonTable1.rowType();
                result = tblRowType1;

                // Table 2
                paimonNativeCatalog.getTable(tblIdentifier2);
                result = mockPaimonTable2;
                paimonNativeCatalog.listPartitions(tblIdentifier2);
                result = Lists.newArrayList(db2PaimonPartition1, db2PaimonPartition2);
                mockPaimonTable2.partitionKeys();
                result = partitionKeys2;
                mockPaimonTable2.rowType();
                result = tblRowType2;

            }
        };

        List<String> result1 = newMetadata.listPartitionNames("db1", "tbl1", null);
        List<String> result2 = newMetadata.listPartitionNames("db2", "tbl2", null);

        // Before fix, if the key does not contain "db" and "table", it will return 2. After fix, it will return 3.
        assertEquals(3, result1.size() + result2.size());

    }

    @Test
    public void testView(@Mocked org.apache.paimon.view.View paimonView) throws Exception {
        //test createview
        new Expectations() {
            {
                paimonNativeCatalog.getView((org.apache.paimon.catalog.Identifier) any);
                result = new Catalog.ViewNotExistException(new Identifier("test", "ViewNotExist"));
            }
        };
        CreateViewStmt stmt = new CreateViewStmt(false, false,
                new TableRef(QualifiedName.of(
                        Lists.newArrayList("paimon_catalog", "db", "test_view")), null, NodePosition.ZERO),
                Lists.newArrayList(new ColWithComment("k1", "", NodePosition.ZERO)),
                "", false, null, NodePosition.ZERO);
        stmt.setColumns(Lists.newArrayList(new Column("k1", INT)));
        metadata.createView(connectContext, stmt);

        //test getview
        new Expectations() {
            {
                paimonNativeCatalog.getView((Identifier) any);
                result = paimonView;
                paimonView.query();
                result = "select * from table";
            }
        };
        PaimonView view = (PaimonView) metadata.getView("db", "test_view");
        assertEquals(PAIMON_VIEW, view.getType());
        assertEquals("test_view", view.getName());
        assertEquals("select * from table", view.getInlineViewDef());
        assertThrows(StarRocksPlannerException.class, view::getQueryStatement);

        //test drop normal
        DropTableStmt dropStmt = new DropTableStmt(true, new TableRef(QualifiedName.of(
                Lists.newArrayList("paimon_catalog", "db", "test_view")), null, NodePosition.ZERO),
                true, true);
        metadata.dropTable(connectContext, dropStmt);

        //test drop not exist
        new Expectations() {
            {
                paimonNativeCatalog.dropView((Identifier) any, true);
                result = new Catalog.ViewNotExistException(new Identifier("test", "ViewNotExist"));
            }
        };
        org.junit.jupiter.api.Assertions.assertThrows(DdlException.class, () -> metadata.dropTable(connectContext, dropStmt));
    }
}
