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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudType;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.transformation.ExternalScanPartitionPruneRule;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.system.PartitionsTable;
import org.apache.paimon.table.system.SchemasTable;
import org.apache.paimon.table.system.SnapshotsTable;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.SerializationUtils;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.io.DataFileMeta.DUMMY_LEVEL;
import static org.apache.paimon.io.DataFileMeta.EMPTY_MAX_KEY;
import static org.apache.paimon.io.DataFileMeta.EMPTY_MIN_KEY;
import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.junit.Assert.assertEquals;

public class PaimonMetadataTest {
    @Mocked
    Catalog paimonNativeCatalog;
    private PaimonMetadata metadata;
    private final List<DataSplit> splits = new ArrayList<>();

    @Before
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
    }

    @Test
    public void testRowCount() {
        long rowCount = metadata.getRowCount(splits);
        Assert.assertEquals(900, rowCount);
    }

    @Test
    public void testGetTable(@Mocked FileStoreTable paimonNativeTable) throws Catalog.TableNotExistException {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(1, "col2", new IntType(true)));
        fields.add(new DataField(2, "col3", new DoubleType(false)));
        new MockUp<PaimonMetadata>() {
            @Mock
            public long getTableCreateTime(String dbName, String tblName) {
                return 0L;
            }
        };
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
            }
        };
        com.starrocks.catalog.Table table = metadata.getTable("db1", "tbl1");
        PaimonTable paimonTable = (PaimonTable) table;
        Assert.assertTrue(metadata.tableExists("db1", "tbl1"));
        Assert.assertEquals("db1", paimonTable.getCatalogDBName());
        Assert.assertEquals("tbl1", paimonTable.getCatalogTableName());
        Assert.assertEquals(Lists.newArrayList("col1"), paimonTable.getPartitionColumnNames());
        Assert.assertEquals("hdfs://127.0.0.1:10000/paimon", paimonTable.getTableLocation());
        Assert.assertEquals(ScalarType.INT, paimonTable.getBaseSchema().get(0).getType());
        Assert.assertTrue(paimonTable.getBaseSchema().get(0).isAllowNull());
        Assert.assertEquals(ScalarType.DOUBLE, paimonTable.getBaseSchema().get(1).getType());
        Assert.assertTrue(paimonTable.getBaseSchema().get(1).isAllowNull());
        Assert.assertEquals("paimon_catalog", paimonTable.getCatalogName());
        Assert.assertEquals("paimon_catalog.db1.tbl1.0", paimonTable.getUUID());
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
        Assert.assertNull(metadata.getDb("nonexistentDb"));
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
        Assert.assertFalse(metadata.tableExists("nonexistentDb", "nonexistentTbl"));
        Assert.assertNull(metadata.getTable("nonexistentDb", "nonexistentTbl"));
    }

    @Test
    public void testListPartitionNames(@Mocked FileStoreTable mockPaimonTable,
                                       @Mocked PartitionsTable mockPartitionTable,
                                       @Mocked RecordReader<InternalRow> mockRecordReader)
            throws Catalog.TableNotExistException, IOException {

        RowType tblRowType = RowType.of(
                new DataType[] {
                        new IntType(true),
                        new IntType(true)
                },
                new String[] {"year", "month"});

        List<String> partitionNames = Lists.newArrayList("year", "month");

        Identifier tblIdentifier = new Identifier("db1", "tbl1");
        Identifier partitionTblIdentifier = new Identifier("db1", "tbl1$partitions");

        RowType partitionRowType = new RowType(
                Arrays.asList(
                        new DataField(0, "partition", SerializationUtils.newStringType(true)),
                        new DataField(1, "record_count", new BigIntType(false)),
                        new DataField(2, "file_size_in_bytes", new BigIntType(false)),
                        new DataField(3, "file_count", new BigIntType(false)),
                        new DataField(4, "last_update_time", DataTypes.TIMESTAMP_MILLIS())
                ));

        GenericRow row1 = new GenericRow(2);
        row1.setField(0, BinaryString.fromString("[2020, 1]"));
        row1.setField(1, Timestamp.fromLocalDateTime(LocalDateTime.of(2023, 1, 1, 0, 0, 0, 0)));

        GenericRow row2 = new GenericRow(2);
        row2.setField(0, BinaryString.fromString("[2020, 2]"));
        row2.setField(1, Timestamp.fromLocalDateTime(LocalDateTime.of(2023, 2, 1, 0, 0, 0, 0)));
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
                paimonNativeCatalog.getTable(tblIdentifier);
                result = mockPaimonTable;
                mockPaimonTable.partitionKeys();
                result = partitionNames;
                mockPaimonTable.rowType();
                result = tblRowType;
                paimonNativeCatalog.getTable(partitionTblIdentifier);
                result = mockPartitionTable;
                mockPartitionTable.rowType();
                result = partitionRowType;

                mockPartitionTable.newReadBuilder().withProjection((int[]) any).newRead().createReader((TableScan.Plan) any);
                result = mockRecordReader;
            }
        };
        List<String> result = metadata.listPartitionNames("db1", "tbl1", ConnectorMetadatRequestContext.DEFAULT);
        Assert.assertEquals(2, result.size());
        List<String> expections = Lists.newArrayList("year=2020/month=1", "year=2020/month=2");
        Assertions.assertThat(result).hasSameElementsAs(expections);
    }

    @Test
    public void testGetRemoteFiles(@Mocked FileStoreTable paimonNativeTable,
                                   @Mocked ReadBuilder readBuilder)
            throws Catalog.TableNotExistException {
        new MockUp<PaimonMetadata>() {
            @Mock
            public long getTableCreateTime(String dbName, String tblName) {
                return 0L;
            }
        };
        new Expectations() {
            {
                paimonNativeCatalog.getTable((Identifier) any);
                result = paimonNativeTable;
                paimonNativeTable.newReadBuilder();
                result = readBuilder;
                readBuilder.withFilter((List<Predicate>) any).withProjection((int[]) any).newScan().plan().splits();
                result = splits;
            }
        };
        PaimonTable paimonTable = (PaimonTable) metadata.getTable("db1", "tbl1");
        List<String> requiredNames = Lists.newArrayList("f2", "dt");
        List<RemoteFileInfo> result =
                metadata.getRemoteFiles(paimonTable, GetRemoteFilesParams.newBuilder().setFieldNames(requiredNames).build());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1, result.get(0).getFiles().size());
        PaimonRemoteFileDesc desc = (PaimonRemoteFileDesc) result.get(0).getFiles().get(0);
        Assert.assertEquals(2, desc.getPaimonSplitsInfo().getPaimonSplits().size());
    }

    @Test
    public void testGetCloudConfiguration() {
        CloudConfiguration cc = metadata.getCloudConfiguration();
        Assert.assertEquals(cc.getCloudType(), CloudType.DEFAULT);
    }

    @Test
    public void testGetCreateTime(@Mocked SchemasTable schemasTable,
                                  @Mocked RecordReader<InternalRow> recordReader) throws Exception {
        RowType rowType = new RowType(Arrays.asList(
                new DataField(0, "schema_id", new BigIntType(false)),
                new DataField(1, "fields", SerializationUtils.newStringType(false)),
                new DataField(2, "partition_keys", SerializationUtils.newStringType(false)),
                new DataField(3, "primary_keys", SerializationUtils.newStringType(false)),
                new DataField(4, "options", SerializationUtils.newStringType(false)),
                new DataField(5, "comment", SerializationUtils.newStringType(true)),
                new DataField(6, "update_time", new TimestampType(false, 3))));

        GenericRow row1 = new GenericRow(2);
        row1.setField(0, (long) 0);
        row1.setField(1, Timestamp.fromLocalDateTime(LocalDateTime.of(2023, 1, 1, 0, 0, 0, 0)));

        GenericRow row2 = new GenericRow(2);
        row2.setField(1, (long) 1);
        row2.setField(1, Timestamp.fromLocalDateTime(LocalDateTime.of(2023, 2, 1, 0, 0, 0, 0)));

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
                result = schemasTable;
                schemasTable.rowType();
                result = rowType;
                schemasTable.newReadBuilder().withProjection((int[]) any)
                        .withFilter((Predicate) any).newRead().createReader((TableScan.Plan) any);
                result = recordReader;
            }
        };

        long createTime = metadata.getTableCreateTime("db1", "tbl1");
        Assert.assertEquals(1672531200000L, createTime);
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
        Assert.assertEquals(1675209600000L, updateTime);
    }

    @Test
    public void testPrunePaimonPartition() {
        new MockUp<MetadataMgr>() {
            @Mock
            public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
                return Lists.newArrayList(RemoteFileInfo.builder()
                        .setFiles(Lists.newArrayList(PaimonRemoteFileDesc.createPamonRemoteFileDesc(
                                new PaimonSplitsInfo(null, Lists.newArrayList((Split) splits.get(0))))))
                        .build());
            }
        };
        new MockUp<PaimonMetadata>() {
            @Mock
            public long getTableCreateTime(String dbName, String tblName) {
                return 0L;
            }
        };

        PaimonTable paimonTable = (PaimonTable) metadata.getTable("db1", "tbl1");

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
}
