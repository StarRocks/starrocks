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
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.ExternalScanPartitionPruneRule;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
<<<<<<< HEAD
=======
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.SerializationUtils;
import org.assertj.core.api.Assertions;
>>>>>>> 601559f82b ([Feature] Support paimon materialized view (#29476))
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.io.DataFileMeta.DUMMY_LEVEL;
import static org.apache.paimon.io.DataFileMeta.EMPTY_KEY_STATS;
import static org.apache.paimon.io.DataFileMeta.EMPTY_MAX_KEY;
import static org.apache.paimon.io.DataFileMeta.EMPTY_MIN_KEY;
import static org.junit.Assert.assertEquals;

public class PaimonMetadataTest {
    @Mocked
    Catalog paimonNativeCatalog;
    private PaimonMetadata metadata;
    private final List<DataSplit> splits = new ArrayList<>();

    @Before
    public void setUp() {

        this.metadata = new PaimonMetadata("paimon_catalog", new HdfsEnvironment(), paimonNativeCatalog,
                "filesystem", null, "hdfs://127.0.0.1:9999/warehouse");

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
        meta1.add(new DataFileMeta("file1", 100, 200, EMPTY_MIN_KEY, EMPTY_MAX_KEY, EMPTY_KEY_STATS, null,
                1, 1, 1, DUMMY_LEVEL));
        meta1.add(new DataFileMeta("file2", 100, 300, EMPTY_MIN_KEY, EMPTY_MAX_KEY, EMPTY_KEY_STATS, null,
                1, 1, 1, DUMMY_LEVEL));

        List<DataFileMeta> meta2 = new ArrayList<>();
        meta2.add(new DataFileMeta("file3", 100, 400, EMPTY_MIN_KEY, EMPTY_MAX_KEY, EMPTY_KEY_STATS, null,
                1, 1, 1, DUMMY_LEVEL));

        this.splits.add(DataSplit.builder().withSnapshot(1L).withPartition(row1).withBucket(1).withDataFiles(meta1)
                .isStreaming(false).build());
        this.splits.add(DataSplit.builder().withSnapshot(1L).withPartition(row2).withBucket(1).withDataFiles(meta2)
                .isStreaming(false).build());
    }

    @Test
    public void testRowCount() {
        long rowCount = metadata.getRowCount(splits);
        Assert.assertEquals(900, rowCount);
    }

    @Test
    public void testGetTable(@Mocked AbstractFileStoreTable paimonNativeTable) throws Catalog.TableNotExistException {
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
            }
        };
        com.starrocks.catalog.Table table = metadata.getTable("db1", "tbl1");
        PaimonTable paimonTable = (PaimonTable) table;
        Assert.assertEquals("db1", paimonTable.getDbName());
        Assert.assertEquals("tbl1", paimonTable.getTableName());
        Assert.assertEquals(Lists.newArrayList("col1"), paimonTable.getPartitionColumnNames());
        Assert.assertEquals("hdfs://127.0.0.1:10000/paimon", paimonTable.getTableLocation());
        Assert.assertEquals(ScalarType.INT, paimonTable.getBaseSchema().get(0).getType());
        Assert.assertTrue(paimonTable.getBaseSchema().get(0).isAllowNull());
        Assert.assertEquals(ScalarType.DOUBLE, paimonTable.getBaseSchema().get(1).getType());
        Assert.assertTrue(paimonTable.getBaseSchema().get(1).isAllowNull());
        Assert.assertEquals("paimon_catalog", paimonTable.getCatalogName());
        Assert.assertEquals("paimon_catalog.db1.tbl1", paimonTable.getUUID());
    }

    @Test
    public void testListPartitionNames(@Mocked AbstractFileStoreTable paimonNativeTable,
                                       @Mocked ReadBuilder readBuilder) throws Catalog.TableNotExistException {

        RowType partitionRowType = RowType.of(
                new DataType[] {
                        new DateType(false),
                        new IntType(true)
                },
                new String[] {"dt", "hr"});

        List<String> partitionNames = Lists.newArrayList("dt", "hr");

        new Expectations() {
            {
                paimonNativeCatalog.getTable((Identifier) any);
                result = paimonNativeTable;
                paimonNativeTable.partitionKeys();
                result = partitionNames;
                paimonNativeTable.schema().logicalPartitionType();
                result = partitionRowType;
                paimonNativeTable.newReadBuilder();
                result = readBuilder;
                readBuilder.newScan().plan().splits();
                result = splits;
            }
        };
        List<String> result = metadata.listPartitionNames("db1", "tbl1");
        Assert.assertEquals(2, result.size());
        List<String> expections = Lists.newArrayList("dt=1975-06-24/hr=4444", "dt=1978-03-20/hr=5555");
        Assertions.assertThat(result).hasSameElementsAs(expections);
    }

    @Test
    public void testGetRemoteFileInfos(@Mocked AbstractFileStoreTable paimonNativeTable,
                                       @Mocked ReadBuilder readBuilder)
            throws Catalog.TableNotExistException {
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
        List<RemoteFileInfo> result = metadata.getRemoteFileInfos(paimonTable, null, -1, null, requiredNames, -1);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1, result.get(0).getFiles().size());
        Assert.assertEquals(2, result.get(0).getFiles().get(0).getPaimonSplitsInfo().getPaimonSplits().size());
    }

    @Test
    public void testPrunePaimonPartition(@Mocked AbstractFileStoreTable paimonNativeTable,
                                         @Mocked ReadBuilder readBuilder) {
        new MockUp<MetadataMgr>() {
            @Mock
            public List<RemoteFileInfo> getRemoteFileInfos(String catalogName, Table table, List<PartitionKey> partitionKeys,
                                                           long snapshotId, ScalarOperator predicate, List<String> fieldNames,
                                                           long limit) {
                return Lists.newArrayList(RemoteFileInfo.builder()
                        .setFiles(Lists.newArrayList(RemoteFileDesc.createPamonRemoteFileDesc(
                                new PaimonSplitsInfo(null, Lists.newArrayList((Split) splits.get(0))))))
                        .build());
            }
        };
        PaimonTable paimonTable = (PaimonTable) metadata.getTable("db1", "tbl1");

        ExternalScanPartitionPruneRule rule0 = ExternalScanPartitionPruneRule.PAIMON_SCAN;

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
        rule0.transform(scan, new OptimizerContext(new Memo(), new ColumnRefFactory()));
        assertEquals(1, ((LogicalPaimonScanOperator) scan.getOp()).getScanOperatorPredicates()
                .getSelectedPartitionIds().size());
    }
}
