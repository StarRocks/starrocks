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

package com.starrocks.sql.plan;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.Pair;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.connector.iceberg.TableTestBase;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.expressions.Expressions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static com.starrocks.sql.plan.PlanTestNoneDBBase.assertContains;

public class IcebergEqualityDeletePlanTest extends TableTestBase {
    private final StarRocksAssert starRocksAssert = new StarRocksAssert();

    @Before
    public void before() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        String createCatalog = "CREATE EXTERNAL CATALOG iceberg_catalog PROPERTIES(\"type\"=\"iceberg\", " +
                "\"iceberg.catalog.hive.metastore.uris\"=\"thrift://127.0.0.1:9083\", \"iceberg.catalog.type\"=\"hive\"," +
                "\"enable_get_stats_from_external_metadata\"=\"true\")";
        starRocksAssert.withCatalog(createCatalog);
        new MockUp<IcebergMetadata>() {
            @Mock
            public Database getDb(ConnectContext context, String dbName) {
                return new Database(1, "db");
            }
        };
    }

    @After
    public void after() throws Exception {
        starRocksAssert.dropCatalog("iceberg_catalog");
    }

    public IcebergEqualityDeletePlanTest() throws IOException {
    }
    
    public void testNormalPlan() throws Exception {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableB.refresh();
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(String dbName, String tableName) throws StarRocksConnectorException {
                return mockedNativeTableB;
            }
        };

        String sql = "select * from iceberg_catalog.db.tbl";
        String plan = UtFrameUtils.getFragmentPlan(starRocksAssert.getCtx(), sql);
        assertContains(plan, "cardinality=3");
    }

    @Test
    public void testNormalEqDeletePlan() throws Exception {
        DataFile dataFileA =
                DataFiles.builder(SPEC_A)
                        .withPath("/path/to/data-a.parquet")
                        .withFileSizeInBytes(10)
                        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
                        .withRecordCount(200000)
                        .build();

        mockedNativeTableA.newAppend().appendFile(dataFileA).commit();
        mockedNativeTableA.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_A2_DELETES).commit();
        mockedNativeTableA.refresh();
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(String dbName, String tableName) throws StarRocksConnectorException {
                return mockedNativeTableA;
            }
        };

        String sql = "select data from iceberg_catalog.db.tbl";
        String plan = UtFrameUtils.getFragmentPlan(starRocksAssert.getCtx(), sql);
        assertContains(plan, "9:Project\n" +
                "  |  <slot 2> : 2: data\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "  |  \n" +
                "  |----8:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 08\n" +
                "    RANDOM\n" +
                "\n" +
                "  7:Project\n" +
                "  |  <slot 3> : 3: id\n" +
                "  |  <slot 4> : 4: data\n" +
                "  |  \n" +
                "  6:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 3: id = 6: id\n" +
                "  |  other join predicates: 5: $data_sequence_number < 7: $data_sequence_number\n" +
                "  |  \n" +
                "  |----5:EXCHANGE\n" +
                "  |    \n" +
                "  3:IcebergScanNode\n" +
                "     TABLE: db.tbl_with_delete_file\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0");

        // check iceberg scan node
        List<IcebergMORParams> morParamsList = Lists.newArrayList(
                IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE,
                IcebergMORParams.DATA_FILE_WITH_EQ_DELETE,
                IcebergMORParams.of(IcebergMORParams.ScanTaskType.EQ_DELETE, Lists.newArrayList(1))
        );
        Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(starRocksAssert.getCtx(), sql);
        ScanNode scanNode = pair.second.getFragments().get(4).collectScanNodes().get(new PlanNodeId(1));
        Assert.assertTrue(scanNode instanceof IcebergScanNode);
        IcebergScanNode icebergScanNode = (IcebergScanNode) scanNode;
        Assert.assertEquals(IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE, icebergScanNode.getMORParams());
        Assert.assertEquals(morParamsList, icebergScanNode.getTableFullMORParams().getMorParamsList());
        Assert.assertTrue(icebergScanNode.getExtendedColumnSlotIds().isEmpty());

        scanNode = pair.second.getFragments().get(2).collectScanNodes().get(new PlanNodeId(3));
        Assert.assertTrue(scanNode instanceof IcebergScanNode);
        icebergScanNode = (IcebergScanNode) scanNode;
        Assert.assertEquals(IcebergMORParams.DATA_FILE_WITH_EQ_DELETE, icebergScanNode.getMORParams());
        Assert.assertEquals(morParamsList, icebergScanNode.getTableFullMORParams().getMorParamsList());
        Assert.assertFalse(icebergScanNode.getExtendedColumnSlotIds().isEmpty());

        // check iceberg equality scan node
        scanNode = pair.second.getFragments().get(3).collectScanNodes().get(new PlanNodeId(4));
        Assert.assertTrue(scanNode instanceof IcebergScanNode);
        IcebergScanNode eqScanNode = (IcebergScanNode) scanNode;
        Assert.assertTrue(eqScanNode.getExplainString().contains("TABLE: db.tbl_eq_delete_id\n" +
                "   cardinality=1\n" +
                "   avgRowSize=2.0\n" +
                "   dataCacheOptions={populate: false}\n" +
                "   partitions=1/1\n" +
                "   Iceberg identifier columns: [id]"));
    }

    @Test
    public void testPartitionTableNormalEqDeletePlan() throws Exception {
        DataFile dataFileA =
                DataFiles.builder(SPEC_B)
                        .withPath("/path/to/data-a.parquet")
                        .withFileSizeInBytes(10)
                        .withPartitionPath("k2=111") // easy way to set partition data for now
                        .withRecordCount(200000)
                        .build();

        DeleteFile fileADeletes =
                FileMetadata.deleteFileBuilder(SPEC_B)
                        .ofPositionDeletes()
                        .withPath("/path/to/data-a-deletes.orc")
                        .withFormat(FileFormat.ORC)
                        .withFileSizeInBytes(10)
                        .withPartitionPath("k2=111") // easy way to set partition data for now
                        .withRecordCount(1)
                        .build();

        DeleteFile fileA2Deletes =
                FileMetadata.deleteFileBuilder(SPEC_B)
                        .ofEqualityDeletes(1, 2)
                        .withPath("/path/to/data-a2-deletes.orc")
                        .withFormat(FileFormat.ORC)
                        .withFileSizeInBytes(10)
                        .withPartitionPath("k2=111")
                        .withRecordCount(200000)
                        .build();

        mockedNativeTableC.newAppend().appendFile(dataFileA).commit();
        mockedNativeTableC.newRowDelta().addDeletes(fileADeletes).addDeletes(fileA2Deletes).commit();
        mockedNativeTableC.refresh();
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(String dbName, String tableName) throws StarRocksConnectorException {
                return mockedNativeTableC;
            }
        };

        String sql = "select k1, k2 from iceberg_catalog.db.tbl where k1 = 1 limit 1";
        String plan = UtFrameUtils.getFragmentPlan(starRocksAssert.getCtx(), sql);
        assertContains(plan, "0:UNION\n" +
                "  |  limit: 1\n" +
                "  |  \n" +
                "  |----8:EXCHANGE\n" +
                "  |       limit: 1\n" +
                "  |    \n" +
                "  2:EXCHANGE\n" +
                "     limit: 1\n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 08\n" +
                "    RANDOM\n" +
                "\n" +
                "  7:Project\n" +
                "  |  <slot 3> : 3: k1\n" +
                "  |  <slot 4> : 4: k2\n" +
                "  |  limit: 1\n" +
                "  |  \n" +
                "  6:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 3: k1 = 6: k1\n" +
                "  |  equal join conjunct: 4: k2 = 7: k2\n" +
                "  |  other join predicates: 5: $data_sequence_number < 8: $data_sequence_number\n" +
                "  |  limit: 1\n" +
                "  |  \n" +
                "  |----5:EXCHANGE\n" +
                "  |    \n" +
                "  3:IcebergScanNode\n" +
                "     TABLE: db.tbl_with_delete_file\n" +
                "     PREDICATES: 3: k1 = 1\n" +
                "     MIN/MAX PREDICATES: 3: k1 <= 1, 3: k1 >= 1\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0\n" +
                "\n" +
                "PLAN FRAGMENT 3\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 05\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  4:IcebergEqualityDeleteScanNode\n" +
                "     TABLE: db.tbl_eq_delete_k1_k2\n" +
                "     PREDICATES: 6: k1 = 1\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0\n" +
                "     Iceberg identifier columns: [k1, k2]");

        Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(starRocksAssert.getCtx(), sql);
        List<ScanNode> scanNodes = pair.second.getScanNodes();
        Assert.assertEquals(3, scanNodes.size());
        // check without eq-delete files scan node
        IcebergScanNode withoutDeleteFileScanNode = (IcebergScanNode) scanNodes.get(0);
        Assert.assertEquals(IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE, withoutDeleteFileScanNode.getMORParams());
        Assert.assertTrue(withoutDeleteFileScanNode.getExtendedColumnSlotIds().isEmpty());

        // check with eq-delete scan node
        IcebergScanNode left = (IcebergScanNode) scanNodes.get(1);
        Assert.assertEquals(IcebergMORParams.DATA_FILE_WITH_EQ_DELETE, left.getMORParams());
        IcebergScanNode right = (IcebergScanNode) scanNodes.get(2);
        Assert.assertEquals(IcebergMORParams.of(IcebergMORParams.ScanTaskType.EQ_DELETE, List.of(1, 2)), right.getMORParams());

        Assert.assertTrue(right.getExplainString().contains("4:IcebergEqualityDeleteScanNode\n" +
                "   TABLE: db.tbl_eq_delete_k1_k2\n" +
                "   PREDICATES: 6: k1 = 1\n" +
                "   cardinality=1\n" +
                "   avgRowSize=3.0\n" +
                "   dataCacheOptions={populate: false}\n" +
                "   partitions=1/1\n" +
                "   Iceberg identifier columns: [k1, k2]\n" +
                "   tuple ids: 5 \n"));

        Assert.assertNotNull(right.getConjuncts());
        Assert.assertEquals(1, right.getConjuncts().size());
        Assert.assertEquals(1, right.getSeenEqualityDeleteFiles().size());
        Assert.assertTrue(left.getIcebergJobPlanningPredicate().equivalent(right.getIcebergJobPlanningPredicate()));
        Assert.assertEquals(-1, left.getLimit());
        Assert.assertEquals(-1, right.getLimit());
        Assert.assertEquals(1, left.getExtendedColumnSlotIds().size());
        Assert.assertEquals(1, right.getExtendedColumnSlotIds().size());
    }

    @Test
    public void testMutableIdentifierColumnsCase() throws Exception {
        DataFile dataFileA =
                DataFiles.builder(SPEC_B)
                        .withPath("/path/to/data-a.parquet")
                        .withFileSizeInBytes(10)
                        .withPartitionPath("k2=111") // easy way to set partition data for now
                        .withRecordCount(200000)
                        .build();

        DeleteFile fileADeletes =
                FileMetadata.deleteFileBuilder(SPEC_B)
                        .ofPositionDeletes()
                        .withPath("/path/to/data-a-deletes.orc")
                        .withFormat(FileFormat.ORC)
                        .withFileSizeInBytes(10)
                        .withPartitionPath("k2=111") // easy way to set partition data for now
                        .withRecordCount(1)
                        .build();

        DeleteFile fileA2Deletes =
                FileMetadata.deleteFileBuilder(SPEC_B)
                        .ofEqualityDeletes(1)
                        .withPath("/path/to/data-a2-deletes.orc")
                        .withFormat(FileFormat.ORC)
                        .withFileSizeInBytes(10)
                        .withPartitionPath("k2=111")
                        .withRecordCount(200)
                        .build();

        mockedNativeTableC.newAppend().appendFile(dataFileA).commit();
        mockedNativeTableC.newRowDelta().addDeletes(fileADeletes).addDeletes(fileA2Deletes).commit();
        mockedNativeTableC.refresh();

        DataFile dataFileB =
                DataFiles.builder(SPEC_B)
                        .withPath("/path/to/data-b.parquet")
                        .withFileSizeInBytes(10)
                        .withPartitionPath("k2=111") // easy way to set partition data for now
                        .withRecordCount(10000)
                        .build();

        DeleteFile fileBDeletes =
                FileMetadata.deleteFileBuilder(SPEC_B)
                        .ofPositionDeletes()
                        .withPath("/path/to/data-b-deletes.orc")
                        .withFormat(FileFormat.ORC)
                        .withFileSizeInBytes(10)
                        .withPartitionPath("k2=111") // easy way to set partition data for now
                        .withRecordCount(1)
                        .build();

        DeleteFile fileB2Deletes =
                FileMetadata.deleteFileBuilder(SPEC_B)
                        .ofEqualityDeletes(2)
                        .withPath("/path/to/data-b2-deletes.orc")
                        .withFormat(FileFormat.ORC)
                        .withFileSizeInBytes(10)
                        .withPartitionPath("k2=111")
                        .withRecordCount(222)
                        .build();

        mockedNativeTableC.newAppend().appendFile(dataFileB).commit();
        mockedNativeTableC.newRowDelta().addDeletes(fileBDeletes).addDeletes(fileB2Deletes).commit();
        mockedNativeTableC.refresh();

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(String dbName, String tableName) throws StarRocksConnectorException {
                return mockedNativeTableC;
            }
        };

        String sql = "select k2 from iceberg_catalog.db.tbl where k1 = 1 limit 1";
        String plan = UtFrameUtils.getFragmentPlan(starRocksAssert.getCtx(), sql);
        assertContains(plan, "12:Project\n" +
                "  |  <slot 2> : 2: k2\n" +
                "  |  limit: 1\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "  |  limit: 1\n" +
                "  |  \n" +
                "  |----11:EXCHANGE\n" +
                "  |       limit: 1\n" +
                "  |    \n" +
                "  2:EXCHANGE\n" +
                "     limit: 1\n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 11\n" +
                "    RANDOM\n" +
                "\n" +
                "  10:Project\n" +
                "  |  <slot 3> : 3: k1\n" +
                "  |  <slot 4> : 4: k2\n" +
                "  |  limit: 1\n" +
                "  |  \n" +
                "  9:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 3: k1 = 8: k1\n" +
                "  |  other join predicates: 5: $data_sequence_number < 9: $data_sequence_number\n" +
                "  |  limit: 1\n" +
                "  |  \n" +
                "  |----8:EXCHANGE\n" +
                "  |    \n" +
                "  6:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: k2 = 6: k2\n" +
                "  |  other join predicates: 5: $data_sequence_number < 7: $data_sequence_number\n" +
                "  |  \n" +
                "  |----5:EXCHANGE\n" +
                "  |    \n" +
                "  3:IcebergScanNode\n" +
                "     TABLE: db.tbl_with_delete_file\n" +
                "     PREDICATES: 3: k1 = 1\n" +
                "     MIN/MAX PREDICATES: 3: k1 <= 1, 3: k1 >= 1\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0\n" +
                "\n" +
                "PLAN FRAGMENT 3\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 08\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  7:IcebergEqualityDeleteScanNode\n" +
                "     TABLE: db.tbl_eq_delete_k1\n" +
                "     PREDICATES: 8: k1 = 1\n" +
                "     cardinality=1\n" +
                "     avgRowSize=2.0\n" +
                "     Iceberg identifier columns: [k1]\n" +
                "\n" +
                "PLAN FRAGMENT 4\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 05\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  4:IcebergEqualityDeleteScanNode\n" +
                "     TABLE: db.tbl_eq_delete_k2\n" +
                "     cardinality=1\n" +
                "     avgRowSize=2.0\n" +
                "     Iceberg identifier columns: [k2]");

        Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(starRocksAssert.getCtx(), sql);
        List<ScanNode> scanNodes = pair.second.getScanNodes();
        Assert.assertEquals(4, scanNodes.size());
        IcebergScanNode eqK1 = (IcebergScanNode) scanNodes.get(3);
        IcebergScanNode eqK2 = (IcebergScanNode) scanNodes.get(2);
        Assert.assertEquals(IcebergMORParams.of(IcebergMORParams.ScanTaskType.EQ_DELETE,
                Lists.newArrayList(1)), eqK1.getMORParams());
        Assert.assertEquals(IcebergMORParams.of(IcebergMORParams.ScanTaskType.EQ_DELETE,
                Lists.newArrayList(2)), eqK2.getMORParams());
    }

    @Test
    public void testPartitionEvolution() throws Exception {
        DataFile dataFileA =
                DataFiles.builder(SPEC_B)
                        .withPath("/path/to/data-a.parquet")
                        .withFileSizeInBytes(10)
                        .withPartitionPath("k2=111") // easy way to set partition data for now
                        .withRecordCount(200000)
                        .build();

        DeleteFile fileADeletes =
                FileMetadata.deleteFileBuilder(SPEC_B)
                        .ofPositionDeletes()
                        .withPath("/path/to/data-a-deletes.orc")
                        .withFormat(FileFormat.ORC)
                        .withFileSizeInBytes(10)
                        .withPartitionPath("k2=111") // easy way to set partition data for now
                        .withRecordCount(1)
                        .build();

        DeleteFile fileA2Deletes =
                FileMetadata.deleteFileBuilder(SPEC_B)
                        .ofEqualityDeletes(2)
                        .withPath("/path/to/data-a2-deletes.orc")
                        .withFormat(FileFormat.ORC)
                        .withFileSizeInBytes(10)
                        .withPartitionPath("k2=111")
                        .withRecordCount(200)
                        .build();

        mockedNativeTableC.newAppend().appendFile(dataFileA).commit();
        mockedNativeTableC.newRowDelta().addDeletes(fileADeletes).addDeletes(fileA2Deletes).commit();
        mockedNativeTableC.refresh();

        mockedNativeTableC.updateSpec()
                .removeField("k2")
                .addField(Expressions.bucket("k2", 5))
                .commit();
        mockedNativeTableC.refresh();

        PartitionSpec updatedSpec = mockedNativeTableC.spec();

        DataFile dataFileB =
                DataFiles.builder(updatedSpec)
                        .withPath("/path/to/data-bucket.parquet")
                        .withFileSizeInBytes(10)
                        .withPartitionPath("k2_bucket_5=4") // easy way to set partition data for now
                        .withRecordCount(200000)
                        .build();

        DeleteFile fileBDeletes =
                FileMetadata.deleteFileBuilder(updatedSpec)
                        .ofPositionDeletes()
                        .withPath("/path/to/data-bucket-b-deletes.orc")
                        .withFormat(FileFormat.ORC)
                        .withFileSizeInBytes(10)
                        .withPartitionPath("k2_bucket_5=4") // easy way to set partition data for now
                        .withRecordCount(1)
                        .build();

        DeleteFile fileB2Deletes =
                FileMetadata.deleteFileBuilder(updatedSpec)
                        .ofEqualityDeletes(2)
                        .withPath("/path/to/data-bucket-b2-deletes.orc")
                        .withFormat(FileFormat.ORC)
                        .withFileSizeInBytes(10)
                        .withPartitionPath("k2_bucket_5=4")
                        .withRecordCount(200)
                        .build();

        mockedNativeTableC.newAppend().appendFile(dataFileB).commit();
        mockedNativeTableC.newRowDelta().addDeletes(fileBDeletes).addDeletes(fileB2Deletes).commit();
        mockedNativeTableC.refresh();
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(String dbName, String tableName) throws StarRocksConnectorException {
                return mockedNativeTableC;
            }
        };

        String sql = "select k2 from iceberg_catalog.db.tbl";
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Equality delete files aren't supported for tables with partition evolution",
                () -> UtFrameUtils.getFragmentPlan(starRocksAssert.getCtx(), sql));

        starRocksAssert.getCtx().getSessionVariable().setEnableReadIcebergEqDeleteWithPartitionEvolution(true);
        String plan = UtFrameUtils.getFragmentPlan(starRocksAssert.getCtx(), sql);
        assertContains(plan, "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:2: k2\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  10:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 10\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  9:Project\n" +
                "  |  <slot 2> : 2: k2\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "  |  \n" +
                "  |----8:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 08\n" +
                "    RANDOM\n" +
                "\n" +
                "  7:Project\n" +
                "  |  <slot 3> : 3: k1\n" +
                "  |  <slot 4> : 4: k2\n" +
                "  |  \n" +
                "  6:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: k2 = 7: k2\n" +
                "  |  equal join conjunct: 6: $spec_id = 9: $spec_id\n" +
                "  |  other join predicates: 5: $data_sequence_number < 8: $data_sequence_number\n" +
                "  |  \n" +
                "  |----5:EXCHANGE\n" +
                "  |    \n" +
                "  3:IcebergScanNode\n" +
                "     TABLE: db.tbl_with_delete_file\n" +
                "     cardinality=1\n" +
                "     avgRowSize=4.0\n" +
                "\n" +
                "PLAN FRAGMENT 3\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 05\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  4:IcebergEqualityDeleteScanNode\n" +
                "     TABLE: db.tbl_eq_delete_k2\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0\n" +
                "     Iceberg identifier columns: [k2]\n" +
                "\n" +
                "PLAN FRAGMENT 4\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:IcebergScanNode\n" +
                "     TABLE: db.tbl\n" +
                "     cardinality=400000\n" +
                "     avgRowSize=2.0\n");

        // check iceberg scan node
        Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(starRocksAssert.getCtx(), sql);
        List<ScanNode> scanNodes = pair.second.getScanNodes();
        IcebergScanNode left = (IcebergScanNode) scanNodes.get(1);
        IcebergScanNode right = (IcebergScanNode) scanNodes.get(2);
        Assert.assertEquals(2, left.getExtendedColumnSlotIds().size());
        Assert.assertEquals(2, right.getExtendedColumnSlotIds().size());
    }
}
