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
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.connector.iceberg.TableTestBase;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.planner.IcebergEqualityDeleteScanNode;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TIcebergFileContent;
import com.starrocks.thrift.TScanRangeLocations;
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
            public Database getDb(String dbName) {
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

    @Test
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
        assertContains(plan, "4:Project\n" +
                "  |  <slot 2> : 2: data\n" +
                "  |  \n" +
                "  3:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: id = 4: id\n" +
                "  |  other join predicates: 3: $data_sequence_number < 5: $data_sequence_number\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:IcebergScanNode\n" +
                "     TABLE: db.tbl\n" +
                "     cardinality=200000\n" +
                "     avgRowSize=3.0");

        // check iceberg scan node
        Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(starRocksAssert.getCtx(), sql);
        ScanNode scanNode = pair.second.getFragments().get(1).collectScanNodes().get(new PlanNodeId(0));
        Assert.assertTrue(scanNode instanceof IcebergScanNode);
        List<TScanRangeLocations> scanRangeLocations = scanNode.getScanRangeLocations(100);
        THdfsScanRange scanRange = scanRangeLocations.get(0).getScan_range().getHdfs_scan_range();
        Assert.assertNotNull(scanRange);
        Assert.assertEquals(1, scanRange.getDelete_files().size());
        Assert.assertEquals(TIcebergFileContent.POSITION_DELETES, scanRange.getDelete_files().get(0).file_content);

        // check iceberg equality scan node
        scanNode = pair.second.getFragments().get(2).collectScanNodes().get(new PlanNodeId(1));
        Assert.assertTrue(scanNode instanceof IcebergEqualityDeleteScanNode);
        IcebergEqualityDeleteScanNode eqScanNode = (IcebergEqualityDeleteScanNode) scanNode;
        Assert.assertTrue(eqScanNode.getExplainString().contains("TABLE: db.tbl_eq_delete_id\n" +
                "   cardinality=1\n" +
                "   avgRowSize=2.0\n" +
                "   dataCacheOptions={populate: false}\n" +
                "   partitions=1/1\n" +
                "   Iceberg identifier columns: [id]"));
        scanRangeLocations = scanNode.getScanRangeLocations(100);
        scanRange = scanRangeLocations.get(0).getScan_range().getHdfs_scan_range();
        Assert.assertNotNull(scanRange);
        Assert.assertFalse(scanRange.isSetDelete_files());
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
        assertContains(plan, "5:Project\n" +
                "  |  <slot 1> : 1: k1\n" +
                "  |  <slot 2> : 2: k2\n" +
                "  |  limit: 1\n" +
                "  |  \n" +
                "  4:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: k1 = 4: k1\n" +
                "  |  equal join conjunct: 2: k2 = 5: k2\n" +
                "  |  other join predicates: 3: $data_sequence_number < 6: $data_sequence_number\n" +
                "  |  limit: 1\n" +
                "  |  \n" +
                "  |----3:EXCHANGE\n" +
                "  |    \n" +
                "  1:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 4: k1, 5: k2\n" +
                "\n" +
                "  2:IcebergEqualityDeleteScanNode\n" +
                "     TABLE: db.tbl_eq_delete_k1_k2\n" +
                "     PREDICATES: 4: k1 = 1\n" +
                "     cardinality=100000\n" +
                "     avgRowSize=3.0\n" +
                "     Iceberg identifier columns: [k1, k2]\n" +
                "\n" +
                "\n" +
                "PLAN FRAGMENT 3\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    HASH_PARTITIONED: 1: k1, 2: k2\n" +
                "\n" +
                "  0:IcebergScanNode\n" +
                "     TABLE: db.tbl\n" +
                "     PREDICATES: 1: k1 = 1\n" +
                "     MIN/MAX PREDICATES: 1: k1 <= 1, 1: k1 >= 1\n" +
                "     cardinality=100000\n" +
                "     avgRowSize=3.0");


        // check iceberg scan node
        Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(starRocksAssert.getCtx(), sql);
        List<ScanNode> scanNodes = pair.second.getScanNodes();
        IcebergScanNode left = (IcebergScanNode) scanNodes.get(0);
        IcebergEqualityDeleteScanNode right = (IcebergEqualityDeleteScanNode) scanNodes.get(1);
        Assert.assertTrue(right.getExplainString().contains("TABLE: db.tbl_eq_delete_k1_k2\n" +
                "   PREDICATES: 4: k1 = 1\n" +
                "   cardinality=100000\n" +
                "   avgRowSize=3.0\n" +
                "   dataCacheOptions={populate: false}\n" +
                "   partitions=1/1\n" +
                "   Iceberg identifier columns: [k1, k2]"));

        Assert.assertNotNull(right.getConjuncts());
        Assert.assertEquals(Lists.newArrayList(1, 2), right.getEqualityIds());
        Assert.assertEquals(1, right.getSeenFiles().size());
        Assert.assertFalse(right.isNeedCheckEqualityIds());
        Assert.assertEquals(1, right.getConjuncts().size());
        Assert.assertEquals(left.getIcebergJobPlanningPredicate(), right.getIcebergJobPlanningPredicate());
        Assert.assertEquals(-1, left.getLimit());
        Assert.assertEquals(-1, right.getLimit());
        Assert.assertEquals(1, left.getExtendedColumnSlotIds().size());
        Assert.assertEquals(1, right.getExtendedColumnSlotIds().size());


        THdfsScanRange leftScanRange = left.getScanRangeLocations(100).get(0).getScan_range().getHdfs_scan_range();
        THdfsScanRange rightScanRange = right.getScanRangeLocations(100).get(0).getScan_range().getHdfs_scan_range();
        Assert.assertEquals(1, leftScanRange.getIdentity_partition_slot_ids().size());
        Assert.assertEquals(1, rightScanRange.getIdentity_partition_slot_ids().size());
        Assert.assertEquals(1, leftScanRange.getExtended_columns().size());
        Assert.assertEquals(1, rightScanRange.getExtended_columns().size());
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
        assertContains(plan, "8:Project\n" +
                "  |  <slot 2> : 2: k2\n" +
                "  |  limit: 1\n" +
                "  |  \n" +
                "  7:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: k2 = 4: k2\n" +
                "  |  other join predicates: 3: $data_sequence_number < 5: $data_sequence_number\n" +
                "  |  limit: 1\n" +
                "  |  \n" +
                "  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  4:Project\n" +
                "  |  <slot 2> : 2: k2\n" +
                "  |  <slot 3> : 3: $data_sequence_number\n" +
                "  |  \n" +
                "  3:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: k1 = 6: k1\n" +
                "  |  other join predicates: 3: $data_sequence_number < 7: $data_sequence_number\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:IcebergScanNode\n" +
                "     TABLE: db.tbl\n" +
                "     PREDICATES: 1: k1 = 1\n" +
                "     MIN/MAX PREDICATES: 1: k1 <= 1, 1: k1 >= 1\n" +
                "     cardinality=105000\n" +
                "     avgRowSize=3.0");

        Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(starRocksAssert.getCtx(), sql);
        List<ScanNode> scanNodes = pair.second.getScanNodes();
        IcebergEqualityDeleteScanNode eqK1 = (IcebergEqualityDeleteScanNode) scanNodes.get(1);
        IcebergEqualityDeleteScanNode eqK2 = (IcebergEqualityDeleteScanNode) scanNodes.get(2);
        Assert.assertEquals(Lists.newArrayList(1), eqK1.getEqualityIds());
        Assert.assertEquals(Lists.newArrayList(2), eqK2.getEqualityIds());
        Assert.assertTrue(eqK1.isNeedCheckEqualityIds());
        Assert.assertTrue(eqK2.isNeedCheckEqualityIds());
        Assert.assertEquals(1, eqK1.getScanRangeLocations(100).size());
        Assert.assertEquals(1, eqK2.getScanRangeLocations(100).size());
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
        assertContains(plan, "4:Project\n" +
                "  |  <slot 2> : 2: k2\n" +
                "  |  \n" +
                "  3:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: k2 = 5: k2\n" +
                "  |  equal join conjunct: 4: $spec_id = 7: $spec_id\n" +
                "  |  other join predicates: 3: $data_sequence_number < 6: $data_sequence_number\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:IcebergScanNode\n" +
                "     TABLE: db.tbl\n" +
                "     cardinality=400000\n" +
                "     avgRowSize=4.0");

        // check iceberg scan node
        Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(starRocksAssert.getCtx(), sql);
        List<ScanNode> scanNodes = pair.second.getScanNodes();
        IcebergScanNode left = (IcebergScanNode) scanNodes.get(0);
        IcebergEqualityDeleteScanNode right = (IcebergEqualityDeleteScanNode) scanNodes.get(1);
        Assert.assertEquals(2, left.getExtendedColumnSlotIds().size());
        Assert.assertEquals(2, right.getExtendedColumnSlotIds().size());

        List<TScanRangeLocations> dataScanRanges = left.getScanRangeLocations(100);
        Assert.assertEquals(2, dataScanRanges.size());
        Integer specSlotId = left.getExtendedColumnSlotIds().get(1);

        for (TScanRangeLocations scanRangeLocations : dataScanRanges) {
            THdfsScanRange scanRange = scanRangeLocations.getScan_range().getHdfs_scan_range();
            Assert.assertEquals(2, scanRange.getExtended_columns().size());
            if (scanRange.getFull_path().contains("bucket")) {
                Assert.assertNull(scanRange.getIdentity_partition_slot_ids());
                Assert.assertEquals(1, scanRange.getExtended_columns().get(specSlotId)
                        .getNodes().get(0).getInt_literal().getValue());
            } else {
                Assert.assertNotNull(scanRange.getIdentity_partition_slot_ids());
                Assert.assertEquals(0, scanRange.getExtended_columns().get(specSlotId)
                        .getNodes().get(0).getInt_literal().getValue());
            }
        }

        List<TScanRangeLocations> eqScanRange = right.getScanRangeLocations(100);
        Assert.assertEquals(2, dataScanRanges.size());
        specSlotId = right.getExtendedColumnSlotIds().get(1);

        for (TScanRangeLocations scanRangeLocations : eqScanRange) {
            THdfsScanRange scanRange = scanRangeLocations.getScan_range().getHdfs_scan_range();
            Assert.assertEquals(2, scanRange.getExtended_columns().size());
            if (scanRange.getFull_path().contains("bucket")) {
                Assert.assertNull(scanRange.getIdentity_partition_slot_ids());
                Assert.assertEquals(1, scanRange.getExtended_columns().get(specSlotId)
                        .getNodes().get(0).getInt_literal().getValue());
            } else {
                Assert.assertNotNull(scanRange.getIdentity_partition_slot_ids());
                Assert.assertEquals(0, scanRange.getExtended_columns().get(specSlotId)
                        .getNodes().get(0).getInt_literal().getValue());
            }
        }
    }
}
