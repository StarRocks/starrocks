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

package com.starrocks.catalog;

import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.StarRocksTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.when;

public class CatalogUtilsTest extends StarRocksTestBase {

    @Mock
    private OlapTable olapTable;

    @Mock
    private PartitionInfo partitionInfo;

    @Mock
    private Partition partition;

    @Mock
    private PhysicalPartition physicalPartition;

    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
    }

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testCalAvgBucketNumOfRecentPartitions_FewerPartitionsThanRecent() {
        when(olapTable.getPartitions()).thenReturn(new ArrayList<>());
        when(olapTable.getPartitionInfo()).thenReturn(partitionInfo);
        when(partitionInfo.isPartitioned()).thenReturn(false);

        int bucketNum = CatalogUtils.calAvgBucketNumOfRecentPartitions(olapTable, 5, true);

        assertEquals(FeConstants.DEFAULT_UNPARTITIONED_TABLE_BUCKET_NUM, bucketNum);
    }

    @Test
    public void testCheckPartitionNameExistForCreatingPartitionNames() {
        Set<String> partitionNames = new HashSet<>();
        partitionNames.addAll(List.of("p1", "p2", "p3"));
        when(olapTable.checkPartitionNameExist("p1")).thenReturn(true);
        when(olapTable.checkPartitionNameExist("p2")).thenReturn(false);
        when(olapTable.checkPartitionNameExist("p3")).thenReturn(true);

        boolean willCreateNewPartition = CatalogUtils.checkIfNewPartitionExists(olapTable, partitionNames);
        Assertions.assertTrue(willCreateNewPartition);

        when(olapTable.checkPartitionNameExist("p1")).thenReturn(true);
        when(olapTable.checkPartitionNameExist("p2")).thenReturn(true);
        when(olapTable.checkPartitionNameExist("p3")).thenReturn(true);
        willCreateNewPartition = CatalogUtils.checkIfNewPartitionExists(olapTable, partitionNames);
        Assertions.assertFalse(willCreateNewPartition);
    }

    @Test
    public void testCalAvgBucketNumOfRecentPartitions_CalculateByDataSize() {
        List<Partition> partitions = new ArrayList<>();
        partitions.add(partition);
        when(olapTable.getPartitions()).thenReturn(partitions);
        when(olapTable.getRecentPartitions(anyInt())).thenReturn(partitions);
        when(partition.getDefaultPhysicalPartition()).thenReturn(physicalPartition);
        when(physicalPartition.getVisibleVersion()).thenReturn(2L);
        when(partition.getDataSize()).thenReturn(2L * FeConstants.AUTO_DISTRIBUTION_UNIT);

        int bucketNum = CatalogUtils.calAvgBucketNumOfRecentPartitions(olapTable, 1, true);

        assertEquals(2, bucketNum); // 2 tablets based on 2GB size
    }

    @Test
    public void testDivisibleByTwo() {
        Assertions.assertEquals(1, CatalogUtils.divisibleBucketNum(1));
        Assertions.assertEquals(2, CatalogUtils.divisibleBucketNum(2));
        Assertions.assertEquals(3, CatalogUtils.divisibleBucketNum(3));
        Assertions.assertEquals(4, CatalogUtils.divisibleBucketNum(4));
        Assertions.assertEquals(5, CatalogUtils.divisibleBucketNum(5));
        Assertions.assertEquals(6, CatalogUtils.divisibleBucketNum(6));
        Assertions.assertEquals(7, CatalogUtils.divisibleBucketNum(7));
        Assertions.assertEquals(4, CatalogUtils.divisibleBucketNum(8));
        Assertions.assertEquals(3, CatalogUtils.divisibleBucketNum(9));
        Assertions.assertEquals(5, CatalogUtils.divisibleBucketNum(10));
        Assertions.assertEquals(5, CatalogUtils.divisibleBucketNum(11));
        Assertions.assertEquals(6, CatalogUtils.divisibleBucketNum(12));
        Assertions.assertEquals(6, CatalogUtils.divisibleBucketNum(13));
        Assertions.assertEquals(7, CatalogUtils.divisibleBucketNum(14));
        Assertions.assertEquals(5, CatalogUtils.divisibleBucketNum(15));
        Assertions.assertEquals(4, CatalogUtils.divisibleBucketNum(16));
        Assertions.assertEquals(4, CatalogUtils.divisibleBucketNum(17));
        Assertions.assertEquals(3, CatalogUtils.divisibleBucketNum(18));
        Assertions.assertEquals(3, CatalogUtils.divisibleBucketNum(19));
        Assertions.assertEquals(5, CatalogUtils.divisibleBucketNum(20));
        Assertions.assertEquals(7, CatalogUtils.divisibleBucketNum(21));
    }

    @Test
    public void testExtractTxnPrefix() {
        // Valid txn prefix cases
        assertEquals("txn12345_", CatalogUtils.extractTxnPrefix("txn12345_p0"));
        assertEquals("txn1_", CatalogUtils.extractTxnPrefix("txn1_partition"));
        assertEquals("txn999999999_", CatalogUtils.extractTxnPrefix("txn999999999_abc"));

        // Invalid cases - should return null
        assertNull(CatalogUtils.extractTxnPrefix(null));
        assertNull(CatalogUtils.extractTxnPrefix(""));
        assertNull(CatalogUtils.extractTxnPrefix("p0"));
        assertNull(CatalogUtils.extractTxnPrefix("partition"));
        assertNull(CatalogUtils.extractTxnPrefix("txn"));  // no underscore
        assertNull(CatalogUtils.extractTxnPrefix("txn_"));  // no digits between txn and _
        assertNull(CatalogUtils.extractTxnPrefix("txnabc_p0"));  // non-digit characters
        assertNull(CatalogUtils.extractTxnPrefix("txn123abc_p0"));  // mixed digits and letters
        assertNull(CatalogUtils.extractTxnPrefix("TXN123_p0"));  // uppercase
        assertNull(CatalogUtils.extractTxnPrefix("txn_123_p0"));  // underscore before digits
    }

    private void alterTableWithNewAnalyzer(String sql, boolean expectedException) throws Exception {
        try {
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            DDLStmtExecutor.execute(alterTableStmt, ctx);
            if (expectedException) {
                Assertions.fail("expected exception not thrown");
            }
        } catch (Exception e) {
            if (expectedException) {
                logSysInfo("got expected exception: " + e.getMessage());
            } else {
                throw e;
            }
        }
    }

    @Test
    public void testCheckPartitionValuesExistForAddListPartition_TxnPrefixTempPartition() throws Exception {
        // Create a list partition table
        starRocksAssert.withDatabase("test_catalog_utils").useDatabase("test_catalog_utils")
                .withTable("CREATE TABLE test_catalog_utils.tbl_list(\n" +
                        "    id bigint not null,\n" +
                        "    province varchar(20) not null\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(id)\n" +
                        "PARTITION BY LIST (province) (\n" +
                        "   PARTITION p_fj VALUES IN (\"fuzhou\", \"xiamen\"),\n" +
                        "   PARTITION p_gd VALUES IN (\"shenzhen\", \"guangzhou\")\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test_catalog_utils");
        OlapTable table = (OlapTable) db.getTable("tbl_list");

        // Add a temp partition with txn prefix (simulating insert overwrite)
        String addTempStmt = "alter table test_catalog_utils.tbl_list add temporary partition txn100_p_bj " +
                "VALUES IN (\"beijing\");";
        alterTableWithNewAnalyzer(addTempStmt, false);

        // Add another temp partition from a different txn with the same value - should succeed
        // because txn prefixed temp partitions only check against the same txn
        String addTempStmt2 = "alter table test_catalog_utils.tbl_list add temporary partition txn200_p_bj " +
                "VALUES IN (\"beijing\");";
        alterTableWithNewAnalyzer(addTempStmt2, false);

        // Add a temp partition from the same txn with duplicate value - should fail
        String addTempStmt3 = "alter table test_catalog_utils.tbl_list add temporary partition txn100_p_bj2 " +
                "VALUES IN (\"beijing\");";
        alterTableWithNewAnalyzer(addTempStmt3, true);

        // Add a temp partition from the same txn with different value - should succeed
        String addTempStmt4 = "alter table test_catalog_utils.tbl_list add temporary partition txn100_p_sh " +
                "VALUES IN (\"shanghai\");";
        alterTableWithNewAnalyzer(addTempStmt4, false);

        // Clean up
        starRocksAssert.dropTable("test_catalog_utils.tbl_list");
    }

    @Test
    public void testCheckPartitionValuesExistForAddListPartition_NonTxnTempPartition() throws Exception {
        // Create a list partition table
        starRocksAssert.withDatabase("test_catalog_utils2").useDatabase("test_catalog_utils2")
                .withTable("CREATE TABLE test_catalog_utils2.tbl_list2(\n" +
                        "    id bigint not null,\n" +
                        "    province varchar(20) not null\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(id)\n" +
                        "PARTITION BY LIST (province) (\n" +
                        "   PARTITION p_fj VALUES IN (\"fuzhou\", \"xiamen\"),\n" +
                        "   PARTITION p_gd VALUES IN (\"shenzhen\", \"guangzhou\")\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");");

        // Add a non-txn temp partition
        String addTempStmt = "alter table test_catalog_utils2.tbl_list2 add temporary partition tp_bj " +
                "VALUES IN (\"beijing\");";
        alterTableWithNewAnalyzer(addTempStmt, false);

        // Add another non-txn temp partition with duplicate value - should fail
        String addTempStmt2 = "alter table test_catalog_utils2.tbl_list2 add temporary partition tp_bj2 " +
                "VALUES IN (\"beijing\");";
        alterTableWithNewAnalyzer(addTempStmt2, true);

        // Add a txn-prefixed temp partition with the same value - should succeed
        // because non-txn temp partitions don't check against txn-prefixed ones
        String addTempStmt3 = "alter table test_catalog_utils2.tbl_list2 add temporary partition txn100_p_bj " +
                "VALUES IN (\"beijing\");";
        alterTableWithNewAnalyzer(addTempStmt3, false);

        // Clean up
        starRocksAssert.dropTable("test_catalog_utils2.tbl_list2");
    }

    @Test
    public void testCheckPartitionValuesExistForAddListPartition_TempNotCheckFormal() throws Exception {
        // Create a list partition table with formal partition containing "beijing"
        starRocksAssert.withDatabase("test_catalog_utils3").useDatabase("test_catalog_utils3")
                .withTable("CREATE TABLE test_catalog_utils3.tbl_list3(\n" +
                        "    id bigint not null,\n" +
                        "    province varchar(20) not null\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(id)\n" +
                        "PARTITION BY LIST (province) (\n" +
                        "   PARTITION p_bj VALUES IN (\"beijing\"),\n" +
                        "   PARTITION p_gd VALUES IN (\"shenzhen\", \"guangzhou\")\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");");

        // Add a temp partition with the same value as formal partition - should succeed
        // because temp partitions don't check against formal partitions
        String addTempStmt = "alter table test_catalog_utils3.tbl_list3 add temporary partition tp_bj " +
                "VALUES IN (\"beijing\");";
        alterTableWithNewAnalyzer(addTempStmt, false);

        // Add a txn-prefixed temp partition with the same value as formal partition - should succeed
        String addTempStmt2 = "alter table test_catalog_utils3.tbl_list3 add temporary partition txn100_p_bj " +
                "VALUES IN (\"beijing\");";
        alterTableWithNewAnalyzer(addTempStmt2, false);

        // Clean up
        starRocksAssert.dropTable("test_catalog_utils3.tbl_list3");
    }

    @Test
    public void testCheckPartitionValuesExistForAddListPartition_FormalChecksFormal() throws Exception {
        // Create a list partition table
        starRocksAssert.withDatabase("test_catalog_utils4").useDatabase("test_catalog_utils4")
                .withTable("CREATE TABLE test_catalog_utils4.tbl_list4(\n" +
                        "    id bigint not null,\n" +
                        "    province varchar(20) not null\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(id)\n" +
                        "PARTITION BY LIST (province) (\n" +
                        "   PARTITION p_fj VALUES IN (\"fuzhou\", \"xiamen\"),\n" +
                        "   PARTITION p_gd VALUES IN (\"shenzhen\", \"guangzhou\")\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");");

        // Add a formal partition with duplicate value - should fail
        String addStmt = "alter table test_catalog_utils4.tbl_list4 add partition p_fj2 " +
                "VALUES IN (\"fuzhou\");";
        alterTableWithNewAnalyzer(addStmt, true);

        // Add a formal partition with new value - should succeed
        String addStmt2 = "alter table test_catalog_utils4.tbl_list4 add partition p_bj " +
                "VALUES IN (\"beijing\");";
        alterTableWithNewAnalyzer(addStmt2, false);

        // Add a temp partition with duplicate value of formal partition, then test adding formal partition
        // First add temp partition with "shanghai"
        String addTempStmt = "alter table test_catalog_utils4.tbl_list4 add temporary partition tp_sh " +
                "VALUES IN (\"shanghai\");";
        alterTableWithNewAnalyzer(addTempStmt, false);

        // Add formal partition with same value as temp partition - should succeed
        // because formal partitions don't check against temp partitions
        String addStmt3 = "alter table test_catalog_utils4.tbl_list4 add partition p_sh " +
                "VALUES IN (\"shanghai\");";
        alterTableWithNewAnalyzer(addStmt3, false);

        // Clean up
        starRocksAssert.dropTable("test_catalog_utils4.tbl_list4");
    }

    @Test
    public void testCheckPartitionValuesExistForAddListPartition_DirectMethodCall() throws Exception {
        // Create a list partition table for direct method testing
        starRocksAssert.withDatabase("test_catalog_utils5").useDatabase("test_catalog_utils5")
                .withTable("CREATE TABLE test_catalog_utils5.tbl_list5(\n" +
                        "    id bigint not null,\n" +
                        "    province varchar(20) not null\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(id)\n" +
                        "PARTITION BY LIST (province) (\n" +
                        "   PARTITION p_bj VALUES IN (\"beijing\"),\n" +
                        "   PARTITION p_gd VALUES IN (\"shenzhen\")\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test_catalog_utils5");
        OlapTable table = (OlapTable) db.getTable("tbl_list5");

        // Create a SingleItemListPartitionDesc for testing
        SingleItemListPartitionDesc partitionDesc = new SingleItemListPartitionDesc(
                false, "tp_test", Arrays.asList("hangzhou"), null);
        // Set column def list for the partition desc via analyze method
        List<ColumnDef> columnDefList = new ArrayList<>();
        columnDefList.add(new ColumnDef("province", new TypeDef(ScalarType.createVarcharType(20))));
        partitionDesc.analyze(columnDefList, null);

        // Test: temp partition should not throw exception for value not in formal partitions
        Assertions.assertDoesNotThrow(() -> {
            CatalogUtils.checkPartitionValuesExistForAddListPartition(table, partitionDesc, true);
        });

        // Test: formal partition with duplicate value should throw exception
        SingleItemListPartitionDesc dupPartitionDesc = new SingleItemListPartitionDesc(
                false, "p_test", Arrays.asList("beijing"), null);
        dupPartitionDesc.analyze(columnDefList, null);

        Assertions.assertThrows(DdlException.class, () -> {
            CatalogUtils.checkPartitionValuesExistForAddListPartition(table, dupPartitionDesc, false);
        });

        // Clean up
        starRocksAssert.dropTable("test_catalog_utils5.tbl_list5");
    }
}
