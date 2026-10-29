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

package com.starrocks.alter;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.compaction.PartitionIdentifier;
import com.starrocks.lake.compaction.PartitionStatistics;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.CompactionClause;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class CompactionHandlerTest {

    private static Database db;
    private static OlapTable olapTable;
    private static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;

    private CompactionHandler compactionHandler = new CompactionHandler();
    private final boolean savedEnableRangeDistribution = Config.enable_range_distribution;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(GlobalStateMgrTestUtil.testDb1)
                    .useDatabase(GlobalStateMgrTestUtil.testDb1);

        starRocksAssert.withTable("CREATE TABLE testTable1\n" +
                    "(\n" +
                    "    v1 date,\n" +
                    "    v2 int,\n" +
                    "    v3 int\n" +
                    ")\n" +
                    "DUPLICATE KEY(`v1`)\n" +
                    "PARTITION BY RANGE(v1)\n" +
                    "(\n" +
                    "    PARTITION p1 values less than('2020-02-01'),\n" +
                    "    PARTITION p2 values less than('2020-03-01')\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(v1) BUCKETS 3\n" +
                    "PROPERTIES('replication_num' = '1');");

        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
    }

    @AfterEach
    public void tearDown() {
        Config.enable_range_distribution = savedEnableRangeDistribution;
        GlobalStateMgr.getCurrentState().getCompactionMgr().clearPartitions();
        db.dropTable(olapTable.getName());
    }

    private void processAlterClauses(List<String> partitionNames, int expectedValue) {
        CompactionClause compactionClause = new CompactionClause(partitionNames, true, null);
        List<AlterClause> alterList = Collections.singletonList(compactionClause);
        try {
            compactionHandler.process(alterList, db, olapTable);
            Assertions.assertEquals(expectedValue, GlobalStateMgr.getCurrentState().getCompactionMgr().getPartitionStatsCount());
        } catch (StarRocksException e) {
            e.printStackTrace();
            Assertions.fail("process should not throw exceptions here");
        }
    }

    @Test
    public void testProcessCompactionClauseWithOnePartitionForLakeTable() {
        processAlterClauses(Collections.singletonList("p1"), 1);
    }

    @Test
    public void testProcessCompactionClauseWithTwoPartitionForLakeTable() {
        processAlterClauses(Arrays.asList("p1", "p2"), 2);
    }

    @Test
    public void testProcessCompactionClauseWithNoPartitionsForLakeTable() {
        // compaction should trigger on all partitions
        processAlterClauses(Collections.emptyList(), 2);
    }

    @Test
    public void testProcessNonCompactionClause() {
        assertThrows(IllegalStateException.class, () -> {
            AlterClause nonCompactionClause = mock(AlterClause.class);
            List<AlterClause> alterList = Collections.singletonList((nonCompactionClause));
            try {
                compactionHandler.process(alterList, db, olapTable);
            } catch (StarRocksException e) {
                Assertions.fail("process should not throw user exceptions here");
            }
        });
    }

    /** Marks every tablet clean except the first one, which gets {@code firstShared}. */
    private void markTablets(OlapTable table, boolean firstShared) {
        boolean first = true;
        for (PhysicalPartition partition : table.getPhysicalPartitions()) {
            for (MaterializedIndex index : partition.getLatestMaterializedIndices(
                    MaterializedIndex.IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    ((LakeTablet) tablet).setHasSharedFiles(first && firstShared);
                    first = false;
                }
            }
        }
    }

    /**
     * Creates the table {@code name (k1 int, v1 int)} with the given key and distribution clauses, runs
     * ALTER TABLE COMPACT on it, and returns the compaction priority this leaves on its first partition.
     */
    private PartitionStatistics.CompactionPriority compactTable(String name, String clauses, boolean firstShared)
            throws Exception {
        Config.enable_range_distribution = true;
        starRocksAssert.withTable("CREATE TABLE " + name + " (k1 int, v1 int) " + clauses
                + " PROPERTIES('replication_num' = '1')");
        OlapTable table =
                (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), name);
        try {
            markTablets(table, firstShared);
            PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
            compactionHandler.process(
                    Collections.singletonList(new CompactionClause(Collections.emptyList(), true, null)), db, table);
            return GlobalStateMgr.getCurrentState().getCompactionMgr()
                    .getStatistics(new PartitionIdentifier(db.getId(), table.getId(), partition.getId())).getPriority();
        } finally {
            db.dropTable(name);
        }
    }

    private static final String RANGE_PK_CLAUSES = "PRIMARY KEY(k1) ORDER BY(k1)";

    @Test
    public void testRangePrimaryKeyTableWithSharedTabletRoutesToUnshare() throws Exception {
        Assertions.assertEquals(PartitionStatistics.CompactionPriority.UNSHARE,
                compactTable("range_pk", RANGE_PK_CLAUSES, true));
    }

    /**
     * The FE's view of shared files can be stale, as after a restart, and does not cover an index file that reaches
     * outside its tablet, so COMPACT does not consult it: every tablet decides for itself whether it unshares or
     * runs the manual compaction.
     */
    @Test
    public void testRangePrimaryKeyTableWithoutSharedTabletRoutesToUnshare() throws Exception {
        Assertions.assertEquals(PartitionStatistics.CompactionPriority.UNSHARE,
                compactTable("range_pk", RANGE_PK_CLAUSES, false));
    }

    @Test
    public void testRangePrimaryKeyTableWithSeparateSortKeyKeepsManualCompact() throws Exception {
        Assertions.assertEquals(PartitionStatistics.CompactionPriority.MANUAL_COMPACT,
                compactTable("range_pk_sk", "PRIMARY KEY(k1) ORDER BY(v1)", true));
    }

    @Test
    public void testHashPrimaryKeyTableWithSharedTabletKeepsManualCompact() throws Exception {
        Assertions.assertEquals(PartitionStatistics.CompactionPriority.MANUAL_COMPACT,
                compactTable("hash_pk", "PRIMARY KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 3", true));
    }

    @Test
    public void testRangeDuplicateKeyTableWithSharedTabletKeepsManualCompact() throws Exception {
        Assertions.assertEquals(PartitionStatistics.CompactionPriority.MANUAL_COMPACT,
                compactTable("range_dup", "ORDER BY(k1)", true));
    }
}
