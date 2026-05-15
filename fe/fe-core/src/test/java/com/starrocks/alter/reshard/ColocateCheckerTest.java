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

package com.starrocks.alter.reshard;

import com.staros.proto.PlacementPolicy;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateRangeMgr;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Range;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Exercises {@link ColocateChecker}'s core decision paths:
 *   - unstable + already-aligned group is marked stable in one cycle;
 *   - unstable + misaligned partition triggers an alignment {@link SplitTabletJob};
 *   - no unstable groups => no work.
 *
 * <p>Calls {@link ColocateChecker#runOneCycle()} directly so the tests focus on the
 * checking body rather than the host {@link TabletReshardJobMgr}'s scheduler.
 */
public class ColocateCheckerTest {

    protected static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;
    private static Database db;

    private OlapTable table;
    private ColocateTableIndex.GroupId groupId;
    private static final AtomicInteger NEXT_TABLE_SEQ = new AtomicInteger();

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        Config.enable_range_distribution = true;

        starRocksAssert.withDatabase("p4_align_test").useDatabase("p4_align_test");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("p4_align_test");
    }

    @BeforeEach
    public void setUp() throws Exception {
        // Each test gets a fresh colocate group so prior tests' ColocateRangeMgr mutations do
        // not leak across tests; the cluster shares one ColocateTableIndex instance.
        int seq = NEXT_TABLE_SEQ.incrementAndGet();
        String tableName = "t_p4_align_" + seq;
        String sql = "create table " + tableName + " (k1 int, k2 int)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1', 'colocate_with' = 'p4_align_grp_" + seq + ":k1');";
        starRocksAssert.withTable(sql);
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), tableName);
        groupId = GlobalStateMgr.getCurrentState().getColocateTableIndex().getGroup(table.getId());
    }

    @AfterEach
    public void tearDown() {
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        if (groupId != null && colocateTableIndex.isGroupUnstable(groupId)) {
            colocateTableIndex.markGroupStable(groupId, /* needEditLog */ false);
        }
    }

    @Test
    public void testAlignedUnstableGroupBecomesStable() throws Exception {
        // The freshly-created table has one tablet covering the single default ColocateRange
        // [MIN, MAX) -> already aligned. Marking the group unstable then running the checker
        // must find every partition aligned and mark every peer GroupId stable.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        colocateTableIndex.markGroupUnstable(groupId, /* needEditLog */ false);
        Assertions.assertTrue(colocateTableIndex.isGroupUnstable(groupId));

        ColocateChecker checker = new ColocateChecker();
        checker.runOneCycle();

        Assertions.assertFalse(colocateTableIndex.isGroupUnstable(groupId),
                "aligned + unstable group must be marked stable after one cycle");
    }

    @Test
    public void testMisalignedTabletTriggersAlignmentJob() throws Exception {
        // Splice a synthetic second ColocateRange entry at colocate-prefix 100 to create
        // misalignment: the table's single tablet spans [MIN, MAX) but two ColocateRanges now exist.
        // The checker must (a) detect misalignment, (b) compute the canonical boundary at k1=100,
        // and (c) submit one alignment SplitTabletJob.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        spliceSecondColocateRangeAt100AndMarkUnstable();

        ColocateChecker checker = new ColocateChecker();
        checker.runOneCycle();

        // Filter by this test's table id: the host TabletReshardJobMgr scheduler may have
        // ticked concurrently and submitted its own alignment job for the same unstable group,
        // so the total job count can be >= 1 — assert at least one alignment job exists for
        // *our* table, which is sufficient evidence the checker did its work.
        Assertions.assertTrue(countAlignmentJobsForTable(table.getId()) >= 1,
                "misaligned partition must trigger at least one alignment SplitTabletJob");

        // Group must remain unstable: alignment job is pending; only after every peer is aligned
        // does the checker mark stable. Submitting the job is not enough.
        Assertions.assertTrue(colocateTableIndex.isGroupUnstable(groupId),
                "group must remain unstable until the alignment job actually publishes");
    }

    @Test
    public void testTableInNonNormalStateIsSkipped() throws Exception {
        // A misaligned partition would normally trigger an alignment job, but if the table
        // is mid-alter (state != NORMAL) the checker must skip it this cycle to avoid the
        // SplitTabletJob.setTableState(NORMAL -> TABLET_RESHARD) precondition failure, and
        // must leave the group unstable so a later cycle retries.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        spliceSecondColocateRangeAt100AndMarkUnstable();

        OlapTable.OlapTableState originalState = table.getState();
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        try {
            long ourJobsBefore = countAlignmentJobsForTable(table.getId());

            ColocateChecker checker = new ColocateChecker();
            checker.runOneCycle();

            long ourJobsAfter = countAlignmentJobsForTable(table.getId());
            Assertions.assertEquals(ourJobsBefore, ourJobsAfter,
                    "non-NORMAL table state must skip alignment job submission");
            Assertions.assertTrue(colocateTableIndex.isGroupUnstable(groupId),
                    "group must remain unstable so a later cycle retries once state returns to NORMAL");
        } finally {
            table.setState(originalState);
        }
    }

    private static long countAlignmentJobsForTable(long tableId) {
        return GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().getTabletReshardJobs().values().stream()
                .filter(j -> j.getJobType() == TabletReshardJob.JobType.SPLIT_TABLET)
                .filter(j -> j instanceof SplitTabletJob)
                .map(j -> (SplitTabletJob) j)
                .filter(j -> j.getTableId() == tableId)
                .count();
    }

    /**
     * Synthetically splices a second {@link ColocateRange} entry at colocate-prefix 100 and
     * marks the group unstable. The table starts with a single tablet spanning {@code [MIN, MAX)}
     * — afterwards there are two ColocateRanges {@code [MIN, 100)} / {@code [100, MAX)} but the
     * tablet still spans the whole space, so the layout is misaligned against the group's
     * expected ranges.
     */
    private void spliceSecondColocateRangeAt100AndMarkUnstable() throws DdlException {
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateRangeMgr rangeMgr = colocateTableIndex.getColocateRangeMgr();
        long colocateGroupId = groupId.grpId;
        long firstShardGroupId = rangeMgr.getColocateRanges(colocateGroupId).get(0).getShardGroupId();
        long secondShardGroupId = GlobalStateMgr.getCurrentState().getStarOSAgent().createShardGroup(
                db.getId(), table.getId(), 0L, 0L, PlacementPolicy.PACK);
        Tuple boundaryPrefix = new Tuple(Arrays.asList(Variant.of(IntegerType.INT, "100")));
        rangeMgr.setColocateRanges(colocateGroupId, Arrays.asList(
                new ColocateRange(Range.lt(boundaryPrefix), firstShardGroupId),
                new ColocateRange(Range.ge(boundaryPrefix), secondShardGroupId)));
        colocateTableIndex.markGroupUnstable(groupId, /* needEditLog */ false);
    }

    @Test
    public void testNoUnstableGroupNoOp() throws Exception {
        // With no unstable groups, the checker must produce no work even with the table present.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        Assertions.assertFalse(colocateTableIndex.isGroupUnstable(groupId));

        int jobsBefore = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr()
                .getTabletReshardJobs().size();

        ColocateChecker checker = new ColocateChecker();
        checker.runOneCycle();

        Assertions.assertFalse(colocateTableIndex.isGroupUnstable(groupId));
        Assertions.assertEquals(jobsBefore,
                GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().getTabletReshardJobs().size(),
                "no unstable groups => no alignment jobs submitted");
    }
}
