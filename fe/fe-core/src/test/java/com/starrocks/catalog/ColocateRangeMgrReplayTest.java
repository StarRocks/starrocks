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

import com.google.common.collect.Multimap;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.common.DdlException;
import com.starrocks.common.Range;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.persist.ColocateRangePersistInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Exercises the OP_COLOCATE_RANGE_UPDATE journal-load path end-to-end via
 * {@link UtFrameUtils.PseudoJournalReplayer#replayJournalToEnd()} so the test covers
 * {@link com.starrocks.persist.EditLogDeserializer} registration plus
 * {@link com.starrocks.persist.EditLog#loadJournal} routing in addition to the replay handler.
 *
 * <p>Removal of range mgr state is intentionally not journaled (the parent drop/erase journal
 * drives followers through removeTable's in-memory cleanup), so removal coverage exercises
 * removeTable directly rather than the journal path.
 */
public class ColocateRangeMgrReplayTest {

    private static final long COLOCATE_GROUP_ID = 9101L;
    private static final long DB_ID = 9100L;
    private static final long PEER_DB_ID = 9200L;
    private static final long TABLE_ID = 9102L;
    private static final long PEER_TABLE_ID = 9202L;

    private ColocateTableIndex colocateTableIndex;

    private static Tuple makeTuple(int value) {
        return new Tuple(Collections.singletonList(Variant.of(IntegerType.INT, String.valueOf(value))));
    }

    @BeforeEach
    public void setUp() {
        UtFrameUtils.setUpForPersistTest();
        colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        colocateTableIndex.getColocateRangeMgr().removeColocateGroup(COLOCATE_GROUP_ID);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
        colocateTableIndex.getColocateRangeMgr().removeColocateGroup(COLOCATE_GROUP_ID);
    }

    @Test
    public void testReplayColocateRangeUpdateViaLoadJournal() throws Exception {
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        List<ColocateRange> ranges = Arrays.asList(
                new ColocateRange(Range.lt(makeTuple(100)), 1001L),
                new ColocateRange(Range.gelt(makeTuple(100), makeTuple(200)), 1002L),
                new ColocateRange(Range.ge(makeTuple(200)), 1003L));

        // Direct journal write (mirrors what OlapTable.onCreate does after CREATE_TABLE is durable).
        GlobalStateMgr.getCurrentState().getEditLog().logColocateRangeUpdate(
                ColocateRangePersistInfo.create(COLOCATE_GROUP_ID, ranges));

        // Range mgr should be empty until replay applies the journal.
        Assertions.assertFalse(
                colocateTableIndex.getColocateRangeMgr().containsColocateGroup(COLOCATE_GROUP_ID));

        UtFrameUtils.PseudoJournalReplayer.replayJournalToEnd();

        Assertions.assertEquals(ranges,
                colocateTableIndex.getColocateRangeMgr().getColocateRanges(COLOCATE_GROUP_ID));
    }

    @Test
    public void testRemoveTableLastTableLeavesClearsRangeMgrInMemory() {
        GroupId groupId = installRangeColocateGroup(DB_ID, "test_range_group", TABLE_ID);
        colocateTableIndex.getColocateRangeMgr().setColocateRanges(COLOCATE_GROUP_ID,
                Collections.singletonList(new ColocateRange(Range.all(), 5050L)));

        Assertions.assertTrue(colocateTableIndex.removeTable(TABLE_ID, null, false /* isReplay */));
        Assertions.assertFalse(
                colocateTableIndex.getColocateRangeMgr().containsColocateGroup(COLOCATE_GROUP_ID));
    }

    @Test
    public void testRemoveTableKeepsRangeMgrWhenAnotherDbStillHasGroup() {
        // Two DBs share the same grpId via cross-DB colocation.
        installRangeColocateGroup(DB_ID, "shared_range_group", TABLE_ID);
        installRangeColocateGroup(PEER_DB_ID, "peer_shared_range_group", PEER_TABLE_ID);
        colocateTableIndex.getColocateRangeMgr().setColocateRanges(COLOCATE_GROUP_ID,
                Collections.singletonList(new ColocateRange(Range.all(), 6060L)));

        // Drop the first DB's table — peer DB still holds the group, range mgr must stay.
        Assertions.assertTrue(colocateTableIndex.removeTable(TABLE_ID, null, false /* isReplay */));
        Assertions.assertTrue(
                colocateTableIndex.getColocateRangeMgr().containsColocateGroup(COLOCATE_GROUP_ID));

        // Now drop the peer DB's table — the last reference is gone, range mgr should clear.
        Assertions.assertTrue(colocateTableIndex.removeTable(PEER_TABLE_ID, null, false /* isReplay */));
        Assertions.assertFalse(
                colocateTableIndex.getColocateRangeMgr().containsColocateGroup(COLOCATE_GROUP_ID));
    }

    @Test
    public void testModifyTableColocateRejectsRangeDistribution() {
        OlapTable table = Mockito.mock(OlapTable.class);
        // OlapTable.isRangeDistribution() reads the defaultDistributionInfo field directly rather
        // than via the accessor, so the mock has to stub it explicitly.
        Mockito.when(table.getDefaultDistributionInfo()).thenReturn(new RangeDistributionInfo());
        Mockito.when(table.isRangeDistribution()).thenReturn(true);
        Mockito.when(table.getName()).thenReturn("range_tbl");
        Mockito.when(table.getColocateGroup()).thenReturn(null);

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> colocateTableIndex.modifyTableColocate(null, table, "another_group",
                        false /* isReplay */, null));
        Assertions.assertTrue(ex.getMessage().contains("range distribution"),
                "expected range-distribution rejection message, got: " + ex.getMessage());
    }

    /** Installs a single-table range colocate group with the given dbId / table-id pair. */
    private GroupId installRangeColocateGroup(long dbId, String fullName, long tableId) {
        GroupId groupId = new GroupId(dbId, COLOCATE_GROUP_ID);
        ColocateGroupSchema schema = new ColocateGroupSchema(groupId,
                Collections.singletonList(new Column("k1", IntegerType.INT)),
                0, (short) 1, DistributionInfoType.RANGE);

        Map<GroupId, ColocateGroupSchema> group2Schema =
                Deencapsulation.getField(colocateTableIndex, "group2Schema");
        Multimap<GroupId, Long> group2Tables =
                Deencapsulation.getField(colocateTableIndex, "group2Tables");
        Map<Long, GroupId> table2Group =
                Deencapsulation.getField(colocateTableIndex, "table2Group");
        Map<String, GroupId> groupName2Id =
                Deencapsulation.getField(colocateTableIndex, "groupName2Id");

        group2Schema.put(groupId, schema);
        group2Tables.put(groupId, tableId);
        table2Group.put(tableId, groupId);
        groupName2Id.put(dbId + "_" + fullName, groupId);
        return groupId;
    }
}
