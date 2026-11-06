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

import com.google.common.collect.Sets;
import com.starrocks.catalog.OlapTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.MultiColumnCombinedStatistics;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import mockit.Expectations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JoinMultiColumnNDVCardinalityTest extends PlanWithCostTestBase {
    private OlapTable t0;
    private OlapTable t1;

    @BeforeEach
    public void before() {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        t0 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t0");
        t1 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t1");
        // crossRows = 1000 * 1000 = 1,000,000
        setTableStatistics(t0, 1000);
        setTableStatistics(t1, 1000);

        StatisticStorage ss = GlobalStateMgr.getCurrentState().getStatisticStorage();
        new Expectations(ss) {
            {
                // single-column distinct count 100 for every key column (min/max/nulls/avgSize/ndv)
                ss.getColumnStatistic(t0, "v1");
                result = new ColumnStatistic(0, 1000, 0, 8, 100);
                minTimes = 0;
                ss.getColumnStatistic(t0, "v2");
                result = new ColumnStatistic(0, 1000, 0, 8, 100);
                minTimes = 0;
                ss.getColumnStatistic(t0, "v3");
                result = new ColumnStatistic(0, 1000, 0, 8, 100);
                minTimes = 0;
                ss.getColumnStatistic(t1, "v4");
                result = new ColumnStatistic(0, 1000, 0, 8, 100);
                minTimes = 0;
                ss.getColumnStatistic(t1, "v5");
                result = new ColumnStatistic(0, 1000, 0, 8, 100);
                minTimes = 0;
                ss.getColumnStatistic(t1, "v6");
                result = new ColumnStatistic(0, 1000, 0, 8, 100);
                minTimes = 0;
            }
        };
    }

    private void mockCombinedStatsOnBothSides() {
        StatisticStorage ss = GlobalStateMgr.getCurrentState().getStatisticStorage();
        new Expectations(ss) {
            {
                ss.getMultiColumnCombinedStatistics(t0.getId());
                result = new MultiColumnCombinedStatistics(
                        Sets.newHashSet(t0.getColumn("v1").getUniqueId(), t0.getColumn("v2").getUniqueId()), 200);
                minTimes = 0;
                ss.getMultiColumnCombinedStatistics(t1.getId());
                result = new MultiColumnCombinedStatistics(
                        Sets.newHashSet(t1.getColumn("v4").getUniqueId(), t1.getColumn("v5").getUniqueId()), 100);
                minTimes = 0;
            }
        };
    }

    @Test
    public void testCompositeKeyUsesCombinedNDV() throws Exception {
        mockCombinedStatsOnBothSides();
        // 1,000,000 / max(ndv(v1,v2)=200, ndv(v4,v5)=100) = 5000
        String plan = getCostExplain("select * from t0 join t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5");
        assertContains(plan, "cardinality: 5000");
    }

    @Test
    public void testCombinedNDVIndependentOfPredicateOperandOrder() throws Exception {
        mockCombinedStatsOnBothSides();
        // operands not normalized to (left-table, right-table): the second predicate is written reversed.
        // Grouping key columns by relation (not by predicate side) must still find both per-table keys.
        String plan = getCostExplain("select * from t0 join t1 on t0.v1 = t1.v4 and t1.v5 = t0.v2");
        assertContains(plan, "cardinality: 5000");
    }

    @Test
    public void testFallsBackWhenOneSideUncovered() throws Exception {
        StatisticStorage ss = GlobalStateMgr.getCurrentState().getStatisticStorage();
        new Expectations(ss) {
            {
                // only t0 has a combined-NDV stat; t1's composite key is uncovered
                ss.getMultiColumnCombinedStatistics(t0.getId());
                result = new MultiColumnCombinedStatistics(
                        Sets.newHashSet(t0.getColumn("v1").getUniqueId(), t0.getColumn("v2").getUniqueId()), 200);
                minTimes = 0;
                ss.getMultiColumnCombinedStatistics(t1.getId());
                result = MultiColumnCombinedStatistics.EMPTY;
                minTimes = 0;
            }
        };
        // falls back to the per-predicate driving heuristic: 1,000,000 / 100 * 0.9 = 9000, not 5000
        String plan = getCostExplain("select * from t0 join t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5");
        assertContains(plan, "cardinality: 9000");
        assertNotContains(plan, "cardinality: 5000");
    }

    @Test
    public void testNullSafeEqualFallsBackToPerPredicate() throws Exception {
        mockCombinedStatsOnBothSides();
        // '<=>' matches NULL key values, so the combined-NDV path must not apply (it discounts NULLs). Falls back
        // to the per-predicate heuristic: 1,000,000 / 100 * 0.9 = 9000, not the combined 5000.
        String plan = getCostExplain("select * from t0 join t1 on t0.v1 <=> t1.v4 and t0.v2 <=> t1.v5");
        assertContains(plan, "cardinality: 9000");
        assertNotContains(plan, "cardinality: 5000");
    }
}
