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

import com.starrocks.catalog.OlapTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import mockit.Expectations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// Verifies the FE gate that decides whether the group-by NDV estimate is propagated
// to the BE via TAggregationNode.estimated_cardinality (the hash-table reserve input).
public class AggReserveCardinalityTest extends PlanWithCostTestBase {
    private static final long T0_ROWS = 1_000_000_000L;

    @BeforeEach
    public void before() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable t0 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t0");
        setTableStatistics(t0, T0_ROWS);

        StatisticStorage ss = GlobalStateMgr.getCurrentState().getStatisticStorage();
        new Expectations(ss) {
            {
                // v1: high NDV -> agg cardinality well above the min-reserve threshold.
                ss.getColumnStatistic(t0, "v1");
                result = new ColumnStatistic(1, T0_ROWS, 0, 8, T0_ROWS / 2.0);
                minTimes = 0;
                // v2: aggregated column, arbitrary known stat.
                ss.getColumnStatistic(t0, "v2");
                result = new ColumnStatistic(1, 1000, 0, 8, 1000);
                minTimes = 0;
                // v3: low NDV -> agg cardinality below the threshold.
                ss.getColumnStatistic(t0, "v3");
                result = new ColumnStatistic(1, 100, 0, 8, 100);
                minTimes = 0;
            }
        };
    }

    @Test
    public void highNdvAggCarriesEstimate() throws Exception {
        String thrift = getThriftPlan("select v1, count(v2) from t0 group by v1");
        Assertions.assertTrue(thrift.contains("estimated_cardinality"),
                "high-NDV aggregation with known stats should carry the NDV estimate to the BE");
    }

    @Test
    public void lowNdvBelowThresholdHasNoEstimate() throws Exception {
        String thrift = getThriftPlan("select v3, count(v2) from t0 group by v3");
        Assertions.assertFalse(thrift.contains("estimated_cardinality"),
                "below-threshold cardinality must not carry an NDV estimate");
    }
}
