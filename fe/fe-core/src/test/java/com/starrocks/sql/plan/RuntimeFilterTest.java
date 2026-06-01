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
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class RuntimeFilterTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(0);
        // Dedicated fact/dim tables so the gate tests can set row counts without leaking onto t0/t1
        // used by other tests in this class.
        starRocksAssert.withTable("CREATE TABLE `rf_fact` (`f_cust` bigint NULL, `f_region` bigint NULL,"
                + " `f_amt` bigint NULL) DUPLICATE KEY(`f_cust`) DISTRIBUTED BY HASH(`f_cust`) BUCKETS 3"
                + " PROPERTIES (\"replication_num\" = \"1\");");
        starRocksAssert.withTable("CREATE TABLE `rf_dim` (`d_cust` bigint NULL, `d_region` bigint NULL,"
                + " `d_name` bigint NULL) DUPLICATE KEY(`d_cust`) DISTRIBUTED BY HASH(`d_cust`) BUCKETS 3"
                + " PROPERTIES (\"replication_num\" = \"1\");");
    }

    private static ColumnStatistic colStat(double ndv) {
        return new ColumnStatistic(0, ndv, 0, 8, ndv);
    }

    // The build-side bloom filter is sized by the build key's NDV, not by the build row count. A shuffle
    // join whose build side has many rows but a low-cardinality join key (e.g. a 100-value region column
    // on a 5M-row dimension) must still get a runtime filter, even though the row count exceeds the limit.
    @Test
    public void testBuildGateUsesBuildKeyNdvNotRows() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        long savedMax = sv.getGlobalRuntimeFilterBuildMaxSize();
        try {
            sv.setGlobalRuntimeFilterBuildMaxSize(1_000_000);
            setTableStatistics((OlapTable) getTable("rf_fact"), 100_000_000);
            setTableStatistics((OlapTable) getTable("rf_dim"), 5_000_000);
            // build key d_region is low-cardinality (100 regions); dim has 5M rows.
            new MockUp<MockTpchStatisticStorage>() {
                @Mock
                public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                    return columns.stream().map(c -> c.equals("d_region") ? colStat(100) : colStat(5_000_000))
                            .collect(Collectors.toList());
                }
            };

            String plan = getVerboseExplain("select * from rf_fact join [shuffle] rf_dim on f_region = d_region");
            // NDV(100) is below the 1M limit, so the filter is built; the 5M row count alone would drop it.
            assertContains(plan, "build runtime filters");
        } finally {
            sv.setGlobalRuntimeFilterBuildMaxSize(savedMax);
        }
    }

    // Per-conjunct gating: a two-key shuffle join where one key is narrow (100-value region) and the other
    // is wide (5M distinct customers). The narrow key keeps its runtime filter; the wide key's filter is
    // dropped on its own NDV, not together with the narrow one (the old per-join gate dropped both).
    @Test
    public void testBuildGatePerConjunctKeepsNarrowKey() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        long savedMax = sv.getGlobalRuntimeFilterBuildMaxSize();
        try {
            sv.setGlobalRuntimeFilterBuildMaxSize(1_000_000);
            setTableStatistics((OlapTable) getTable("rf_fact"), 100_000_000);
            setTableStatistics((OlapTable) getTable("rf_dim"), 5_000_000);
            new MockUp<MockTpchStatisticStorage>() {
                @Mock
                public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                    return columns.stream().map(c -> {
                        if (c.equals("d_region")) {
                            return colStat(100);         // narrow key -> RF kept
                        } else if (c.equals("d_cust")) {
                            return colStat(5_000_000);   // wide key -> RF dropped
                        }
                        return colStat(1000);
                    }).collect(Collectors.toList());
                }
            };

            String plan = getVerboseExplain(
                    "select * from rf_fact join [shuffle] rf_dim on f_region = d_region and f_cust = d_cust");
            // Narrow key d_region keeps a filter; wide key d_cust does not.
            assertContains(plan, "build_expr = (5: d_region)");
            assertNotContains(plan, "build_expr = (4: d_cust)");
        } finally {
            sv.setGlobalRuntimeFilterBuildMaxSize(savedMax);
        }
    }

    // NDV unknown -> the gate falls back to the build row count (the original behavior). The 5M-row build
    // side exceeds the 1M limit, so the filter is dropped just as before - no regression where stats are
    // absent.
    @Test
    public void testBuildGateFallsBackToRowsWhenNdvUnknown() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        long savedMax = sv.getGlobalRuntimeFilterBuildMaxSize();
        try {
            sv.setGlobalRuntimeFilterBuildMaxSize(1_000_000);
            setTableStatistics((OlapTable) getTable("rf_fact"), 100_000_000);
            setTableStatistics((OlapTable) getTable("rf_dim"), 5_000_000);
            new MockUp<MockTpchStatisticStorage>() {
                @Mock
                public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                    return columns.stream().map(c -> ColumnStatistic.unknown()).collect(Collectors.toList());
                }
            };

            String plan = getVerboseExplain("select * from rf_fact join [shuffle] rf_dim on f_region = d_region");
            assertNotContains(plan, "build runtime filters");
        } finally {
            sv.setGlobalRuntimeFilterBuildMaxSize(savedMax);
        }
    }

    @Test
    public void testDeterministicBroadcastJoinForColocateJoin() throws Exception {
        String sql = "select * from \n" +
                "  t0 vt1 join [bucket] t0 vt2 on vt1.v1 = vt2.v1\n" +
                "  join [broadcast] t1 vt3 on vt1.v1 = vt3.v4\n" +
                "  join [colocate] t0 vt4 on vt1.v1 = vt4.v1";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  6:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  equal join conjunct: [1: v1, BIGINT, true] = [7: v4, BIGINT, true]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 1, build_expr = (7: v4), remote = true\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  |----5:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 1");

    }

    @Test
    public void testDeterministicBroadcastJoinForBroadcastJoin() throws Exception {
        String sql = "select * from \n" +
                "  t0 vt1 join [bucket] t0 vt2 on vt1.v1 = vt2.v1\n" +
                "  join [broadcast] t1 vt3 on vt1.v1 = vt3.v4\n" +
                "  join [broadcast] t0 vt4 on vt1.v1 = vt4.v1";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  |----5:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 1\n" +
                "  |       probe runtime filters:\n" +
                "  |       - filter_id = 2, probe_expr = (7: v4)");
    }
}
