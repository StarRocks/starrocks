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

import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.sql.optimizer.statistics.ColumnMinMaxMgr;
import com.starrocks.sql.optimizer.statistics.IMinMaxStatsMgr;
import com.starrocks.sql.optimizer.statistics.StatsVersion;
import com.starrocks.statistic.StatisticUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Which group-by keys ApplyMinMaxStatisticRule asks ColumnMinMaxMgr about. Asking triggers a [_META_]
// collection over the whole table on a miss, so the rule must not ask for a key whose range cannot be used,
// nor pay for the table's last update time (a walk over every partition) for a scan it asks nothing about.
public class ApplyMinMaxStatisticRuleTest extends PlanTestBase {
    // Column names getStats() was called for, and table names getTableLastUpdateTimestamp() was called for.
    private final List<String> statsRequests = Collections.synchronizedList(new ArrayList<>());
    private final List<String> lastUpdateRequests = Collections.synchronizedList(new ArrayList<>());

    @BeforeEach
    public void setUp() {
        statsRequests.clear();
        lastUpdateRequests.clear();
        new MockUp<ColumnMinMaxMgr>() {
            @Mock
            public Optional<IMinMaxStatsMgr.ColumnMinMax> getStats(ColumnIdentifier identifier, StatsVersion version) {
                statsRequests.add(identifier.getColumnName().getId());
                return Optional.of(new IMinMaxStatsMgr.ColumnMinMax("0", "100"));
            }
        };
        new MockUp<StatisticUtils>() {
            @Mock
            public Long getTableLastUpdateTimestamp(Invocation invocation, Table table) {
                lastUpdateRequests.add(table.getName());
                return invocation.proceed();
            }
        };
    }

    private static int countMinMaxStats(String plan) {
        return plan.split("group by min-max stats:", -1).length - 1;
    }

    private List<String> requestedColumns() {
        return statsRequests.stream().distinct().sorted().toList();
    }

    private List<String> requestedTables() {
        return lastUpdateRequests.stream().distinct().sorted().toList();
    }

    // A1: t1a is a string without a global dict, so the aggregation can never use the compressed key, and
    // t1c must not be asked for.
    @Test
    public void testKeyWithoutRangeRulesOutAggregation() throws Exception {
        String plan = getVerboseExplain("select t1a, t1c, count(*) from test_all_type group by t1a, t1c");
        assertEquals(0, countMinMaxStats(plan), plan);
        assertEquals(List.of(), requestedColumns());
        assertEquals(List.of(), requestedTables());
    }

    // A1: an expression key has no range either.
    @Test
    public void testExpressionKeyRulesOutAggregation() throws Exception {
        String plan = getVerboseExplain("select v1 + 1, v2, count(*) from t0 group by v1 + 1, v2");
        assertEquals(0, countMinMaxStats(plan), plan);
        assertEquals(List.of(), requestedColumns());
        assertEquals(List.of(), requestedTables());
    }

    // A1: decided per aggregation. Only the aggregation whose keys may all have a range asks for them, and
    // gets the compressed key.
    @Test
    public void testMixedAggregations() throws Exception {
        String sql = "select a.v1, a.c, b.t1a, b.t1c, b.c from " +
                "(select v1, count(*) c from t0 group by v1) a join " +
                "(select t1a, t1c, count(*) c from test_all_type group by t1a, t1c) b on a.v1 = b.t1c";
        String plan = getVerboseExplain(sql);
        assertTrue(countMinMaxStats(plan) > 0, plan);
        assertEquals(List.of("v1"), requestedColumns());
        assertEquals(List.of("t0"), requestedTables());
    }

    // A2: the table's last update time is only computed for a scan that has a key to ask about.
    @Test
    public void testLastUpdateTimeOnlyForScansWithKeys() throws Exception {
        String plan = getVerboseExplain("select t0.v1, count(*) from t0 join t1 on t0.v2 = t1.v4 group by t0.v1");
        assertTrue(countMinMaxStats(plan) > 0, plan);
        assertEquals(List.of("v1"), requestedColumns());
        assertEquals(List.of("t0"), requestedTables());

        statsRequests.clear();
        lastUpdateRequests.clear();
        getVerboseExplain("select count(*) from t0 join t1 on t0.v2 = t1.v4");
        assertEquals(List.of(), requestedColumns());
        assertEquals(List.of(), requestedTables());
    }
}
