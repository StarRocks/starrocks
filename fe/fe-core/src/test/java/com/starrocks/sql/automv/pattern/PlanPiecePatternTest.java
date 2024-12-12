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

package com.starrocks.sql.automv.pattern;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.automv.qe.RboOptimizer;
import com.starrocks.sql.automv.util.TestUtil;
import com.starrocks.sql.automv.util.Util;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PlanPiecePatternTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert getStarRocksAssert() {
        if (STARROCKS_ASSERT.get() == null) {
            try {
                STARROCKS_ASSERT.set(TestUtil.prepareTables("tpcds", TestUtil::getTPCDSCreateTableSqlList));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return STARROCKS_ASSERT.get();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        getStarRocksAssert();
    }

    @Test
    public void testSPJGPattern() throws Exception {
        Object[][] expectResults = new Object[][] {
                {"query01", 2},
                {"query02", 0},
                {"query03", 1},
                {"query04", 6},
                {"query05", 0},
                {"query06", 2},
                {"query07", 1},
                {"query08", 1},
                {"query09", 15},
                {"query10", 0},
                {"query11", 4},
                {"query12", 1},
                {"query13", 1},
                {"query14", 0},
                {"query14-1", 0},
                {"query14-2", 0},
                {"query15", 1},
                {"query16", 1},
                {"query17", 1},
                {"query18", 0},
                {"query19", 1},
                {"query20", 1},
                {"query21", 1},
                {"query22", 0},
                {"query23", 6},
                {"query23-1", 6},
                {"query23-2", 6},
                {"query24", 2},
                {"query24-1", 2},
                {"query24-2", 2},
                {"query25", 1},
                {"query26", 1},
                {"query27", 0},
                {"query28", 6},
                {"query29", 1},
                {"query30", 2},
                {"query31", 6},
                {"query32", 1},
                {"query33", 3},
                {"query34", 1},
                {"query35", 0},
                {"query36", 0},
                {"query37", 1},
                {"query38", 3},
                {"query39", 2},
                {"query39-1", 2},
                {"query39-1-2", 2},
                {"query39-2", 2},
                {"query39-2-2", 2},
                {"query40", 1},
                {"query41", 1},
                {"query42", 1},
                {"query43", 1},
                {"query44", 4},
                {"query45", 0},
                {"query46", 1},
                {"query47", 3},
                {"query48", 1},
                {"query49", 3},
                {"query50", 1},
                {"query51", 2},
                {"query52", 1},
                {"query53", 1},
                {"query54", 2},
                {"query55", 1},
                {"query56", 3},
                {"query57", 3},
                {"query58", 0},
                {"query59", 2},
                {"query60", 3},
                {"query61", 2},
                {"query62", 1},
                {"query63", 1},
                {"query64", 2},
                {"query64-2", 2},
                {"query65", 2},
                {"query66", 2},
                {"query67", 0},
                {"query68", 1},
                {"query69", 1},
                {"query70", 1},
                {"query71", 0},
                {"query72", 1},
                {"query73", 1},
                {"query74", 4},
                {"query75", 0},
                {"query76", 0},
                {"query77", 6},
                {"query78", 3},
                {"query79", 1},
                {"query80", 3},
                {"query81", 2},
                {"query82", 1},
                {"query83", 3},
                {"query84", 0},
                {"query85", 1},
                {"query86", 0},
                {"query87", 3},
                {"query88", 8},
                {"query89", 1},
                {"query90", 2},
                {"query91", 1},
                {"query92", 1},
                {"query93", 1},
                {"query94", 1},
                {"query95", 1},
                {"query96", 1},
                {"query97", 2},
                {"query98", 1},
                {"query99", 1},
        };
        Iterator<Object[]> nextResult = Arrays.asList(expectResults).iterator();
        TestUtil.getTPCDSQueryList().forEach(p -> {
            String name = p.first;
            String sql = p.second;
            ConnectContext ctx = getStarRocksAssert().getCtx();
            List<OptExpression> subPlans = RboOptimizer.getSubPlans(sql, ctx, PlanPiecePatterns.getSPJG()).second;
            Assert.assertTrue(nextResult.hasNext());
            Object[] result = nextResult.next();
            String expectName = (String) result[0];
            Integer expectSize = (Integer) result[1];
            // System.out.printf("{\"%s\", %d},\n", name, subPlans.size());
            Assert.assertEquals(expectName, name);
            Assert.assertEquals(expectSize.intValue(), subPlans.size());
            Set<OperatorType> acceptedTypes = ImmutableSet.of(
                    OperatorType.LOGICAL_PROJECT,
                    OperatorType.LOGICAL_AGGR,
                    OperatorType.LOGICAL_JOIN,
                    OperatorType.LOGICAL_OLAP_SCAN);
            for (OptExpression optExp : subPlans) {
                List<Operator> operators = Util.getStream(optExp).collect(Collectors.toList());
                Assert.assertEquals(operators.get(0).getOpType(), OperatorType.LOGICAL_AGGR);
                Assert.assertTrue(
                        operators.stream().map(Operator::getOpType).allMatch(acceptedTypes::contains));
            }
        });
    }

    @Test
    public void test11MVPattern() throws Exception {
        List<Pair<String, List<OptExpression>>> subPlanLists = TestUtil.getTPCDSQueryList().stream()
                //.filter(p -> p.first.equals("query01"))
                .map(p -> {
                    String name = p.first;
                    String sql = p.second;
                    ConnectContext ctx = getStarRocksAssert().getCtx();
                    List<OptExpression> subPlans =
                            RboOptimizer.getSubPlans(sql, ctx, PlanPiecePatterns.get11MV()).second;
                    Map<String, List<OptExpression>> subPlanGroups = subPlans.stream()
                            .collect(Collectors.groupingBy(sp -> sp.getOp().getOpType().name()));
                    return Pair.create(name, subPlanGroups);
                })
                .flatMap(p -> p.second.entrySet()
                        .stream()
                        .map(e -> Pair.create(p.first + "." + e.getKey(), e.getValue())))
                .collect(Collectors.toList());

        Function<String, String> lastComp = s -> {
            String[] comps = s.split("\\.");
            return comps[comps.length - 1];
        };

        Map<String, List<OptExpression>> subPlanGroupByRootOp = subPlanLists.stream()
                .map(p -> Pair.create(lastComp.apply(p.first), p.second))
                .collect(Collectors.groupingBy(p -> p.first,
                        Collectors.flatMapping(p -> p.second.stream(), Collectors.toList())));
        Map<String, Integer> expectResults = ImmutableMap.<String, Integer>builder()
                .put("LOGICAL_JOIN", 122)
                .put("LOGICAL_AGGR", 128)
                .put("LOGICAL_OLAP_SCAN", 55)
                .build();
        Map<String, Integer> actualResults = subPlanGroupByRootOp.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
        Assert.assertEquals(expectResults, actualResults);
    }
}
