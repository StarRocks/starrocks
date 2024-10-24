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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.catalog.OlapTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.logical.LogicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionCloner;
import com.starrocks.sql.plan.MockTpchStatisticStorage;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class OptExpressionClonerTPCHTest extends MaterializedViewTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        MaterializedViewTestBase.beforeClass();
        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);

        executeSqlFile("sql/materialized-view/tpch/ddl_tpch.sql");
        connectContext.getSessionVariable().setEnableMaterializedViewTextMatchRewrite(true);

        int scale = 1;
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        connectContext.getGlobalStateMgr().setStatisticStorage(new MockTpchStatisticStorage(connectContext, scale));
        OlapTable t4 = (OlapTable) globalStateMgr.getLocalMetastore().getDb(MATERIALIZED_DB_NAME).getTable("customer");
        setTableStatistics(t4, 150000 * scale);
        OlapTable t7 = (OlapTable) globalStateMgr.getLocalMetastore().getDb(MATERIALIZED_DB_NAME).getTable("lineitem");
        setTableStatistics(t7, 6000000 * scale);
    }

    @ParameterizedTest(name = "Tpch.{0}")
    @MethodSource("tpchSource")
    public void testTPCH(String name, String sql) {
        OptExpression optExpression = UtFrameUtils.getQueryOptExpression(connectContext, sql);
        OptExpression cloned = OptExpressionCloner.clone(optExpression);
        Assert.assertNotNull(cloned);
        cloned.getOp().equals(optExpression.getOp());
        Assert.assertTrue(areOptExpressionEquals(optExpression, cloned));
    }

    public static boolean areOptExpressionEquals(OptExpression optExpression, OptExpression cloned) {
        if (optExpression.getOp() != null && cloned == null) {
            System.out.println("OptExpression op not equals: " + optExpression.getOp() + " != null");
            return false;
        }

        // TODO: LogicalFilter's equals use reference equals
        if (!(optExpression.getOp() instanceof LogicalFilterOperator)
                && !(optExpression.getOp() instanceof LogicalAssertOneRowOperator)
                && !optExpression.getOp().equals(cloned.getOp())) {
            System.out.println("OptExpression op not equals: " + optExpression.getOp() + " != " + cloned.getOp());
            return false;
        }
        if (optExpression.getInputs().size() != cloned.getInputs().size()) {
            System.out.println("OptExpression size not equals: " + optExpression.getInputs().size()
                    + " != " + cloned.getInputs().size());
            return false;
        }
        int size = optExpression.getInputs().size();
        for (int i = 0; i < size; i ++) {
            OptExpression child1 = optExpression.getInputs().get(i);
            OptExpression child2 = cloned.getInputs().get(i);

            if (!areOptExpressionEquals(child1, child2)) {
                System.out.println("OptExpression not equals: " + child1.getOp() + " != " + child2.getOp());
                return false;
            }
        }
        return true;
    }

    private static Stream<Arguments> tpchSource() {
        List<Arguments> cases = Lists.newArrayList();
        for (Map.Entry<String, String> entry : TpchSQL.getAllSQL().entrySet()) {
            cases.add(Arguments.of(entry.getKey(), entry.getValue()));
        }
        return cases.stream();
    }
}
