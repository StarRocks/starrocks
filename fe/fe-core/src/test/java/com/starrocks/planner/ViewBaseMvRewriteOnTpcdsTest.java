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
import com.google.common.collect.Sets;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.plan.TPCDSPlanTestBase;
import com.starrocks.sql.plan.TPCDSTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class ViewBaseMvRewriteOnTpcdsTest extends MaterializedViewTestBase {
    private static final List<List<Arguments>> ARGUMENTS = Lists.newArrayList();
    private static final int N = 4;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        MaterializedViewTestBase.beforeClass();

        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(true);

        starRocksAssert.useDatabase("test");
        connectContext.executeSql("drop table if exists customer");
        TPCDSTestUtil.prepareTables(starRocksAssert);

        // prepare argument, split arguments into 4 parts to avoid cost too much time
        for (int i = 0; i < N; i++) {
            ARGUMENTS.add(Lists.newArrayList());
        }

        int i = 0;
        for (Map.Entry<String, String> entry : TPCDSPlanTestBase.getSqlMap().entrySet()) {
            // these queries are not supported because they has duplicate output column names
            Set<String> filteredQueries = Sets.newHashSet("query39-1", "query39-2", "query64");
            if (!filteredQueries.contains(entry.getKey())) {
                ARGUMENTS.get(i++ % N).add(Arguments.of(entry.getKey(), entry.getValue()));
            }
        }
    }

    private void testMvRewrite(String name, String sql) throws Exception {
        String viewName = String.format("v_%s", name).replace("-", "_");
        String dropViewSql = "drop view if exists " + viewName;
        connectContext.executeSql(dropViewSql);
        String createViewSql = String.format("create view %s as %s", viewName, sql);
        starRocksAssert.withView(createViewSql);
        {
            String query = String.format("select * from %s", viewName);
            String mv = query;
            testRewriteOK(mv, query);
        }
    }

    private static Stream<Arguments> tpcdsSource0() {
        return ARGUMENTS.get(0).stream();
    }

    private static Stream<Arguments> tpcdsSource1() {
        return ARGUMENTS.get(1).stream();
    }

    private static Stream<Arguments> tpcdsSource2() {
        return ARGUMENTS.get(2).stream();
    }

    private static Stream<Arguments> tpcdsSource3() {
        return ARGUMENTS.get(3).stream();
    }

    @ParameterizedTest(name = "ViewBasedRewriteOnTpcds.{0}")
    @MethodSource("tpcdsSource0")
    public void testViewRewriteWithTPCDS0(String name, String sql) throws Exception {
        testMvRewrite(name, sql);
    }

    @ParameterizedTest(name = "ViewBasedRewriteOnTpcds.{0}")
    @MethodSource("tpcdsSource1")
    public void testViewRewriteWithTPCDS1(String name, String sql) throws Exception {
        testMvRewrite(name, sql);
    }

    @ParameterizedTest(name = "ViewBasedRewriteOnTpcds.{0}")
    @MethodSource("tpcdsSource2")
    public void testViewRewriteWithTPCDS2(String name, String sql) throws Exception {
        testMvRewrite(name, sql);
    }

    @ParameterizedTest(name = "ViewBasedRewriteOnTpcds.{0}")
    @MethodSource("tpcdsSource3")
    public void testViewRewriteWithTPCDS3(String name, String sql) throws Exception {
        testMvRewrite(name, sql);
    }
}
