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
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.sql.plan.TPCDSPlanTestBase;
import com.starrocks.sql.plan.TPCDSTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class MaterializedViewTextBasedRewriteTPCDSTest extends MaterializedViewTestBase {
    private static final List<List<Arguments>> ARGUMENTS = Lists.newArrayList();
    private static final int N = 5;

    private static final String MATERIALIZED_DB_NAME = "mv_db";
    private static final String TABLE_DB_NAME = "table_db";
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        MaterializedViewTestBase.beforeClass();
        starRocksAssert.withDatabase(MATERIALIZED_DB_NAME);
        starRocksAssert
                .withDatabase(TABLE_DB_NAME)
                .useDatabase(TABLE_DB_NAME);
        connectContext.getSessionVariable().setEnableMaterializedViewTextMatchRewrite(true);
        TPCDSTestUtil.prepareTables(starRocksAssert);
        prepareArguments();
    }

    @ParameterizedTest(name = "Tpcds.{0}")
    @MethodSource("tpcdsSource0")
    public void testTPCDS0(String name, String sql) {
        starRocksAssert.useDatabase(TABLE_DB_NAME);
        testRewriteOK(MATERIALIZED_DB_NAME, sql, sql, "");
    }

    @ParameterizedTest(name = "Tpcds.{0}")
    @MethodSource("tpcdsSource1")
    public void testTPCDS1(String name, String sql) {
        testRewriteOK(sql, sql);
    }

    @ParameterizedTest(name = "Tpcds.{0}")
    @MethodSource("tpcdsSource2")
    public void testTPCDS2(String name, String sql) {
        testRewriteOK(sql, sql);
    }

    @ParameterizedTest(name = "Tpcds.{0}")
    @MethodSource("tpcdsSource3")
    public void testTPCDS3(String name, String sql) {
        testRewriteOK(sql, sql);
    }

    @ParameterizedTest(name = "Tpcds.{0}")
    @MethodSource("tpcdsSource4")
    public void testTPCDS4(String name, String sql) {
        testRewriteOK(sql, sql);
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

    private static Stream<Arguments> tpcdsSource4() {
        return ARGUMENTS.get(4).stream();
    }

    private static Stream<Arguments> prepareArguments() {
        List<Arguments> cases = Lists.newArrayList();
        Set<String> filteredQueries = Sets.newHashSet("query39-1", "query39-2", "query64");

        // prepare argument, split arguments into 4 parts to avoid cost too much time
        for (int i = 0; i < N; i++) {
            ARGUMENTS.add(Lists.newArrayList());
        }

        int i = 0;
        for (Map.Entry<String, String> entry : TPCDSPlanTestBase.getSqlMap().entrySet()) {
            if (filteredQueries.contains(entry.getKey())) {
                continue;
            }
            ARGUMENTS.get(i++ % N).add(Arguments.of(entry.getKey(), entry.getValue()));
        }
        return cases.stream();
    }
}
