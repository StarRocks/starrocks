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
import com.google.common.collect.Maps;
import com.starrocks.common.FeConstants;
import com.starrocks.common.structure.Pair;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.plan.TPCDSTestUtil;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.DEFAULT_CREATE_TABLE_TEMPLATE;

public class TablePruningTestTPCDS extends TablePruningTestBase {
    private static void prepareTPCDSTables() throws Exception {
        TPCDSTestUtil.prepareTables(starRocksAssert);
    }

    private static void prepareUniqueKeys() {
        getSqlList("sql/tpcds_constraints/", "AddUniqueKeys")
                .forEach(stmts -> Arrays.stream(stmts.split("\n")).forEach(q ->
                        {
                            try {
                                starRocksAssert.alterTableProperties(q);
                            } catch (Exception e) {
                                Assert.fail();
                            }
                        }
                ));
    }

    private static void prepareForeignKeys() {
        getSqlList("sql/tpcds_constraints/", "AddForeignKeys")
                .forEach(stmts -> Arrays.stream(stmts.split("\n")).forEach(q ->
                        {
                            try {
                                starRocksAssert.alterTableProperties(q);
                            } catch (Exception e) {
                                Assert.fail();
                            }
                        }
                ));
    }

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setEnablePipelineEngine(true);
        FeConstants.runningUnitTest = true;
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase(StatsConstants.STATISTICS_DB_NAME)
                .useDatabase(StatsConstants.STATISTICS_DB_NAME)
                .withTable(DEFAULT_CREATE_TABLE_TEMPLATE);
        starRocksAssert.withDatabase("tpcds_db0").useDatabase("tpcds_db0");
        prepareTPCDSTables();
        prepareUniqueKeys();
        prepareForeignKeys();

        starRocksAssert.withDatabase("tpcds_db1").useDatabase("tpcds_db1");
        prepareTPCDSTables();
        prepareUniqueKeys();
    }

    private void testTPCDS(String db, Map<String, List<Integer>> result) {
        starRocksAssert.useDatabase(db);
        List<Pair<String, String>> queryList = getQueryList("sql/tpcds", Pattern.compile("(query\\d+)\\.sql"));
        Consumer<SessionVariable> enablePruning = (sv -> {
            sv.setEnableCboTablePrune(true);
            sv.setEnableRboTablePrune(true);
        });

        Consumer<SessionVariable> disablePruning = (sv -> {
            sv.setEnableCboTablePrune(false);
            sv.setEnableRboTablePrune(false);
        });
        for (Pair<String, String> p : queryList) {
            String n = p.first;
            String sql = p.second;
            if (!n.equals("query45")) {
                continue;
            }
            Pair<Integer, String> r0 = getHashJoinCount(sql, disablePruning);
            Pair<Integer, String> r1 = getHashJoinCount(sql, enablePruning);
            if (result.containsKey(n)) {
                List<Integer> res = result.get(n);
                Assert.assertEquals(r0.first, res.get(0));
                Assert.assertEquals(r1.first, res.get(1));
            } else {
                Assert.assertEquals(r0.first, r1.first);
            }
        }
    }

    @Test
    public void testTPCDSPruningBasedOnUniqueKeys() {
        Map<String, List<Integer>> result = Maps.newHashMap();
        result.put("query72", Lists.newArrayList(10, 9));
        result.put("query78", Lists.newArrayList(8, 5));
        testTPCDS("tpcds_db0", result);
    }

    @Test
    public void testTPCDSPruningBasedOnForeignKeys() {
        Map<String, List<Integer>> result = Maps.newHashMap();
        result.put("query72", Lists.newArrayList(10, 9));
        result.put("query78", Lists.newArrayList(8, 5));
        testTPCDS("tpcds_db1", result);
    }
}