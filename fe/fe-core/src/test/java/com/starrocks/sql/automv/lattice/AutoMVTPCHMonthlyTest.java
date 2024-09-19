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

package com.starrocks.sql.automv.lattice;

import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.automv.util.TestUtil;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AutoMVTPCHMonthlyTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert getStarRocksAssert() {
        FeConstants.runningUnitTest = true;
        if (STARROCKS_ASSERT.get() == null) {
            STARROCKS_ASSERT.set(TestUtil.prepareTables("tpch", TestUtil::getTPCHMonthlyCreateTableSqlList,
                    TestUtil::getTPCHMonthCreateViewSqlList));
        }
        return STARROCKS_ASSERT.get();
    }

    public static Stream<Arguments> nextQuery() {
        return TestUtil.getTPCHQueryList().stream()
                .map(p -> Arguments.of(p.first));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nextQuery")
    public void testSingleQuery(String name) throws Exception {
        List<Pair<String, String>> queryList = TestUtil.getTPCHQueryList()
                .stream()
                .filter(p -> p.first.equals(name))
                .collect(Collectors.toList());
        ConnectContext ctx = getStarRocksAssert().getCtx();
        AutoMVUtil.testHelper(ctx, queryList,
                sv -> {
                    sv.setAutoMVDefaultPartitionByTimeGranule("month");
                },
                results -> {

                });
    }

    @Test
    public void testAll() {
        List<Pair<String, String>> queryList = TestUtil.getTPCHQueryList();
        ConnectContext ctx = getStarRocksAssert().getCtx();
        AutoMVUtil.testHelper(ctx, queryList,
                sv -> {
                    sv.setAutoMVDefaultPartitionByTimeGranule("month");
                },
                results -> {

                });
    }

    @Test
    public void testQ13() {
        String sql = "SELECT\n" +
                "  _ta0000.o_orderdate\n" +
                "  ,_ta0000.c_custkey\n" +
                "  ,(count(_ta0000.o_orderkey)) AS _ca0003\n" +
                "FROM\n" +
                "  (\n" +
                "    SELECT\n" +
                "      `tpch`.`orders_monthly`.o_orderkey\n" +
                "      ,`tpch`.`orders_monthly`.o_orderdate\n" +
                "      ,`tpch`.`customer`.c_custkey\n" +
                "    FROM\n" +
                "      `tpch`.`customer`\n" +
                "      LEFT OUTER JOIN\n" +
                "      `tpch`.`orders_monthly`\n" +
                "      ON (`tpch`.`customer`.c_custkey = `tpch`.`orders_monthly`.o_custkey)\n" +
                "         AND (NOT (`tpch`.`orders_monthly`.o_comment like \"%special%requests%\"))\n" +
                "  ) _ta0000\n" +
                "GROUP BY\n" +
                "  _ta0000.o_orderdate\n" +
                "  ,_ta0000.c_custkey\n";
        List<Pair<String, String>> queryList = Arrays.asList(Pair.create("q13", sql));
        AutoMVUtil.testHelper(getStarRocksAssert().getCtx(), queryList,
                sv -> {
                    sv.setAutoMVDefaultPartitionByTimeGranule("month");
                },
                results -> {
                    Assert.assertEquals(1, results.size());
                    String mv = results.get(0).get(2);
                    Assert.assertTrue(mv, mv.contains("date_trunc(\"month\""));
                }
        );
    }
}
