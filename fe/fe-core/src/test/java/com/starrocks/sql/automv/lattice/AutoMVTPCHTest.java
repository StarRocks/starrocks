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

import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.automv.util.TestUtil;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AutoMVTPCHTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert getStarRocksAssert() {
        if (STARROCKS_ASSERT.get() == null) {
            STARROCKS_ASSERT.set(TestUtil.prepareTables("tpch", TestUtil::getTPCHCreateTableSqlList));
        }
        return STARROCKS_ASSERT.get();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        getStarRocksAssert();
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
        AutoMVUtil.defaultTestHelper(ctx, queryList);
    }

    @Test
    public void testAll() throws Exception {
        AutoMVUtil.defaultTestHelper(getStarRocksAssert().getCtx(), TestUtil.getTPCHQueryList());
    }

    @Test
    public void testMetricContainLambda() throws Exception {
        String q = "select\n" +
                "  l_shipdate,\n" +
                "  sum(\n" +
                "    array_map((x, y) -> x + y, [l_shipdate], [l_orderkey]) [1]\n" +
                "  )\n" +
                "from\n" +
                "  lineitem\n" +
                "group by\n" +
                "  l_shipdate";
        AutoMVUtil.testHelper(getStarRocksAssert().getCtx(), Arrays.asList(Pair.create("q", q)),
                AutoMVUtil::configDefaultAutoMV, results -> {
                    Assert.assertEquals(results.size(), 1);
                    String mv = results.get(0).get(2);
                    Assert.assertTrue(mv, mv.contains("(sum(array_map((x_0, x_1)->(x_0 + x_1), " +
                            "[`tpch`.`lineitem`.l_shipdate], " +
                            "[`tpch`.`lineitem`.l_orderkey])[1]))"));
                });
    }
}
