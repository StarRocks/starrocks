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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.wildfly.common.Assert;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AutoMVClickBenchTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert getStarRocksAssert() {
        if (STARROCKS_ASSERT.get() == null) {
            STARROCKS_ASSERT.set(TestUtil.prepareTables("click_bench", TestUtil::getClickBenchCreateTableSqlList));
        }
        return STARROCKS_ASSERT.get();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        getStarRocksAssert();
    }

    public static Stream<Arguments> nextQuery() {
        return TestUtil.getClickBenchQueryList().stream()
                .map(p -> Arguments.of(p.first));
    }

    @Test
    public void testAll() {
        AutoMVUtil.defaultTestHelper(STARROCKS_ASSERT.get().getCtx(), TestUtil.getClickBenchQueryList());
    }

    @Test
    public void testQ40() {
        List<Pair<String, String>> queryList = TestUtil.getClickBenchQueryList()
                .stream()
                .filter(p -> p.first.equals("Q40"))
                .collect(Collectors.toList());

        AutoMVUtil.testHelper(STARROCKS_ASSERT.get().getCtx(),
                queryList,
                sv -> sv.setAutoMVEnableComplexDerivedDimensions(false),
                results -> Assert.assertTrue(results.isEmpty()));

        AutoMVUtil.testHelper(STARROCKS_ASSERT.get().getCtx(),
                queryList,
                sv -> sv.setAutoMVEnableComplexDerivedDimensions(true),
                results -> Assert.assertFalse(results.isEmpty()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nextQuery")
    public void testSingleQuery(String name) throws Exception {
        List<Pair<String, String>> queryList = TestUtil.getClickBenchQueryList()
                .stream()
                .filter(p -> p.first.equals(name))
                .collect(Collectors.toList());
        ConnectContext ctx = getStarRocksAssert().getCtx();
        AutoMVUtil.defaultTestHelper(ctx, queryList);
    }
}
