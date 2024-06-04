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

import com.google.common.collect.Sets;
import com.starrocks.analysis.TableName;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.automv.ast.ShowRecommendationsStmt;
import com.starrocks.sql.automv.qe.TunespaceExecutor;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TestUtil;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AutoMVSSBTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert getStarRocksAssert() {
        if (STARROCKS_ASSERT.get() == null) {
            STARROCKS_ASSERT.set(TestUtil.prepareTables("ssb", TestUtil::getSsbCreateTableSqlList));
        }
        return STARROCKS_ASSERT.get();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        getStarRocksAssert();
    }

    public static Stream<Arguments> nextQuery() {
        return TestUtil.getSsbQueryList().stream()
                .map(p -> Arguments.of(p.first));
    }

    public static Stream<Arguments> nextFlatQuery() {
        return TestUtil.getSsbLineorderFlatQueryList().stream()
                .map(p -> Arguments.of(p.first));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nextQuery")
    public void testSingleQuery(String name) throws Exception {
        List<Pair<String, String>> queryList = TestUtil.getSsbQueryList()
                .stream()
                .filter(p -> p.first.equals(name))
                .collect(Collectors.toList());
        ConnectContext ctx = getStarRocksAssert().getCtx();
        AutoMVUtil.defaultTestHelper(ctx, queryList);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nextFlatQuery")
    public void testSingleFlatQuery(String name) throws Exception {
        List<Pair<String, String>> queryList = TestUtil.getSsbLineorderFlatQueryList()
                .stream()
                .filter(p -> p.first.equals(name))
                .collect(Collectors.toList());
        ConnectContext ctx = getStarRocksAssert().getCtx();
        AutoMVUtil.defaultTestHelper(ctx, queryList);
    }

    @Test
    public void testFlatQ33AndQ34Merge() {
        ConnectContext ctx = STARROCKS_ASSERT.get().getCtx();
        Set<String> queryNames = Sets.newHashSet("Q3.3", "Q3.4");
        List<Pair<String, String>> queryList =
                TestUtil.getSsbLineorderFlatQueryList().stream().filter(p -> queryNames.contains(p.first))
                        .collect(Collectors.toList());
        AutoMVUtil.mockUpCustomizedQueryExecutor(queryList);
        TableName tableName = new TableName(null, "db", "_tunespace_");
        ShowRecommendationsStmt stmt = new ShowRecommendationsStmt(tableName, -1, -1);
        ctx.getSessionVariable().setAutoMVCardRowCountRatioLWM(0);
        ctx.getSessionVariable().setAutoMVCardRowCountRatioHWM(1.0);
        ctx.getSessionVariable().setAutoMVUseHllCountDistinct(true);
        ShowResultSet showResultSet = TunespaceExecutor.execute(stmt, ctx);
        for (List<String> row : showResultSet.getResultRows()) {
            PrettyPrinter printer = new PrettyPrinter();
            printer.addItemsWithDelNl(";", row);
            System.out.println(printer.getResult());
        }
    }

    @Test
    public void testFlatQ3Merge() {
        ConnectContext ctx = STARROCKS_ASSERT.get().getCtx();
        Set<String> queryNames = Sets.newHashSet("Q3.1", "Q3.2", "Q3.3", "Q3.4");
        List<Pair<String, String>> queryList =
                TestUtil.getSsbLineorderFlatQueryList().stream().filter(p -> queryNames.contains(p.first))
                        .collect(Collectors.toList());
        AutoMVUtil.mockUpCustomizedQueryExecutor(queryList);
        TableName tableName = new TableName(null, "db", "_tunespace_");
        ShowRecommendationsStmt stmt = new ShowRecommendationsStmt(tableName, -1, -1);
        ctx.getSessionVariable().setAutoMVCardRowCountRatioLWM(0);
        ctx.getSessionVariable().setAutoMVCardRowCountRatioHWM(1.0);
        ctx.getSessionVariable().setAutoMVUseHllCountDistinct(true);
        ShowResultSet showResultSet = TunespaceExecutor.execute(stmt, ctx);
        for (List<String> row : showResultSet.getResultRows()) {
            PrettyPrinter printer = new PrettyPrinter();
            printer.addItemsWithDelNl(";", row);
            System.out.println(printer.getResult());
        }
    }

    @Test
    public void testFlatQ3Q1Merge() {
        ConnectContext ctx = STARROCKS_ASSERT.get().getCtx();
        Set<String> queryNames = Sets.newHashSet("Q1.1", "Q3.1");
        List<Pair<String, String>> queryList =
                TestUtil.getSsbLineorderFlatQueryList().stream().filter(p -> queryNames.contains(p.first))
                        .collect(Collectors.toList());
        AutoMVUtil.mockUpCustomizedQueryExecutor(queryList);
        TableName tableName = new TableName(null, "db", "_tunespace_");
        ShowRecommendationsStmt stmt = new ShowRecommendationsStmt(tableName, -1, -1);
        ctx.getSessionVariable().setAutoMVCardRowCountRatioLWM(0);
        ctx.getSessionVariable().setAutoMVCardRowCountRatioHWM(1.0);
        ctx.getSessionVariable().setAutoMVUseHllCountDistinct(true);
        ShowResultSet showResultSet = TunespaceExecutor.execute(stmt, ctx);
        for (List<String> row : showResultSet.getResultRows()) {
            PrettyPrinter printer = new PrettyPrinter();
            printer.addItemsWithDelNl(";", row);
            System.out.println(printer.getResult());
        }
    }

    @Test
    public void testFlatAll() {
        AutoMVUtil.defaultTestHelper(STARROCKS_ASSERT.get().getCtx(), TestUtil.getSsbLineorderFlatQueryList());
    }

    @Test
    public void testAll() {
        AutoMVUtil.defaultTestHelper(STARROCKS_ASSERT.get().getCtx(), TestUtil.getSsbQueryList());
    }
}
