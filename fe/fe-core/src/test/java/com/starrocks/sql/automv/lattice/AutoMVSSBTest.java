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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.starrocks.analysis.TableName;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.automv.ast.ShowRecommendationsStmt;
import com.starrocks.sql.automv.options.AutoMVOptions;
import com.starrocks.sql.automv.pattern.PlanPiecePatterns;
import com.starrocks.sql.automv.pieces.FQTable;
import com.starrocks.sql.automv.qe.PartitionExtractor;
import com.starrocks.sql.automv.qe.RboOptimizer;
import com.starrocks.sql.automv.qe.TunespaceExecutor;
import com.starrocks.sql.automv.tunespace.PlanPieceInfo;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TestUtil;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.wildfly.common.Assert;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AutoMVSSBTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert getStarRocksAssert() {
        FeConstants.runningUnitTest = true;
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
    public void testFlatQ11Q12Q13Merge() {
        ConnectContext ctx = STARROCKS_ASSERT.get().getCtx();
        List<Pair<String, String>> queryList = ImmutableList.of(
                Pair.create("ssb1.1", "select sum(lo_revenue) as revenue\n" +
                        "from lineorder join dates on lo_orderdate = d_datekey\n" +
                        "where d_year = 1993 and lo_discount between 1 and 3 and lo_quantity < 25"),
                Pair.create("ssb1.2", "select sum(lo_revenue) as revenue\n" +
                        "from lineorder\n" +
                        "join dates on lo_orderdate = d_datekey\n" +
                        "where d_yearmonthnum = 199401\n" +
                        "and lo_discount between 4 and 6\n" +
                        "and lo_quantity between 26 and 35"),
                Pair.create("ssb1.3", "select sum(lo_revenue) as revenue\n" +
                        "from lineorder\n" +
                        "join dates on lo_orderdate = d_datekey\n" +
                        "where d_weeknuminyear = 6 and d_year = 1994\n" +
                        "and lo_discount between 5 and 7\n" +
                        "and lo_quantity between 26 and 35")

        );
        AutoMVUtil.mockUpCustomizedQueryExecutor(queryList);
        TableName tableName = new TableName(null, "db", "_tunespace_");
        ShowRecommendationsStmt stmt = new ShowRecommendationsStmt(tableName, -1, -1);
        ctx.getSessionVariable().setAutoMVCardRowCountRatioLWM(1.0);
        ctx.getSessionVariable().setAutoMVCardRowCountRatioHWM(1.0);
        ctx.getSessionVariable().setAutoMVUseHllCountDistinct(true);
        ShowResultSet showResultSet = TunespaceExecutor.execute(stmt, ctx);
        String expectString = "[\"ssb1.1.part.0\", \"ssb1.2.part.0\", \"ssb1.3.part.0\"]";
        boolean result = showResultSet.getResultRows().stream().anyMatch(row -> row.get(14).equals(expectString));
        Assert.assertTrue(result);
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
    public void testFlatQ13() {
        String name = "Q1.3";
        Optional<Pair<String, String>> optNameAndQuery = TestUtil.getSsbLineorderFlatQueryList()
                .stream()
                .filter(p -> p.first.equals(name))
                .findFirst();
        Assert.assertTrue(optNameAndQuery.isPresent());
        String query = optNameAndQuery.get().second;
        Pair<Map<String, FQTable>, List<OptExpression>> fqTablesAndSubPlans =
                RboOptimizer.getSubPlans(query, getStarRocksAssert().getCtx(), PlanPiecePatterns.getSPJG());
        AutoMVOptions options =
                AutoMVOptions.of(new PartitionExtractor(), getStarRocksAssert().getCtx().getSessionVariable());
        Map<String, FQTable> fqTableMap = fqTablesAndSubPlans.first;
        List<OptExpression> subPlans = fqTablesAndSubPlans.second;
        Assert.assertFalse(subPlans.isEmpty());
        PlanPieceInfo pieceInfo = PlanPieceInfo.from(options, name, subPlans.get(0), false, fqTableMap);
        System.out.println(pieceInfo.getQuery());
        Assert.assertTrue(pieceInfo.getQuery().contains("SELECT\n" +
                "  (1) AS _ca0003\n" +
                "  ,(sum((_ta0000.lo_extendedprice * _ta0000.lo_discount))) AS _ca0004\n" +
                "FROM\n" +
                "  (\n" +
                "    SELECT\n" +
                "      `ssb`.`lineorder_flat`.lo_extendedprice\n" +
                "      ,`ssb`.`lineorder_flat`.lo_discount\n" +
                "    FROM\n" +
                "      `ssb`.`lineorder_flat`\n" +
                "    WHERE\n" +
                "      (6 = weekofyear(`ssb`.`lineorder_flat`.lo_orderdate))\n" +
                "      AND (\"1994-01-01\" <= `ssb`.`lineorder_flat`.lo_orderdate)\n" +
                "      AND (`ssb`.`lineorder_flat`.lo_orderdate <= \"1994-12-31\")\n" +
                "      AND (5 <= `ssb`.`lineorder_flat`.lo_discount)\n" +
                "      AND (`ssb`.`lineorder_flat`.lo_discount <= 7)\n" +
                "      AND (26 <= `ssb`.`lineorder_flat`.lo_quantity)\n" +
                "      AND (`ssb`.`lineorder_flat`.lo_quantity <= 35)\n" +
                "  ) _ta0000"));
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
