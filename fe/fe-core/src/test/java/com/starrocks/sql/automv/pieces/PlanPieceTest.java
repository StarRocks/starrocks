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

package com.starrocks.sql.automv.pieces;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.automv.generator.QueryGenerateContext;
import com.starrocks.sql.automv.generator.QueryGenerator;
import com.starrocks.sql.automv.policies.AggregatePolicy;
import com.starrocks.sql.automv.policies.EliminateSemiAntiJoinPolicy;
import com.starrocks.sql.automv.qe.QueryStatementPlus;
import com.starrocks.sql.automv.qe.RboOptimizer;
import com.starrocks.sql.automv.qe.TableInfo;
import com.starrocks.sql.automv.qe.TypePlus;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TestUtil;
import com.starrocks.type.DecimalType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PlanPieceTest {
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

    @Ignore
    @Test
    public void testCategorizePieces() {
        ConnectContext ctx = getStarRocksAssert().getCtx();
        Set<String> excludeQuerySet = ImmutableSet.of("query61");

        List<Pair<String, String>> queryList = TestUtil.getTPCDSQueryList()
                .stream()
                .filter(p -> !excludeQuerySet.contains(p.first))
                .collect(Collectors.toList());
        List<Pair<String, AggregatePiece>> pieces = AutoMVUtil.getPieces(ctx, queryList, name -> true);
        Assert.assertEquals(pieces.size(), 198);
        System.out.println("pieceSize=" + pieces.size());
        pieces.forEach(p -> p.second.assignPieceIds());
        Map<String, List<Pair<String, PlanPiece>>> pieceGroups = pieces.stream()
                .map(p -> Pair.create(p.first, PlanPieceNormalizer.normalize(p.second)))
                .collect(Collectors.groupingBy(p ->
                        p.second.mustCast(AggregatePiece.class).getFlatTable().getPiece().getAuxState().getNormHash()));
        System.out.println("groupSize=" + pieceGroups.size());
        Assert.assertEquals(pieceGroups.size(), 85);
        pieceGroups.forEach((k, v) -> {
            Set<String> querySet = v.stream().map(p -> p.first).collect(Collectors.toSet());
            String querySetStr = querySet.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(", "));
            System.out.printf("{\"%s\", ImmutableSet.of(%s)},\n", k, querySetStr);
            //Assert.assertEquals(categoryMap.get(k), querySet);
        });
    }

    @Test
    public void testTableInfo() {
        String q0 = TestUtil.getTPCDSQuery("query01");
        QueryStatementPlus stmtPlus = RboOptimizer.getQueryStatement(getStarRocksAssert().getCtx(), q0);
        FQTable fqTable = stmtPlus.getFqTableMap().values().iterator().next();
        TableInfo tableInfo = TableInfo.from(fqTable);
        Assert.assertNull(tableInfo.getCatalogName());
        Assert.assertEquals(tableInfo.getDatabaseName(), "tpcds");
    }

    @Test
    public void testTypePlus() {
        Type type = TypeFactory.createDecimalV3NarrowestType(21, 3);
        TypePlus typePlus = TypePlus.of(type, -1, 21, 3);
        Assert.assertEquals(typePlus.getDecayedType(), DecimalType.DECIMAL128);
        Assert.assertEquals(typePlus.getLen(), -1);
        Assert.assertEquals(typePlus.getPrecision(), 21);
        Assert.assertEquals(typePlus.getScale(), 3);
        Assert.assertEquals(typePlus.getType(), type);
    }

    @Test
    public void testPlanPiecePrinter() {
        String q0 = TestUtil.getTPCDSQuery("query01");
        List<PlanPiece> planPieces = RboOptimizer.getPlanPieces(q0, getStarRocksAssert().getCtx());
        Assert.assertFalse(planPieces.isEmpty());
        PlanPiece piece = planPieces.get(0);
        String s = PlanPiecePrinter.print(piece);
        String snippet0 = "[3]: AggregatePiece\n" +
                "  InputPieces: [2]\n" +
                "  Dimensions:\n" +
                "    Dimensions.tier#0\n" +
                "      {5} = O:`tpcds`.`store_returns`.`sr_customer_sk`\n" +
                "      {9} = O:`tpcds`.`store_returns`.`sr_store_sk`\n" +
                "  RollupDimensions:\n" +
                "    RollupDimensions.tier#0\n" +
                "      {27} = O:`tpcds`.`date_dim`.`d_year`\n" +
                "  Metrics:\n" +
                "    Metrics.tier#0\n" +
                "      {49} = D:(sum[decimal(38, 2)][O] (var[decimal(7, 2)] #12))\n" +
                "  DistinctMetrics:\n" +
                "  NonHoistConjuncts:\n" +
                "    NonHoistConjuncts.tier#0\n" +
                "      [0] = ($inRange[boolean][O] (var[int(11)] #27) ($setOf[int(11)][U] " +
                "($closedRangeOf[int(11)][O] (val[int(11)] 2000) (val[int(11)] 2000))))\n" +
                "  HoistConjuncts:\n" +
                "    HoistConjuncts.tier#0\n" +
                "      [0] = ($modify[boolean][O] (val[varchar] M_IS_NOT_NULL) (var[int(11)] #9))\n" +
                "  Columns:\n" +
                "    Columns.tier#0\n" +
                "      {5} = O:`tpcds`.`store_returns`.`sr_customer_sk`\n" +
                "      {9} = O:`tpcds`.`store_returns`.`sr_store_sk`\n" +
                "    Columns.tier#1\n" +
                "      {27} = O:`tpcds`.`date_dim`.`d_year`\n" +
                "    Columns.tier#2\n" +
                "      {49} = D:(sum[decimal(38, 2)][O] (var[decimal(7, 2)] #12))\n" +
                "  Conjuncts:";
        Assert.assertTrue(s, s.contains(snippet0));
        List<String> lines = Stream.of(s.split("\n")).collect(Collectors.toList());
        Assert.assertEquals(s, 1, lines.stream().filter(ln -> ln.contains("StarJoinPiece")).count());
        Assert.assertEquals(s, 2, lines.stream().filter(ln -> ln.contains("TablePiece")).count());
        String s1 = PlanPiecePrinter.print(piece, new PrettyPrinter(), 1).getResult();
        List<String> lines1 = Stream.of(s1.split("\n")).collect(Collectors.toList());
        Assert.assertTrue(s1, s1.contains(snippet0));
        Assert.assertEquals(s1, 0, lines1.stream().filter(ln -> ln.contains("StarJoinPiece")).count());
        Assert.assertEquals(s1, 0, lines1.stream().filter(ln -> ln.contains("TablePiece")).count());
        String s2 = PlanPiecePrinter.print(piece, new PrettyPrinter(), 2).getResult();
        List<String> lines2 = Stream.of(s2.split("\n")).collect(Collectors.toList());
        Assert.assertTrue(s2, s2.contains(snippet0));
        Assert.assertEquals(s2, 1, lines2.stream().filter(ln -> ln.contains("StarJoinPiece")).count());
        Assert.assertEquals(s2, 0, lines2.stream().filter(ln -> ln.contains("TablePiece")).count());
    }

    @Test
    public void testEliminateLeftSemiAntiJoinPolicy() {
        String q16 = TestUtil.getTPCDSQuery("query16");
        List<PlanPiece> planPieces = RboOptimizer.getPlanPieces(q16, getStarRocksAssert().getCtx());
        Assert.assertFalse(planPieces.isEmpty());
        AggregatePiece aggPiece = planPieces.get(0).mustCast(AggregatePiece.class);
        String[] lines = PlanPiecePrinter.print(aggPiece).split("\n");
        Optional<AggregatePiece> optAggPiece2 = EliminateSemiAntiJoinPolicy.INSTANCE.convert(aggPiece);
        Assert.assertTrue(optAggPiece2.isPresent());
        AggregatePiece aggPiece2 = optAggPiece2.get();
        String[] lines2 = PlanPiecePrinter.print(aggPiece2).split("\n");
        Assert.assertEquals(1, Stream.of(lines).filter(ln -> ln.contains("LEFT SEMI JOIN")).count());
        Assert.assertEquals(1, Stream.of(lines).filter(ln -> ln.contains("LEFT ANTI JOIN")).count());
        Assert.assertTrue(Stream.of(lines2).noneMatch(ln -> ln.contains("LEFT SEMI JOIN")));
        Assert.assertTrue(Stream.of(lines2).noneMatch(ln -> ln.contains("LEFT ANTI JOIN")));
    }

    @Test
    public void testNotPolicy() {
        String q0 = "select cc_company_name, count(1) from call_center group by cc_company_name";
        List<PlanPiece> pieces = RboOptimizer.getPlanPieces(q0, getStarRocksAssert().getCtx());
        Assert.assertFalse(pieces.isEmpty());
        AggregatePiece aggPiece = pieces.get(0).mustCast(AggregatePiece.class);
        {
            AggregatePolicy.AbstractAggregatePolicy policy = AggregatePolicy.IDENTITY_POLICY;
            AggregatePolicy notPolicy = AggregatePolicy.not(policy);
            Assert.assertTrue(policy.convert(aggPiece).isPresent());
            Assert.assertFalse(notPolicy.convert(aggPiece).isPresent());
        }
        {
            AggregatePolicy.AbstractAggregatePolicy policy = AggregatePolicy.NONE_POLICY;
            AggregatePolicy notPolicy = AggregatePolicy.not(policy);
            Assert.assertFalse(policy.convert(aggPiece).isPresent());
            Assert.assertTrue(notPolicy.convert(aggPiece).isPresent());
        }
    }

    @Test
    public void test11MVPieces() {
        ConnectContext ctx = getStarRocksAssert().getCtx();
        List<Pair<String, String>> queryList = TestUtil.getTPCDSQueryList();
        List<Pair<String, PlanPiece>> pieces =
                AutoMVUtil.get11MVPieces(ctx, queryList, name -> true);

        Map<String, List<PlanPiece>> pieceGroups = pieces.stream().map(p -> p.second)
                .collect(Collectors.groupingBy(p -> p.getClass().getSimpleName()));
        Map<String, Integer> expectResults = ImmutableMap.<String, Integer>builder()
                .put("AggregatePiece", 117)
                .put("TablePiece", 54)
                .put("StarJoinPiece", 116)
                .build();
        Map<String, Integer> actualResults =
                pieceGroups.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
        Assert.assertEquals(expectResults, actualResults);
    }

    @Test
    public void test11MVTablePieceUsage() {
        ConnectContext ctx = getStarRocksAssert().getCtx();
        String q = TestUtil.getTPCDSQuery("query01");
        List<Pair<String, PlanPiece>> pieces =
                AutoMVUtil.get11MVPieces(ctx, ImmutableList.of(Pair.create("query01", q)), name -> true);

        List<TableUsage> tableUsages = pieces.stream()
                .map(p -> p.second)
                .map(TableUsage::analyzeUsage)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        Assert.assertEquals(tableUsages.size(), 6);
        List<TableUsage> mergedTableUsages = TableUsage.mergeUsages(tableUsages);
        Assert.assertEquals(mergedTableUsages.size(), 4);
        String[] expectResults = new String[] {
                "SELECT\n" +
                        "  `tpcds`.`customer`.c_customer_sk\n" +
                        "  ,`tpcds`.`customer`.c_customer_id\n" +
                        "FROM\n" +
                        "  `tpcds`.`customer`",
                "SELECT\n" +
                        "  `tpcds`.`store`.s_store_sk\n" +
                        "  ,`tpcds`.`store`.s_state\n" +
                        "FROM\n" +
                        "  `tpcds`.`store`",
                "SELECT\n" +
                        "  `tpcds`.`date_dim`.d_date_sk\n" +
                        "  ,`tpcds`.`date_dim`.d_year\n" +
                        "FROM\n" +
                        "  `tpcds`.`date_dim`\n" +
                        "WHERE\n" +
                        "  (`tpcds`.`date_dim`.d_year = 2000)",

                "SELECT\n" +
                        "  `tpcds`.`store_returns`.sr_returned_date_sk\n" +
                        "  ,`tpcds`.`store_returns`.sr_customer_sk\n" +
                        "  ,`tpcds`.`store_returns`.sr_store_sk\n" +
                        "  ,`tpcds`.`store_returns`.sr_return_amt\n" +
                        "FROM\n" +
                        "  `tpcds`.`store_returns`\n" +
                        "WHERE\n" +
                        "  (`tpcds`.`store_returns`.sr_store_sk IS NOT NULL)",
        };

        for (int i = 0; i < mergedTableUsages.size(); ++i) {
            PlanPiece tablePiece = PieceColumnPruner.prune(mergedTableUsages.get(i).getTablePiece());
            QueryGenerateContext queryGenContext = QueryGenerateContext.of(false, true, false);
            String s = QueryGenerator.generate(tablePiece, queryGenContext).getSubquery().getResult();
            Assert.assertEquals(expectResults[i], s);
        }
    }
}
