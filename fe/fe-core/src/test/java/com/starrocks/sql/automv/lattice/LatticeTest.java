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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.sql.automv.column.ColumnAlias;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.estimation.CardEstimator;
import com.starrocks.sql.automv.options.AutoMVOptions;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.PlanPieceNormalizer;
import com.starrocks.sql.automv.pn.Apply;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.pn.Var;
import com.starrocks.sql.automv.qe.PartitionExtractor;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.automv.util.TestUtil;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.automv.util.Util;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class LatticeTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert getStarRocksAssert() {
        FeConstants.runningUnitTest = true;
        if (STARROCKS_ASSERT.get() == null) {
            try {
                STARROCKS_ASSERT.set(TestUtil.prepareTables("ssb", TestUtil::getSsbCreateTableSqlList));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return STARROCKS_ASSERT.get();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        StarRocksAssert starRocksAssert = getStarRocksAssert();
        starRocksAssert.getCtx().getSessionVariable().setAutoMVDefaultPartitionByTimeGranule("none");
    }

    private CardEstimator getEstimator(ConnectContext ctx, List<Pair<String, String>> queryList) {
        List<Pair<String, AggregatePiece>> pieces = AutoMVUtil.getPieces(ctx, queryList);

        pieces.forEach(p -> p.second.assignPieceIds());
        Map<String, List<Pair<String, PlanPiece>>> pieceGroups = pieces.stream()
                .map(p -> Pair.create(p.first, PlanPieceNormalizer.normalize(p.second)))
                .collect(Collectors.groupingBy(p -> p.second.getFlatTableNormHash()));

        AutoMVOptions options = AutoMVOptions.of(new PartitionExtractor(), ctx.getSessionVariable());
        Assert.assertEquals(pieceGroups.size(), 1);

        List<PlanPiece> pieceList =
                pieceGroups.values().iterator().next().stream().map(p -> p.second).collect(Collectors.toList());
        Assert.assertEquals(pieceList.size(), queryList.size());
        Lattice lattice = Lattice.createLattice(pieceList, false);
        return new CardEstimator(options, lattice);
    }

    @Test
    public void testEstimateCardSqlOfNoGroupBy() {
        ConnectContext ctx = getStarRocksAssert().getCtx();
        List<Pair<String, String>> queryList = Lists.newArrayList(
                Pair.create("q1", "select count(1) from lineorder")
        );

        CardEstimator estimator = getEstimator(ctx, queryList);
        Optional<String> optEstimateSql0 = estimator.getEstimateSql();
        Assert.assertTrue(optEstimateSql0.isPresent());
        String estimateSql0 = optEstimateSql0.get();
        Assert.assertEquals(estimateSql0, estimateSql0, "WITH cte_0 AS (\n" +
                "  SELECT\n" +
                "    1\n" +
                "  FROM\n" +
                "    `ssb`.`lineorder`\n" +
                ")\n" +
                "SELECT\n" +
                "  COUNT(1) as rowCount\n" +
                "  ,json_array(\n" +
                "    1\n" +
                "  ) AS cards\n" +
                "FROM cte_0");
        Assert.assertFalse(estimator.getEstimateSql().isPresent());
    }

    @Test
    public void testEstimateCardSqlOfGroupBy() {
        ConnectContext ctx = getStarRocksAssert().getCtx();
        List<Pair<String, String>> queryList = Lists.newArrayList(
                Pair.create("q1", "select count(1) from lineorder group by lo_orderdate")
        );
        CardEstimator estimator = getEstimator(ctx, queryList);
        Optional<String> optEstimateSql0 = estimator.getEstimateSql();
        Assert.assertTrue(optEstimateSql0.isPresent());
        String estimateSql0 = optEstimateSql0.get();
        Assert.assertEquals(estimateSql0, estimateSql0, "WITH cte_0 AS (\n" +
                "  SELECT\n" +
                "    coalesce(murmur_hash3_32(_ta0000.lo_orderdate), 1) AS c0\n" +
                "  FROM (\n" +
                "    SELECT\n" +
                "      `ssb`.`lineorder`.lo_orderdate\n" +
                "    FROM\n" +
                "      `ssb`.`lineorder`\n" +
                "    WHERE\n" +
                "      (coalesce(((murmur_hash3_32(`ssb`.`lineorder`.lo_orderdate) % 512) + 512), 0) <= 16)\n" +
                "  ) _ta0000\n" +
                ")\n" +
                "SELECT\n" +
                "  COUNT(1) as rowCount\n" +
                "  ,json_array(\n" +
                "    ndv(c0)\n" +
                "  ) AS cards\n" +
                "FROM cte_0");
        Assert.assertTrue(estimator.getEstimateSql().isPresent());
    }

    @Test
    public void testEstimateCardSqlOfBothGroupByAndNoGroupBy() {
        ConnectContext ctx = getStarRocksAssert().getCtx();
        List<Pair<String, String>> queryList = Lists.newArrayList(
                Pair.create("q1", "select count(1) from lineorder group by lo_orderdate"),
                Pair.create("q0", "select count(1) from lineorder")
        );
        CardEstimator estimator = getEstimator(ctx, queryList);
        Optional<String> optEstimateSql0 = estimator.getEstimateSql();
        Assert.assertTrue(optEstimateSql0.isPresent());
        String estimateSql0 = optEstimateSql0.get();
        Assert.assertEquals(estimateSql0, estimateSql0, "WITH cte_0 AS (\n" +
                "  SELECT\n" +
                "    coalesce(murmur_hash3_32(_ta0000.lo_orderdate), 1) AS c0\n" +
                "  FROM (\n" +
                "    SELECT\n" +
                "      `ssb`.`lineorder`.lo_orderdate\n" +
                "    FROM\n" +
                "      `ssb`.`lineorder`\n" +
                "    WHERE\n" +
                "      (coalesce(((murmur_hash3_32(`ssb`.`lineorder`.lo_orderdate) % 512) + 512), 0) <= 16)\n" +
                "  ) _ta0000\n" +
                ")\n" +
                "SELECT\n" +
                "  COUNT(1) as rowCount\n" +
                "  ,json_array(\n" +
                "    ndv(c0)\n" +
                "    ,1\n" +
                "  ) AS cards\n" +
                "FROM cte_0");
        Assert.assertTrue(estimator.getEstimateSql().isPresent());
    }

    @Test
    public void testEstimateCardSqlWithStiffConjuncts() {
        ConnectContext ctx = getStarRocksAssert().getCtx();
        List<Pair<String, String>> queryList = Lists.newArrayList(
                Pair.create("q1", "select count(1) from lineorder " +
                        "where lo_shipmode != '' " +
                        "group by lo_orderdate")
        );
        CardEstimator estimator = getEstimator(ctx, queryList);
        Optional<String> optEstimateSql = estimator.getEstimateSql();
        Assert.assertTrue(optEstimateSql.isPresent());
        String estimateSql = optEstimateSql.get();
        String expectSql = "WITH cte_0 AS (\n" +
                "  SELECT\n" +
                "    _ta0000._ca0001 AS __STIFF__\n" +
                "    ,coalesce(murmur_hash3_32(_ta0000.lo_orderdate), 1) AS c0\n" +
                "  FROM (\n" +
                "    SELECT\n" +
                "      ((`ssb`.`lineorder`.lo_shipmode != \"\")) AS _ca0001\n" +
                "      ,`ssb`.`lineorder`.lo_orderdate\n" +
                "      ,`ssb`.`lineorder`.lo_shipmode\n" +
                "    FROM\n" +
                "      `ssb`.`lineorder`\n" +
                "    WHERE\n" +
                "      (coalesce(((murmur_hash3_32(`ssb`.`lineorder`.lo_orderdate) % 512) + 512), 0) <= 16)\n" +
                "  ) _ta0000\n" +
                ")\n" +
                "SELECT\n" +
                "  COUNT(1) as rowCount\n" +
                "  ,json_array(\n" +
                "    ndv(CASE WHEN(__STIFF__)THEN(c0)ELSE NULL END)\n" +
                "  ) AS cards\n" +
                "FROM cte_0";
        Assert.assertEquals(estimateSql, estimateSql, expectSql);
    }

    @Test
    public void testEstimateCardSql() {
        ConnectContext ctx = getStarRocksAssert().getCtx();
        Set<String> selectedQuerySet = ImmutableSet.of("Q1.1", "Q1.2", "Q1.3");
        List<Pair<String, AggregatePiece>> pieces =
                AutoMVUtil.getPieces(ctx, TestUtil.getSsbQueryList(), selectedQuerySet::contains);

        pieces.forEach(p -> p.second.assignPieceIds());
        Map<String, List<Pair<String, PlanPiece>>> pieceGroups = pieces.stream()
                .map(p -> Pair.create(p.first, PlanPieceNormalizer.normalize(p.second)))
                .collect(Collectors.groupingBy(p -> p.second.getFlatTableNormHash()));

        AutoMVOptions options = AutoMVOptions.of(new PartitionExtractor(), ctx.getSessionVariable());
        Assert.assertEquals(pieceGroups.size(), 1);
        List<PlanPiece> pieceList =
                pieceGroups.values().iterator().next().stream().map(p -> p.second).collect(Collectors.toList());
        Assert.assertEquals(pieceList.size(), 3);
        Lattice lattice = Lattice.createLattice(pieceList, false);

        CardEstimator estimator = new CardEstimator(options, lattice);
        String commonSnippet = "    coalesce(murmur_hash3_32(_ta0002.d_weeknuminyear), 1) AS c0\n" +
                "    ,coalesce(murmur_hash3_32(_ta0002.d_year), 2) AS c1\n" +
                "    ,coalesce(murmur_hash3_32(_ta0002.d_yearmonthnum), 3) AS c2\n" +
                "    ,coalesce(murmur_hash3_32(_ta0002.lo_discount), 4) AS c3\n" +
                "    ,coalesce(murmur_hash3_32(_ta0002.lo_quantity), 5) AS c4";
        String[][] snippetPerIteration = new String[][] {
                {"(coalesce(((murmur_hash3_32(`ssb`.`dates`.d_year) % 512) + 512), 0) <= 16)"},
                {"(coalesce(((murmur_hash3_32(`ssb`.`dates`.d_year) % 512) + 512), 0) <= 32)"},
                {"(coalesce(((murmur_hash3_32(`ssb`.`dates`.d_year) % 512) + 512), 0) <= 64)"},
                {"(coalesce(((murmur_hash3_32(`ssb`.`dates`.d_year) % 512) + 512), 0) <= 128)"},
                {"(coalesce(((murmur_hash3_32(`ssb`.`dates`.d_year) % 512) + 512), 0) <= 256)"},
                {"(coalesce(((murmur_hash3_32(`ssb`.`dates`.d_year) % 512) + 512), 0) <= 512)"},
                {"(coalesce(((murmur_hash3_32(`ssb`.`dates`.d_year) % 512) + 512), 0) <= 1024)"},
        };

        List<String> sqlList = IntStream.range(0, 10)
                .mapToObj(i -> estimator.getEstimateSql())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        Assert.assertEquals(sqlList.size(), snippetPerIteration.length);
        for (String sql : sqlList) {
            System.out.println(sql);
        }
        Assert.assertTrue(sqlList.stream().allMatch(sql -> sql.contains(commonSnippet)));
        Assert.assertTrue(IntStream.range(0, snippetPerIteration.length)
                .allMatch(i -> sqlList.get(i).contains(snippetPerIteration[i][0])));
    }

    @Test
    public void testConjunctsGenerator() {
        List<Var> vars = Lists.newArrayList();
        TableName tableName = new TableName("default_catalog", "tpch", "lineitem");
        TieredMap.Builder<Integer, ColumnAlias> aliasesBuilder = TieredMap.<Integer, ColumnAlias>newGenesisTier();
        for (int i = 0; i < 3; ++i) {
            vars.add(Apply.var(Type.BIGINT, i));
            Column column = new Column("col_" + i, Type.BIGINT);
            aliasesBuilder.put(i, ColumnAlias.of(tableName.toSql(), column.getName()));
            vars.get(i).getSymbol().tenured(GenericColumn.original(tableName, column));
        }

        Function<Op, String> opToSql = OpUtil.toOpToSqlConverter(aliasesBuilder.build());
        Supplier<Optional<List<Op>>> conjunctsGen = OpUtil.getSamplingConjunctsGenerator(vars, 512);
        String[][] expectResults = {
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 2)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 4)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 8)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 16)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 32)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 64)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 128)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 256)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 512)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 2)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 4)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 8)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 16)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 32)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 64)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 128)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 256)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 512)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 2)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 4)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 8)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 16)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 32)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 64)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 128)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 256)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 512)"},
                {"(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_0) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_1) % 512) + 512), 0) <= 1024)",
                        "(coalesce(((murmur_hash3_32(`tpch`.`lineitem`.col_2) % 512) + 512), 0) <= 1024)"}
        };
        List<List<Op>> conjunctsList = IntStream.range(1, 1000)
                .mapToObj(i -> conjunctsGen.get())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        /*
        String lines = conjunctsList.stream().map(conjuncts -> conjuncts.stream().map(opToSql)
                        .map(PrettyPrinter::escapedDoubleQuoted)
                        .map(PrettyPrinter::getResult)
                        .collect(Collectors.joining(",\n")))
                .map(s -> "{" + s + "},").collect(Collectors.joining("\n"));
        System.out.print(lines);
         */
        Assert.assertEquals(conjunctsList.size(), expectResults.length);
        for (int i = 0; i < expectResults.length; ++i) {
            List<Op> conjuncts = conjunctsList.get(i);
            String[] expectResult = expectResults[i];
            Assert.assertEquals(conjuncts.size(), expectResult.length);
            for (int j = 0; j < expectResult.length; ++j) {
                Assert.assertEquals(opToSql.apply(conjuncts.get(j)), expectResult[j]);
            }
        }
    }

    @Test
    public void testValuesGenerator() {
        List<Supplier<Integer>> gens = Lists.newArrayList();
        for (int i = 0; i < 3; ++i) {
            gens.add(Util.nextExpGenerator(2, 0));
        }
        Supplier<Optional<List<Integer>>> valuesGen = Util.nextValuesGenerator(1024, gens);

        String[] result = new String[] {"1,1,1",
                "2,1,1",
                "4,1,1",
                "8,1,1",
                "16,1,1",
                "32,1,1",
                "64,1,1",
                "128,1,1",
                "256,1,1",
                "512,1,1",
                "1024,1,1",
                "1024,2,1",
                "1024,4,1",
                "1024,8,1",
                "1024,16,1",
                "1024,32,1",
                "1024,64,1",
                "1024,128,1",
                "1024,256,1",
                "1024,512,1",
                "1024,1024,1",
                "1024,1024,2",
                "1024,1024,4",
                "1024,1024,8",
                "1024,1024,16",
                "1024,1024,32",
                "1024,1024,64",
                "1024,1024,128",
                "1024,1024,256",
                "1024,1024,512",
                "1024,1024,1024",
        };

        List<String> actual = IntStream.range(0, 100)
                .mapToObj(i -> valuesGen.get())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(values -> values.stream().map(Object::toString).collect(Collectors.joining(",")))
                .collect(Collectors.toList());

        Assert.assertEquals(actual.size(), result.length);
        for (int i = 0; i < result.length; ++i) {
            Assert.assertEquals(actual.get(i), result[i]);
        }
    }

    @Test
    public void testConsolidateMV() {
        ConnectContext ctx = getStarRocksAssert().getCtx();

        String q1 = "select sum(lo_revenue) as revenue\n" +
                "from lineorder join dates on lo_orderdate = d_datekey\n" +
                "where d_year = 1993 and lo_discount between 1 and 3 and lo_quantity < 25;";

        String q2 = "select count(lo_revenue) as revenue\n" +
                "from lineorder join dates on lo_orderdate = d_datekey\n" +
                "where d_year = 1994 and lo_discount between 2 and 7 and lo_quantity < 10;";

        String q3 = "select count(distinct lo_orderkey)\n" +
                "from lineorder join dates on lo_orderdate = d_datekey\n" +
                "where d_year = 1995 and lo_discount between 8 and 10 and lo_quantity < 5;";

        String q4 = "select d_year, max(lo_orderkey)\n" +
                "from lineorder join dates on lo_orderdate = d_datekey\n" +
                "where lo_discount between 8 and 10 and lo_quantity < 5\n" +
                "group by d_year;";

        List<Pair<String, String>> queryList = Lists.newArrayList(
                Pair.create("q1", q1),
                Pair.create("q2", q2),
                Pair.create("q3", q3),
                Pair.create("q4", q4));

        AutoMVUtil.mockUpCustomizedQueryExecutor(queryList);
        ctx.getSessionVariable().setAutoMVCardRowCountRatioHWM(1.0);
        ctx.getSessionVariable().setAutoMVCardRowCountRatioLWM(1.0);
        GlobalVariable.setAutoMVPerLatticeMVLimit(-1);
        GlobalVariable.setAutoMVPerLatticeMVSelectivityRatio(-1.0);
        List<Pair<String, AggregatePiece>> pieces = AutoMVUtil.getPieces(ctx, queryList);
        List<PlanPiece> pieceList = pieces.stream().map(p -> p.second).collect(Collectors.toList());
        AutoMVOptions options = AutoMVOptions.of(new PartitionExtractor(), ctx.getSessionVariable());
        SPJGMVRecommender mvRecommender = new SPJGMVRecommender(ctx, options);
        List<MVRecommendation> resultList = mvRecommender.recommend(pieceList, 0, Integer.MAX_VALUE);
        Assert.assertEquals(resultList.size(), 1);
        Assert.assertNotNull(resultList.get(0).getMvResult());
        String mv = resultList.get(0).getMvResult().getSubquery().getResult();
        String[] lines = new String[] {
                "_ta0000.lo_discount",
                "_ta0000.d_year",
                "_ta0000.lo_quantity",
                "(max(_ta0000.lo_orderkey)) AS _ca0003",
                "(bitmap_agg(_ta0000.lo_orderkey)) AS _ca0004",
                "(count(_ta0000.lo_revenue)) AS _ca0005",
                "(sum(_ta0000.lo_revenue)) AS _ca0006"
        };
        Assert.assertTrue(mv, Stream.of(lines).allMatch(mv::contains));
    }
}
