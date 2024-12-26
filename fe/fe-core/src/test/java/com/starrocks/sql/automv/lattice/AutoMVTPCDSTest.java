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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.automv.ast.CreateTunespaceStmt;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;
import com.starrocks.sql.automv.options.AutoMVOptions;
import com.starrocks.sql.automv.pattern.PlanPiecePatterns;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.FQTable;
import com.starrocks.sql.automv.pieces.PlanPieceBuilder;
import com.starrocks.sql.automv.policies.AggregatePolicies;
import com.starrocks.sql.automv.policies.AggregatePolicy;
import com.starrocks.sql.automv.qe.ColumnPlus;
import com.starrocks.sql.automv.qe.PartitionExtractor;
import com.starrocks.sql.automv.qe.QueryStatementPlus;
import com.starrocks.sql.automv.qe.RboOptimizer;
import com.starrocks.sql.automv.qe.TablePlus;
import com.starrocks.sql.automv.tunespace.MaterializedViewPlus;
import com.starrocks.sql.automv.tunespace.PlanPieceInfo;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.automv.util.MetaUtil;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TestUtil;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.thrift.TRowFormat;
import com.starrocks.utframe.StarRocksAssert;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AutoMVTPCDSTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert getStarRocksAssert() {
        FeConstants.runningUnitTest = true;
        if (STARROCKS_ASSERT.get() == null) {
            STARROCKS_ASSERT.set(TestUtil.prepareTables("tpcds", TestUtil::getTPCDSCreateTableSqlList));
        }
        return STARROCKS_ASSERT.get();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        getStarRocksAssert();
    }

    public static Stream<Arguments> nextQuery() {
        return TestUtil.getTPCDSQueryList().stream()
                .map(p -> Arguments.of(p.first));
    }

    @Test
    public void testAll() {
        List<Pair<String, String>> queryList = Lists.newArrayList(TestUtil.getTPCHQueryList());
        Collections.shuffle(queryList);
        queryList = queryList.subList(0, 5);
        AutoMVUtil.defaultTestHelper(STARROCKS_ASSERT.get().getCtx(), queryList);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nextQuery")
    public void testSingleQuery(String name) throws Exception {
        List<Pair<String, String>> queryList = TestUtil.getTPCDSQueryList()
                .stream()
                .filter(p -> p.first.equals(name))
                .collect(Collectors.toList());
        ConnectContext ctx = getStarRocksAssert().getCtx();
        AutoMVUtil.defaultTestHelper(ctx, queryList);
    }

    @Test
    public void testSingleQuery28() throws Exception {
        String query28 = TestUtil.getTPCDSQuery("query28");
        List<Pair<String, String>> queryList = Collections.singletonList(Pair.create("query28", query28));
        AutoMVUtil.testHelper(
                getStarRocksAssert().getCtx(),
                queryList,
                AutoMVUtil::configDefaultAutoMV,
                results -> {
                    Assert.assertFalse(results.isEmpty());
                }
        );
    }

    @Test
    public void testShowRecommendations1() {
        String q0 = "SELECT COUNT(DISTINCT ss_item_sk) FROM store_sales";
        String q1 = "SELECT MIN(ss_quantity), MAX(ss_quantity) FROM store_sales";
        String q2 = "SELECT ss_sold_date_sk, COUNT(*) FROM store_sales WHERE ss_sold_date_sk <> 0 " +
                "GROUP BY ss_sold_date_sk ORDER BY COUNT(*) DESC";
        List<Pair<String, String>> queryList = Arrays.asList(
                Pair.create("q0", q0),
                Pair.create("q1", q1),
                Pair.create("q2", q2));

        AutoMVUtil.testHelper(getStarRocksAssert().getCtx(), queryList,
                sv -> {
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                },
                results -> {
                    Assert.assertEquals(results.size(), 2);
                    List<String> r0 = results.get(0);
                    List<String> r1 = results.get(1);
                    String mv0 = r0.get(2);
                    String numAcceleratedQueries0 = r0.get(12);
                    Assert.assertTrue(mv0, mv0.contains("SELECT\n" +
                            "  `tpcds`.`store_sales`.ss_sold_date_sk\n" +
                            "  ,(bitmap_agg(`tpcds`.`store_sales`.ss_item_sk)) AS _ca0002\n" +
                            "  ,(max(`tpcds`.`store_sales`.ss_quantity)) AS _ca0003\n" +
                            "  ,(min(`tpcds`.`store_sales`.ss_quantity)) AS _ca0004\n" +
                            "  ,(count(1)) AS _ca0005\n" +
                            "FROM"));
                    Assert.assertEquals(numAcceleratedQueries0, "3");

                    String mv1 = r1.get(2);
                    String numAcceleratedQueries1 = r1.get(12);
                    Assert.assertTrue(mv1, mv1.contains("SELECT\n" +
                            "  (1) AS _ca0002\n" +
                            "  ,(max(`tpcds`.`store_sales`.ss_quantity)) AS _ca0003\n" +
                            "  ,(min(`tpcds`.`store_sales`.ss_quantity)) AS _ca0004\n" +
                            "  ,(bitmap_agg(`tpcds`.`store_sales`.ss_item_sk)) AS _ca0005\n" +
                            "FROM"));
                    Assert.assertEquals(numAcceleratedQueries1, "2");
                });

    }

    @Test
    public void testShowRecommendations2() {
        String q0 =
                "SELECT ss_cdemo_sk, COUNT(DISTINCT ss_item_sk,ss_ticket_number) FROM store_sales group by ss_cdemo_sk";
        String q1 = "SELECT COUNT(DISTINCT ss_item_sk,ss_ticket_number) FROM store_sales";
        List<Pair<String, String>> queryList = Arrays.asList(
                Pair.create("q0", q0),
                Pair.create("q2", q1));

        AutoMVUtil.testHelper(
                getStarRocksAssert().getCtx(),
                queryList,
                sv -> {
                    sv.setAutoMVEnableComplexDerivedMetrics(false);
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                },
                results -> {
                    Assert.assertTrue(results.isEmpty());
                });

        AutoMVUtil.testHelper(
                getStarRocksAssert().getCtx(),
                queryList,
                sv -> {
                    sv.setAutoMVEnableComplexDerivedMetrics(true);
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                },
                results -> {
                    Assert.assertEquals(results.size(), 2);
                    String mv0 = results.get(0).get(2);
                    String mv1 = results.get(1).get(2);
                    Assert.assertTrue(mv0, mv0.contains("SELECT\n" +
                            "  `tpcds`.`store_sales`.ss_cdemo_sk\n" +
                            "  ,(bitmap_agg(if((`tpcds`.`store_sales`.ss_item_sk IS NULL)," +
                            " NULL, `tpcds`.`store_sales`.ss_ticket_number))) AS _ca0002\n" +
                            "FROM\n" +
                            "  `tpcds`.`store_sales`\n" +
                            "GROUP BY\n" +
                            "  `tpcds`.`store_sales`.ss_cdemo_sk"));
                    Assert.assertTrue(mv1, mv1.contains("SELECT\n" +
                            "  (1) AS _ca0002\n" +
                            "  ,(bitmap_agg(if((`tpcds`.`store_sales`.ss_item_sk IS NULL), NULL, " +
                            "`tpcds`.`store_sales`.ss_ticket_number))) AS _ca0003\n" +
                            "FROM\n" +
                            "  `tpcds`.`store_sales`"));
                });
    }

    @Test
    public void testShowRecommendations3() {
        String q0 = "select cc_mkt_desc,  count(distinct cc_sq_ft) as cnt " +
                "from call_center where cc_mkt_desc <> '' " +
                "GROUP BY cc_mkt_desc ORDER BY cnt DESC LIMIT 10;";
        List<Pair<String, String>> queryList = Arrays.asList(Pair.create("q0", q0));
        AutoMVUtil.testHelper(
                getStarRocksAssert().getCtx(),
                queryList,
                sv -> {
                    AutoMVUtil.configDefaultAutoMV(sv);
                    sv.setAutoMVDefaultPartitionByTimeGranule("none");
                },
                results -> {
                    String mv = results.get(0).get(2);
                    Assert.assertTrue(mv, mv.contains("SELECT\n" +
                            "  `tpcds`.`call_center`.cc_mkt_desc\n" +
                            "  ,(bitmap_agg(`tpcds`.`call_center`.cc_sq_ft)) AS _ca0002\n" +
                            "FROM\n" +
                            "  `tpcds`.`call_center`\n" +
                            "GROUP BY\n" +
                            "  `tpcds`.`call_center`.cc_mkt_desc"));
                });
    }

    @Test
    public void testShowRecommendations4() {
        String q0 = "select count(distinct cc_sq_ft) as cnt from call_center";
        String q1 = "select sum(cc_sq_ft) as cnt from call_center";
        String q2 = "select count(cc_mkt_desc),  max(distinct cc_sq_ft) as cnt from call_center";
        List<Pair<String, String>> queryList = Arrays.asList(
                Pair.create("q0", q0),
                Pair.create("q1", q1),
                Pair.create("q2", q2));

        AutoMVUtil.testHelper(
                getStarRocksAssert().getCtx(),
                queryList,
                sv -> {
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                    sv.setAutoMVDefaultPartitionByTimeGranule("none");
                },
                results -> {
                    String mv = results.get(0).get(2);
                    Assert.assertTrue(mv, mv.contains("SELECT\n" +
                            "  (1) AS _ca0002\n" +
                            "  ,(count(`tpcds`.`call_center`.cc_mkt_desc)) AS _ca0003\n" +
                            "  ,(max(`tpcds`.`call_center`.cc_sq_ft)) AS _ca0004\n" +
                            "  ,(sum(`tpcds`.`call_center`.cc_sq_ft)) AS _ca0005\n" +
                            "  ,(bitmap_agg(`tpcds`.`call_center`.cc_sq_ft)) AS _ca0006\n" +
                            "FROM\n" +
                            "  `tpcds`.`call_center`"));
                });
    }

    @Test
    public void testShowRecommendations5() {
        String q0 = "select cc_mkt_desc, count(distinct cc_sq_ft) as cnt from call_center group by cc_mkt_desc";
        String q1 = "select sum(cc_sq_ft) as cnt from call_center group by cc_mkt_desc";
        String q2 = "select cc_mkt_desc, count(distinct cc_sq_ft) as cnt \n" +
                "from call_center \n" +
                "where cc_name <> '' \n" +
                "group by cc_mkt_desc";

        List<Pair<String, String>> queryList = Arrays.asList(
                Pair.create("q0", q0),
                Pair.create("q1", q1),
                Pair.create("q2", q2));

        AutoMVUtil.testHelper(
                getStarRocksAssert().getCtx(),
                queryList,
                sv -> {
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                    sv.setAutoMVDefaultPartitionByTimeGranule("none");
                },
                results -> {
                    String mv0 = results.get(0).get(2);
                    String numAcceleratedQueries0 = results.get(0).get(12);
                    Assert.assertTrue(mv0, mv0.contains("SELECT\n" +
                            "  `tpcds`.`call_center`.cc_mkt_desc\n" +
                            "  ,(sum(`tpcds`.`call_center`.cc_sq_ft)) AS _ca0002\n" +
                            "  ,(bitmap_agg(`tpcds`.`call_center`.cc_sq_ft)) AS _ca0003\n" +
                            "FROM\n" +
                            "  `tpcds`.`call_center`\n" +
                            "GROUP BY\n" +
                            "  `tpcds`.`call_center`.cc_mkt_desc"));
                    Assert.assertEquals(numAcceleratedQueries0, "2");

                    String mv1 = results.get(1).get(2);
                    String numAcceleratedQueries1 = results.get(1).get(12);
                    Assert.assertTrue(mv1, mv1.contains("SELECT\n" +
                            "  _ta0000.cc_mkt_desc\n" +
                            "  ,(bitmap_agg(_ta0000.cc_sq_ft)) AS _ca0003\n" +
                            "FROM\n" +
                            "  (\n" +
                            "    SELECT\n" +
                            "      `tpcds`.`call_center`.cc_sq_ft\n" +
                            "      ,`tpcds`.`call_center`.cc_mkt_desc\n" +
                            "    FROM\n" +
                            "      `tpcds`.`call_center`\n" +
                            "    WHERE\n" +
                            "      (`tpcds`.`call_center`.cc_name != \"\")\n" +
                            "  ) _ta0000\n" +
                            "GROUP BY\n" +
                            "  _ta0000.cc_mkt_desc"));
                    Assert.assertEquals(numAcceleratedQueries1, "1");
                });
    }

    @Test
    public void testShowRecommendations6() {
        String q0 = "select cc_mkt_desc, count(distinct cc_sq_ft) as cnt from call_center group by cc_mkt_desc";
        String q1 = "select cc_mkt_desc, count(distinct cc_sq_ft) as cnt \n" +
                "from call_center \n" +
                "where cc_name <> '' \n" +
                "group by cc_mkt_desc";

        List<Pair<String, String>> queryList = Arrays.asList(
                Pair.create("q0", q0),
                Pair.create("q1", q1),
                Pair.create("q2", q0),
                Pair.create("q3", q1));

        AutoMVUtil.testHelper(
                getStarRocksAssert().getCtx(),
                queryList,
                sv -> {
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                    sv.setAutoMVDefaultPartitionByTimeGranule("none");
                },
                results -> {
                    String mv0 = results.get(0).get(2);
                    String numAcceleratedQueries0 = results.get(0).get(12);

                    String mv1 = results.get(1).get(2);
                    String numAcceleratedQueries1 = results.get(1).get(12);

                    Assert.assertTrue(mv0, mv0.contains("SELECT\n" +
                            "  `tpcds`.`call_center`.cc_mkt_desc\n" +
                            "  ,(bitmap_agg(`tpcds`.`call_center`.cc_sq_ft)) AS _ca0002\n" +
                            "FROM\n" +
                            "  `tpcds`.`call_center`\n" +
                            "GROUP BY\n" +
                            "  `tpcds`.`call_center`.cc_mkt_desc"));

                    Assert.assertEquals(numAcceleratedQueries0, "2");

                    Assert.assertTrue(mv1, mv1.contains("SELECT\n" +
                            "  _ta0000.cc_mkt_desc\n" +
                            "  ,(bitmap_agg(_ta0000.cc_sq_ft)) AS _ca0003\n" +
                            "FROM\n" +
                            "  (\n" +
                            "    SELECT\n" +
                            "      `tpcds`.`call_center`.cc_sq_ft\n" +
                            "      ,`tpcds`.`call_center`.cc_mkt_desc\n" +
                            "    FROM\n" +
                            "      `tpcds`.`call_center`\n" +
                            "    WHERE\n" +
                            "      (`tpcds`.`call_center`.cc_name != \"\")\n" +
                            "  ) _ta0000\n" +
                            "GROUP BY\n" +
                            "  _ta0000.cc_mkt_desc"));
                    Assert.assertEquals(numAcceleratedQueries1, "2");
                });

        AutoMVUtil.testHelper(
                getStarRocksAssert().getCtx(),
                queryList,
                sv -> {
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                    sv.setAutoMVUseBitmapCountDistinct(false);
                    sv.setAutoMVPruneRollupUnableAggregateWithConjuncts(true);
                },
                results -> {
                    Assert.assertEquals(results.size(), 1);
                    String mv = results.get(0).get(2);
                    Assert.assertTrue(mv, mv.contains("SELECT\n" +
                            "  `tpcds`.`call_center`.cc_mkt_desc\n" +
                            "  ,(count(DISTINCT `tpcds`.`call_center`.cc_sq_ft)) AS _ca0002\n" +
                            "FROM\n" +
                            "  `tpcds`.`call_center`\n" +
                            "GROUP BY\n" +
                            "  `tpcds`.`call_center`.cc_mkt_desc"));
                });

        AutoMVUtil.testHelper(
                getStarRocksAssert().getCtx(),
                queryList,
                sv -> {
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                    sv.setAutoMVUseBitmapCountDistinct(false);
                    sv.setAutoMVUseHllCountDistinct(true);
                    sv.setAutoMVPruneRollupUnableAggregateWithConjuncts(true);
                    sv.setAutoMVDefaultPartitionByTimeGranule("none");
                },
                results -> {
                    Assert.assertEquals(results.size(), 2);
                    List<String> mvList = results.stream().map(r -> r.get(2)).collect(Collectors.toList());
                    Assert.assertTrue(String.join("\n", mvList), mvList.stream().anyMatch(mv -> mv.contains("SELECT\n" +
                            "  _ta0000.cc_mkt_desc\n" +
                            "  ,(hll_union(hll_hash(_ta0000.cc_sq_ft))) AS _ca0003\n" +
                            "FROM\n" +
                            "  (\n" +
                            "    SELECT\n" +
                            "      `tpcds`.`call_center`.cc_sq_ft\n" +
                            "      ,`tpcds`.`call_center`.cc_mkt_desc\n" +
                            "    FROM\n" +
                            "      `tpcds`.`call_center`\n" +
                            "    WHERE\n" +
                            "      (`tpcds`.`call_center`.cc_name != \"\")\n" +
                            "  ) _ta0000\n" +
                            "GROUP BY\n" +
                            "  _ta0000.cc_mkt_desc")));
                });

        AutoMVUtil.testHelper(
                getStarRocksAssert().getCtx(),
                queryList,
                sv -> {
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                    sv.setAutoMVUseBitmapCountDistinct(false);
                    sv.setAutoMVUseHllCountDistinct(false);
                    sv.setAutoMVUseArrayAggCountDistinct(true);
                    sv.setAutoMVPruneRollupUnableAggregateWithConjuncts(true);
                    sv.setAutoMVDefaultPartitionByTimeGranule("none");
                },
                results -> {
                    Assert.assertEquals(results.size(), 2);
                    String mv = results.get(0).get(2);
                    Assert.assertTrue(mv, mv.contains("SELECT\n" +
                            "  `tpcds`.`call_center`.cc_mkt_desc\n" +
                            "  ,(array_agg(DISTINCT `tpcds`.`call_center`.cc_sq_ft)) AS _ca0002\n" +
                            "FROM\n" +
                            "  `tpcds`.`call_center`\n" +
                            "GROUP BY\n" +
                            "  `tpcds`.`call_center`.cc_mkt_desc"));
                });

    }

    @Test
    public void testShowRecommendations7() {
        String q0 = "SELECT\n" +
                "  cc_market_manager,\n" +
                "  MIN(cc_name),\n" +
                "  MIN(cc_class),\n" +
                "  COUNT(*) AS c,\n" +
                "  COUNT(DISTINCT cc_call_center_id)\n" +
                "FROM\n" +
                "  call_center\n" +
                "WHERE\n" +
                "  cc_class LIKE '%Google%'\n" +
                "  AND cc_name NOT LIKE '%.google.%'\n" +
                "  AND cc_market_manager <> ''\n" +
                "GROUP BY\n" +
                "  cc_market_manager\n" +
                "ORDER BY\n" +
                "  c Desc,\n" +
                "  cc_market_manager,\n" +
                "  MIN(cc_name),\n" +
                "  MIN(cc_class)\n" +
                "LIMIT\n" +
                "  10;";

        List<Pair<String, String>> queryList = Arrays.asList(
                Pair.create("q0", q0),
                Pair.create("q1", q0));

        AutoMVUtil.testHelper(
                getStarRocksAssert().getCtx(),
                queryList,
                sv -> {
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                },
                results -> {
                    Assert.assertTrue(results.isEmpty());
                });

        AutoMVUtil.testHelper(
                getStarRocksAssert().getCtx(),
                queryList,
                sv -> {
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                    sv.setAutoMVPruneRollupUnableAggregateWithConjuncts(false);
                },
                results -> {
                    Assert.assertEquals(results.size(), 1);
                    String mv = results.get(0).get(2);
                    Assert.assertTrue(mv, mv.contains("SELECT\n" +
                            "  _ta0000.cc_market_manager\n" +
                            "  ,(count(DISTINCT _ta0000.cc_call_center_id)) AS _ca0003\n" +
                            "  ,(count(1)) AS _ca0004\n" +
                            "  ,(min(_ta0000.cc_class)) AS _ca0005\n" +
                            "  ,(min(_ta0000.cc_name)) AS _ca0006\n" +
                            "FROM\n" +
                            "  (\n" +
                            "    SELECT\n" +
                            "      `tpcds`.`call_center`.cc_call_center_id\n" +
                            "      ,`tpcds`.`call_center`.cc_name\n" +
                            "      ,`tpcds`.`call_center`.cc_class\n" +
                            "      ,`tpcds`.`call_center`.cc_market_manager\n" +
                            "    FROM\n" +
                            "      `tpcds`.`call_center`\n" +
                            "    WHERE\n" +
                            "      (`tpcds`.`call_center`.cc_class like \"%Google%\")\n" +
                            "      AND (NOT (`tpcds`.`call_center`.cc_name like \"%.google.%\"))\n" +
                            "  ) _ta0000\n" +
                            "GROUP BY\n" +
                            "  _ta0000.cc_market_manager"));
                });
    }

    @Test
    public void testShowRecommendations8() {
        String q0 = "SELECT\n" +
                "  cc_call_center_id,\n" +
                "  cc_open_date_sk,\n" +
                "  CASE\n" +
                "    WHEN (\n" +
                "      cc_call_center_id = 0\n" +
                "      AND cc_open_date_sk = 0\n" +
                "    ) THEN cc_mkt_desc\n" +
                "    ELSE ''\n" +
                "  END AS Src,\n" +
                "  cc_market_manager AS Dst,\n" +
                "  COUNT(*) AS cnt\n" +
                "FROM\n" +
                "  call_center\n" +
                "WHERE\n" +
                "  cc_mkt_id = 62\n" +
                "  AND cc_rec_start_date >= '2013-07-01'\n" +
                "  AND cc_rec_start_date <= '2013-07-31'\n" +
                "  AND cc_sq_ft = 0\n" +
                "GROUP BY\n" +
                "  cc_call_center_id,\n" +
                "  cc_open_date_sk,\n" +
                "  Src,\n" +
                "  Dst\n" +
                "ORDER BY\n" +
                "  cnt DESC;";

        List<Pair<String, String>> queryList = Arrays.asList(
                Pair.create("q0", q0),
                Pair.create("q1", q0)
        );

        AutoMVUtil.testHelper(
                getStarRocksAssert().getCtx(),
                queryList,
                sv -> {
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                    sv.setAutoMVPruneRollupUnableAggregateWithConjuncts(false);
                    sv.setAutoMVEnableComplexDerivedDimensions(true);
                },
                results -> {
                    Assert.assertEquals(1, results.size());
                    String mv = results.get(0).get(2);
                    Assert.assertTrue(mv, mv.contains("GROUP BY\n" +
                            "  `tpcds`.`call_center`.cc_rec_start_date\n" +
                            "  ,`tpcds`.`call_center`.cc_sq_ft\n" +
                            "  ,`tpcds`.`call_center`.cc_mkt_id\n" +
                            "  ,`tpcds`.`call_center`.cc_open_date_sk\n" +
                            "  ,`tpcds`.`call_center`.cc_call_center_id"));
                    Assert.assertTrue(mv, mv.contains(",if(((`tpcds`.`call_center`.cc_call_center_id"));
                    Assert.assertTrue(mv, mv.contains(",`tpcds`.`call_center`.cc_market_manager"));
                });
        AutoMVUtil.testHelper(
                getStarRocksAssert().getCtx(),
                queryList,
                sv -> {
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVPruneRollupUnableAggregateWithConjuncts(false);
                    sv.setAutoMVEnableComplexDerivedDimensions(false);
                },
                results -> {
                    Assert.assertTrue(results.isEmpty());
                });
    }

    @Test
    public void testShowRecommendations9() {
        String q0 = "SELECT\n" +
                "  sum(cc_call_center_id),\n" +
                "  sum(cc_call_center_id+1),\n" +
                "  sum(cc_call_center_id-2),\n" +
                "  sum(cc_call_center_id*4+3),\n" +
                "  sum(cc_call_center_id*4-4),\n" +
                "  sum(cc_call_center_id),\n" +
                "  sum(1+cc_call_center_id),\n" +
                "  sum(2-cc_call_center_id),\n" +
                "  sum(3+cc_call_center_id*4),\n" +
                "  sum(4-cc_call_center_id*4)\n" +
                "FROM\n" +
                "  call_center\n" +
                "WHERE\n" +
                "  cc_mkt_id = 62\n" +
                "  AND cc_rec_start_date >= '2013-07-01'\n" +
                "  AND cc_rec_start_date <= '2013-07-31'\n" +
                "  AND cc_sq_ft = 0\n" +
                "GROUP BY\n" +
                "  cc_open_date_sk;";

        List<Pair<String, String>> queryList = Arrays.asList(
                Pair.create("q0", q0),
                Pair.create("q1", q0)
        );
        AutoMVUtil.testHelper(
                getStarRocksAssert().getCtx(),
                queryList,
                sv -> {
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVPruneRollupUnableAggregateWithConjuncts(false);
                },
                results -> {
                    Assert.assertEquals(results.size(), 1);
                    String mv = results.get(0).get(2);
                    Assert.assertTrue(mv, mv.contains("SELECT\n" +
                            "  `tpcds`.`call_center`.cc_rec_start_date\n" +
                            "  ,`tpcds`.`call_center`.cc_sq_ft\n" +
                            "  ,`tpcds`.`call_center`.cc_mkt_id\n" +
                            "  ,`tpcds`.`call_center`.cc_open_date_sk\n" +
                            "  ,(count(`tpcds`.`call_center`.cc_call_center_id)) AS _ca0002\n" +
                            "  ,(count((`tpcds`.`call_center`.cc_call_center_id * 4))) AS _ca0003\n" +
                            "  ,(sum(`tpcds`.`call_center`.cc_call_center_id)) AS _ca0004\n" +
                            "  ,(sum((`tpcds`.`call_center`.cc_call_center_id * 4))) AS _ca0005\n" +
                            "FROM\n" +
                            "  `tpcds`.`call_center`\n" +
                            "GROUP BY\n" +
                            "  `tpcds`.`call_center`.cc_rec_start_date\n" +
                            "  ,`tpcds`.`call_center`.cc_sq_ft\n" +
                            "  ,`tpcds`.`call_center`.cc_mkt_id\n" +
                            "  ,`tpcds`.`call_center`.cc_open_date_sk"));
                });
    }

    @Test
    public void testShowRecommendations10() {
        String q0 = "SELECT\n" +
                "  sum(cc_call_center_id*cc_company)" +
                "FROM\n" +
                "  call_center\n" +
                "GROUP BY\n" +
                "  cc_open_date_sk;";
        String q1 = "SELECT\n" +
                "  max(cc_call_center_id*cc_company)" +
                "FROM\n" +
                "  call_center\n" +
                "GROUP BY\n" +
                "  cc_open_date_sk;";

        List<Pair<String, String>> queryList = Arrays.asList(
                Pair.create("q0", q0),
                Pair.create("q1", q1)
        );
        AutoMVUtil.testHelper(
                getStarRocksAssert().getCtx(),
                queryList,
                sv -> {
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                    sv.setAutoMVPruneRollupUnableAggregateWithConjuncts(false);
                    sv.setAutoMVDefaultPartitionByTimeGranule("none");
                },
                results -> {
                    Assert.assertEquals(results.size(), 1);
                    String mv = results.get(0).get(2);
                    Assert.assertTrue(mv, mv.contains("SELECT\n" +
                            "  `tpcds`.`call_center`.cc_open_date_sk\n" +
                            "  ,(max((`tpcds`.`call_center`.cc_call_center_id * `tpcds`.`call_center`.cc_company)))" +
                            " AS _ca0002\n" +
                            "  ,(sum((`tpcds`.`call_center`.cc_call_center_id * `tpcds`.`call_center`.cc_company)))" +
                            " AS _ca0003\n" +
                            "FROM\n" +
                            "  `tpcds`.`call_center`\n" +
                            "GROUP BY\n" +
                            "  `tpcds`.`call_center`.cc_open_date_sk"));
                });

    }

    @Test
    public void testShowRecommendationsWithPartitionPredicatesReserved() {

        String sqlFmt = "select  \n" +
                "   substr(w_warehouse_name,1,20)\n" +
                "  ,sm_type\n" +
                "  ,cc_name\n" +
                "  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk <= 30 ) then 1 else 0 end) as \"30 days\"\n" +
                "from\n" +
                "   catalog_sales\n" +
                "  ,warehouse\n" +
                "  ,ship_mode\n" +
                "  ,call_center\n" +
                "  ,date_dim\n" +
                "where\n" +
                "    d_month_seq between 1200 and 1200 + 11\n" +
                "and %s\n" +
                "and cs_ship_date_sk   = d_date_sk\n" +
                "and cs_warehouse_sk   = w_warehouse_sk\n" +
                "and cs_ship_mode_sk   = sm_ship_mode_sk\n" +
                "and cs_call_center_sk = cc_call_center_sk\n" +
                "\n" +
                "group by\n" +
                "   substr(w_warehouse_name,1,20)\n" +
                "  ,sm_type\n" +
                "  ,cc_name;";

        List<Pair<String, String>> queryList = Stream.of(
                        Pair.create("q0", "cc_rec_start_date in ('2023-06-02','2024-06-09')"),
                        Pair.create("q1", "cc_rec_start_date > '2023-06-06'"),
                        Pair.create("q2", "cc_rec_start_date > '2023-06-04'"))
                .map(p -> Pair.create(p.first, String.format(sqlFmt, p.second)))
                .collect(Collectors.toList());

        AutoMVUtil.testHelper(
                getStarRocksAssert().getCtx(),
                queryList,
                sv -> {
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                    sv.setAutoMVPruneRollupUnableAggregateWithConjuncts(false);
                    sv.setAutoMVEnableComplexDerivedMetrics(true);
                    sv.setAutoMVPartialRollupMinAggPieces(2);
                },
                results -> {
                    Assert.assertEquals(results.size(), 1);
                    String mv = results.get(0).get(2);
                    Assert.assertTrue(mv, mv.contains("SELECT\n" +
                            "  _ta0001.d_month_seq\n" +
                            "  ,(substr(_ta0001.w_warehouse_name, 1, 20)) AS _ca0004\n" +
                            "  ,_ta0001.sm_type\n" +
                            "  ,_ta0001.cc_name\n" +
                            "  ,(sum(if(((_ta0001.cs_ship_date_sk - _ta0001.cs_sold_date_sk) <= 30), 1, 0))) AS _ca0005\n" +
                            "FROM\n" +
                            "  (\n" +
                            "    SELECT\n" +
                            "      `tpcds`.`date_dim`.d_month_seq\n" +
                            "      ,`tpcds`.`call_center`.cc_name\n" +
                            "      ,`tpcds`.`ship_mode`.sm_type\n" +
                            "      ,`tpcds`.`warehouse`.w_warehouse_name\n" +
                            "      ,`tpcds`.`catalog_sales`.cs_sold_date_sk\n" +
                            "      ,`tpcds`.`catalog_sales`.cs_ship_date_sk\n" +
                            "    FROM\n" +
                            "      `tpcds`.`catalog_sales`\n" +
                            "      INNER JOIN\n" +
                            "      `tpcds`.`warehouse`\n" +
                            "      ON (`tpcds`.`catalog_sales`.cs_warehouse_sk = `tpcds`.`warehouse`.w_warehouse_sk)\n" +
                            "      INNER JOIN\n" +
                            "      `tpcds`.`ship_mode`\n" +
                            "      ON (`tpcds`.`catalog_sales`.cs_ship_mode_sk = `tpcds`.`ship_mode`.sm_ship_mode_sk)\n" +
                            "      INNER JOIN\n" +
                            "      `tpcds`.`call_center`\n" +
                            "      ON (`tpcds`.`catalog_sales`.cs_call_center_sk = `tpcds`.`call_center`.cc_call_center_sk)\n" +
                            "      INNER JOIN\n" +
                            "      `tpcds`.`date_dim`\n" +
                            "      ON (`tpcds`.`catalog_sales`.cs_ship_date_sk = `tpcds`.`date_dim`.d_date_sk)\n" +
                            "    WHERE\n" +
                            "      (\"2023-06-02\" <= `tpcds`.`call_center`.cc_rec_start_date)\n" +
                            "  ) _ta0001\n" +
                            "GROUP BY\n" +
                            "  _ta0001.d_month_seq\n" +
                            "  ,substr(_ta0001.w_warehouse_name, 1, 20)\n" +
                            "  ,_ta0001.sm_type\n" +
                            "  ,_ta0001.cc_name"));
                });
    }

    @Test
    public void testPopolateLegacyMV() throws Exception {
        String sql = TestUtil.getTPCDSQuery("query01");

        ConnectContext ctx = getStarRocksAssert().getCtx();
        Map<String, String> mvMap = AutoMVUtil.getMaterializedViews(ctx, sql);
        for (Map.Entry<String, String> entry : mvMap.entrySet()) {
            String mvName = entry.getKey();
            String mvSchema = entry.getValue();
            getStarRocksAssert().withMaterializedView(mvSchema);
            String db = ctx.getDatabase();
            MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(db)
                    .getTable(mvName);
            TableName mvFqName = new TableName(null, ctx.getDatabase(), mvName);
            MaterializedViewPlus mvPlus = MaterializedViewPlus.of(mv, mvFqName);
            String mvSchema2 = mvPlus.getCreateMaterializedViewSql();
            System.out.println(mvSchema2);
            getStarRocksAssert().dropMaterializedView(mvName);

            mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(db)
                    .getTable(mvName);
            Assert.assertNull(mv);
            getStarRocksAssert().withMaterializedView(mvSchema2);
            mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(db)
                    .getTable(mvName);
            mvPlus = MaterializedViewPlus.of(mv, mvFqName);
            String mvSchema3 = mvPlus.getCreateMaterializedViewSql();
            Assert.assertEquals(mvSchema2, mvSchema3);
        }
    }

    @Test
    public void testTunespaceOperations() {
        ConnectContext ctx = getStarRocksAssert().getCtx();
        StatementBase stmt = RboOptimizer.parseAndAnalyze(ctx, "CREATE TUNESPACE _tunespace_");
        Assert.assertTrue(stmt instanceof CreateTunespaceStmt);
        CreateTunespaceStmt createTunespaceStmt = (CreateTunespaceStmt) stmt;
        Assert.assertEquals("_tunespace_", createTunespaceStmt.getTableName().getTbl());
    }

    @Test
    public void testAppendStmt() throws Exception {
        ConnectContext ctx = getStarRocksAssert().getCtx();
        TablePlus table = PlanPieceInfo.getTable("_tunespace_", 1, 1);
        String name = "query01";
        String sql = TestUtil.getTPCDSQuery(name);
        getStarRocksAssert().withTable(table.getCreateTableSql());
        TableName tsName =
                new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "tpcds", table.getTable().getName());
        while (true) {
            boolean hasPartitions = MetaUtil.checkTable(tsName, false, (db, tbl) -> !tbl.getPartitions().isEmpty());
            if (hasPartitions) {
                break;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
        try {
            QueryStatementPlus stmt = RboOptimizer.getQueryStatement(ctx, sql);
            QueryStatement queryStmt = stmt.getQueryStatement();
            Map<String, FQTable> fqTableMap = stmt.getFqTableMap();
            List<OptExpression> subPlans =
                    RboOptimizer.getSubPlans(queryStmt, ctx, PlanPiecePatterns.getSPJG());

            AutoMVOptions options = AutoMVOptions.of(new PartitionExtractor(), ctx.getSessionVariable());
            List<PlanPieceInfo> pieceInfos = subPlans.stream()
                    .map(subPlan -> PlanPieceInfo.from(options, name, subPlan, false, fqTableMap))
                    .collect(Collectors.toList());
            String insertSql = table.getInsertSql(pieceInfos);
            StatementBase insertStmt = RboOptimizer.parseAndAnalyze(ctx, insertSql);
            Assert.assertTrue(insertStmt instanceof InsertStmt);
            InsertStmt insertStmt1 = (InsertStmt) insertStmt;
            Assert.assertTrue(insertStmt1.getQueryStatement().getQueryRelation() instanceof ValuesRelation);
            ValuesRelation values = (ValuesRelation) insertStmt1.getQueryStatement().getQueryRelation();
            Assert.assertEquals(values.getRows().size(), 2);
        } finally {
            getStarRocksAssert().dropTable("_tunespace_");
        }
    }

    @Test
    public void testParseRowFormat() throws Exception {
        ConnectContext ctx = getStarRocksAssert().getCtx();
        String name = "query01";
        String sql = TestUtil.getTPCDSQuery(name);
        QueryStatementPlus queryStmtPlus = RboOptimizer.getQueryStatement(ctx, sql);
        QueryStatement queryStmt = queryStmtPlus.getQueryStatement();
        Map<String, FQTable> fqTableMap = queryStmtPlus.getFqTableMap();
        List<OptExpression> subPlans = RboOptimizer.getSubPlans(queryStmt, ctx, PlanPiecePatterns.getSPJG());
        OptExpression subPlan = subPlans.get(0);
        ColumnRefToIdConverter idConverter = new ColumnRefToIdConverter();
        Optional<AggregatePiece> optPlanPiece =
                PlanPieceBuilder.createPlanPiece(name, subPlan, idConverter, fqTableMap).cast(AggregatePiece.class);
        Preconditions.checkArgument(optPlanPiece.isPresent());
        AggregatePiece planPiece = optPlanPiece.get();
        PrettyPrinter traceLog = new PrettyPrinter();

        AutoMVOptions options = AutoMVOptions.of(new PartitionExtractor(), ctx.getSessionVariable());
        AggregatePolicy policy = AggregatePolicies.defaultPolicies(options, traceLog);
        PlanPieceInfo pieceInfo = PlanPieceInfo.from(planPiece, policy, fqTableMap);
        TablePlus table = PlanPieceInfo.getTable("tunespace", 1, 1);
        String insertSql = table.getInsertSql(Collections.singletonList(pieceInfo));
        System.out.println(insertSql);
        List<ColumnPlus> columns = table.getColumnPluses();
        TRowFormat row = ColumnPlus.pack(pieceInfo, columns);
        TSerializer serializer = new TSerializer(TCompactProtocol::new);
        byte[] data = serializer.serialize(row);
        TDeserializer deserializer = new TDeserializer(TCompactProtocol::new);
        TRowFormat newRow = new TRowFormat();
        deserializer.deserialize(newRow, data);
        PlanPieceInfo newPieceInfo = ColumnPlus.unpack(PlanPieceInfo.class, columns, newRow);
        String newInsertSql = table.getInsertSql(Collections.singletonList(newPieceInfo));
        Assert.assertEquals(newInsertSql, insertSql);
    }

    @Test
    public void recommendRollupCountDistinct() throws IOException {
        String sql = "SELECT\n" +
                "  (count(distinct _ta0000.ss_quantity)) AS _ca0004\n" +
                "  ,_ta0000.ca_country\n" +
                "FROM\n" +
                "  (\n" +
                "    SELECT\n" +
                "      store_sales.ss_ext_wholesale_cost\n" +
                "      ,store_sales.ss_quantity\n" +
                "      ,store_sales.ss_ext_sales_price\n" +
                "      ,date_dim.d_year\n" +
                "      ,customer_address.ca_state\n" +
                "      ,customer_address.ca_country\n" +
                "      ,household_demographics.hd_dep_count\n" +
                "      ,customer_demographics.cd_marital_status\n" +
                "      ,customer_demographics.cd_education_status\n" +
                "      ,store_sales.ss_sales_price\n" +
                "      ,store_sales.ss_net_profit\n" +
                "    FROM\n" +
                "      store_sales\n" +
                "      INNER JOIN\n" +
                "      store\n" +
                "      ON (store.s_store_sk = store_sales.ss_store_sk)\n" +
                "      INNER JOIN\n" +
                "      customer_demographics\n" +
                "      ON (customer_demographics.cd_demo_sk = store_sales.ss_cdemo_sk)\n" +
                "      INNER JOIN\n" +
                "      household_demographics\n" +
                "      ON (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)\n" +
                "      INNER JOIN\n" +
                "      customer_address\n" +
                "      ON (store_sales.ss_addr_sk = customer_address.ca_address_sk)\n" +
                "      INNER JOIN\n" +
                "      date_dim\n" +
                "      ON (store_sales.ss_sold_date_sk = date_dim.d_date_sk)\n" +
                "  ) _ta0000\n" +
                "WHERE\n" +
                "  _ta0000.d_year between 1992 and 2023\n" +
                "  and _ta0000.ca_state = 'ASIA'\n" +
                "GROUP BY\n" +
                "  _ta0000.ca_country";

        ConnectContext ctx = getStarRocksAssert().getCtx();
        String savedSVs = ctx.getSessionVariable().getJsonString();
        Object[][] testCases = new Object[][] {
                {true, false, false, "(bitmap_agg(_ta0000.ss_quantity)) AS _ca0003"},
                {false, true, false, "(hll_union(hll_hash(_ta0000.ss_quantity))) AS _ca0003"},
                {false, false, true, "(array_agg(DISTINCT _ta0000.ss_quantity)) AS _ca0003"}
        };
        for (Object[] tc : testCases) {
            boolean useBitmap = (Boolean) tc[0];
            boolean useHll = (Boolean) tc[1];
            boolean useArrayAgg = (Boolean) tc[2];
            String snippet = (String) tc[3];
            ctx.getSessionVariable().setAutoMVUseBitmapCountDistinct(useBitmap);
            ctx.getSessionVariable().setAutoMVUseHllCountDistinct(useHll);
            ctx.getSessionVariable().setAutoMVUseArrayAggCountDistinct(useArrayAgg);
            Map<String, String> mvs = AutoMVUtil.getMaterializedViews(ctx, sql);
            String mv = mvs.values().iterator().next();
            Assert.assertTrue(mv, mv.contains(snippet));
        }
        ctx.getSessionVariable().replayFromJson(savedSVs);
    }
}
