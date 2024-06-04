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

package com.starrocks.sql.automv.generator;

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;
import com.starrocks.sql.automv.options.AutoMVOptions;
import com.starrocks.sql.automv.pattern.PlanPiecePattern;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.FQTable;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.PlanPieceBuilder;
import com.starrocks.sql.automv.pieces.PlanPieceNormalizer;
import com.starrocks.sql.automv.policies.AggregatePolicies;
import com.starrocks.sql.automv.policies.AggregatePolicy;
import com.starrocks.sql.automv.qe.QueryStatementPlus;
import com.starrocks.sql.automv.qe.RboOptimizer;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TestUtil;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

public class GeneratorTest {
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

    //TODO(by satanson): should be removed after all PRs merged
    @Deprecated
    private static Map<String, String> getMaterializedViews(ConnectContext ctx, String sql) {
        QueryStatementPlus stmt = RboOptimizer.getQueryStatement(ctx, sql);
        QueryStatement queryStmt = stmt.getQueryStatement();
        Map<String, FQTable> fqTableMap = stmt.getFqTableMap();
        Map<String, String> mvMap = Maps.newHashMap();
        List<OptExpression> subPlans = RboOptimizer.getSubPlans(queryStmt, ctx, PlanPiecePattern.getSPJG());
        for (OptExpression subPlan : subPlans) {
            ColumnRefToIdConverter idConverter = new ColumnRefToIdConverter();
            Optional<AggregatePiece> optPlanPiece =
                    PlanPieceBuilder.createPlanPiece(subPlan, idConverter, fqTableMap).cast(AggregatePiece.class);
            Preconditions.checkArgument(optPlanPiece.isPresent());
            AggregatePiece planPiece = optPlanPiece.get();

            PlanPieceNormalizer.normalize(planPiece);
            AutoMVOptions options = AutoMVOptions.of(ctx.getSessionVariable());
            MVGenerateContext mvGenerateContext = MVGenerateContext.builder()
                    .setMvNameGenerator(query -> MVName.generateFromQuery(query).toString())
                    .setNextId(idConverter::nextId)
                    .setOptions(options)
                    .build();
            planPiece = AggregatePolicies.defaultPolicies(options).convert(planPiece).orElse(planPiece);
            planPiece = AggregatePolicies.applyRollupOrPerfectMatch(planPiece);
            Optional<QueryGenerateResult> optResult = AggregateMVGenerator.generate(planPiece, mvGenerateContext);
            Assert.assertTrue(optResult.isPresent());
            QueryGenerateResult result = optResult.get();
            mvMap.put(result.getMvName(), result.getSubquery().getResult());
        }
        return mvMap;
    }

    @Test
    public void testMVName() {
        MVName mvName = MVName.generateFromQuery("select l_orderkey from lineitem");
        MVName mvName2 = MVName.generateFromQuery("select l_partkey from lineitem");
        System.out.println(mvName);
        Optional<MVName> optNewMVName = MVName.parse(mvName.toString());
        Preconditions.checkArgument(optNewMVName.isPresent());
        MVName newMvName = optNewMVName.get();
        Assert.assertEquals(mvName, newMvName);
        Assert.assertTrue(mvName.collidesWith(newMvName));
        Assert.assertTrue(mvName.collidesWith(newMvName.toString()));
        Assert.assertNotEquals(mvName, mvName2);
        Assert.assertFalse(mvName.collidesWith(mvName2));
    }

    @Test
    public void testAllQueriesOfTPCDS() throws IOException {
        ConnectContext ctx = getStarRocksAssert().getCtx();
        List<String> mvList = Lists.newArrayList();
        for (Pair<String, String> p : TestUtil.getTPCDSQueryList()) {
            String sql = p.second;
            ColumnRefToIdConverter idConverter = new ColumnRefToIdConverter();
            Pair<Map<String, FQTable>, List<OptExpression>>
                    fQTableMapAndSubPlans = RboOptimizer.getSubPlans(sql, ctx, PlanPiecePattern.getSPJG());
            List<OptExpression> subPlans = fQTableMapAndSubPlans.second;
            Map<String, FQTable> fqTableMap = fQTableMapAndSubPlans.first;
            PrettyPrinter traceLog = new PrettyPrinter();
            AutoMVOptions options = AutoMVOptions.of(ctx.getSessionVariable());
            for (OptExpression subPlan : subPlans) {
                PlanPiece planPiece = PlanPieceBuilder.createPlanPiece(subPlan, idConverter, fqTableMap);
                AggregatePiece aggPiece = planPiece.mustCast(AggregatePiece.class);
                AggregatePolicy policy = AggregatePolicies.defaultPolicies(options, traceLog);
                aggPiece = policy.convert(aggPiece).orElse(aggPiece);
                aggPiece = AggregatePolicies.applyRollupOrPerfectMatch(aggPiece);
                QueryGenerateContext queryGenerateContext = QueryGenerateContext.of(false, true);

                String query = QueryGenerator.generate(aggPiece, queryGenerateContext).getSubquery().getResult();

                //TODO(by satanson): should be removed after all PRs merged
                Map<String, String> mvs = getMaterializedViews(ctx, query);
                mvList.addAll(mvs.values());
            }
        }
        Assert.assertEquals(mvList.size(), 200);
    }

    @Test
    public void testNullAwareLeftAntiJoin() {
        String q0 = "select\n" +
                "  i_brand,\n" +
                "  sum(ss_quantity)\n" +
                "from\n" +
                "  store_sales\n" +
                "  inner join item on ss_item_sk = i_item_sk\n" +
                "where\n" +
                "  ss_customer_sk not in (\n" +
                "    select\n" +
                "      c_customer_sk\n" +
                "    from\n" +
                "      customer\n" +
                "    where\n" +
                "      c_current_cdemo_sk != 1\n" +
                "  )\n" +
                "group by\n" +
                "  i_brand";
        Map<String, String> mvs = getMaterializedViews(getStarRocksAssert().getCtx(), q0);
        Assert.assertEquals(1, mvs.size());
        String mv = mvs.values().iterator().next();
        String expectMv = "CREATE MATERIALIZED VIEW _mv_20240523T200111_094c498c29bd6f97f89a6e03f92463a0d1d03f4c (\n" +
                "  i_brand\n" +
                "  , _ca0004\n" +
                ")\n" +
                "COMMENT \"MV recommended by AutoMV\"\n" +
                "DISTRIBUTED BY HASH (i_brand) BUCKETS 64\n" +
                "ORDER BY (i_brand)\n" +
                "REFRESH ASYNC START(\"2023-12-01 10:00:00\") EVERY(INTERVAL 1 DAY)\n" +
                "PROPERTIES (\n" +
                "  \"replicated_storage\" = \"true\",\n" +
                "  \"storage_medium\" = \"HDD\",\n" +
                "  \"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS\n" +
                "SELECT\n" +
                "  _ta0001.i_brand\n" +
                "  ,(sum(_ta0001.ss_quantity)) AS _ca0004\n" +
                "FROM\n" +
                "  (\n" +
                "    SELECT\n" +
                "      _ta0000.i_brand\n" +
                "      ,_ta0000.ss_quantity\n" +
                "    FROM\n" +
                "      (\n" +
                "        SELECT\n" +
                "          `tpcds`.`item`.i_item_sk\n" +
                "          , `tpcds`.`item`.i_brand\n" +
                "          , `tpcds`.`store_sales`.ss_item_sk\n" +
                "          , `tpcds`.`store_sales`.ss_customer_sk\n" +
                "          , `tpcds`.`store_sales`.ss_quantity\n" +
                "        FROM\n" +
                "        `tpcds`.`store_sales`\n" +
                "        INNER JOIN\n" +
                "        `tpcds`.`item`\n" +
                "        ON (`tpcds`.`store_sales`.ss_item_sk = `tpcds`.`item`.i_item_sk)\n" +
                "        WHERE\n" +
                "          (`tpcds`.`store_sales`.ss_customer_sk NOT IN (\n" +
                "            SELECT `tpcds`.`customer`.c_customer_sk\n" +
                "            FROM \n" +
                "              `tpcds`.`customer`\n" +
                "            WHERE\n" +
                "              (`tpcds`.`customer`.c_current_cdemo_sk != 1)\n" +
                "            )\n" +
                "          )\n" +
                "      ) _ta0000\n" +
                "  ) _ta0001\n" +
                "GROUP BY\n" +
                "  _ta0001.i_brand";
        Pattern mvNamePattern = Pattern.compile("_mv_\\d{8}T\\d{6}_[a-f0-9]{40}");
        Assert.assertEquals(mv, mv.replaceAll(mvNamePattern.pattern(), "_mv"),
                expectMv.replaceAll(mvNamePattern.pattern(), "_mv"));
    }
}
