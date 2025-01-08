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

package com.starrocks.sql.automv.tunespace;

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.automv.ast.AlterTunespaceClause;
import com.starrocks.sql.automv.ast.AlterTunespaceStmt;
import com.starrocks.sql.automv.ast.CreateTunespaceStmt;
import com.starrocks.sql.automv.qe.RboOptimizer;
import com.starrocks.sql.automv.qe.TablePlus;
import com.starrocks.sql.automv.qe.TunespaceExecutor;
import com.starrocks.sql.automv.util.TestUtil;
import com.starrocks.utframe.StarRocksAssert;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class TunespaceTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert getStarRocksAssert() {
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
        getStarRocksAssert();
    }

    public static void mockTunespaceExecuteVisitor(List<String> result) {
        new MockUp<TunespaceExecutor.TunespaceExecuteVisitor>() {
            @Mock
            public void exec(String sql, Class<?> klass, ConnectContext context) throws Exception {
                List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, context.getSessionVariable());
                Preconditions.checkArgument(stmts.size() == 1 && stmts.get(0).getClass().equals(klass));
                result.add(sql);
            }
        };
    }

    @Test
    public void testTuneSpace() {
        TablePlus table = PlanPieceInfo.getTable("_auto_tuning_.tunespace", 8, 1);
        PlanPieceInfo info = new PlanPieceInfo();
        info.setId(1);
        info.setQuery("select * from t0");
        info.setTs(Timestamp.valueOf("2024-01-01 12:59:59"));
        info.setOriginalQuery("select * from t1");
        info.setCategory(PlanPieceInfo.Category.MV);
        //PlanPieceInfo.
        PieceTraits traits = new PieceTraits();
        traits.setVersion(1);
        traits.setNumMetrics(10);
        info.setTraits(traits);
        String createSql = table.getCreateTableSql();
        String expectCreateSql = "CREATE TABLE IF NOT EXISTS _auto_tuning_.tunespace(\n" +
                "  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT \"\",\n" +
                "  `ts` datetime NOT NULL COMMENT \"\",\n" +
                "  `originalQuery` varbinary NOT NULL COMMENT \"\",\n" +
                "  `query` varbinary NULL COMMENT \"\",\n" +
                "  `category` varchar(255) NOT NULL COMMENT \"\",\n" +
                "  `traits` json NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`id`, `ts`)\n" +
                "PARTITION BY RANGE (`ts`)()\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 8\n";
        Assert.assertTrue(createSql, createSql.startsWith(expectCreateSql));

        String insertSql = table.getInsertSql(ImmutableList.of(info));
        String expectInsertSql = "INSERT INTO _auto_tuning_.tunespace(ts, originalQuery, query, " +
                "category, traits) VALUES \n" +
                "  (\"2024-01-01 12:59:59\", to_binary(\"select * from t1\", \"utf8\"), " +
                "to_binary(\"select * from t0\", \"utf8\"), \"MV\", \"{\\\"version\\\":1," +
                "\\\"isNonSPJG\\\":false,\\\"numDimensions\\\":0,\\\"numRollupDimensions" +
                "\\\":0,\\\"numMetrics\\\":10,\\\"numDistinctMetrics\\\":0,\\\"numHoistedConjuncts\\\":0}\")";
        Assert.assertEquals(insertSql, insertSql, expectInsertSql);

        String insertAsSelectSql = table.getInsertAsSelectSql("ts0");
        String expectInsertAsSelectSql =
                "INSERT INTO _auto_tuning_.tunespace(ts, originalQuery, query, category, traits)\n" +
                        "SELECT ts, originalQuery, query, category, traits FROM ts0";
        Assert.assertEquals(insertAsSelectSql, insertAsSelectSql, expectInsertAsSelectSql);

        String selectSql = table.getSelectSql(
                table.getColumnPluses().stream().map(col -> col.getColumn().getName()).collect(Collectors.toList()),
                Collections.emptyList());
        String expectSelectSql = "SELECT \n" +
                "  id\n" +
                "  ,ts\n" +
                "  ,originalQuery\n" +
                "  ,query\n" +
                "  ,category\n" +
                "  ,traits\n" +
                "FROM _auto_tuning_.tunespace";
        Assert.assertEquals(selectSql, selectSql, expectSelectSql);
    }

    private void handleTunespaceStmt(String sql,
                                     Consumer<StatementBase> stmtChecker,
                                     Consumer<String> resultChecker) throws Exception {
        StatementBase statementBase = RboOptimizer.parseAndAnalyze(getStarRocksAssert().getCtx(), sql);
        stmtChecker.accept(statementBase);
        List<String> result = Lists.newArrayList();
        mockTunespaceExecuteVisitor(result);
        StmtExecutor executor = new StmtExecutor(getStarRocksAssert().getCtx(), statementBase);
        executor.execute();
        Assert.assertEquals(result.size(), 1);
        resultChecker.accept(result.get(0));
    }

    @Test
    public void testCreateTunespace() throws Exception {
        String createSql = "create tunespace ts0";
        Consumer<StatementBase> stmtChecker = stmt -> {
            Assert.assertTrue(stmt instanceof CreateTunespaceStmt);
            CreateTunespaceStmt createTunespaceStmt = (CreateTunespaceStmt) stmt;
            Assert.assertEquals(createTunespaceStmt.getTableName().getTbl(), "ts0");
        };
        Consumer<String> resultChecker = result -> {
            Assert.assertTrue(result, result.startsWith("CREATE TABLE IF NOT EXISTS `default_catalog`.`ssb`.`ts0`(\n" +
                    "  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT \"\",\n" +
                    "  `ts` datetime NOT NULL COMMENT \"\",\n" +
                    "  `originalQuery` varbinary NOT NULL COMMENT \"\",\n" +
                    "  `query` varbinary NULL COMMENT \"\",\n" +
                    "  `category` varchar(255) NOT NULL COMMENT \"\",\n" +
                    "  `traits` json NOT NULL COMMENT \"\"\n" +
                    ") ENGINE=OLAP\n" +
                    "PRIMARY KEY(`id`, `ts`)\n" +
                    "PARTITION BY RANGE (`ts`)()\n" +
                    "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n"
            ));
        };
        handleTunespaceStmt(createSql, stmtChecker, resultChecker);
    }

    private void createTunespace(String tsName) throws Exception {
        String createSqlFmt = "CREATE TABLE %s (\n" +
                "  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT \"\",\n" +
                "  `ts` datetime NOT NULL COMMENT \"\",\n" +
                "  `originalQuery` varbinary NOT NULL COMMENT \"\",\n" +
                "  `query` varbinary NULL COMMENT \"\",\n" +
                "  `category` varchar(255) NOT NULL COMMENT \"\",\n" +
                "  `traits` json NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(id)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 8\n" +
                "PROPERTIES (\n" +
                "  \"replication_num\" = \"1\"\n" +
                ")";
        getStarRocksAssert().withTable(String.format(createSqlFmt, tsName));
    }

    private void dropTunespace(String tsName) throws Exception {
        getStarRocksAssert().dropTable(tsName);
    }

    @Test
    public void testAlterTunespaceAppendQuery() throws Exception {

        createTunespace("ts0");
        try {
            String q11 = TestUtil.getSsbQueryList().get(0).second;
            String appendSql = "alter tunespace ts0 append " + q11;
            Consumer<StatementBase> stmtChecker = stmt -> {
                Assert.assertTrue(stmt instanceof AlterTunespaceStmt);
                AlterTunespaceStmt alterTunespaceStmt = (AlterTunespaceStmt) stmt;
                Assert.assertTrue(alterTunespaceStmt.getAlterClause() instanceof AlterTunespaceClause.AppendClause);
            };
            Consumer<String> resultChecker = result -> {
                Assert.assertTrue(result, result.contains("INSERT INTO `default_catalog`.`ssb`.`ts0`" +
                        "(ts, originalQuery, query, category, traits) VALUES "));
            };
            handleTunespaceStmt(appendSql, stmtChecker, resultChecker);

        } finally {
            dropTunespace("ts0");
        }
    }

    @Test
    public void testAlterTunespacePopulateAsSelect() throws Exception {

        createTunespace("ts0");
        createTunespace("ts1");
        try {
            String populateAsSelect = "alter tunespace ts0 populate as select * from ts1 ";
            Consumer<StatementBase> stmtChecker = stmt -> {
                Assert.assertTrue(stmt instanceof AlterTunespaceStmt);
                AlterTunespaceStmt alterTunespaceStmt = (AlterTunespaceStmt) stmt;
                Assert.assertTrue(alterTunespaceStmt.getAlterClause()
                        instanceof AlterTunespaceClause.PopulateAsQueryClause);
            };
            Consumer<String> resultChecker = result -> {
                Assert.assertTrue(result, result.contains("INSERT INTO `default_catalog`.`ssb`.`ts0`" +
                        "(ts, originalQuery, query, category, traits) VALUES "));
            };
            try {
                handleTunespaceStmt(populateAsSelect, stmtChecker, resultChecker);
                Assert.fail();
            } catch (Throwable ignored) {
            }
        } finally {
            dropTunespace("ts1");
            dropTunespace("ts0");
        }
    }

    @Test
    public void testAlterTunespacePopulateFromOtherTunespace() throws Exception {

        createTunespace("ts0");
        createTunespace("ts1");
        try {
            String appendSql = "alter tunespace ts0 populate from tunespace ts1";
            Consumer<StatementBase> stmtChecker = stmt -> {
                Assert.assertTrue(stmt instanceof AlterTunespaceStmt);
                AlterTunespaceStmt alterTunespaceStmt = (AlterTunespaceStmt) stmt;
                Assert.assertTrue(alterTunespaceStmt.getAlterClause()
                        instanceof AlterTunespaceClause.PopulateFromTunespaceClause);
            };
            Consumer<String> resultChecker = result -> {
                Assert.assertEquals(result, result, "INSERT INTO `default_catalog`.`ssb`.`ts0`" +
                        "(ts, originalQuery, query, category, traits)\n" +
                        "SELECT ts, originalQuery, query, category, traits FROM `default_catalog`.`ssb`.`ts1`");
            };
            handleTunespaceStmt(appendSql, stmtChecker, resultChecker);

        } finally {
            dropTunespace("ts1");
            dropTunespace("ts0");
        }
    }

    @Test
    public void testAlterTunespaceDeleteRows() throws Exception {

        createTunespace("ts0");
        try {
            String deleteSql = "alter tunespace ts0 delete where id = 1";
            StatementBase stmt = RboOptimizer.parseAndAnalyze(getStarRocksAssert().getCtx(), deleteSql);
            Assert.assertTrue(stmt instanceof DeleteStmt);
        } finally {
            dropTunespace("ts0");
        }
    }

    @Test
    public void testAlterTunespacePopulateFromLegacyMVs() throws Exception {

        createTunespace("ts0");
        String mv = "CREATE MATERIALIZED VIEW mv_0 (\n" +
                "  p_brand\n" +
                "  , d_year\n" +
                "  , s_region\n" +
                "  , p_category\n" +
                "  , _ca0003\n" +
                ")\n" +
                "DISTRIBUTED BY HASH (p_brand, d_year, s_region, p_category) BUCKETS 64\n" +
                "ORDER BY (p_brand, d_year, s_region)\n" +
                "REFRESH ASYNC START(\"2023-12-01 10:00:00\") EVERY(INTERVAL 1 DAY)\n" +
                "PROPERTIES (\n" +
                "  \"replicated_storage\" = \"true\",\n" +
                "  \"storage_medium\" = \"HDD\",\n" +
                "  \"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS\n" +
                "SELECT\n" +
                "  _ta0000.p_brand\n" +
                "  ,_ta0000.d_year\n" +
                "  ,_ta0000.s_region\n" +
                "  ,_ta0000.p_category\n" +
                "  ,(sum(_ta0000.lo_revenue)) AS _ca0003\n" +
                "FROM\n" +
                "  (\n" +
                "    SELECT\n" +
                "      `ssb`.`supplier`.s_region\n" +
                "      ,`ssb`.`part`.p_category\n" +
                "      ,`ssb`.`part`.p_brand\n" +
                "      ,`ssb`.`dates`.d_year\n" +
                "      ,`ssb`.`lineorder`.lo_revenue\n" +
                "    FROM\n" +
                "      `ssb`.`lineorder`\n" +
                "      INNER JOIN\n" +
                "      `ssb`.`dates`\n" +
                "      ON (`ssb`.`lineorder`.lo_orderdate = `ssb`.`dates`.d_datekey)\n" +
                "      INNER JOIN\n" +
                "      `ssb`.`part`\n" +
                "      ON (`ssb`.`lineorder`.lo_partkey = `ssb`.`part`.p_partkey)\n" +
                "      INNER JOIN\n" +
                "      `ssb`.`supplier`\n" +
                "      ON (`ssb`.`lineorder`.lo_suppkey = `ssb`.`supplier`.s_suppkey)\n" +
                "  ) _ta0000\n" +
                "GROUP BY\n" +
                "  _ta0000.p_brand\n" +
                "  ,_ta0000.d_year\n" +
                "  ,_ta0000.s_region\n" +
                "  ,_ta0000.p_category\n";
        getStarRocksAssert().withMaterializedView(mv);
        try {
            String appendSql = "alter tunespace ts0 populate from database ssb";
            Consumer<StatementBase> stmtChecker = stmt -> {
                Assert.assertTrue(stmt instanceof AlterTunespaceStmt);
                AlterTunespaceStmt alterTunespaceStmt = (AlterTunespaceStmt) stmt;
                Assert.assertTrue(alterTunespaceStmt.getAlterClause()
                        instanceof AlterTunespaceClause.PopulateFromLegacyMVClause);
            };
            Consumer<String> resultChecker = result -> {
                Assert.assertTrue(result, result.contains("INSERT INTO `default_catalog`.`ssb`.`ts0`" +
                        "(ts, originalQuery, query, category, traits) VALUES"));
            };
            handleTunespaceStmt(appendSql, stmtChecker, resultChecker);

        } finally {
            getStarRocksAssert().dropMaterializedView("mv_0");
            dropTunespace("ts0");
        }
    }

    @Test
    public void testDropTunespace() throws Exception {
        createTunespace("ts0");
        try {
            String sql = "drop tunespace ts0";
            StatementBase stmt = RboOptimizer.parseAndAnalyze(getStarRocksAssert().getCtx(), sql);
            Assert.assertTrue(stmt instanceof DropTableStmt);
            DropTableStmt dropTableStmt = (DropTableStmt) stmt;
            Assert.assertEquals(dropTableStmt.getTableName(), "ts0");
        } finally {
            dropTunespace("ts0");
        }
    }

    @Test
    public void testTruncateTunespace() throws Exception {
        createTunespace("ts0");
        try {
            String sql = "truncate tunespace ts0";
            StatementBase stmt = RboOptimizer.parseAndAnalyze(getStarRocksAssert().getCtx(), sql);
            Assert.assertTrue(stmt instanceof TruncateTableStmt);
            TruncateTableStmt truncateTableStmt = (TruncateTableStmt) stmt;
            Assert.assertEquals(truncateTableStmt.getTblName(), "ts0");
        } finally {
            dropTunespace("ts0");
        }
    }
}
