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


package com.starrocks.sql.plan;

import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.NormalizedTableFunctionRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.formatter.AST2SQLVisitor;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DigestTest extends PlanTestBase {

    @Test
    public void testWhere() throws Exception {
        String sql1 = "select s_address from supplier where a > 1";
        String sql2 = "select s_address from supplier where a > 5";
        String digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        String digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assertions.assertEquals(digest1, digest2);

        sql1 = "select s_address from supplier where a like 'xxx' ";
        sql2 = "select s_address from supplier where a like 'kkskkkkkkkkk' ";
        digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assertions.assertEquals(digest1, digest2);

        sql1 = "select s_address from supplier where a < 2 and b > -1 ";
        sql2 = "select s_address from supplier where a < 1000      and b > 100000 ";
        digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assertions.assertEquals(digest1, digest2);

        sql1 = "select s_address from supplier where a < 2 or b > -1 ";
        sql2 = "select s_address from supplier where a < 3 or  b > 100000 ";
        digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assertions.assertEquals(digest1, digest2);

        sql1 = "select s_address from supplier where not a < 2  ";
        sql2 = "select s_address from supplier where not a < 3";
        digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assertions.assertEquals(digest1, digest2);

        sql1 = "select s_address from supplier where not a < 2  ";
        sql2 = "select s_address from supplier where not a > 3";
        digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assertions.assertNotEquals(digest1, digest2);
    }

    @Test
    public void testLimit() throws Exception {
        String sql1 = "select s_address from supplier where a > 1 limit 1";
        String sql2 = "select s_address from supplier where a > 5 limit 20";
        String digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        String digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assertions.assertEquals(digest1, digest2);

        sql1 = "select s_address from supplier where a > 1 order by a limit 1";
        sql2 = "select s_address from supplier where a > 5 order by a limit 20";
        digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assertions.assertEquals(digest1, digest2);
    }

    @Test
    public void testFunction() throws Exception {
        String sql1 = "select substr(s_address, 1, 2) from supplier where a > 1 limit 1";
        String sql2 = "select substr(s_address, 1, 5) from supplier where a > 1 limit 1";
        String digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        String digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assertions.assertEquals(digest1, digest2);
    }

    @Test
    public void testArithmetic() throws Exception {
        String sql1 = "select a + 1 from supplier";
        String sql2 = "select a + 2 from supplier";
        String digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        String digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assertions.assertEquals(digest1, digest2);
    }

    @Test
    public void testCaseWhen() throws Exception {
        String sql1 = "select v1+20, case v2 when v3 then 1 else 0 end from t0 where v1 is null";
        String sql2 = "select v1+20, case v2 when v3 then 1000 else 9999999 end from t0 where v1 is null";
        String digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        String digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assertions.assertEquals(digest1, digest2);
    }

    @Test
    public void testSubquery() throws Exception {
        String sql1 = "select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem, part where p_partkey = " +
                "l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX' " +
                "and l_quantity < ( select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey );";
        String sql2 = "select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem, part where p_partkey = " +
                "l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX' " +
                "and l_quantity < ( select 1 * avg(l_quantity) from lineitem where l_partkey = p_partkey );";
        String digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        String digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);
        Assertions.assertEquals(digest1, digest2);
    }

    @Test
    public void testWindowSubQuery() throws Exception {
        String sql1 = "select max(a) from (select ROW_NUMBER() OVER (PARTITION BY l_partkey ORDER BY l_quantity DESC " +
                "NULLS LAST ) as a from lineitem where L_SHIPDATE BETWEEN DATE'2020-01-01' AND DATE'2020-12-31') t";
        String sql2 = "select max(a) from (select ROW_NUMBER() OVER (PARTITION BY l_partkey ORDER BY l_quantity DESC " +
                "NULLS LAST ) as a from lineitem where L_SHIPDATE BETWEEN DATE'2020-10-01' AND DATE'2020-12-31') t";

        String digest1 = UtFrameUtils.getStmtDigest(connectContext, sql1);
        String digest2 = UtFrameUtils.getStmtDigest(connectContext, sql2);

        Assertions.assertEquals(digest1, digest2);
    }

    @Test
    public void testAnalyzeError() throws Exception {
        new MockUp<AST2SQLVisitor>() {
            @Mock
            public String visitNormalizedTableFunction(NormalizedTableFunctionRelation node, Void scope) {
                throw new NullPointerException();
            }
        };

        String originStmt = "SELECT ltrim(rand(), '0.') from TABLE(generate_series(0, 100000000))";
        String digest1 = UtFrameUtils.getStmtDigest(connectContext, originStmt);
        Assertions.assertEquals(digest1, "");
    }

    @Test
    public void testExcludeDbFromDigest() throws Exception {
        // SQL with explicit db qualification
        String sqlWithDb = "select s_address from test.supplier where a > 1";
        // Same SQL without db qualification
        String sqlWithoutDb = "select s_address from supplier where a > 1";

        // With excludeDb=false (default), different digests because one has "test." prefix
        String digest1Default = UtFrameUtils.getStmtDigest(connectContext, sqlWithDb, false);
        String digest2Default = UtFrameUtils.getStmtDigest(connectContext, sqlWithoutDb, false);
        Assertions.assertNotEquals(digest1Default, digest2Default,
                "Default mode: qualified and unqualified table should have different digests");

        // With excludeDb=true, both should produce the same digest
        String digest1ExcludeDb = UtFrameUtils.getStmtDigest(connectContext, sqlWithDb, true);
        String digest2ExcludeDb = UtFrameUtils.getStmtDigest(connectContext, sqlWithoutDb, true);
        Assertions.assertEquals(digest1ExcludeDb, digest2ExcludeDb,
                "ExcludeDb mode: qualified and unqualified table should have same digest");

        // Verify the normalized SQL content
        StatementBase stmt = SqlParser.parse(sqlWithDb, connectContext.getSessionVariable()).get(0);
        String normalizedWithDb = AstToSQLBuilder.toDigest(stmt, false);
        String normalizedWithoutDb = AstToSQLBuilder.toDigest(stmt, true);
        Assertions.assertTrue(normalizedWithDb.contains("`test`"),
                "Default digest should contain db name for qualified table");
        Assertions.assertFalse(normalizedWithoutDb.contains("`test`"),
                "ExcludeDb digest should not contain db name");
        Assertions.assertTrue(normalizedWithoutDb.contains("`supplier`"),
                "ExcludeDb digest should still contain table name");

        // Test no-arg AstToSQLBuilder.toDigest() (default includes db name)
        String normalizedDefault = AstToSQLBuilder.toDigest(stmt);
        Assertions.assertEquals(normalizedWithDb, normalizedDefault,
                "No-arg toDigest should behave same as toDigest(stmt, false)");

        // Test no-arg ConnectProcessor.computeStatementDigest()
        String digestNoArg = ConnectProcessor.computeStatementDigest(stmt);
        Assertions.assertEquals(digest1Default, digestNoArg,
                "No-arg computeStatementDigest should behave same as computeStatementDigest(stmt, false)");

        // Test session variable sql_digest_exclude_db
        Assertions.assertFalse(connectContext.getSessionVariable().isSqlDigestExcludeDb(),
                "sql_digest_exclude_db should default to false");
    }

    @Test
    public void testExcludeDbFromDigestCrossDatabase() throws Exception {
        // Create a second database with the same table schema
        String db2 = "test2";
        starRocksAssert.withDatabase(db2);
        starRocksAssert.useDatabase(db2);
        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        String sql = "select * from t0 where v1 > 1";

        // Simulate: USE test; SELECT * FROM t0 WHERE v1 > 1;
        starRocksAssert.useDatabase("test");
        connectContext.setDatabase("test");
        String digestDb1Default = UtFrameUtils.getStmtDigest(connectContext, "select * from test.t0 where v1 > 1", false);
        String digestDb1ExcludeDb = UtFrameUtils.getStmtDigest(connectContext, "select * from test.t0 where v1 > 1", true);

        // Simulate: USE test2; SELECT * FROM t0 WHERE v1 > 1;
        starRocksAssert.useDatabase(db2);
        connectContext.setDatabase(db2);
        String digestDb2Default = UtFrameUtils.getStmtDigest(connectContext, "select * from test2.t0 where v1 > 1", false);
        String digestDb2ExcludeDb = UtFrameUtils.getStmtDigest(connectContext, "select * from test2.t0 where v1 > 1", true);

        // Default mode: different db → different digest
        Assertions.assertNotEquals(digestDb1Default, digestDb2Default,
                "Default mode: same SQL in different databases should have different digests");

        // ExcludeDb mode: different db → same digest
        Assertions.assertEquals(digestDb1ExcludeDb, digestDb2ExcludeDb,
                "ExcludeDb mode: same SQL in different databases should have same digest");

        // Restore database context
        starRocksAssert.useDatabase("test");
        connectContext.setDatabase("test");
    }

    @Test
    public void testDigestInAuditPath() throws Exception {
        String sql = "select s_address from supplier where a > 1";
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);

        // Set up context for audit path
        ConnectContext testContext = new ConnectContext();
        testContext.setGlobalStateMgr(connectContext.getGlobalStateMgr());
        testContext.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        testContext.setQualifiedUser(connectContext.getQualifiedUser());
        testContext.setDatabase("test");
        testContext.setCurrentCatalog("default_catalog");
        testContext.setQueryId(UUIDUtil.genUUID());
        testContext.setStartTime();
        testContext.getState().setIsQuery(true);
        testContext.setThreadLocalInfo();

        // Enable sql_digest via Config to trigger audit path digest computation
        boolean origConfigDigest = Config.enable_sql_digest;
        try {
            Config.enable_sql_digest = true;

            ConnectProcessor processor = new ConnectProcessor(testContext);
            processor.auditAfterExec(sql, stmt, null);

            // Verify digest was set in audit event
            AuditEvent event = testContext.getAuditEventBuilder().build();
            Assertions.assertNotNull(event.digest, "Digest should be set in audit event");
            Assertions.assertFalse(event.digest.isEmpty(), "Digest should not be empty");
        } finally {
            Config.enable_sql_digest = origConfigDigest;
        }
    }
}
