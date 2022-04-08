// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TablePattern;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivBitSet;
import com.starrocks.mysql.privilege.Privilege;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class PrivilegeCheckerTest {
    private static StarRocksAssert starRocksAssert;
    private static UserIdentity testUser;
    private static Auth auth;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        String createTblStmtStr = "create table db1.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db1");
        starRocksAssert.withTable(createTblStmtStr);

        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String createUserSql = "CREATE USER 'test' IDENTIFIED BY ''";
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseAndAnalyzeStmt(createUserSql, ctx);

        testUser = new UserIdentity("test", "%");
        testUser.analyze("default_cluster");

        auth = new Auth();
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze("default_cluster");
        auth.createUser(createUserStmt);
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
    }

    @Test
    public void testTableAs() throws Exception {
        ConnectContext ctx = UtFrameUtils.createTestUserCtx(testUser);
        String sql = "select count(*) from db1.tbl1 as a";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewAnalyzer(sql, ctx);
        PrivilegeChecker.check(statementBase, auth, ctx);
    }

    @Test
    public void testInlineView() throws Exception {
        ConnectContext ctx = UtFrameUtils.createTestUserCtx(testUser);
        String sql = "select count(*) from (select count(*) from db1.tbl1) as a";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewAnalyzer(sql, ctx);
        PrivilegeChecker.check(statementBase, auth, ctx);

    }

    @Test
    public void testWithNormal() throws Exception {
        ConnectContext ctx = UtFrameUtils.createTestUserCtx(testUser);
        String sql = "with tmp as (select * from db1.tbl1) select count(*) from tmp;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewAnalyzer(sql, ctx);
        PrivilegeChecker.check(statementBase, auth, ctx);
    }

    @Test
    public void testWithNested() throws Exception {
        ConnectContext ctx = UtFrameUtils.createTestUserCtx(testUser);
        String sql = "with tmp as (select * from db1.tbl1) " +
                "select a.k1, b.k2, b.k1 from (select k1, k2 from tmp) a " +
                "left join (select k1, k2 from tmp) b on a.k1 = b.k1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewAnalyzer(sql, ctx);
        PrivilegeChecker.check(statementBase, auth, ctx);
    }

}
