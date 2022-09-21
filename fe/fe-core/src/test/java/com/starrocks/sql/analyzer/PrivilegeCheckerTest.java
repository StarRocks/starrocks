// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TablePattern;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivBitSet;
import com.starrocks.mysql.privilege.Privilege;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
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
        auth = starRocksAssert.getCtx().getCatalog().getAuth();

        String createUserSql = "CREATE USER 'test' IDENTIFIED BY ''";
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseAndAnalyzeStmt(createUserSql, starRocksAssert.getCtx());
        auth.createUser(createUserStmt);

        testUser = new UserIdentity("test", "%");
        testUser.analyze("default_cluster");
    }

    @Test
    public void testTableAs() throws Exception {
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze("default_cluster");
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        String sql = "select count(*) from db1.tbl1 as a";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
    }

    @Test
    public void testInlineView() throws Exception {
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze("default_cluster");
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        String sql = "select count(*) from (select count(*) from db1.tbl1) as a";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

    }

    @Test
    public void testWithNormal() throws Exception {
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze("default_cluster");
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        String sql = "with tmp as (select * from db1.tbl1) select count(*) from tmp;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
    }

    @Test
    public void testWithNested() throws Exception {
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze("default_cluster");
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        String sql = "with tmp as (select * from db1.tbl1) " +
                "select a.k1, b.k2, b.k1 from (select k1, k2 from tmp) a " +
                "left join (select k1, k2 from tmp) b on a.k1 = b.k1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
    }

    @Test
    public void testSelectTable() throws Exception {
        auth = starRocksAssert.getCtx().getCatalog().getAuth();
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze("default_cluster");
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        String sql = "select count(*) from db1.tbl1 as a";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testInsertStatement() throws Exception {
        auth = starRocksAssert.getCtx().getCatalog().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze("default_cluster");

        String sql = "insert into db1.tbl1 select 1,2,3,4";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.LOAD_PRIV), true);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.LOAD_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testCreateView() throws Exception {
        auth = starRocksAssert.getCtx().getCatalog().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze("default_cluster");

        String sql = "create view db1.v as select 1,2,3";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.CREATE_PRIV), true);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        sql = "create view db1.v as select * from db1.tbl1";
        StatementBase statementBase2 = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx()));

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.CREATE_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testSelectView() throws Exception {
        auth = starRocksAssert.getCtx().getCatalog().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        // create view
        TablePattern tablePattern = new TablePattern("db1", "tbl1");
        tablePattern.analyze("default_cluster");
        auth.grantPrivs(testUser, tablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), false);
        tablePattern = new TablePattern("db1", "*");
        tablePattern.analyze("default_cluster");
        auth.grantPrivs(testUser, tablePattern, PrivBitSet.of(Privilege.CREATE_PRIV), false);
        String sql = "create view db1.view1 as select k1 from db1.tbl1;";
        starRocksAssert.withView(sql);

        // select privilege on base table
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(
                "select * from db1.view1", starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        // revoke select privilege on base table
        tablePattern = new TablePattern("db1", "tbl1");
        tablePattern.analyze("default_cluster");
        auth.revokePrivs(testUser, tablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), false);
        // grant select privilege on view
        tablePattern = new TablePattern("db1", "view1");
        tablePattern.analyze("default_cluster");
        auth.grantPrivs(testUser, tablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), false);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        // no select privilege on neither the base table nor the view
        UserIdentity testUser2 = new UserIdentity("test2", "%");
        testUser2.analyze("default_cluster");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser2);
        try {
            PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
            Assert.fail();
        } catch (SemanticException e) {
            Assert.assertTrue(e.getMessage().contains("SELECT command denied to user"));
        }
    }
}
