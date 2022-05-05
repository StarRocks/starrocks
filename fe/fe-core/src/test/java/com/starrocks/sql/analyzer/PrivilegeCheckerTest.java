// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.CreateRoleStmt;
import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TablePattern;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivBitSet;
import com.starrocks.mysql.privilege.Privilege;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();

        String createUserSql = "CREATE USER 'test' IDENTIFIED BY ''";
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseAndAnalyzeStmt(createUserSql, starRocksAssert.getCtx());
        auth.createUser(createUserStmt);

        testUser = new UserIdentity("test", "%");
        testUser.analyze("default_cluster");
    }

    @Test
    public void testAlterTable() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze("default_cluster");

        String sql = "alter table db1.table1 rename table2";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ALTER_PRIV), true);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ALTER_PRIV), true);
        sql = "alter table db1.table1 rename table2";
        StatementBase statementBase2 = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx()));
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
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
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
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
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
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
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
    public void testDropTable() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze("default_cluster");
        String sql = "drop table if exists db1.tbl1";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.DROP_PRIV), true);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.DROP_PRIV), true);

        sql = "drop table if exists db1.tbl1";
        StatementBase statementBase2 = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx()));
    }

    @Test
    public void testDropMaterializedView() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");
        starRocksAssert.getCtx().setDatabase("default_cluster:db1");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze("default_cluster");

        String sql = "drop materialized view mv1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.DROP_PRIV), true);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.DROP_PRIV), true);

        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testGrantRole() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");
        starRocksAssert.getCtx().setDatabase("default_cluster:db1");
        TablePattern db1TablePattern = new TablePattern("*", "*");
        db1TablePattern.analyze("default_cluster");

        // Here we hack `create role` statement because it was still in old framework
        auth.createRole(new CreateRoleStmt("default_cluster:test_role"));

        String sql = "grant test_role to test;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.GRANT_PRIV), true);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.GRANT_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));

    }

    @Test
    public void testAdminSet() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("*", "*");
        db1TablePattern.analyze("default_cluster");

        String adminSetConfigsql = "admin set frontend config(\"alter_table_timeout_second\" = \"60\");";
        String adminSetReplicaStatusSql = "admin set replica status properties(\"tablet_id\" = \"10003\",\"backend_id\" = \"10001\",\"status\" = \"ok\");";
        StatementBase statementBase1 = UtFrameUtils.parseStmtWithNewParser(adminSetConfigsql, starRocksAssert.getCtx());
        StatementBase statementBase2 = UtFrameUtils.parseStmtWithNewParser(adminSetReplicaStatusSql, starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        PrivilegeChecker.check(statementBase1, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase1, starRocksAssert.getCtx()));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx()));
    }

    @Test
    public void testAdminShow() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("*", "*");
        db1TablePattern.analyze("default_cluster");

        String adminShowConfigsql = "admin show frontend config like '%parallel%';";
        String adminShowReplicaDistributionsql = "ADMIN SHOW REPLICA DISTRIBUTION FROM db1.tbl1;";
        StatementBase statementBase1 = UtFrameUtils.parseStmtWithNewParser(adminShowConfigsql, starRocksAssert.getCtx());
        StatementBase statementBase2 = UtFrameUtils.parseStmtWithNewParser(adminShowReplicaDistributionsql, starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        PrivilegeChecker.check(statementBase1, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase1, starRocksAssert.getCtx()));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx()));
    }
}
