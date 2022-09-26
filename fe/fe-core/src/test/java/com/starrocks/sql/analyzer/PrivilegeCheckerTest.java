// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.collect.Maps;
import com.starrocks.analysis.CreateRoleStmt;
import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.DropRoleStmt;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TablePattern;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.backup.BlobStorage;
import com.starrocks.backup.Repository;
import com.starrocks.backup.Status;
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivBitSet;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.mysql.privilege.Privilege;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

public class PrivilegeCheckerTest {
    private static StarRocksAssert starRocksAssert;
    private static UserIdentity testUser;
    private static UserIdentity testUser2;

    private static String role = null;
    private static Auth auth;
    private static UserIdentity rootUser;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        String createTblStmtStr = "create table db1.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db1");
        starRocksAssert.withDatabase("db2");
        starRocksAssert.withTable(createTblStmtStr);
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        rootUser = starRocksAssert.getCtx().getCurrentUserIdentity();
    }

    @Before
    public void beforeMethod() throws Exception {
        starRocksAssert.getCtx().setCurrentUserIdentity(rootUser);
        dropUsersAndRole();
        createUsersAndRole();
    }

    private void createUsersAndRole() throws Exception {
        String createUserSql = "CREATE USER 'test' IDENTIFIED BY ''";
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());
        auth.createUser(createUserStmt);

        testUser = createUserStmt.getUserIdent();

        createUserSql = "CREATE USER 'test2' IDENTIFIED BY ''";
        createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());
        auth.createUser(createUserStmt);

        testUser2 = createUserStmt.getUserIdent();

        role = "role0";
        auth.createRole((CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE ROLE " + role, starRocksAssert.getCtx()));
    }

    @Test
    public void testCreateUser() throws Exception {
        starRocksAssert.getCtx().setQualifiedUser("root");
        starRocksAssert.getCtx().setCurrentUserIdentity(rootUser);
        String sql = "CREATE USER 'test2'";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertTrue(statementBase.isSupportNewPlanner());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        StatementBase statementBase2 = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx()));
    }

    @Test
    public void testCreateRole() throws Exception {
        starRocksAssert.getCtx().setQualifiedUser("root");
        starRocksAssert.getCtx().setCurrentUserIdentity(rootUser);
        String sql = "CREATE ROLE role_test";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertTrue(statementBase.isSupportNewPlanner());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        StatementBase statementBase2 = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx()));
    }

    @Test
    public void testAlterUser() throws Exception {
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        String sql = "ALTER USER 'root' IDENTIFIED BY ''";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testDropUser() throws Exception {
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        String sql = "Drop USER 'root'";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    private void dropUsersAndRole() throws Exception {
        if (testUser != null) {
            auth.replayDropUser(testUser);
        }
        if (testUser2 != null) {
            auth.replayDropUser(testUser2);
        }

        if (role != null) {
            auth.dropRole((DropRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                    "DROP ROLE " + role, starRocksAssert.getCtx()));
            role = null;
        }
    }

    @Test
    public void testShowRoles() throws Exception {
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        String sql = "SHOW ROLES";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testShowGrants() throws Exception {
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        String sql = "SHOW ALL GRANTS";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testShowCreateDb() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        String sql = "show create database db1";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertTrue(statementBase.isSupportNewPlanner());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
        String sql2 = "show create database";
        Assert.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql2, starRocksAssert.getCtx()));
    }

    @Test
    public void testAlterDbQuota() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern globalPattern = new TablePattern("*", "*");
        globalPattern.analyze();
        auth.grantPrivs(testUser, globalPattern, PrivPredicate.ADMIN.getPrivs(), true);
        String sql = "alter database db1 set data quota 1000K";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertTrue(statementBase.isSupportNewPlanner());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, globalPattern, PrivPredicate.ADMIN.getPrivs(), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));

        String sql2 = "alter database db1 set data quota 1000F";
        Assert.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql2, starRocksAssert.getCtx()));

        String sql3 = "alter database db1 set replica quota 3";
        UtFrameUtils.parseStmtWithNewParser(sql3, starRocksAssert.getCtx());

        String sql4 = "alter database db1 set replica quota 3K";
        Assert.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql2, starRocksAssert.getCtx()));
    }

    @Test
    public void testCreateDb() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.CREATE_PRIV), true);
        String sql = "create database if not exists db1 ";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertTrue(statementBase.isSupportNewPlanner());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.CREATE_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
        String sql2 = "create database if not exists 1db";
        Assert.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql2, starRocksAssert.getCtx()));
    }

    @Test
    public void testDropDb() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.DROP_PRIV), true);
        String sql = "drop database if exists db1 force";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertTrue(statementBase.isSupportNewPlanner());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.DROP_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
        String sql2 = "drop database if exist information_schema";
        Assert.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql2, starRocksAssert.getCtx()));
    }

    @Test
    public void testRenameDb() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ALTER_PRIV), true);
        String sql = "alter database db1 rename db01";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertTrue(statementBase.isSupportNewPlanner());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ALTER_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
        String sql2 = "alter database db1 rename 1db";
        Assert.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql2, starRocksAssert.getCtx()));
    }

    @Test
    public void testRecoverDb() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.CREATE_PRIV), true);
        String sql = "recover database db1";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertTrue(statementBase.isSupportNewPlanner());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.CREATE_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testRecoverTable() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        String sql = "recover table db1.test";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertTrue(statementBase.isSupportNewPlanner());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testShowData() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        starRocksAssert.getCtx().setDatabase("");
        String sql1 = "show data"; // db is null
        Assert.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql1, starRocksAssert.getCtx()));

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.CREATE_PRIV), true);

        starRocksAssert.useDatabase("db1");
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql1, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        ShowExecutor executor = new ShowExecutor(starRocksAssert.getCtx(), (ShowStmt) statementBase);
        ShowResultSet resultSet = executor.execute();
        System.out.print(resultSet.getResultRows());
        Assert.assertEquals(resultSet.getResultRows().toArray().length, 4);
        Assert.assertEquals(resultSet.getResultRows().get(0).get(0), "tbl1");

        String sql2 = "show data from db1.tbl1";
        StatementBase statementBase2 = UtFrameUtils.parseStmtWithNewParser(sql2, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx());

        ShowExecutor executor2 = new ShowExecutor(starRocksAssert.getCtx(), (ShowStmt) statementBase2);
        ShowResultSet resultSet2 = executor2.execute();
        System.out.print(resultSet2.getResultRows());
        Assert.assertEquals(resultSet2.getResultRows().toArray().length, 2);
        Assert.assertEquals(resultSet2.getResultRows().get(0).get(0), "tbl1");

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.CREATE_PRIV), true);
        // test no privilege
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx()));

        String sql3 = "show data from dbNoExist.tbl1"; // dbNoExist no exist
        StatementBase dbNoExist = UtFrameUtils.parseStmtWithNewParser(sql3, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(dbNoExist, starRocksAssert.getCtx()));
    }

    @Test
    public void testCreateTable() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.CREATE_PRIV), true);
        String sql = "create table db1.table1 (col1 int, col2 varchar(10)) engine=olap " +
                "duplicate key(col1, col2) distributed by hash(col1) buckets 10";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.CREATE_PRIV), true);
        // sql = "alter table db1.table1 rename table2";
        StatementBase statementBase2 = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx()));
    }

    @Test
    public void testAlterTable() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ALTER_PRIV), true);
        String sql = "alter table db1.tbl1 rename table2";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ALTER_PRIV), true);
        sql = "alter table db1.tbl1 rename table2";
        StatementBase statementBase2 = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx()));
    }

    @Test
    public void testTableAs() throws Exception {
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        String sql = "select count(*) from db1.tbl1 as a";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
    }

    @Test
    public void testInlineView() throws Exception {
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        String sql = "select count(*) from (select count(*) from db1.tbl1) as a";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
    }

    @Test
    public void testWithNormal() throws Exception {
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        String sql = "with tmp as (select * from db1.tbl1) select count(*) from tmp;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
    }

    @Test
    public void testWithNested() throws Exception {
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
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
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();

        String sql = "select count(*) from db1.tbl1 as a";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testShowUserProperty() throws Exception {
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        String sql = "SHOW PROPERTY FOR 'test' LIKE '%load_cluster%'";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testSetUserProperty() throws Exception {
        starRocksAssert.getCtx().setQualifiedUser("test");
        String sql = "SET PROPERTY FOR 'test' 'max_user_connections' = 'value', 'test' = '400'";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
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
        db1TablePattern.analyze();

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
        db1TablePattern.analyze();

        String sql = "create view db1.v as select 1,2,3";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.CREATE_PRIV, Privilege.SELECT_PRIV), true);
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
        db1TablePattern.analyze();
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
    public void testCreateMaterializedView() throws Exception {

        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");
        Config.enable_experimental_mv = true;

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();

        String sql = "create materialized view db1.abc " +
                "distributed by hash(k1) " +
                "refresh async every (interval 2 MINUTE)" +
                "as select k1, k4 from db1.tbl1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.CREATE_PRIV), true);
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.CREATE_PRIV), true);

        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testDropMaterializedView() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");
        starRocksAssert.getCtx().setDatabase("db1");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();

        String sql = "drop materialized view mv1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.DROP_PRIV), true);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.DROP_PRIV), true);

        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testAlterMaterializedView() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");
        starRocksAssert.getCtx().setDatabase("db1");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();

        String sql = "alter materialized view mv1 rename mv2;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ALTER_PRIV), true);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ALTER_PRIV), true);

        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testRefreshMaterializedView() throws Exception {
        Config.enable_experimental_mv = true;
        starRocksAssert.withDatabase("db_mv").useDatabase("db_mv")
                .withTable("CREATE TABLE db_mv.tbl_with_mv\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2022-02-01'),('2022-02-16')),\n" +
                        "    PARTITION p2 values [('2022-02-16'),('2022-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withNewMaterializedView("create materialized view mv1\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select k2, sum(v1) as total from tbl_with_mv group by k2;");
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");
        starRocksAssert.getCtx().setDatabase("db_mv");

        TablePattern db1TablePattern = new TablePattern("db_mv", "*");
        db1TablePattern.analyze();

        String sql = "refresh materialized view mv1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ALTER_PRIV), true);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ALTER_PRIV), true);

        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testCancelRefreshMaterializedView() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");
        starRocksAssert.getCtx().setDatabase("db1");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();

        String sql = "cancel refresh materialized view mv1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ALTER_PRIV), true);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ALTER_PRIV), true);

        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testGrantRole() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");
        starRocksAssert.getCtx().setDatabase("db1");
        TablePattern db1TablePattern = new TablePattern("*", "*");
        db1TablePattern.analyze();

        // Here we hack `create role` statement because it was still in old framework
        String createRoleSql = "CREATE ROLE test_role";
        auth.createRole((CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(createRoleSql, starRocksAssert.getCtx()));

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
        db1TablePattern.analyze();

        String adminSetConfigsql = "admin set frontend config(\"alter_table_timeout_second\" = \"60\");";
        String adminSetReplicaStatusSql = "admin set replica status " +
                "properties(\"tablet_id\" = \"10003\",\"backend_id\" = \"10001\",\"status\" = \"ok\");";
        StatementBase statementBase1 = UtFrameUtils.parseStmtWithNewParser(adminSetConfigsql, starRocksAssert.getCtx());
        StatementBase statementBase2 =
                UtFrameUtils.parseStmtWithNewParser(adminSetReplicaStatusSql, starRocksAssert.getCtx());

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
        db1TablePattern.analyze();

        String adminShowConfigsql = "admin show frontend config like '%parallel%';";
        String adminShowReplicaDistributionsql = "ADMIN SHOW REPLICA DISTRIBUTION FROM db1.tbl1;";
        String adminShowReplicaStatussql = "ADMIN SHOW REPLICA Status FROM db1.tbl1;";
        StatementBase statementBase1 = UtFrameUtils.parseStmtWithNewParser(adminShowConfigsql,
                starRocksAssert.getCtx());
        StatementBase statementBase2 = UtFrameUtils.parseStmtWithNewParser(adminShowReplicaDistributionsql,
                starRocksAssert.getCtx());
        StatementBase statementBase3 = UtFrameUtils.parseStmtWithNewParser(adminShowReplicaStatussql,
                starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        PrivilegeChecker.check(statementBase1, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase3, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase1, starRocksAssert.getCtx()));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx()));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase3, starRocksAssert.getCtx()));
    }

    @Test
    public void testAdminRepairTable() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("*", "*");
        db1TablePattern.analyze();

        String adminRepairTable = "ADMIN REPAIR TABLE default_cluster.test PARTITION(p1, p2, p3);";
        String adminCancelRepairTable = "ADMIN CANCEL REPAIR TABLE default_cluster.test PARTITION(p1, p2, p3);";
        String adminCheckTablets = "ADMIN CHECK TABLET (10000, 10001) PROPERTIES(\"type\" = \"consistency\");";
        StatementBase statementBase1 = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(adminRepairTable,
                starRocksAssert.getCtx());
        StatementBase statementBase2 = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(adminCancelRepairTable,
                starRocksAssert.getCtx());
        StatementBase statementBase3 = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(adminCheckTablets,
                starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        PrivilegeChecker.check(statementBase1, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase3, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase1, starRocksAssert.getCtx()));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx()));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase3, starRocksAssert.getCtx()));
    }

    @Test
    public void testResourceStmt() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("*", "*");
        db1TablePattern.analyze();

        String createResourceStmt = "CREATE EXTERNAL RESOURCE 'spark0' PROPERTIES(\"type\"  =  \"spark\");";
        String alterResourceStmt =
                "alter RESOURCE hive0 SET PROPERTIES (\"hive.metastore.uris\" = \"thrift://10.10.44.91:9083\");";
        String dropResourceStmt = "drop resource hive01;";
        StatementBase statementBase1 = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                createResourceStmt, starRocksAssert.getCtx());
        StatementBase statementBase2 = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                alterResourceStmt, starRocksAssert.getCtx());
        StatementBase statementBase3 = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                dropResourceStmt, starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        PrivilegeChecker.check(statementBase1, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase3, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase1, starRocksAssert.getCtx()));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx()));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase3, starRocksAssert.getCtx()));
    }

    @Test
    public void testSqlBlacklist() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("*", "*");
        db1TablePattern.analyze();

        String addSqlBlacklistStmt = "ADD SQLBLACKLIST \"select count\\(distinct .+\\) from .+\";";
        String delSqlBlacklistStmt = "delete sqlblacklist  2, 6;";
        String showSqlBlacklistStmt = "show sqlblacklist";

        StatementBase statementBase1 = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                addSqlBlacklistStmt, starRocksAssert.getCtx());
        StatementBase statementBase2 = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                delSqlBlacklistStmt, starRocksAssert.getCtx());
        StatementBase statementBase3 = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                showSqlBlacklistStmt, starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        PrivilegeChecker.check(statementBase1, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase3, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase1, starRocksAssert.getCtx()));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx()));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase3, starRocksAssert.getCtx()));
    }

    @Test
    public void testUpdateTable() throws Exception {

        String createPrimaryTblStmtStr = "CREATE TABLE db2.tbl2 (k1 int, k2 int, k3 varchar(32)) PRIMARY KEY(k1) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 8 properties('replication_num' = '1');";
        starRocksAssert.getCtx().setDatabase("db2");
        starRocksAssert.withTable(createPrimaryTblStmtStr);

        TablePattern db2TablePattern = new TablePattern("db2", "*");
        db2TablePattern.analyze();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        auth.grantPrivs(testUser, db2TablePattern, PrivBitSet.of(Privilege.LOAD_PRIV), true);
        String sql = "update db2.tbl2 set k3 = 20 where k1 = 1";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
            PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

            auth.revokePrivs(testUser, db2TablePattern, PrivBitSet.of(Privilege.LOAD_PRIV), true);
            Assert.assertThrows(SemanticException.class,
                    () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testDeleteTable() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.LOAD_PRIV), true);
        String sql = "delete from db1.tbl1 where k4 = 1";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.LOAD_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testGrantImpersonate() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        TablePattern pattern = new TablePattern("*", "*");
        pattern.analyze();
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setDatabase("db1");

        String sql = "grant impersonate on test2 to test";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        auth.grantPrivs(testUser, pattern, PrivBitSet.of(Privilege.GRANT_PRIV), true);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        auth.revokePrivs(testUser, pattern, PrivBitSet.of(Privilege.GRANT_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testExecuteAs() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setDatabase("db1");

        String sql = "GRANT impersonate on test2 to test";
        auth.grant((GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx()));
        sql = "execute as test2 with no revert";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        sql = "REVOKE impersonate on test2 from test";
        auth.revoke((RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx()));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testShowCreateTable() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        String sql = "show create table db1.tbl1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testShowAlterTable() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        String sql = "SHOW ALTER TABLE COLUMN FROM db1";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testShowFunctions() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        String sql = "show functions from db1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testDropFunction() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("*", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);

        String sql = "drop function db1.abc(string);";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testCreateFunction() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");
        starRocksAssert.getCtx().setDatabase("testDb");

        TablePattern db1TablePattern = new TablePattern("*", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);

        String sql = "create function abc(string) returns int;";
        StatementBase statementBase =
                UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testAnalyzeStatement() throws Exception {
        starRocksAssert.useDatabase("db1");
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.LOAD_PRIV), true);
        String sql = "analyze table db1.tbl1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        sql = "create analyze full all";
        CreateAnalyzeJobStmt createAnalyzeJobStmt =
                (CreateAnalyzeJobStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class, () ->
                PrivilegeChecker.check(createAnalyzeJobStmt, starRocksAssert.getCtx()));

        sql = "create analyze database db1";
        CreateAnalyzeJobStmt createAnalyzeJobStmt1 =
                (CreateAnalyzeJobStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(createAnalyzeJobStmt1, starRocksAssert.getCtx());

        sql = "create analyze database db2";
        CreateAnalyzeJobStmt createAnalyzeJobStmt2 =
                (CreateAnalyzeJobStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class, () -> PrivilegeChecker.check(createAnalyzeJobStmt2,
                starRocksAssert.getCtx()));

        sql = "create analyze table tbl1";
        CreateAnalyzeJobStmt createAnalyzeJobStmt3 =
                (CreateAnalyzeJobStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(createAnalyzeJobStmt3, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testShowDelete() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        String sql = "show delete from db1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testDescTable() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        String sql = "desc db1.tbl1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testShowTablet() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("*", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        String sql = "SHOW TABLET FROM db1.tbl1";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertTrue(statementBase.isSupportNewPlanner());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testCancelAlterTable() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ALTER_PRIV), true);
        String sql = "cancel alter table rollup from db1.tbl2 (1, 2, 3)";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ALTER_PRIV), true);
        StatementBase statementBase2 = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx()));
    }

    @Test
    public void testModifyTableProperties() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ALTER_PRIV), true);
        String sql = "ALTER TABLE db1.tbl1 SET (\"default.replication_num\" = \"2\")";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ALTER_PRIV), true);
        StatementBase statementBase2 = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx()));
    }

    @Test
    public void testTruncateTable() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.LOAD_PRIV), true);
        String sql = "TRUNCATE TABLE db1.test";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertTrue(statementBase.isSupportNewPlanner());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.LOAD_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testShowIndex() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        String sql = "SHOW INDEX FROM db1.tbl1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testCreateTableLike() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();

        String sql = "CREATE TABLE db1.table2 LIKE db1.tbl1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.CREATE_PRIV), true);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.CREATE_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testShowPartitions() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        String sql = "show partitions from db1.tbl1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testShowBroker() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");
        TablePattern globalPattern = new TablePattern("*", "*");
        globalPattern.analyze();
        auth.grantPrivs(testUser, globalPattern, PrivPredicate.ADMIN.getPrivs(), true);
        String sql = "show broker";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertTrue(statementBase.isSupportNewPlanner());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, globalPattern, PrivPredicate.ADMIN.getPrivs(), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));

        auth.grantPrivs(testUser, globalPattern, PrivPredicate.OPERATOR.getPrivs(), true);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, globalPattern, PrivPredicate.OPERATOR.getPrivs(), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testBackup() throws Exception {
        new MockUp<Repository>() {
            @Mock
            public Status initRepository() {
                return Status.OK;
            }
        };

        Collection<Pair<String, Integer>> addresses = new ArrayList<>();
        Pair<String, Integer> pair = new Pair<String, Integer>("127.0.0.1", 8080);
        addresses.add(pair);
        String brokerName = "broker";
        String location = "bos://backup-cmy";
        starRocksAssert.getCtx().getGlobalStateMgr().getBrokerMgr().addBrokers(brokerName, addresses);

        BlobStorage storage = new BlobStorage(brokerName, Maps.newHashMap());
        Repository repo = new Repository(10000, "repo", false, location, storage);
        repo.initRepository();
        starRocksAssert.getCtx().getGlobalStateMgr().getBackupHandler().getRepoMgr()
                .addAndInitRepoIfNotExist(repo, false);

        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.LOAD_PRIV), true);
        String sql = "BACKUP SNAPSHOT db1.snapshot_label2 TO `repo` ON ( tbl1 );";

        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertTrue(statementBase.isSupportNewPlanner());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.LOAD_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testShowBackup() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.LOAD_PRIV), true);
        String sql = "SHOW BACKUP FROM db1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.LOAD_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testRestore() throws Exception {
        new MockUp<Repository>() {
            @Mock
            public Status initRepository() {
                return Status.OK;
            }
        };

        Collection<Pair<String, Integer>> addresses = new ArrayList<>();
        Pair<String, Integer> pair = new Pair<>("127.0.0.1", 8081);
        addresses.add(pair);
        String brokerName = "broker";
        String location = "bos://backup-cmy";
        starRocksAssert.getCtx().getGlobalStateMgr().getBrokerMgr().addBrokers(brokerName, addresses);

        BlobStorage storage = new BlobStorage(brokerName, Maps.newHashMap());
        Repository repo = new Repository(10000, "repo", false, location, storage);
        repo.initRepository();
        starRocksAssert.getCtx().getGlobalStateMgr().getBackupHandler().getRepoMgr()
                .addAndInitRepoIfNotExist(repo, false);

        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.LOAD_PRIV), true);
        String sql =
                "RESTORE SNAPSHOT db1.`snapshot_2` FROM `repo` ON ( `tbl1` ) " +
                        "PROPERTIES ( \"backup_timestamp\"=\"2018-05-04-17-11-01\" ) ;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertTrue(statementBase.isSupportNewPlanner());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.LOAD_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testShowRestore() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.LOAD_PRIV), true);
        String sql = "SHOW RESTORE FROM db1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.LOAD_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testShowAuthentication() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);

        String sql = "SHOW authentication;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        sql = "SHOW authentication FOR test;";
        statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        sql = "SHOW authentication FOR ROOT";
        StatementBase badStatement = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(badStatement, starRocksAssert.getCtx()));

        sql = "show all authentication;";
        StatementBase badStatement2 = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(badStatement2, starRocksAssert.getCtx()));

        starRocksAssert.getCtx().setCurrentUserIdentity(UserIdentity.ROOT);

        sql = "show authentication for test";
        statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        sql = "show all authentication;";
        statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
    }

    @Test
    public void testSelectView() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        // create view
        TablePattern tablePattern = new TablePattern("db1", "tbl1");
        tablePattern.analyze();
        auth.grantPrivs(testUser, tablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), false);
        tablePattern = new TablePattern("db1", "*");
        tablePattern.analyze();
        auth.grantPrivs(testUser, tablePattern, PrivBitSet.of(Privilege.CREATE_PRIV), false);
        String sql = "create view db1.view1 as select k1 from db1.tbl1;";
        starRocksAssert.withView(sql);

        // select privilege on base table
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(
                "select * from db1.view1", starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        // revoke select privilege on base table
        tablePattern = new TablePattern("db1", "tbl1");
        tablePattern.analyze();
        auth.revokePrivs(testUser, tablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), false);
        // grant select privilege on view
        tablePattern = new TablePattern("db1", "view1");
        tablePattern.analyze();
        auth.grantPrivs(testUser, tablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), false);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        // no select privilege on neither the base table nor the view
        auth.revokePrivs(testUser, tablePattern, PrivBitSet.of(Privilege.SELECT_PRIV), false);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));

    }

    @Test
    public void testGrantRevoke() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setCurrentUserIdentity(testUser);

        GrantPrivilegeStmt grantNode = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT NODE_PRIV ON *.* TO test2", ctx);
        GrantPrivilegeStmt grantSelectOnAllDB = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT SELECT_PRIV ON *.* TO test2", ctx);
        GrantPrivilegeStmt grantSelectOnOneDB = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT SELECT_PRIV ON db1.* TO test2", ctx);
        GrantPrivilegeStmt grantSelectOnOneTable = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT SELECT_PRIV ON db1.tbl1 TO test2", ctx);
        GrantPrivilegeStmt grantUsageOnAllResource = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT USAGE ON RESOURCE * TO test2", ctx);
        GrantPrivilegeStmt grantUsageOnOneResource = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT USAGE ON RESOURCE spark0 TO test2", ctx);
        GrantPrivilegeStmt grantToRole = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT SELECT_PRIV ON db1.tbl1 TO ROLE role0", ctx);

        // 1. global grant can grant anything
        auth.grant((GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT GRANT ON *.* TO test", starRocksAssert.getCtx()));
        PrivilegeChecker.check(grantNode, ctx);
        PrivilegeChecker.check(grantSelectOnAllDB, ctx);
        PrivilegeChecker.check(grantSelectOnOneDB, ctx);
        PrivilegeChecker.check(grantSelectOnOneTable, ctx);
        PrivilegeChecker.check(grantUsageOnAllResource, ctx);
        PrivilegeChecker.check(grantUsageOnOneResource, ctx);
        PrivilegeChecker.check(grantToRole, ctx);
        auth.revoke((RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE GRANT ON *.* FROM test", starRocksAssert.getCtx()));

        // 2. grant on db1.*
        auth.grant((GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT GRANT ON db1.* TO test", starRocksAssert.getCtx()));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(grantNode, ctx));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(grantSelectOnAllDB, ctx));
        PrivilegeChecker.check(grantSelectOnOneDB, ctx);
        PrivilegeChecker.check(grantSelectOnOneTable, ctx);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(grantUsageOnAllResource, ctx));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(grantUsageOnOneResource, ctx));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(grantToRole, ctx));
        auth.revoke((RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE GRANT ON db1.* FROM test", starRocksAssert.getCtx()));

        // 3. grant on db1.tbl1
        auth.grant((GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT GRANT ON db1.tbl1 TO test", starRocksAssert.getCtx()));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(grantNode, ctx));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(grantSelectOnAllDB, ctx));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(grantSelectOnOneDB, ctx));
        PrivilegeChecker.check(grantSelectOnOneTable, ctx);
        ;
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(grantUsageOnAllResource, ctx));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(grantUsageOnOneResource, ctx));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(grantToRole, ctx));
        auth.revoke((RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE GRANT ON db1.tbl1 FROM test", starRocksAssert.getCtx()));

        // 4. grant on resource *
        auth.grant((GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT GRANT ON RESOURCE * TO test", starRocksAssert.getCtx()));
        PrivilegeChecker.check(grantNode, ctx);
        PrivilegeChecker.check(grantSelectOnAllDB, ctx);
        PrivilegeChecker.check(grantSelectOnOneDB, ctx);
        PrivilegeChecker.check(grantSelectOnOneTable, ctx);
        PrivilegeChecker.check(grantUsageOnAllResource, ctx);
        PrivilegeChecker.check(grantUsageOnOneResource, ctx);
        PrivilegeChecker.check(grantToRole, ctx);
        auth.revoke((RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE GRANT ON RESOURCE * FROM test", starRocksAssert.getCtx()));

        // 4. grant on resource spark0
        auth.grant((GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT GRANT ON RESOURCE spark0 TO test", starRocksAssert.getCtx()));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(grantNode, ctx));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(grantSelectOnAllDB, ctx));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(grantSelectOnOneDB, ctx));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(grantSelectOnOneTable, ctx));
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(grantUsageOnAllResource, ctx));
        PrivilegeChecker.check(grantUsageOnOneResource, ctx);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(grantToRole, ctx));
        auth.revoke((RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE GRANT ON RESOURCE spark0 FROM test", starRocksAssert.getCtx()));
    }

    @Test
    public void testCreateRepository() throws Exception {
        new MockUp<BrokerMgr>() {
            @Mock
            public FsBroker getBroker(String name, String host) throws AnalysisException {
                return new FsBroker("10.74.167.16", 8111);
            }

        };

        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        TablePattern db1TablePattern = new TablePattern("*", "*");
        db1TablePattern.analyze();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        String sql = "CREATE REPOSITORY `repo`\n" +
                "WITH BROKER `broker`\n" +
                "ON LOCATION \"bos://backup-cmy\"\n" +
                "PROPERTIES\n" +
                "(\n" +
                "    \"username\" = \"root\",\n" +
                "    \"password\" = \"root\"\n" +
                ");";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        Assert.assertTrue(statementBase.isSupportNewPlanner());
        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }

    @Test
    public void testDropRepository() throws Exception {
        new MockUp<BrokerMgr>() {
            @Mock
            public FsBroker getBroker(String name, String host) throws AnalysisException {
                return new FsBroker("10.74.167.16", 8111);
            }

        };
        new MockUp<Repository>() {
            @Mock
            public Status initRepository() {
                return Status.OK;
            }
        };

        Collection<Pair<String, Integer>> addresses = new ArrayList<>();
        Pair<String, Integer> pair = new Pair<>("127.0.0.1", 8082);
        addresses.add(pair);
        starRocksAssert.getCtx().getGlobalStateMgr().getCurrentState().getBrokerMgr().addBrokers("broker", addresses);

        BlobStorage storage = new BlobStorage("broker", Maps.newHashMap());
        Repository repo = new Repository(10000, "repo", false, "bos://backup-cmy", storage);
        repo.initRepository();
        starRocksAssert.getCtx().getGlobalStateMgr().getCurrentState().getBackupHandler().getRepoMgr()
                .addAndInitRepoIfNotExist(repo, false);

        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        TablePattern db1TablePattern = new TablePattern("*", "*");
        db1TablePattern.analyze();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        String sql = "DROP REPOSITORY `repo`;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        Assert.assertTrue(statementBase.isSupportNewPlanner());
        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ADMIN_PRIV), true);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }
}
