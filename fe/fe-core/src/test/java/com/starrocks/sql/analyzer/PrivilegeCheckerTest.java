// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.CreateRoleStmt;
import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TablePattern;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivBitSet;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.mysql.privilege.Privilege;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.GrantImpersonateStmt;
import com.starrocks.sql.ast.RevokeImpersonateStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PrivilegeCheckerTest {
    private static StarRocksAssert starRocksAssert;
    private static UserIdentity testUser;
    private static UserIdentity testUser2;
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
        dropUsers();
        createUsers();
    }

    private void createUsers() throws Exception {
        String createUserSql = "CREATE USER 'test' IDENTIFIED BY ''";
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());
        auth.createUser(createUserStmt);

        testUser = createUserStmt.getUserIdent();

        createUserSql = "CREATE USER 'test2' IDENTIFIED BY ''";
        createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());
        auth.createUser(createUserStmt);

        testUser2 = createUserStmt.getUserIdent();
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
    public void testAlterUser() throws Exception {
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        String sql = "ALTER USER 'root' IDENTIFIED BY ''";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));
    }
    private void dropUsers() throws Exception {
        if (testUser != null) {
            auth.replayDropUser(testUser);
        }
        if (testUser2 != null) {
            auth.replayDropUser(testUser2);
        }

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
        String sql = "create table db1.table1 (col1 int, col2 varchar(10)) engine=olap duplicate key(col1, col2) distributed by hash(col1) buckets 10";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        auth.revokePrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.CREATE_PRIV), true);
//        sql = "alter table db1.table1 rename table2";
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
        starRocksAssert.getCtx().setQualifiedUser("default_cluster:test");
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
        db1TablePattern.analyze();

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
        db1TablePattern.analyze();

        String adminShowConfigsql = "admin show frontend config like '%parallel%';";
        String adminShowReplicaDistributionsql = "ADMIN SHOW REPLICA DISTRIBUTION FROM db1.tbl1;";
        String adminShowReplicaStatussql = "ADMIN SHOW REPLICA Status FROM db1.tbl1;";
        StatementBase statementBase1 = UtFrameUtils.parseStmtWithNewParser(adminShowConfigsql, starRocksAssert.getCtx());
        StatementBase statementBase2 = UtFrameUtils.parseStmtWithNewParser(adminShowReplicaDistributionsql, starRocksAssert.getCtx());
        StatementBase statementBase3 = UtFrameUtils.parseStmtWithNewParser(adminShowReplicaStatussql, starRocksAssert.getCtx());

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

        String sql = "execute as test2 with no revert";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        auth.grantImpersonate(new GrantImpersonateStmt(testUser, testUser2));
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        auth.revokeImpersonate(new RevokeImpersonateStmt(testUser, testUser2));
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
        Assert.assertThrows(SemanticException.class, () -> PrivilegeChecker.check(createAnalyzeJobStmt2, starRocksAssert.getCtx()));

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

}
