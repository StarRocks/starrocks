// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.CreateRoleStmt;
import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TablePattern;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivBitSet;
import com.starrocks.mysql.privilege.Privilege;
import com.starrocks.sql.ast.GrantImpersonateStmt;
import com.starrocks.sql.ast.RevokeImpersonateStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class PrivilegeCheckerTest {
    private static StarRocksAssert starRocksAssert;
    private static UserIdentity testUser;
    private static UserIdentity testUser2;
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

        createUserSql = "CREATE USER 'test2' IDENTIFIED BY ''";
        createUserStmt = (CreateUserStmt) UtFrameUtils.parseAndAnalyzeStmt(createUserSql, starRocksAssert.getCtx());
        auth.createUser(createUserStmt);

        testUser2 = new UserIdentity("test2", "%");
        testUser2.analyze("default_cluster");
    }

    @Test
    public void testCreateTable() throws Exception {
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze("default_cluster");
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
        db1TablePattern.analyze("default_cluster");
        auth.grantPrivs(testUser, db1TablePattern, PrivBitSet.of(Privilege.ALTER_PRIV), true);
        String sql = "alter table db1.table1 rename table2";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
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
    public void testCreateMaterializedView() throws Exception {

        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        starRocksAssert.getCtx().setQualifiedUser("test");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setRemoteIP("%");
        Config.enable_experimental_mv = true;

        TablePattern db1TablePattern = new TablePattern("db1", "*");
        db1TablePattern.analyze("default_cluster");

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
    public void testUpdateTable() throws Exception {

        String createPrimaryTblStmtStr = "CREATE TABLE db2.tbl2 (k1 int, k2 int, k3 varchar(32)) PRIMARY KEY(k1) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 8 properties('replication_num' = '1');";
        StarRocksAssert starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db2");
        starRocksAssert.withTable(createPrimaryTblStmtStr);

        Auth auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        TablePattern db2TablePattern = new TablePattern("db2", "*");
        db2TablePattern.analyze("default_cluster");
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
        db1TablePattern.analyze("default_cluster");
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
        pattern.analyze("default_cluster");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setDatabase("default_cluster:db1");

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
        starRocksAssert.getCtx().setDatabase("default_cluster:db1");

        String sql = "execute as test2 with no revert";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        auth.grantImpersonate(new GrantImpersonateStmt(testUser, testUser2));
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        auth.revokeImpersonate(new RevokeImpersonateStmt(testUser, testUser2));
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

        TablePattern pattern = new TablePattern("*", "*");
        pattern.analyze("default_cluster");
        auth.grantPrivs(testUser, pattern, PrivBitSet.of(Privilege.GRANT_PRIV), true);

        sql = "show authentication for test;";
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
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser2);
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));

    }

}
