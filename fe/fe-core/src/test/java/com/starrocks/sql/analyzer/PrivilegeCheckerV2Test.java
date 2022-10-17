// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.AuthenticationManager;
import com.starrocks.privilege.PrivilegeManager;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class PrivilegeCheckerV2Test {
    private static StarRocksAssert starRocksAssert;
    private static UserIdentity testUser;
    private static UserIdentity testUser2;
    private static UserIdentity rootUser;

    private static PrivilegeManager privilegeManager;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        String createTblStmtStr = "create table db1.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT));
        starRocksAssert.withDatabase("db1");
        starRocksAssert.withDatabase("db2");
        starRocksAssert.withTable(createTblStmtStr);
        rootUser = starRocksAssert.getCtx().getCurrentUserIdentity();
        privilegeManager = starRocksAssert.getCtx().getGlobalStateMgr().getPrivilegeManager();
        starRocksAssert.getCtx().setRemoteIP("localhost");
        ctxToRoot();
        createUsers();
    }

    private static void ctxToTestUser() {
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setQualifiedUser(testUser.getQualifiedUser());
    }

    private static void ctxToRoot() {
        starRocksAssert.getCtx().setCurrentUserIdentity(UserIdentity.ROOT);
        starRocksAssert.getCtx().setQualifiedUser(UserIdentity.ROOT.getQualifiedUser());
    }

    private static void createUsers() throws Exception {
        String createUserSql = "CREATE USER 'test' IDENTIFIED BY ''";
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());

        AuthenticationManager authenticationManager = starRocksAssert.getCtx().getGlobalStateMgr().getAuthenticationManager();
        authenticationManager.createUser(createUserStmt);
        testUser = createUserStmt.getUserIdent();

        createUserSql = "CREATE USER 'test2' IDENTIFIED BY ''";
        createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());
        authenticationManager.createUser(createUserStmt);
        testUser2 = createUserStmt.getUserIdent();
    }

    private static void verifyGrantRevoke(String sql, String grantSql, String revokeSql, String expectError) throws Exception {
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        // 1. before grant: access denied
        ctxToTestUser();
        try {
            PrivilegeCheckerV2.check(statement, starRocksAssert.getCtx());
            Assert.fail();
        } catch (SemanticException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains(expectError));
        }

        ctxToRoot();
        GrantPrivilegeStmt grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                grantSql, starRocksAssert.getCtx());
        privilegeManager.grant(grantPrivilegeStmt);

        ctxToTestUser();
        PrivilegeCheckerV2.check(statement, starRocksAssert.getCtx());

        ctxToRoot();
        RevokePrivilegeStmt revokePrivilegeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                revokeSql, starRocksAssert.getCtx());
        privilegeManager.revoke(revokePrivilegeStmt);

        ctxToTestUser();
        try {
            PrivilegeCheckerV2.check(statement, starRocksAssert.getCtx());
            Assert.fail();
        } catch (SemanticException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains(expectError));
        }
    }

    @Test
    public void testTableSelectDeleteInsert() throws Exception {
        verifyGrantRevoke(
                "select * from db1.tbl1",
                "grant select on db1.tbl1 to test",
                "revoke select on db1.tbl1 from test",
                "SELECT command denied to user 'test'");
        verifyGrantRevoke(
                "insert into db1.tbl1 values ('petals', 'on', 'a', 99);",
                "grant insert on db1.tbl1 to test",
                "revoke insert on db1.tbl1 from test",
                "INSERT command denied to user 'test'");
        verifyGrantRevoke(
                "delete from db1.tbl1 where k3 = 1",
                "grant delete on db1.tbl1 to test",
                "revoke delete on db1.tbl1 from test",
                "DELETE command denied to user 'test'");
    }

    @Test
    public void testTableCreateDrop() throws Exception {
        String createTableSql = "create table db1.tbl2(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        verifyGrantRevoke(
                createTableSql,
                "grant create_table on database db1 to test",
                "revoke create_table on database db1 from test",
                "Access denied for user 'test' to database 'db1'");
        verifyGrantRevoke(
                "drop table db1.tbl1",
                "grant drop on database db1 to test",
                "revoke drop on database db1 from test",
                "Access denied for user 'test' to database 'db1'");
    }

    @Test
    public void testGrantRevoke() throws Exception {
        verifyGrantRevoke(
                "grant select on db1.tbl1 to test",
                "grant select on db1.tbl1 to test with grant option",
                "revoke select on db1.tbl1 from test",
                "Access denied; you need (at least one of) the GRANT privilege(s) for this operation");
        verifyGrantRevoke(
                "revoke select on db1.tbl1 from test",
                "grant select on db1.tbl1 to test with grant option",
                "revoke select on db1.tbl1 from test with grant option",
                "Access denied; you need (at least one of) the GRANT privilege(s) for this operation");
    }
}
