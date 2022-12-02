// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivEntry;
import com.starrocks.mysql.privilege.TablePrivEntry;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class PrivilegeColumnTest {
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
    public void testGrantColumnPrivilege() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setCurrentUserIdentity(testUser);

        GrantPrivilegeStmt grantColumn = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT SELECT_PRIV ON db1.tbl1 COLUMNS k1,k2 TO test", ctx);
        GrantPrivilegeStmt grantTable = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT SELECT_PRIV,DROP_PRIV ON db1.tbl1 TO test", ctx);
        // ctx.getGlobalStateMgr().getAuth().grant(grantTable);
        ctx.getGlobalStateMgr().getAuth().grant(grantColumn);

        List<PrivEntry> plist = ctx.getGlobalStateMgr().getAuth().getTablePrivTable().getEntry(testUser);
        System.out.println(((TablePrivEntry) plist.get(0)).getColumnNameList());

        String sql = "select k1 from db1.tbl1 as a";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        sql = "select k1,k2 from db1.tbl1 as a";
        statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());

        sql = "select k3 from db1.tbl1 as a";
        StatementBase statementBase2 = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx()));

        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        PrivilegeChecker.check(statementBase2, starRocksAssert.getCtx());
    }
}
