// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.AuthenticationManager;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Test;
import org.wildfly.common.Assert;

import java.util.Arrays;

public class PrivilegeManagerTest {

    @Test
    public void testTableSelectUser(@Mocked GlobalStateMgr mgr,
                                    @Mocked Database database,
                                    @Mocked AuthenticationManager authenticationManager,
                                    @Mocked Table table,
                                    @Mocked EditLog editLog) throws Exception {
        new Expectations(mgr) {
            {
                mgr.isUsingNewPrivilege();
                result = true;
                minTimes = 0;
            }
            {
                mgr.getDb("db");
                result = database;
            }
            {
                mgr.getAuthenticationManager();
                result = authenticationManager;
            }
            {
                mgr.getEditLog();
                result = editLog;
            }
        };
        new Expectations(database) {
            {
                database.getTable("tbl");
                result = table;
            }
            {
                database.getId();
                result = 1;
            }
        };
        new Expectations(table) {
            {
                table.getId();
                result = 11;
            }
        };
        new Expectations(editLog) {
            {
                editLog.logUpdateUserPrivilege((UserIdentity) any, (UserPrivilegeCollection) any);
                minTimes = 0;
            }
        };
        new Expectations(authenticationManager) {
            {
                authenticationManager.doesUserExist((UserIdentity) any);
                result = true;
            }
        };

        PrivilegeManager manager = new PrivilegeManager(mgr, null);
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
        manager.onCreateUser(testUser);
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setCurrentUserIdentity(testUser);
        ctx.setGlobalStateMgr(mgr);

        Assert.assertFalse(manager.hasType(ctx, "TABLE"));
        Assert.assertFalse(manager.checkAnyObject(ctx, "TABLE", "SELECT"));
        Assert.assertFalse(manager.check(ctx, "TABLE", "SELECT", Arrays.asList("db", "tbl")));

        String sql = "grant select on db.tbl to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantStmt);

        Assert.assertTrue(manager.hasType(ctx, "TABLE"));
        Assert.assertTrue(manager.checkAnyObject(ctx, "TABLE", "SELECT"));
        Assert.assertTrue(manager.check(ctx, "TABLE", "SELECT", Arrays.asList("db", "tbl")));

        sql = "revoke select on db.tbl from test_user";
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.revoke(revokeStmt);

        Assert.assertFalse(manager.hasType(ctx, "TABLE"));
        Assert.assertFalse(manager.checkAnyObject(ctx, "TABLE", "SELECT"));
        Assert.assertFalse(manager.check(ctx, "TABLE", "SELECT", Arrays.asList("db", "tbl")));
    }
}
