// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.AuthenticationManager;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.UserPrivilegeCollectionInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Test;
import org.wildfly.common.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PrivilegeManagerTest {
    private static final List<String> DB_TBL_TOKENS = Arrays.asList("db", "tbl");

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
                editLog.logUpdateUserPrivilege((UserIdentity) any, (UserPrivilegeCollection) any, anyShort, anyShort);
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

        Assert.assertFalse(manager.hasType(ctx, PrivilegeTypes.TABLE.toString()));
        Assert.assertFalse(manager.checkAnyObject(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString()));
        Assert.assertFalse(manager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));

        String sql = "grant select on db.tbl to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantStmt);

        Assert.assertTrue(manager.hasType(ctx, PrivilegeTypes.TABLE.toString()));
        Assert.assertTrue(manager.checkAnyObject(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString()));
        Assert.assertTrue(manager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));

        sql = "revoke select on db.tbl from test_user";
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.revoke(revokeStmt);

        Assert.assertFalse(manager.hasType(ctx, PrivilegeTypes.TABLE.toString()));
        Assert.assertFalse(manager.checkAnyObject(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString()));
        Assert.assertFalse(manager.check(
                ctx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));
    }

    private class FakeObject extends PEntryObject {
        @SerializedName(value = "t")
        private List<String> tokens;
        public FakeObject(List<String> tokens) {
            super(10);
            this.tokens = new ArrayList<>(tokens);
        }

        @Override
        public boolean isSame(PEntryObject pEntryObject) {
            return pEntryObject instanceof FakeObject && ((FakeObject) pEntryObject).tokens.equals(tokens);
        }
    }

    private class FakeProvider extends DefaultAuthorizationProvider {
        public FakeProvider() {}
        @Override
        public PEntryObject generateObject(String type, List<String> objectTokens, GlobalStateMgr mgr)
                throws PrivilegeException {
            return new FakeObject(objectTokens);
        }
    }
    @Test
    public void testPersist() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        GlobalStateMgr masterGlobalStateMgr = GlobalStateMgr.getCurrentState();
        masterGlobalStateMgr.initAuth(true);
        PrivilegeManager masterManager = masterGlobalStateMgr.getPrivilegeManager();
        masterManager.provider = new FakeProvider();
        ConnectContext rootCtx = UtFrameUtils.createDefaultCtx();
        rootCtx.setCurrentUserIdentity(UserIdentity.ROOT);
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
        ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
        testCtx.setCurrentUserIdentity(testUser);

        // create user test_user
        String sql = "create user test_user";
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, rootCtx);
        masterGlobalStateMgr.getAuthenticationManager().createUser(createUserStmt);
        Assert.assertTrue(masterGlobalStateMgr.getAuthenticationManager().doesUserExist(testUser));
        Assert.assertFalse(masterManager.check(
                testCtx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();


        sql = "grant select on db.tbl to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, rootCtx);
        masterManager.grant(grantStmt);
        Assert.assertTrue(masterManager.check(
                testCtx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));

        sql = "revoke select on db.tbl from test_user";
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, rootCtx);
        masterManager.revoke(revokeStmt);
        Assert.assertFalse(masterManager.check(
                testCtx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));


        // start to replay
        PrivilegeManager followerManager = new PrivilegeManager(masterGlobalStateMgr, new FakeProvider());

        UserPrivilegeCollectionInfo info = (UserPrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer.replayNextJournal();
        followerManager.replayUpdateUserPrivilegeCollection(
                info.getUserIdentity(), info.getPrivilegeCollection(), info.getPluginId(), info.getPluginVersion());
        Assert.assertTrue(followerManager.check(
                testCtx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));

        info = (UserPrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer.replayNextJournal();
        followerManager.replayUpdateUserPrivilegeCollection(
                info.getUserIdentity(), info.getPrivilegeCollection(), info.getPluginId(), info.getPluginVersion());
        Assert.assertFalse(followerManager.check(
                testCtx,
                PrivilegeTypes.TABLE.toString(),
                PrivilegeTypes.TableActions.SELECT.toString(),
                DB_TBL_TOKENS));

        UtFrameUtils.tearDownForPersisTest();
    }
}
