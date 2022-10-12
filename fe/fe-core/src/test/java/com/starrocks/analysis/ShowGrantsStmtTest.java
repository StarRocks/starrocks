// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.MockedAuth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowGrantsStmtTest {

    @Mocked
    private Analyzer analyzer;
    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private Auth auth;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getAuth();
                minTimes = 0;
                result = auth;
            }
        };
        new Expectations(auth) {
            {
                auth.checkGlobalPriv(ctx, PrivPredicate.GRANT);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testNormal() throws Exception {
        // suppose current user exists
        new Expectations(auth) {
            {
                auth.doesUserExist((UserIdentity)any);
                minTimes = 0;
                result = true;
            }
        };
        String revokeSql = "SHOW GRANTS FOR test_user@'localhost'";
        UtFrameUtils.parseStmtWithNewParser(revokeSql, ctx);
    }

    @Test(expected = AnalysisException.class)
    public void testUserNotExist() throws Exception {
        // suppose current user doesn't exist, check for exception
        new Expectations(auth) {
            {
                auth.doesUserExist((UserIdentity)any);
                minTimes = 0;
                result = false;
            }
        };
        String revokeSql = "SHOW GRANTS FOR fake_user@'localhost'";
        UtFrameUtils.parseStmtWithNewParser(revokeSql, ctx);
        Assert.fail("No exception throws.");
    }
}
