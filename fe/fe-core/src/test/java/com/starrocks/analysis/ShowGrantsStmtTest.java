// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.MockedAuth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
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

                globalStateMgr.isUsingNewPrivilege();
                minTimes = 0;
                result = false;
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
