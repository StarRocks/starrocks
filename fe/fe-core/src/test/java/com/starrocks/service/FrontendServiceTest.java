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


package com.starrocks.service;

import com.starrocks.catalog.AccessPrivilege;
import com.starrocks.common.conf.Config;
import com.starrocks.common.exception.DdlException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TAuthenticateParams;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class FrontendServiceTest {

    private Auth auth;
    @Mocked
    public GlobalStateMgr globalStateMgr;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() throws NoSuchMethodException, SecurityException {
        auth = new Auth();

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getAuth();
                minTimes = 0;
                result = auth;

                ConnectContext.get();
                minTimes = 0;
                result = ctx;

                ctx.getQualifiedUser();
                minTimes = 0;
                result = "root";

                ctx.getRemoteIP();
                minTimes = 0;
                result = "192.168.1.1";

                ctx.getState();
                minTimes = 0;
                result = new QueryState();

                ctx.getCurrentUserIdentity();
                minTimes = 0;
                result = UserIdentity.createAnalyzedUserIdentWithIp("root", "%");
            }
        };
    }

    //@Test
    public void testCheckPasswordAndLoadPrivilege() {
        createUser("abc", "123", "192.168.92.3");

        grantTable("abc", "192.168.92.3", "db1", "t1",
                Collections.singletonList(AccessPrivilege.LOAD_PRIV));
        grantTable("abc", "192.168.92.3", "db1", "t2",
                Collections.singletonList(AccessPrivilege.LOAD_PRIV));
        grantTable("abc", "192.168.92.3", "db1", "t3",
                Collections.singletonList(AccessPrivilege.LOAD_PRIV));

        Config.enable_starrocks_external_table_auth_check = true;
        // test check passed
        TAuthenticateParams authParam = createTAuthenticateParams(
                "abc", "123", "192.168.92.3", "db1", Arrays.asList("t1", "t2", "t3"));
        verifyCheckResult(true, null, FrontendServiceImpl.checkPasswordAndLoadPrivilege(authParam));

        String hintMsg = "Set the configuration 'enable_starrocks_external_table_auth_check' to 'false' on the target " +
                "cluster if you don't want to check the authorization and privilege.";

        // test password check failed
        authParam = createTAuthenticateParams(
                "abc", "12", "192.168.92.3", "db1", Arrays.asList("t1", "t2", "t3"));
        verifyCheckResult(false, Arrays.asList("Access denied for abc@192.168.92.3", "Please check that your user " +
                        "or password is correct", hintMsg),
                FrontendServiceImpl.checkPasswordAndLoadPrivilege(authParam));

        // test privilege check failed on different tables
        String errMsgFormat = "Access denied; user 'abc'@'192.168.92.3' need (at least one of) the " +
                "privilege(s) in [Admin_priv Load_priv] for table '%s' in database 'db1'";

        authParam = createTAuthenticateParams(
                "abc", "123", "192.168.92.3", "db1", Arrays.asList("t4", "t2", "t3"));
        verifyCheckResult(false, Arrays.asList(String.format(errMsgFormat, "t4"), hintMsg),
                FrontendServiceImpl.checkPasswordAndLoadPrivilege(authParam));

        authParam = createTAuthenticateParams(
                "abc", "123", "192.168.92.3", "db1", Arrays.asList("t1", "t5", "t3"));
        verifyCheckResult(false, Arrays.asList(String.format(errMsgFormat, "t5"), hintMsg),
                FrontendServiceImpl.checkPasswordAndLoadPrivilege(authParam));

        authParam = createTAuthenticateParams(
                "abc", "123", "192.168.92.3", "db1", Arrays.asList("t1", "t2", "t6"));
        verifyCheckResult(false, Arrays.asList(String.format(errMsgFormat, "t6"), hintMsg),
                FrontendServiceImpl.checkPasswordAndLoadPrivilege(authParam));

        // test disable check configuration
        Config.enable_starrocks_external_table_auth_check = false;

        // normal passed
        authParam = createTAuthenticateParams(
                "abc", "123", "192.168.92.3", "db1", Arrays.asList("t1", "t2", "t3"));
        verifyCheckResult(true, null, FrontendServiceImpl.checkPasswordAndLoadPrivilege(authParam));

        // incorrect password passed
        authParam = createTAuthenticateParams(
                "abc", "12", "192.168.92.3", "db1", Arrays.asList("t1", "t2", "t3"));
        verifyCheckResult(true, null, FrontendServiceImpl.checkPasswordAndLoadPrivilege(authParam));

        // incorrect privilege passed
        authParam = createTAuthenticateParams(
                "abc", "123", "192.168.92.3", "db1", Arrays.asList("t4", "t2", "t3"));
        verifyCheckResult(true, null, FrontendServiceImpl.checkPasswordAndLoadPrivilege(authParam));
    }

    private void verifyCheckResult(boolean expectOk, List<String> errMsgs, TStatus actualStatus) {
        assertEquals(expectOk, actualStatus.getStatus_code() == TStatusCode.OK);
        if (errMsgs == null) {
            assertNull(actualStatus.getError_msgs());
        } else {
            for (int i = 0; i < errMsgs.size(); i++) {
                assertEquals(errMsgs.get(i), actualStatus.getError_msgs().get(i));
            }
        }
    }

    private TAuthenticateParams createTAuthenticateParams(
            String userName, String password, String host, String dbName, List<String> tableNames) {
        TAuthenticateParams authParams = new TAuthenticateParams();
        authParams.setUser(userName);
        authParams.setPasswd(password);
        authParams.setHost(host);
        authParams.setDb_name(dbName);
        authParams.setTable_names(tableNames);
        return authParams;
    }

    private void createUser(String userName, String password, String host) {
        String sql = String.format("create user '%s'@'%s' identified by '%s'", userName, host, password);
        CreateUserStmt createUserStmt = null;
        try {
            createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
    }

    private void grantTable(String userName, String host, String dbName, String tableName, List<AccessPrivilege> privileges) {
        String sql = String.format("GRANT %s on %s.%s to '%s'@'%s'",
                privileges.stream().map(AccessPrivilege::name).collect(Collectors.joining(",")),
                dbName, tableName, userName, host);

        GrantPrivilegeStmt grantStmt = null;
        try {
            grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            auth.grant(grantStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
    }
}
