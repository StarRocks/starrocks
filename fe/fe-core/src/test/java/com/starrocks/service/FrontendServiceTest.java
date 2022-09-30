// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.service;

import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.AccessPrivilege;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.thrift.TAuthenticateParams;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

    @Test
    public void testCheckPasswordAndPrivilege() {
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
        verifyCheckResult(true, null, FrontendServiceImpl.checkPasswordAndPrivilege(authParam));

        // test password check failed
        authParam = createTAuthenticateParams(
                "abc", "12", "192.168.92.3", "db1", Arrays.asList("t1", "t2", "t3"));
        verifyCheckResult(false, "Access denied for abc@192.168.92.3",
                FrontendServiceImpl.checkPasswordAndPrivilege(authParam));

        // test privilege check failed on different tables
        String errMsgFormat = "Access denied; user 'abc'@'192.168.92.3' need (at least one of) the Admin_priv Load_priv " +
                        "privilege(s) for table '%s' in db 'db1'";
        authParam = createTAuthenticateParams(
                "abc", "123", "192.168.92.3", "db1", Arrays.asList("t4", "t2", "t3"));
        verifyCheckResult(false, String.format(errMsgFormat, "t4"), FrontendServiceImpl.checkPasswordAndPrivilege(authParam));

        authParam = createTAuthenticateParams(
                "abc", "123", "192.168.92.3", "db1", Arrays.asList("t1", "t5", "t3"));
        verifyCheckResult(false, String.format(errMsgFormat, "t5"), FrontendServiceImpl.checkPasswordAndPrivilege(authParam));

        authParam = createTAuthenticateParams(
                "abc", "123", "192.168.92.3", "db1", Arrays.asList("t1", "t2", "t6"));
        verifyCheckResult(false, String.format(errMsgFormat, "t6"), FrontendServiceImpl.checkPasswordAndPrivilege(authParam));


        // test disable check configuration
        Config.enable_starrocks_external_table_auth_check = false;

        // normal passed
        authParam = createTAuthenticateParams(
                "abc", "123", "192.168.92.3", "db1", Arrays.asList("t1", "t2", "t3"));
        verifyCheckResult(true, null, FrontendServiceImpl.checkPasswordAndPrivilege(authParam));

        // incorrect password passed
        authParam = createTAuthenticateParams(
                "abc", "12", "192.168.92.3", "db1", Arrays.asList("t1", "t2", "t3"));
        verifyCheckResult(true, null, FrontendServiceImpl.checkPasswordAndPrivilege(authParam));

        // incorrect privilege passed
        authParam = createTAuthenticateParams(
                "abc", "123", "192.168.92.3", "db1", Arrays.asList("t4", "t2", "t3"));
        verifyCheckResult(true, null, FrontendServiceImpl.checkPasswordAndPrivilege(authParam));

    }

    private void verifyCheckResult(boolean expectOk, String errMsg, TStatus actualStatus) {
        assertEquals(expectOk, actualStatus.getStatus_code() == TStatusCode.OK);
        if (errMsg == null) {
            assertNull(actualStatus.getError_msgs());
        } else {
            assertEquals(1, actualStatus.getError_msgs().size());
            assertEquals(errMsg, actualStatus.getError_msgs().get(0));
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
