// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.service;

import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.GrantStmt;
import com.starrocks.analysis.TablePattern;
import com.starrocks.analysis.UserDesc;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.AccessPrivilege;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TAuthenticateParams;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class FrontendServiceTest {

    private Auth auth;
    @Mocked
    public GlobalStateMgr globalStateMgr;
    @Mocked
    private Analyzer analyzer;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() throws NoSuchMethodException, SecurityException {
        auth = new Auth();

        new Expectations() {
            {
                analyzer.getClusterName();
                minTimes = 0;
                result = SystemInfoService.DEFAULT_CLUSTER;

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
        verifyCheckResult(false, Arrays.asList("Access denied for default_cluster:abc@192.168.92.3", "Please " +
                        "check that your user or password is correct", hintMsg),
                        FrontendServiceImpl.checkPasswordAndLoadPrivilege(authParam));

        // test privilege check failed on different tables
        String errMsgFormat = "Access denied; user 'default_cluster:abc'@'192.168.92.3' need (at least one of) the " +
                "privilege(s) in [Admin_priv Load_priv] for table '%s' in database 'default_cluster:db1'";
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
        UserIdentity userIdentity = new UserIdentity(userName, host);
        UserDesc userDesc = new UserDesc(userIdentity, password, true);
        CreateUserStmt createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            Assert.fail();
        }

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
    }

    private void grantTable(String userName, String host, String dbName, String tableName, List<AccessPrivilege> privileges) {
        TablePattern tablePattern = new TablePattern(dbName, tableName);
        GrantStmt grantStmt = new GrantStmt(new UserIdentity(userName, host), null, tablePattern, privileges);
        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e) {
            Assert.fail();
        }

        try {
            auth.grant(grantStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
    }
}
