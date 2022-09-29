// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.service;

import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.GrantStmt;
import com.starrocks.analysis.TablePattern;
import com.starrocks.analysis.UserDesc;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.AccessPrivilege;
import com.starrocks.catalog.Catalog;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
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
    public Catalog catalog;
    @Mocked
    private Analyzer analyzer;


    @Before
    public void setUp() throws NoSuchMethodException, SecurityException {
        auth = new Auth();

        new Expectations() {
            {
                analyzer.getClusterName();
                minTimes = 0;
                result = SystemInfoService.DEFAULT_CLUSTER;

                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getAuth();
                minTimes = 0;
                result = auth;
            }
        };
    }

    @Test
    public void testCheckPasswordAndPrivilege() {
        createUser("abc", "123", "192.168.92.3");

        grantTable("abc", "192.168.92.3", "db1", "t1",
                Collections.singletonList(AccessPrivilege.LOAD_PRIV));
        grantTable("abc", "192.168.92.3", "db2", "t2",
                Collections.singletonList(AccessPrivilege.LOAD_PRIV));
        grantTable("abc", "192.168.92.3", "db2", "t3",
                Collections.singletonList(AccessPrivilege.LOAD_PRIV));

        Config.enable_starrocks_external_table_auth_check = true;
        // test check passed
        TAuthenticateParams authParam = createTAuthenticateParams(
                "abc", "123", "192.168.92.3", "db1", Arrays.asList("t1", "t2", "t3"));
        verifyCheckResult(true, null, FrontendServiceImpl.checkPasswordAndPrivilege(authParam));

        // test password check failed
        authParam = createTAuthenticateParams(
                "abc", "12", "192.168.92.3", "db1", Arrays.asList("t1", "t2", "t3"));
        verifyCheckResult(false, "Access denied for abc @ 192.168.92.3",
                FrontendServiceImpl.checkPasswordAndPrivilege(authParam));

        // test privilege check failed on different tables
        String errMsg = "Access denied; user 'abc'@'192.168.92.3' need (at least one of) the t4 " +
                        "privilege(s) for table '%s' in db 'default_cluster.db1'";
        authParam = createTAuthenticateParams(
                "abc", "123", "192.168.92.3", "db1", Arrays.asList("t4", "t2", "t3"));
        verifyCheckResult(false, errMsg, FrontendServiceImpl.checkPasswordAndPrivilege(authParam));

        authParam = createTAuthenticateParams(
                "abc", "123", "192.168.92.3", "db1", Arrays.asList("t1", "t4", "t3"));
        verifyCheckResult(false, errMsg, FrontendServiceImpl.checkPasswordAndPrivilege(authParam));

        authParam = createTAuthenticateParams(
                "abc", "123", "192.168.92.3", "db1", Arrays.asList("t1", "t2", "t4"));
        verifyCheckResult(false, errMsg, FrontendServiceImpl.checkPasswordAndPrivilege(authParam));


        // test disable check configuration
        Config.enable_starrocks_external_table_auth_check = false;

        // normal passed
        authParam = createTAuthenticateParams(
                "abc", "123", "192.168.92.3", "db1", Arrays.asList("t1", "t2", "t3"));
        verifyCheckResult(true, null, FrontendServiceImpl.checkPasswordAndPrivilege(authParam));

        // incorrect password passed
        authParam = createTAuthenticateParams(
                "abc", "12", "192.168.92.3", "db1", Arrays.asList("t1", "t2", "t3"));
        verifyCheckResult(false, null, FrontendServiceImpl.checkPasswordAndPrivilege(authParam));

        // incorrect privilege passed
        authParam = createTAuthenticateParams(
                "abc", "123", "192.168.92.3", "db1", Arrays.asList("t4", "t2", "t3"));
        verifyCheckResult(false, null, FrontendServiceImpl.checkPasswordAndPrivilege(authParam));

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
