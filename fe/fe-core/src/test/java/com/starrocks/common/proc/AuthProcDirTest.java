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

package com.starrocks.common.proc;

import com.google.common.collect.Lists;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.common.AnalysisException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AuthProcDirTest {
    private static final String TEST_USER = "test_user";
    private static final String TEST_HOST = "%";
    private static final String TEST_USER_IDENT = "'" + TEST_USER + "'@'" + TEST_HOST + "'";

    private UserIdentity testUserIdentity;

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private AuthenticationMgr authenticationMgr;
    @Mocked
    private AuthorizationMgr authorizationMgr;
    @Mocked
    private AuthProcSupplier authProcSupplier;

    @Before
    public void setUp() {
        testUserIdentity = new UserIdentity(TEST_USER, TEST_HOST);
        testUserIdentity.analyze();

        List<List<String>> authInfoRows = new ArrayList<>();
        List<String> userRow = Lists.newArrayList(
                TEST_USER_IDENT,
                "********",
                "mysql_native_password",
                "[]",
                "[]",
                "[]",
                "[]",
                "[]",
                "[]"
        );

        authInfoRows.add(userRow);
        List<List<String>> userPropertyRows = new ArrayList<>();
        userPropertyRows.add(Lists.newArrayList("UserIdentity", TEST_USER_IDENT));
        userPropertyRows.add(Lists.newArrayList("AuthPlugin", "mysql_native_password"));
        userPropertyRows.add(Lists.newArrayList("Roles", "[]"));

        new Expectations() {
            {
                authProcSupplier.getAuthInfo();
                minTimes = 0;
                result = authInfoRows;

                authProcSupplier.getUserPropertyInfo(testUserIdentity);
                minTimes = 0;
                result = userPropertyRows;
            }
        };

        Map<UserIdentity, UserAuthenticationInfo> userMap = new HashMap<>();
        UserAuthenticationInfo authInfo = new UserAuthenticationInfo();
        userMap.put(testUserIdentity, authInfo);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getAuthProcSupplier();
                minTimes = 0;
                result = authProcSupplier;

                authenticationMgr.getUserToAuthenticationInfo();
                minTimes = 0;
                result = userMap;
            }
        };
    }

    @Test
    public void testRegister() {
        AuthProcDir dir = new AuthProcDir();
        Assert.assertFalse(dir.register("test", new BaseProcDir()));
    }

    @Test
    public void testLookupWithValidUserIdentity() throws AnalysisException {
        AuthProcDir dir = new AuthProcDir();
        ProcNodeInterface node = dir.lookup(TEST_USER_IDENT);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof AuthProcNode);
    }

    @Test
    public void testLookupWithBareUsername() throws AnalysisException {
        AuthProcDir dir = new AuthProcDir();
        ProcNodeInterface node = dir.lookup(TEST_USER);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof AuthProcNode);
    }

    @Test
    public void testLookupWithUnquotedUsernameAndHost() throws AnalysisException {
        AuthProcDir dir = new AuthProcDir();
        ProcNodeInterface node = dir.lookup(TEST_USER + "@" + TEST_HOST);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof AuthProcNode);
    }

    @Test(expected = AnalysisException.class)
    public void testLookupWithInvalidUserIdentity() throws AnalysisException {
        AuthProcDir dir = new AuthProcDir();
        dir.lookup("@invalid@user");
    }

    @Test(expected = AnalysisException.class)
    public void testLookupWithEmptyUserIdentity() throws AnalysisException {
        AuthProcDir dir = new AuthProcDir();
        dir.lookup("");
    }

    @Test(expected = AnalysisException.class)
    public void testLookupWithNullUserIdentity() throws AnalysisException {
        AuthProcDir dir = new AuthProcDir();
        dir.lookup(null);
    }

    @Test
    public void testFetchResult() throws AnalysisException {
        AuthProcDir dir = new AuthProcDir();
        ProcResult result = dir.fetchResult();
        
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof BaseProcResult);

        List<String> columnNames = result.getColumnNames();
        Assert.assertEquals(AuthProcDir.TITLE_NAMES, columnNames);

        List<List<String>> rows = result.getRows();
        Assert.assertEquals(1, rows.size());
        
        List<String> row = rows.get(0);
        Assert.assertEquals(TEST_USER_IDENT, row.get(0));
        Assert.assertEquals("********", row.get(1));
    }
} 