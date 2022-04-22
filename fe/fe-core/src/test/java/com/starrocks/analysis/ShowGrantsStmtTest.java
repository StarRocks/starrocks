// This file is made available under Elastic License 2.0.

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.catalog.Catalog;
import com.starrocks.common.AnalysisException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.MockedAuth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.mysql.privilege.UserPrivTable;
import com.starrocks.qe.ConnectContext;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowGrantsStmtTest {

    @Mocked
    private Analyzer analyzer;
    @Mocked
    private Catalog catalog;
    @Mocked
    private Auth auth;
    @Mocked
    private UserPrivTable userPrivTable;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
        new Expectations(analyzer) {
            {
                analyzer.getClusterName();
                minTimes = 0;
                result = "test_cluster";
            }
        };
        new Expectations() {
            {
                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;
            }
        };
        new Expectations(catalog) {
            {
                catalog.getAuth();
                minTimes = 0;
                result = auth;
            }
        };
        new Expectations(auth) {
            {
                auth.getUserPrivTable();
                minTimes = 0;
                result = userPrivTable;

                auth.checkGlobalPriv(ctx, PrivPredicate.GRANT);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testNormal() throws Exception {
        // suppose current user exists
        new Expectations(userPrivTable) {
            {
                userPrivTable.doesUserExist((UserIdentity)any);
                minTimes = 0;
                result = true;
            }
        };
        ShowGrantsStmt stmt = new ShowGrantsStmt(new UserIdentity("test_user", "localhost"), false);
        stmt.analyze(analyzer);
    }

    @Test(expected = AnalysisException.class)
    public void testUserNotExist() throws Exception {
        // suppose current user doesn't exist, check for exception
        new Expectations(userPrivTable) {
            {
                userPrivTable.doesUserExist((UserIdentity)any);
                minTimes = 0;
                result = false;
            }
        };
        ShowGrantsStmt stmt = new ShowGrantsStmt(new UserIdentity("fake_user", "localhost"), false);
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}
