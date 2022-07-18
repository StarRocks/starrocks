// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/ShowFunctionsStmtTest.java

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

import com.starrocks.catalog.FakeGlobalStateMgr;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.MockedAuth;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ShowFunctionsStmtTest {
    @Mocked
    private Analyzer analyzer;
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private Auth auth;
    @Mocked
    private ConnectContext ctx;
    private FakeGlobalStateMgr fakeGlobalStateMgr;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setUp() {
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        globalStateMgr = AccessTestUtil.fetchAdminCatalog();
        MockedAuth.mockedAuth(auth);
        MockedAuth.mockedConnectContext(ctx, "root", "192.188.3.1");
        FakeGlobalStateMgr.setGlobalStateMgr(globalStateMgr);

        new Expectations() {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testDb";

                analyzer.getCatalog();
                minTimes = 0;
                result = globalStateMgr;
            }
        };
    }

    @Test
    public void testNormal() throws UserException {
        ShowFunctionsStmt stmt = new ShowFunctionsStmt(null, true, true, "%year%", null);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW FULL BUILTIN FUNCTIONS FROM `testDb` LIKE `%year%`", stmt.toString());
    }

    @Test
    public void testUnsupportFilter() throws UserException {
        SlotRef slotRef = new SlotRef(null, "Signature");
        StringLiteral stringLiteral = new StringLiteral("year(DATETIME)");
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef, stringLiteral);
        ShowFunctionsStmt stmt = new ShowFunctionsStmt(null, true, true, null, binaryPredicate);
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Only support like 'function_pattern' syntax.");
        stmt.analyze(analyzer);
    }

}
