// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/DropDbStmtTest.java

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

import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.MockedAuth;
import com.starrocks.qe.ConnectContext;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DropDbStmtTest {
    Analyzer analyzer;

    @Mocked
    private Auth auth;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        MockedAuth.mockedAuth(auth);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
    }

    @Test
    public void testNormal() throws UserException, AnalysisException {
        DropDbStmt stmt = new DropDbStmt(false, "test", true);

        stmt.analyze(analyzer);
        Assert.assertEquals("default_cluster:test", stmt.getDbName());
        Assert.assertEquals("DROP DATABASE `default_cluster:test`", stmt.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testFailed() throws UserException, AnalysisException {
        DropDbStmt stmt = new DropDbStmt(false, "", true);

        stmt.analyze(analyzer);
        Assert.fail("no exception");
    }

    @Test(expected = AnalysisException.class)
    public void testNoPriv() throws UserException, AnalysisException {
        DropDbStmt stmt = new DropDbStmt(false, "", true);

        stmt.analyze(AccessTestUtil.fetchBlockAnalyzer());
        Assert.fail("no exception");
    }
}