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
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.MockedAuth;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DropUserStmtTest {

    @Mocked
    private Auth auth;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        MockedAuth.mockedAuth(auth);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
    }

    @Test
    public void testNormal() throws Exception {
        String dropSql = "DROP USER 'user'";
        DropUserStmt stmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(dropSql, ctx);
        Assert.assertEquals("'user'@'%'", stmt.getUserIdentity().toString());
        Assert.assertEquals("user", stmt.getUserIdentity().getQualifiedUser());
    }

    @Test(expected = AnalysisException.class)
    public void testNoUser() throws Exception {
        String dropSql = "DROP USER ''";
        UtFrameUtils.parseStmtWithNewParser(dropSql, ctx);
        Assert.fail("No Exception throws.");
    }
}