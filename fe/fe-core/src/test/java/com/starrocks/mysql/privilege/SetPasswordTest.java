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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/mysql/privilege/SetPasswordTest.java

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

package com.starrocks.mysql.privilege;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.UserDesc;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.PrivInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.SetStmtAnalyzer;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class SetPasswordTest {

    private Auth auth;
    @Mocked
    private Analyzer analyzer;
    @Mocked
    private EditLog editLog;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Before
    public void setUp() throws NoSuchMethodException, SecurityException, AnalysisException {
        auth = new Auth();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public Auth getAuth() {
                return auth;
            }

            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
        };

        new Expectations() {
            {

                editLog.logCreateUser((PrivInfo) any);
                minTimes = 0;

                MysqlPassword.checkPassword(anyString);
                minTimes = 0;
                result = new byte[10];
            }
        };
    }

    @Test
    public void test() throws DdlException {
        UserIdentity userIdentity = new UserIdentity("cmy", "%");
        userIdentity.setIsAnalyzed();
        CreateUserStmt stmt = new CreateUserStmt(false, new UserDesc(userIdentity), Collections.emptyList());
        auth.createUser(stmt);

        ConnectContext ctx = new ConnectContext(null);
        // set password for 'cmy'@'%'
        UserIdentity currentUser1 = new UserIdentity("cmy", "%");
        currentUser1.setIsAnalyzed();
        ctx.setCurrentUserIdentity(currentUser1);
        ctx.setThreadLocalInfo();

        UserIdentity user1 = new UserIdentity("cmy", "%");
        user1.setIsAnalyzed();
        SetPassVar setPassVar = new SetPassVar(user1, null);
        try {
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setPassVar)), ctx);
        } catch (SemanticException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // set password without for
        SetPassVar setPassVar2 = new SetPassVar(null, null);
        try {
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setPassVar2)), ctx);
        } catch (SemanticException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // create user cmy2@'192.168.1.1'
        UserIdentity userIdentity2 = new UserIdentity("cmy2", "192.168.1.1");
        userIdentity2.setIsAnalyzed();
        stmt = new CreateUserStmt(false, new UserDesc(userIdentity2), Collections.emptyList());
        auth.createUser(stmt);

        UserIdentity currentUser2 = new UserIdentity("cmy2", "192.168.1.1");
        currentUser2.setIsAnalyzed();
        ctx.setCurrentUserIdentity(currentUser2);
        ctx.setThreadLocalInfo();

        // set password without for
        SetPassVar setPassVar3 = new SetPassVar(null, null);
        try {
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setPassVar3)), ctx);
        } catch (SemanticException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // set password for cmy2@'192.168.1.1'
        UserIdentity user2 = new UserIdentity("cmy2", "192.168.1.1");
        user2.setIsAnalyzed();
        SetPassVar setPassVar4 = new SetPassVar(user2, null);
        try {
            SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(setPassVar4)), ctx);
        } catch (SemanticException e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

    @Test
    public void testAuditSetPassword() {
        String sql = "SET PASSWORD = PASSWORD('testPass'), pipeline_dop = 2";
        SetStmt setStmt = (SetStmt) analyzeSuccess(sql);
        String setSql = AstToStringBuilder.toString(setStmt);
        Assert.assertTrue(setSql.contains("PASSWORD('***')"));
        Assert.assertTrue(setSql.contains("`pipeline_dop` = 2"));
        System.out.println(setSql);
    }

}
