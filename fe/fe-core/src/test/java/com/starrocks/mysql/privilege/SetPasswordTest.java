// This file is made available under Elastic License 2.0.
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
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.SetPassVar;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SetPasswordTest {

    private Auth auth;
    @Mocked
    public GlobalStateMgr globalStateMgr;
    @Mocked
    private Analyzer analyzer;
    @Mocked
    private EditLog editLog;

    @Before
    public void setUp() throws NoSuchMethodException, SecurityException, AnalysisException {
        auth = new Auth();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getAuth();
                minTimes = 0;
                result = auth;

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;

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
        CreateUserStmt stmt = new CreateUserStmt(new UserDesc(userIdentity));
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
            setPassVar.analyze();
        } catch (SemanticException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // set password without for
        SetPassVar setPassVar2 = new SetPassVar(null, null);
        try {
            setPassVar2.analyze();
        } catch (SemanticException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // create user cmy2@'192.168.1.1'
        UserIdentity userIdentity2 = new UserIdentity("cmy2", "192.168.1.1");
        userIdentity2.setIsAnalyzed();
        stmt = new CreateUserStmt(new UserDesc(userIdentity2));
        auth.createUser(stmt);

        UserIdentity currentUser2 = new UserIdentity("cmy2", "192.168.1.1");
        currentUser2.setIsAnalyzed();
        ctx.setCurrentUserIdentity(currentUser2);
        ctx.setThreadLocalInfo();

        // set password without for
        SetPassVar setPassVar3 = new SetPassVar(null, null);
        try {
            setPassVar3.analyze();
        } catch (SemanticException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // set password for cmy2@'192.168.1.1'
        UserIdentity user2 = new UserIdentity("cmy2", "192.168.1.1");
        user2.setIsAnalyzed();
        SetPassVar setPassVar4 = new SetPassVar(user2, null);
        try {
            setPassVar4.analyze();
        } catch (SemanticException e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

}
