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

package com.starrocks.authentication;

import com.starrocks.common.Config;
import com.starrocks.common.io.Writable;
import com.starrocks.epack.authentication.AuthenticationMgrEPack;
import com.starrocks.epack.authorization.SecurityPolicyMgr;
import com.starrocks.epack.persist.EditLogEPack;
import com.starrocks.epack.sql.ast.CreatePasswordPolicyStmt;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.SqlParser;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class PasswordPolicyTest {
    @Test
    public void testAuthentication() throws Exception {

    }

    @Test
    public void testValidatePassword() throws Exception {
        new MockUp<EditLog>() {
            @Mock
            public void logEdit(short op, Writable writable) {
                return;
            }
        };
        GlobalStateMgr.getCurrentState().setEditLog(new EditLogEPack(null));
        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };

        AuthenticationMgrEPack authenticationMgr = new AuthenticationMgrEPack();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        Config.enable_validate_password = true;
        Config.enable_password_reuse = false;
        ConnectContext context = new ConnectContext();

        CreatePasswordPolicyStmt createPolicyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY pp1 comment \"pp1 comment\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"8\",\n" +
                        "    \"PASSWORD_MIN_UPPER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_LOWER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_NUMERIC_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_SPECIAL_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MAX_AGE_DAYS\" = \"7\",\n" +
                        "    \"PASSWORD_MAX_RETRIES\" = \"3\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(createPolicyStmt, context);
        securityPolicyMgr.createPasswordPolicy(createPolicyStmt);
        securityPolicyMgr.setGlobalPasswordPolicy("pp1");

        CreateUserStmt stmt = (CreateUserStmt) SqlParser.parseSingleStatement(
                "create user u1 identified by '123456abcD!' expire_password = true",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(stmt, context);
        authenticationMgr.createUser(stmt);

        byte[] seed = "data_salt".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "password");
        context.setAuthDataSalt(seed);
        for (int i = 0; i < 3; ++i) {
            try {
                AuthenticationHandler.authenticate(context, "u1", "%", scramble);
                Assert.fail();
            } catch (AuthenticationException e) {

            }
        }

        Assert.assertTrue(authenticationMgr.checkUserLocked(new UserIdentity("u1", "%")));
    }
}
