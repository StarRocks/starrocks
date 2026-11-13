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

package com.starrocks.qe;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ShowAuthenticationStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * SHOW AUTHENTICATION without FOR describes the session's own user. An ephemeral user, one logged
 * in through a security integration, has no stored authentication record: the statement names the
 * integration that authenticated it instead of failing with a NullPointerException.
 */
public class ShowAuthenticationEphemeralUserTest {

    private ConnectContext ctx;

    @BeforeEach
    public void setUp() {
        ctx = new ConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());
    }

    private List<List<String>> show(String sql) throws Exception {
        ShowAuthenticationStmt stmt = (ShowAuthenticationStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        return ShowExecutor.execute(stmt, ctx).getResultRows();
    }

    @Test
    public void testEphemeralUserNamesItsSecurityIntegration() throws Exception {
        UserIdentity ephemeral = UserIdentity.createEphemeralUserIdent("eph_u", "10.0.0.1");
        ctx.setCurrentUserIdentity(ephemeral);
        ctx.setSecurityIntegration("ldap_si");

        Assertions.assertEquals(List.of(List.of(ephemeral.toString(), "No", "ldap_si", FeConstants.NULL_STRING)),
                show("SHOW AUTHENTICATION"));
    }

    @Test
    public void testStoredUserShowsItsRecord() throws Exception {
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);

        List<List<String>> rows = show("SHOW AUTHENTICATION");
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(UserIdentity.ROOT.toString(), rows.get(0).get(0));
        Assertions.assertEquals(rows, show("SHOW AUTHENTICATION FOR root"));
    }
}
