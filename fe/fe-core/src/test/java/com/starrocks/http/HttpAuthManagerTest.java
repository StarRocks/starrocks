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

package com.starrocks.http;

import com.starrocks.http.HttpAuthManager.SessionValue;
import com.starrocks.sql.ast.UserIdentity;
import org.junit.Assert;
import org.junit.Test;

public class HttpAuthManagerTest {

    @Test
    public void testNormal() {
        HttpAuthManager authMgr = HttpAuthManager.getInstance();
        String sessionId = "test_session_id";
        String username = "test-user";
        SessionValue sessionValue = new SessionValue();
        sessionValue.currentUser = UserIdentity.createAnalyzedUserIdentWithIp(username, "%");
        authMgr.addSessionValue(sessionId, sessionValue);
        Assert.assertEquals(1, authMgr.getAuthSessions().size());
        System.out.println("username in test: " + authMgr.getSessionValue(sessionId).currentUser);
        Assert.assertEquals(username, authMgr.getSessionValue(sessionId).currentUser.getUser());

        String noExistSession = "no-exist-session-id";
        Assert.assertNull(authMgr.getSessionValue(noExistSession));
        Assert.assertEquals(1, authMgr.getAuthSessions().size());
    }
}
