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

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.common.Config;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * The /query web page lists cached profiles under the SHOW PROFILELIST rule once the access check is on: the
 * web gate admits NODE holders, who are not thereby OPERATE holders, so the owner-or-OPERATE filter must run
 * on the page itself.
 */
public class QueryActionTest extends StarRocksHttpTestCase {

    private static final String ROOT_SQL = "select 'owned_by_root'";
    private static final String OTHER_SQL = "select 'owned_by_someone_else'";

    @BeforeEach
    public void enableAccessCheck() {
        Config.authorization_enable_query_profile_access_check = true;
        pushProfile("q_root_web", "root", ROOT_SQL);
        pushProfile("q_other_web", "someone_else", OTHER_SQL);
    }

    @AfterEach
    public void restoreDefaults() {
        ProfileManager.getInstance().clearProfiles();
        Config.authorization_enable_query_profile_access_check = false;
    }

    private static void pushProfile(String queryId, String user, String sql) {
        RuntimeProfile profile = new RuntimeProfile("Query");
        RuntimeProfile summary = new RuntimeProfile("Summary");
        summary.addInfoString(ProfileManager.QUERY_ID, queryId);
        summary.addInfoString(ProfileManager.QUERY_TYPE, "Query");
        summary.addInfoString(ProfileManager.USER, user);
        summary.addInfoString(ProfileManager.SQL_STATEMENT, sql);
        profile.addChild(summary);
        ProfileManager.getInstance().pushProfile(null, profile);
    }

    // The web gate asks for NODE, which the stub always grants; only the profile rule asks for OPERATE.
    private static void stubOperate(boolean granted) {
        new MockUp<Authorizer>() {
            @Mock
            public void checkSystemAction(ConnectContext context, PrivilegeType privilegeType)
                    throws AccessDeniedException {
                if (privilegeType == PrivilegeType.OPERATE && !granted) {
                    throw new AccessDeniedException("Access denied; OPERATE on SYSTEM required");
                }
            }
        };
    }

    private String getQueryPageAsRoot() throws IOException {
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/query")
                .build();
        Response response = networkClient.newCall(request).execute();
        String body = response.body().string();
        Assertions.assertEquals(200, response.code(), body);
        return body;
    }

    @Test
    public void testWithoutOperateOnlyOwnQueriesAreListed() throws IOException {
        stubOperate(false);
        String body = getQueryPageAsRoot();
        Assertions.assertTrue(body.contains(ROOT_SQL), body);
        Assertions.assertFalse(body.contains(OTHER_SQL), body);
    }

    @Test
    public void testOperateHolderSeesEveryQuery() throws IOException {
        stubOperate(true);
        String body = getQueryPageAsRoot();
        Assertions.assertTrue(body.contains(ROOT_SQL), body);
        Assertions.assertTrue(body.contains(OTHER_SQL), body);
    }

    @Test
    public void testCheckDisabledListsEveryQuery() throws IOException {
        Config.authorization_enable_query_profile_access_check = false;
        stubOperate(false);
        String body = getQueryPageAsRoot();
        Assertions.assertTrue(body.contains(ROOT_SQL), body);
        Assertions.assertTrue(body.contains(OTHER_SQL), body);
    }
}
