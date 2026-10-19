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
 * /api/profile applies the same rule as SHOW PROFILELIST and ANALYZE PROFILE: the user who ran the query
 * reads its profile; anyone else needs SYSTEM OPERATE. The check is off by default, so each test turns it on
 * first. The requests are made as root, and the OPERATE outcome is stubbed, so each test pins one branch of
 * {@link Authorizer#canReadQueryProfile}.
 */
public class ProfileActionTest extends StarRocksHttpTestCase {

    private static final String PROFILE_URI = "/api/profile";
    private static final String QUERY_ID = "eaff21d2-3734-11ee-909f-8e20563011de";

    @BeforeEach
    public void enableAccessCheck() {
        Config.authorization_enable_query_profile_access_check = true;
    }

    @AfterEach
    public void restoreDefaults() {
        ProfileManager.getInstance().clearProfiles();
        Config.authorization_enable_query_profile_access_check = false;
    }

    private static void pushProfileRunBy(String user) {
        RuntimeProfile profile = new RuntimeProfile("Query");
        RuntimeProfile summary = new RuntimeProfile("Summary");
        summary.addInfoString(ProfileManager.QUERY_ID, QUERY_ID);
        summary.addInfoString(ProfileManager.QUERY_TYPE, "Query");
        summary.addInfoString(ProfileManager.USER, user);
        summary.addInfoString(ProfileManager.SQL_STATEMENT, "select count(*) from lineorder");
        profile.addChild(summary);
        ProfileManager.getInstance().pushProfile(null, profile);
    }

    private static void stubOperateCheck(AccessDeniedException outcome) {
        new MockUp<Authorizer>() {
            @Mock
            public void checkSystemAction(ConnectContext context, PrivilegeType privilegeType)
                    throws AccessDeniedException {
                if (privilegeType != PrivilegeType.OPERATE) {
                    throw new AccessDeniedException("unexpected privilege consulted: " + privilegeType);
                }
                if (outcome != null) {
                    throw outcome;
                }
            }
        };
    }

    private Response getProfileAsRoot() throws IOException {
        Request request = new Request.Builder()
                .get()
                .addHeader(AUTH_KEY, rootAuth)
                .url(BASE_URL + PROFILE_URI + "?query_id=" + QUERY_ID)
                .build();
        return networkClient.newCall(request).execute();
    }

    @Test
    public void testMissingProfileIsNotFound() throws IOException {
        Response response = getProfileAsRoot();
        String body = response.body().string();
        Assertions.assertEquals(404, response.code(), body);
        Assertions.assertTrue(body.contains("not found"), body);
    }

    @Test
    public void testOwnerReadsOwnProfileWithoutOperate() throws IOException {
        pushProfileRunBy("root");
        stubOperateCheck(new AccessDeniedException("OPERATE must not be consulted for the owner"));
        Response response = getProfileAsRoot();
        String body = response.body().string();
        Assertions.assertEquals(200, response.code(), body);
        Assertions.assertTrue(body.contains("Query ID: " + QUERY_ID), body);
    }

    @Test
    public void testOtherUsersProfileDeniedWithoutOperate() throws IOException {
        pushProfileRunBy("someone_else");
        stubOperateCheck(new AccessDeniedException("Access denied; OPERATE on SYSTEM required"));
        Response response = getProfileAsRoot();
        String body = response.body().string();
        Assertions.assertEquals(401, response.code(), body);
        Assertions.assertTrue(body.contains("Access denied"), body);
    }

    @Test
    public void testCheckDisabledOpensOtherUsersProfile() throws IOException {
        pushProfileRunBy("someone_else");
        stubOperateCheck(new AccessDeniedException("OPERATE must not be consulted while the check is off"));
        Config.authorization_enable_query_profile_access_check = false;
        try {
            Response response = getProfileAsRoot();
            String body = response.body().string();
            Assertions.assertEquals(200, response.code(), body);
            Assertions.assertTrue(body.contains("Query ID: " + QUERY_ID), body);
        } finally {
            Config.authorization_enable_query_profile_access_check = true;
        }
    }

    @Test
    public void testOperateHolderReadsOtherUsersProfile() throws IOException {
        pushProfileRunBy("someone_else");
        stubOperateCheck(null);
        Response response = getProfileAsRoot();
        String body = response.body().string();
        Assertions.assertEquals(200, response.code(), body);
        Assertions.assertTrue(body.contains("Query ID: " + QUERY_ID), body);
    }
}
