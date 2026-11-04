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
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryDetail;
import com.starrocks.qe.QueryDetailQueue;
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
 * Query-detail records carry the query's profile text, so with the access check on both query-detail endpoints
 * return every record but drop the profile of queries the caller may not read.
 */
public class QueryDetailActionTest extends StarRocksHttpTestCase {

    private static final String ROOT_PROFILE = "PROFILE_OWNED_BY_ROOT";
    private static final String OTHER_PROFILE = "PROFILE_OWNED_BY_SOMEONE_ELSE";
    private static final String OTHER_EXPLAIN = "EXPLAIN_OWNED_BY_SOMEONE_ELSE";
    private static final String OTHER_QUERY_ID = "query-of-someone-else";

    @BeforeEach
    public void enableAccessCheck() {
        Config.authorization_enable_query_profile_access_check = true;
        QueryDetailQueue.TOTAL_QUERIES.clear();
        QueryDetailQueue.addQueryDetail(detail("query-of-root", "root", ROOT_PROFILE));
        QueryDetailQueue.addQueryDetail(detail(OTHER_QUERY_ID, "someone_else", OTHER_PROFILE));
    }

    @AfterEach
    public void restoreDefaults() {
        QueryDetailQueue.TOTAL_QUERIES.clear();
        Config.authorization_enable_query_profile_access_check = false;
    }

    private static QueryDetail detail(String queryId, String user, String profile) {
        QueryDetail detail = new QueryDetail();
        detail.setQueryId(queryId);
        detail.setUser(user);
        detail.setSql("select 1");
        detail.setProfile(profile);
        // ANALYZE PROFILE renders the target profile into this field on the issuing session's record, so it
        // carries the same payload and is redacted with it.
        detail.setExplain(profile == null ? null : profile.replace("PROFILE", "EXPLAIN"));
        return detail;
    }

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

    private String getAsRoot(String uri) throws IOException {
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + uri + "?event_time=0")
                .build();
        Response response = networkClient.newCall(request).execute();
        String body = response.body().string();
        Assertions.assertEquals(200, response.code(), body);
        return body;
    }

    private static void assertOtherProfileRedacted(String body) {
        Assertions.assertTrue(body.contains(ROOT_PROFILE), body);
        Assertions.assertTrue(body.contains(OTHER_QUERY_ID), body);
        Assertions.assertFalse(body.contains(OTHER_PROFILE), body);
        Assertions.assertFalse(body.contains(OTHER_EXPLAIN), body);
    }

    private static void assertBothProfilesPresent(String body) {
        Assertions.assertTrue(body.contains(ROOT_PROFILE), body);
        Assertions.assertTrue(body.contains(OTHER_PROFILE), body);
        Assertions.assertTrue(body.contains(OTHER_EXPLAIN), body);
    }

    @Test
    public void testV1RedactsOtherUsersProfileWithoutOperate() throws IOException {
        stubOperate(false);
        assertOtherProfileRedacted(getAsRoot("/api/query_detail"));
    }

    @Test
    public void testV2RedactsOtherUsersProfileWithoutOperate() throws IOException {
        stubOperate(false);
        assertOtherProfileRedacted(getAsRoot("/api/v2/query_detail"));
    }

    @Test
    public void testOperateHolderSeesEveryProfile() throws IOException {
        stubOperate(true);
        assertBothProfilesPresent(getAsRoot("/api/query_detail"));
        assertBothProfilesPresent(getAsRoot("/api/v2/query_detail"));
    }

    @Test
    public void testCheckDisabledReturnsEveryProfile() throws IOException {
        Config.authorization_enable_query_profile_access_check = false;
        stubOperate(false);
        assertBothProfilesPresent(getAsRoot("/api/query_detail"));
        assertBothProfilesPresent(getAsRoot("/api/v2/query_detail"));
    }
}
