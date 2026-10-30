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
import org.owasp.encoder.Encode;

import java.io.IOException;
import java.util.Collections;

/**
 * The /query web page lists cached profiles under the SHOW PROFILELIST rule once the access check is on: the
 * web gate admits NODE holders, who are not thereby OPERATE holders, so the owner-or-OPERATE filter must run
 * on the page itself.
 */
public class QueryActionTest extends StarRocksHttpTestCase {

    private static final String ROOT_SQL = "select 'owned_by_root'";
    private static final String OTHER_SQL = "select 'owned_by_someone_else'";
    // The page HTML-encodes cell values, so the quotes in the statements above appear as entities.
    private static final String ROOT_SQL_HTML = Encode.forHtml(ROOT_SQL);
    private static final String OTHER_SQL_HTML = Encode.forHtml(OTHER_SQL);
    private static final String XSS_PAYLOAD = "<img src=x onerror=\"alert(1)\">";
    private static final String XSS_PAYLOAD_HTML = "&lt;img src=x onerror=&#34;alert(1)&#34;&gt;";

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
        pushProfile(queryId, null, user, sql);
    }

    private static void pushProfile(String queryId, String customQueryId, String user, String sql) {
        RuntimeProfile profile = new RuntimeProfile("Query");
        RuntimeProfile summary = new RuntimeProfile("Summary");
        summary.addInfoString(ProfileManager.QUERY_ID, queryId);
        if (customQueryId != null) {
            summary.addInfoString(ProfileManager.CUSTOM_QUERY_ID, customQueryId);
        }
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
        Assertions.assertTrue(body.contains(ROOT_SQL_HTML), body);
        Assertions.assertFalse(body.contains(OTHER_SQL_HTML), body);
    }

    @Test
    public void testOperateHolderSeesEveryQuery() throws IOException {
        stubOperate(true);
        String body = getQueryPageAsRoot();
        Assertions.assertTrue(body.contains(ROOT_SQL_HTML), body);
        Assertions.assertTrue(body.contains(OTHER_SQL_HTML), body);
    }

    @Test
    public void testCheckDisabledListsEveryQuery() throws IOException {
        Config.authorization_enable_query_profile_access_check = false;
        stubOperate(false);
        String body = getQueryPageAsRoot();
        Assertions.assertTrue(body.contains(ROOT_SQL_HTML), body);
        Assertions.assertTrue(body.contains(OTHER_SQL_HTML), body);
    }

    @Test
    public void testCellValuesAreHtmlEncoded() throws IOException {
        stubOperate(true);
        String shortSql = "select '" + XSS_PAYLOAD + "'";
        String longSql = "select '" + XSS_PAYLOAD + "', '"
                + String.join("", Collections.nCopies(Config.ui_queries_sql_statement_max_length, "x")) + "'";
        pushProfile("q_xss_short", "root", shortSql);
        pushProfile("q_xss_long", XSS_PAYLOAD, "root", longSql);

        String body = getQueryPageAsRoot();
        // The page itself has an <img> logo in the navigation bar, so look for the payload rather than any <img>.
        Assertions.assertFalse(body.contains(XSS_PAYLOAD), body);
        // short statement, truncated long statement and custom query id
        Assertions.assertTrue(body.contains("<td>select &#39;" + XSS_PAYLOAD_HTML + "&#39;</td>"), body);
        Assertions.assertTrue(body.contains("\">select &#39;" + XSS_PAYLOAD_HTML), body);
        Assertions.assertTrue(body.contains("<td>" + XSS_PAYLOAD_HTML + "</td>"), body);
        // full long statement in the hover tooltip
        Assertions.assertTrue(body.contains("<span title=\"" + Encode.forHtmlAttribute(longSql) + "\">"), body);
    }
}
