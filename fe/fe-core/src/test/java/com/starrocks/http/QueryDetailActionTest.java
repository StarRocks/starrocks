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

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryDetail;
import com.starrocks.qe.QueryDetailQueue;
import com.starrocks.sql.analyzer.Authorizer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.Connection;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Query-detail records carry the query's profile text, so with the access check on both query-detail endpoints
 * return every record but drop the profile of queries the caller may not read. The window holds whole statements
 * and profiles, so /api/query_detail streams it rather than building it in memory.
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

    private Request request(String uri) {
        return new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + uri + "?event_time=0")
                .build();
    }

    private String getAsRoot(String uri) throws IOException {
        return getAsRoot(networkClient, uri);
    }

    private String getAsRoot(OkHttpClient client, String uri) throws IOException {
        try (Response response = client.newCall(request(uri)).execute()) {
            String body = response.body().string();
            Assertions.assertEquals(200, response.code(), body);
            return body;
        }
    }

    // Several chunks' worth of records with multi-byte statement text, about 480 KB in all.
    private static void addLargeWindow() {
        for (int i = 0; i < 40; i++) {
            QueryDetail detail = detail("large-" + i, "root", ROOT_PROFILE);
            detail.setSql("select '" + Strings.repeat("文", 4096) + "'");
            QueryDetailQueue.addQueryDetail(detail);
        }
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

    @Test
    public void testV1StreamsTheWindowInChunks() throws IOException {
        stubOperate(true);
        addLargeWindow();
        String expected = new Gson().toJson(QueryDetailQueue.getQueryDetailsAfterTime(0));

        try (Response response = networkClient.newCall(request("/api/query_detail")).execute()) {
            Assertions.assertEquals(200, response.code());
            Assertions.assertEquals("chunked", response.header("Transfer-Encoding"));
            Assertions.assertNull(response.header("Content-Length"));
            Assertions.assertEquals(expected, response.body().string());
        }
    }

    @Test
    public void testHttp10ClientGetsABodyEndedByTheConnectionClose() throws IOException {
        stubOperate(true);
        addLargeWindow();
        String expected = new Gson().toJson(QueryDetailQueue.getQueryDetailsAfterTime(0));

        try (Socket socket = new Socket("localhost", HTTP_PORT)) {
            socket.setSoTimeout(20_000);
            OutputStream out = socket.getOutputStream();
            out.write(("GET /api/query_detail?event_time=0 HTTP/1.0\r\nAuthorization: " + rootAuth + "\r\n\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            out.flush();
            // Read to the end of the stream: the server closes the connection after the body.
            String raw = new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

            int headEnd = raw.indexOf("\r\n\r\n");
            String head = raw.substring(0, headEnd).toLowerCase();
            Assertions.assertTrue(head.startsWith("http/1.1 200"), head);
            Assertions.assertFalse(head.contains("transfer-encoding"), head);
            Assertions.assertTrue(head.contains("connection: close"), head);
            Assertions.assertEquals(expected, raw.substring(headEnd + 4));
        }
    }

    @Test
    public void testConnectionServesTheNextRequestAfterAStreamedResponse() throws IOException {
        stubOperate(true);
        addLargeWindow();
        List<Connection> connections = new ArrayList<>();
        // No silent retry on a fresh connection, so a connection left unusable fails the test.
        OkHttpClient client = networkClient.newBuilder()
                .connectionPool(new ConnectionPool())
                .retryOnConnectionFailure(false)
                .addNetworkInterceptor(chain -> {
                    connections.add(chain.connection());
                    return chain.proceed(chain.request());
                })
                .build();

        String first = getAsRoot(client, "/api/query_detail");
        String second = getAsRoot(client, "/api/query_detail");
        // A response built in full after a streamed one, on the same connection.
        String third = getAsRoot(client, "/api/v2/query_detail");

        Assertions.assertEquals(first, second);
        Assertions.assertTrue(third.contains("large-39"), third);
        Assertions.assertEquals(3, connections.size());
        Assertions.assertSame(connections.get(0), connections.get(1));
        Assertions.assertSame(connections.get(0), connections.get(2));
    }

    @Test
    public void testFailureWhileStreamingCutsTheResponse() {
        stubOperate(true);
        addLargeWindow();
        AtomicInteger chunks = new AtomicInteger();
        new MockUp<JsonArrayChunkedInput<?>>() {
            @Mock
            public ByteBuf readChunk(Invocation invocation, ByteBufAllocator allocator) {
                if (chunks.incrementAndGet() > 1) {
                    throw new IllegalStateException("injected failure while writing the body");
                }
                return invocation.proceed(allocator);
            }
        };

        // The status line is already out, so the connection is closed: the client must not get a body that
        // looks complete.
        Assertions.assertThrows(IOException.class, () -> {
            try (Response response = networkClient.newCall(request("/api/query_detail")).execute()) {
                response.body().string();
            }
        });
        Assertions.assertEquals(2, chunks.get());
    }

    @Test
    public void testErrorInTheHandlerIsAnsweredInsteadOfLeavingTheClientWaiting() throws IOException {
        new MockUp<QueryDetailQueue>() {
            @Mock
            public List<QueryDetail> getQueryDetailsAfterTime(long eventTime) {
                throw new OutOfMemoryError("Java heap space (injected)");
            }
        };
        // Well short of the client's own timeout: the request used to stay open until the client gave up.
        OkHttpClient client = networkClient.newBuilder().readTimeout(20, TimeUnit.SECONDS).build();

        try (Response response = client.newCall(request("/api/query_detail")).execute()) {
            Assertions.assertEquals(500, response.code());
            String body = response.body().string();
            Assertions.assertTrue(body.contains("OutOfMemoryError"), body);
        }
    }
}
