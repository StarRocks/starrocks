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

package com.starrocks.authorization.opa;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OpaHttpClientTest {
    private HttpServer server;
    private String lastRequestBody;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @AfterEach
    public void afterEach() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void testCheckPermissionAllowsAndSendsInputEnvelope() throws Exception {
        String policyUrl = startServer("/policy", 200, "{\"result\":true}");
        OpaHttpClient client = new OpaHttpClient(policyUrl, "", "", "", 1000, 1000, false, false);

        boolean allowed = client.checkPermission(OpaRequest.createCheck(context(), PrivilegeType.SELECT,
                ObjectType.TABLE, OpaResource.table(new TableName("default_catalog", "db1", "tbl1"))));

        Assertions.assertTrue(allowed);
        JsonObject input = JsonParser.parseString(lastRequestBody).getAsJsonObject().getAsJsonObject("input");
        Assertions.assertEquals("alice", input.getAsJsonObject("context").get("user").getAsString());
        JsonObject action = input.getAsJsonObject("action");
        Assertions.assertEquals("check", action.get("operation").getAsString());
        Assertions.assertEquals("SELECT", action.get("privilege").getAsString());
        Assertions.assertEquals("TABLE", action.get("objectType").getAsString());
        Assertions.assertEquals("tbl1", action.getAsJsonObject("resource").get("table").getAsString());
        client.close();
    }

    @Test
    public void testCheckPermissionFailsClosedOnDeniedMissingInvalidAndHttpErrors() throws Exception {
        assertDeniedResponse("{\"result\":false}", 200);
        assertDeniedResponse("{\"allowed\":true}", 200);
        assertDeniedResponse("not json", 200);
        assertDeniedResponse("{\"result\":true}", 500);
    }

    @Test
    public void testRowFiltersColumnMaskAndBatchColumnMasks() throws Exception {
        String policyUrl = startServer("/policy", 200, "{\"result\":true}");
        String baseUrl = policyUrl.substring(0, policyUrl.length() - "/policy".length());
        register("/rows", 200, "{\"result\":[{\"expression\":\"v1 = 1\"},\"v2 = 2\"]}");
        register("/mask", 200, "{\"result\":{\"expression\":\"NULL\"}}");
        register("/batch", 200, "{\"result\":[{\"column\":\"v1\",\"expression\":\"NULL\"},{\"index\":1,\"expression\":\"v2\"}]}");
        OpaHttpClient client = new OpaHttpClient(policyUrl, baseUrl + "/rows", baseUrl + "/mask", baseUrl + "/batch",
                1000, 1000, false, false);

        Assertions.assertEquals(List.of("v1 = 1", "v2 = 2"), client.getRowFilters(
                OpaRequest.create(context(), OpaRequest.OPERATION_GET_ROW_FILTERS, PrivilegeType.SELECT,
                        ObjectType.TABLE, OpaResource.table(new TableName("default_catalog", "db1", "tbl1")))));
        Assertions.assertEquals("NULL", client.getColumnMask(
                OpaRequest.create(context(), OpaRequest.OPERATION_GET_COLUMN_MASK, PrivilegeType.SELECT,
                        ObjectType.COLUMN, OpaResource.column(new TableName("default_catalog", "db1", "tbl1"), "v1")))
                .orElseThrow());
        Map<String, String> masks = client.getBatchColumnMasks(
                OpaRequest.createBatchColumnMasks(context(), new TableName("default_catalog", "db1", "tbl1"),
                        List.of(OpaResource.column(new TableName("default_catalog", "db1", "tbl1"), "v1"),
                                OpaResource.column(new TableName("default_catalog", "db1", "tbl1"), "v2"))),
                List.of("v1", "v2"));
        Assertions.assertEquals("NULL", masks.get("v1"));
        Assertions.assertEquals("v2", masks.get("v2"));
        Assertions.assertTrue(client.supportsBatchColumnMasks());
        client.close();
    }

    private void assertDeniedResponse(String responseBody, int status) throws Exception {
        String policyUrl = startServer("/policy", status, responseBody);
        OpaHttpClient client = new OpaHttpClient(policyUrl, "", "", "", 1000, 1000, false, false);
        Assertions.assertFalse(client.checkPermission(OpaRequest.createCheck(context(), PrivilegeType.SELECT,
                ObjectType.TABLE, OpaResource.table(new TableName("default_catalog", "db1", "tbl1")))));
        client.close();
        server.stop(0);
        server = null;
    }

    private String startServer(String path, int status, String responseBody) throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        register(path, status, responseBody);
        server.start();
        return "http://127.0.0.1:" + server.getAddress().getPort() + path;
    }

    private void register(String path, int status, String responseBody) {
        server.createContext(path, exchange -> handle(exchange, status, responseBody));
    }

    private void handle(HttpExchange exchange, int status, String responseBody) throws IOException {
        lastRequestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        byte[] response = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, response.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(response);
        }
    }

    private static ConnectContext context() {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(new UserIdentity("alice", "%"));
        context.setGroups(Set.of("finance"));
        context.setQueryId(UUIDUtil.genUUID());
        return context;
    }
}
