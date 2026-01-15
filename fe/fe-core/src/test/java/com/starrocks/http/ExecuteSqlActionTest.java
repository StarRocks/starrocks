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

import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.ExecuteEnv;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.awaitility.Awaitility;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@TestMethodOrder(MethodName.class)
public class ExecuteSqlActionTest extends StarRocksHttpTestCase {
    private static final String QUERY_EXECUTE_API = "/api/v1/catalogs/default_catalog/sql";

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        setUpWithCatalog();
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> GlobalStateMgr.getCurrentState().getMetadataMgr()
                        .getDb(new ConnectContext(), "default_catalog", DB_NAME) != null);
    }

    @Override
    protected void doSetUp() throws Exception {
        MetricRepo.init();
        ExecuteEnv.setup();
    }

    @Test
    public void test1ExecuteSqlSuccess() throws Exception {
        RequestBody body =
                RequestBody.create(JSON, "{ \"query\" :  \"kill 1\" }");

        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + QUERY_EXECUTE_API)
                .post(body)
                .build();
        Response response = networkClient.newCall(request).execute();

        String respStr = Objects.requireNonNull(response.body()).string();
        String expected = "";
        Assertions.assertEquals(respStr, expected);

        body = RequestBody.create(JSON, "{ \"query\" :  \"show catalogs\" }");
        request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + QUERY_EXECUTE_API)
                .post(body)
                .build();

        response = networkClient.newCall(request).execute();

        respStr = Objects.requireNonNull(response.body()).string();
        expected =
                "{\"meta\":[{\"name\":\"Catalog\",\"type\":\"varchar(256)\"},{\"name\":\"Type\",\"type\":\"varchar(20)\"}," +
                        "{\"name\":\"Comment\",\"type\":\"varchar(30)\"}]," +
                        "\"data\":[{\"Catalog\":\"default_catalog\",\"Type\":\"Internal\"," +
                        "\"Comment\":\"An internal catalog contains this cluster's self-managed tables.\"}]," +
                        "\"statistics\":{\"scanRows\":0,\"scanBytes\":0,\"returnRows\":1}}";
        Assertions.assertEquals(respStr, expected);

        body = RequestBody.create(JSON,
                "{ \"query\" :  \" explain select * from " + DB_NAME + "." + TABLE_NAME + ";\" }");
        request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + QUERY_EXECUTE_API)
                .post(body)
                .build();
        response = networkClient.newCall(request).execute();
        respStr = Objects.requireNonNull(response.body()).string();
        expected = "{\"explain\":\"PLAN FRAGMENT 0\\n OUTPUT EXPRS:1: k1 | 2: k2\\n " +
                " PARTITION: RANDOM\\n\\n  RESULT SINK\\n\\n  0:OlapScanNode\\n  " +
                "   TABLE: testTbl\\n     PREAGGREGATION: OFF. Reason: None aggregate function\\n   " +
                "  partitions=1/1\\n     rollup: testIndex\\n     tabletRatio=1/1\\n     " +
                "tabletList=400\\n     cardinality=1\\n     avgRowSize=2.0\\n\"}";
        Assertions.assertEquals(respStr, expected);
    }

    @Test
    public void test2ExecuteSqlFail() throws IOException {
        RequestBody body =
                RequestBody.create(JSON, "{ \"query\": \" \" }");

        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + QUERY_EXECUTE_API)
                .post(body)
                .build();
        Response response = networkClient.newCall(request).execute();

        String respStr = Objects.requireNonNull(response.body()).string();
        JSONObject jsonObject = new JSONObject(respStr);
        Assertions.assertEquals("FAILED", jsonObject.get("status").toString());
        Assertions.assertEquals("\"query can not be empty\"", jsonObject.get("msg").toString());

        body = RequestBody.create(JSON, "{ \"query\" :  \" desc " + DB_NAME + "." + TABLE_NAME + ";" +
                "select 1" + "  \" }");
        request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + QUERY_EXECUTE_API)
                .post(body)
                .build();
        response = networkClient.newCall(request).execute();
        respStr = Objects.requireNonNull(response.body()).string();
        jsonObject = new JSONObject(respStr);
        Assertions.assertEquals("FAILED", jsonObject.get("status").toString());
        Assertions.assertEquals("http query does not support execute multiple query", jsonObject.get("msg").toString());

        body = RequestBody.create(JSON, "{ \"sql\" :  \" desc " + DB_NAME + "." + TABLE_NAME + " \"");
        request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + QUERY_EXECUTE_API)
                .post(body)
                .build();
        response = networkClient.newCall(request).execute();
        respStr = Objects.requireNonNull(response.body()).string();
        jsonObject = new JSONObject(respStr);
        Assertions.assertEquals("FAILED", jsonObject.get("status").toString());
        Assertions.assertEquals("malformed json [ { \"sql\" :  \" desc testDb.testTbl \" ]",
                jsonObject.get("message").toString());

        body = RequestBody.create(JSON, "{ \"query\" :  \" drop table " + DB_NAME + "." + TABLE_NAME + " \" }");
        request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + QUERY_EXECUTE_API)
                .post(body)
                .build();
        response = networkClient.newCall(request).execute();
        respStr = Objects.requireNonNull(response.body()).string();
        jsonObject = new JSONObject(respStr);
        Assertions.assertEquals("FAILED", jsonObject.get("status").toString());
        Assertions.assertEquals("http query only support SELECT, SHOW, EXPLAIN, DESC, KILL statement",
                jsonObject.get("msg").toString());

        body = RequestBody.create(JSON, "{ \"query\" :  \" select;\" }");
        request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + QUERY_EXECUTE_API)
                .post(body)
                .build();
        response = networkClient.newCall(request).execute();
        respStr = Objects.requireNonNull(response.body()).string();
        jsonObject = new JSONObject(respStr);
        Assertions.assertEquals("FAILED", jsonObject.get("status").toString());
        Assertions.assertEquals(
                "Getting syntax error at line 1, column 7. Detail message: Unexpected input ';'," +
                        " the most similar input is {a legal identifier}.",
                jsonObject.get("msg").toString());

        body = RequestBody.create(JSON, "{ \"query\" :  \" select 1;\" }");
        request = new Request.Builder()
                .get()
                .url(BASE_URL + QUERY_EXECUTE_API)
                .post(body)
                .build();
        response = networkClient.newCall(request).execute();
        respStr = Objects.requireNonNull(response.body()).string();
        jsonObject = new JSONObject(respStr);
        Assertions.assertEquals("FAILED", jsonObject.get("status").toString());
        Assertions.assertEquals("Need auth information.",
                jsonObject.get("msg").toString());

        body = RequestBody.create(JSON, "{ \"query\" :  \" select 1;\", \"sessionVariables\":{\"timeout\":\"10\"}}");
        request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + QUERY_EXECUTE_API)
                .post(body)
                .build();
        response = networkClient.newCall(request).execute();
        respStr = Objects.requireNonNull(response.body()).string();
        jsonObject = new JSONObject(respStr);
        Assertions.assertEquals("FAILED", jsonObject.get("status").toString());
        Assertions.assertTrue(jsonObject.get("msg").toString().contains("Unknown system variable"));
    }
}
