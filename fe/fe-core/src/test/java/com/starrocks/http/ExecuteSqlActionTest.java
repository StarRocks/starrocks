// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.http;

import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;

public class ExecuteSqlActionTest extends StarRocksHttpTestCase {
    private static final String QUERY_EXECUTE_API = "/api/sql";

    @Test
    public void testExecuteSql() throws IOException {
        RequestBody body =
                RequestBody.create(JSON, "{ \"query\" :  \" desc " + DB_NAME + "." + TABLE_NAME + " \" }");

        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + QUERY_EXECUTE_API)
                .post(body)
                .build();
        Response response = networkClient.newCall(request).execute();

        String respStr = Objects.requireNonNull(response.body()).string();
        JSONObject jsonObject = new JSONObject(respStr);
        Assert.assertEquals(EXPECTED_DESC, jsonObject.get("data").toString());
    }

    @Test
    public void testExecuteSqlFail() throws IOException {
        RequestBody body =
                RequestBody.create(JSON, "{ \"sql\" :  \" desc " + DB_NAME + "." + TABLE_NAME + " \" }");

        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + QUERY_EXECUTE_API)
                .post(body)
                .build();
        Response response = networkClient.newCall(request).execute();

        String respStr = Objects.requireNonNull(response.body()).string();
        JSONObject jsonObject = new JSONObject(respStr);
        Assert.assertEquals("FAILED", jsonObject.get("status").toString());
        Assert.assertEquals("query can not be empty", jsonObject.get("msg").toString());

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
        Assert.assertEquals("FAILED", jsonObject.get("status").toString());
        Assert.assertEquals("/api/sql not support execute multiple query", jsonObject.get("msg").toString());

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
        Assert.assertEquals("FAILED", jsonObject.get("status").toString());
        Assert.assertEquals("malformed json [ { \"sql\" :  \" desc testDb.testTbl \" ]",
                jsonObject.get("msg").toString());

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
        Assert.assertEquals("FAILED", jsonObject.get("status").toString());
        Assert.assertEquals("/api/sql only support SELECT, SHOW, EXPLAIN, DESC statement",
                jsonObject.get("msg").toString());
    }
}
