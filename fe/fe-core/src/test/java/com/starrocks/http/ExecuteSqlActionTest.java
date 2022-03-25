// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.http;

import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;


public class ExecuteSqlActionTest extends StarRocksHttpTestCase
{
  private static final String QUERY_EXECUTE_API = "/api/sql";

  @Test
  public void testExecuteSql() throws IOException
  {
    RequestBody body =
        RequestBody.create(TEXT, "desc " + DB_NAME + "." + TABLE_NAME);

    Request request = new Request.Builder()
        .get()
        .addHeader("Authorization", rootAuth)
        .url(BASE_URL + QUERY_EXECUTE_API)
        .post(body)
        .build();
    Response response = networkClient.newCall(request).execute();

    String respStr = Objects.requireNonNull(response.body()).string();
    Assert.assertEquals(EXPECTED_DESC, respStr);
  }
}
