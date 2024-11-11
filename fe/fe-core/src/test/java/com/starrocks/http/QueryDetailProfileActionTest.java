package com.starrocks.http;

import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.QueryDetail;
import com.starrocks.qe.QueryDetailQueue;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.ExecuteEnv;
import io.netty.handler.codec.http.HttpResponseStatus;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class QueryDetailProfileActionTest extends StarRocksHttpTestCase {
    private static final String QUERY_EXECUTE_API = "/api/query_detail_profile";

    @Override
    public void setUp() throws Exception {
        super.setUpWithCatalog();
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> GlobalStateMgr.getCurrentState().getMetadataMgr().getDb("default_catalog", DB_NAME) !=
                        null);
    }

    @Override
    protected void doSetUp() {
        MetricRepo.init();
        ExecuteEnv.setup();
    }

    @Test
    public void testProfile() throws Exception {
        QueryDetail finishedQueryDetail = new QueryDetail("219a2d5443c542d4-8fc938db37c892e3", false, 1, "127.0.0.1",
                System.currentTimeMillis(), -1, -1, QueryDetail.QueryMemState.FINISHED,
                "testDb", "select * from table1 limit 1",
                "root", "", "default_catalog");
        QueryDetailQueue.addQueryDetail(finishedQueryDetail);

        String requestJson = "{\"profileIds\":[\"219a2d5443c542d4-8fc938db37c892e3\",\"non-exist-id\"]}";
        RequestBody body = RequestBody.create(requestJson, JSON);

        // 发送请求
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + QUERY_EXECUTE_API)
                .build();

        Response response = networkClient.newCall(request).execute();

        // 验证响应
        Assert.assertTrue(response.isSuccessful());
        String respStr = response.body().string();
        Assert.assertEquals("[null,null]", respStr);

        finishedQueryDetail.setProfile("test profile content");
        response = networkClient.newCall(request).execute();
        Assert.assertTrue(response.isSuccessful());
        respStr = response.body().string();
        Assert.assertEquals("[\"test profile content\",null]", respStr);
    }

    @Test
    public void testProfileFail() throws Exception {
        // empty profileIds
        String emptyProfileIds = "{\"profileIds\":[]}";
        RequestBody emptyBody = RequestBody.create(emptyProfileIds, JSON);
        Request emptyRequest = new Request.Builder()
                .post(emptyBody)
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + QUERY_EXECUTE_API)
                .build();

        Response emptyResponse = networkClient.newCall(emptyRequest).execute();
        Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), emptyResponse.code());
        Assert.assertEquals("profileIds cannot be empty", emptyResponse.body().string());

        // invalid JSON
        String invalidJson = "{invalid json}";
        RequestBody invalidBody = RequestBody.create(invalidJson, JSON);
        Request invalidRequest = new Request.Builder()
                .post(invalidBody)
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + QUERY_EXECUTE_API)
                .build();

        Response invalidResponse = networkClient.newCall(invalidRequest).execute();
        Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), invalidResponse.code());
        Assert.assertEquals("not valid parameter", invalidResponse.body().string());
    }

}
