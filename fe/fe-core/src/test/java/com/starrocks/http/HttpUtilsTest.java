package com.starrocks.http;

import org.apache.http.HttpHeaders;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class HttpUtilsTest extends StarRocksHttpTestCase {

    private static final String QUERY_PLAN_URI = "/system";

    @Test
    public void testGetHttpClient() {
        CloseableHttpClient httpClient1 = HttpUtils.getInstance();
        CloseableHttpClient httpClient2 = HttpUtils.getInstance();
        Assertions.assertEquals(httpClient1, httpClient2);
    }

    @Test
    public void testHttpGet() {
        Map<String, String> header = Map.of(HttpHeaders.AUTHORIZATION, rootAuth);
        String url = "http://localhost:" + HTTP_PORT + QUERY_PLAN_URI + "?path=/backends";
        String result = HttpUtils.get(url, header);
        Assertions.assertTrue(true);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testHttpPost() {
        Map<String, String> header = Map.of(HttpHeaders.AUTHORIZATION, rootAuth);
        String url = URI + "/_query_plan";
        StringEntity entity = new StringEntity("{ \"sql\" :  \" select k1 as alias_1,k2 from " + DB_NAME + "." + TABLE_NAME + " \" }", StandardCharsets.UTF_8);
        String result = HttpUtils.post(url, entity, header);
        Assertions.assertTrue(true);
        Assertions.assertNull(result);
    }

}
