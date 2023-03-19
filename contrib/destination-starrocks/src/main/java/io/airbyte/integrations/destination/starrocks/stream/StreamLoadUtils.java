package io.airbyte.integrations.destination.starrocks.stream;

import io.airbyte.integrations.destination.starrocks.StarRocksWriteConfig;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class StreamLoadUtils {
    private final HttpClientBuilder httpClientBuilder =
            HttpClients
                    .custom()
                    .setRedirectStrategy(new DefaultRedirectStrategy() {

                        @Override
                        protected boolean isRedirectable(String method) {
                            return true;
                        }

                    });

    public CloseableHttpClient getClient() {
        return httpClientBuilder.build();
    }


    public static String getTableUniqueKey(String database, String table) {
        return database + "-" + table;
    }

    public static String getStreamLoadUrl(String host, String database, String table) {
        if (host == null) {
            throw new IllegalArgumentException("None of the hosts in `load_url` could be connected.");
        }
        return host +
                "/api/" +
                database +
                "/" +
                table +
                "/_stream_load";
    }

    public static String label(String table) {
        return "airbyte_" + table + "_" + UUID.randomUUID() + System.currentTimeMillis();
    }


    public static String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth);
    }

    public static Header[] getHeaders(String user, String pwd) {
        Map<String, String> headers = new HashMap<>();
        headers.put("timeout", "600");
        headers.put(HttpHeaders.AUTHORIZATION, StreamLoadUtils.getBasicAuthHeader(user, pwd));
        headers.put(HttpHeaders.EXPECT, "100-continue");
        return headers.entrySet().stream()
                .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                .toArray(Header[]::new);
    }
}
