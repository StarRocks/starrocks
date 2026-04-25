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

package com.starrocks.connector.delta.unity;

import com.starrocks.connector.exception.StarRocksConnectorException;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class UnityCatalogClientTest {

    private static final String BASE_URL = "https://example.cloud.databricks.com";
    private static final String TOKEN = "dapi_test_token";

    private static OkHttpClient clientWith(StubInterceptor interceptor) {
        return new OkHttpClient.Builder().addInterceptor(interceptor).build();
    }

    @Test
    public void testListSchemasSinglePage() {
        StubInterceptor stub = new StubInterceptor()
                .addResponse(200,
                        "{\"schemas\": [{\"name\": \"sales\"}, {\"name\": \"marketing\"}]}");
        UnityCatalogClient client = new UnityCatalogClient(BASE_URL, TOKEN, clientWith(stub), 0);

        List<UnityCatalogTypes.Schema> schemas = client.listSchemas("main");
        Assertions.assertEquals(2, schemas.size());
        Assertions.assertEquals("sales", schemas.get(0).name);
        Assertions.assertEquals("marketing", schemas.get(1).name);
        Assertions.assertEquals(1, stub.getRequestCount());
        // Verify auth header.
        Assertions.assertEquals("Bearer " + TOKEN,
                stub.getRequests().get(0).header("Authorization"));
    }

    @Test
    public void testListSchemasPaginates() {
        StubInterceptor stub = new StubInterceptor()
                .addResponse(200, "{\"schemas\": [{\"name\": \"a\"}], \"next_page_token\": \"tkn1\"}")
                .addResponse(200, "{\"schemas\": [{\"name\": \"b\"}]}");
        UnityCatalogClient client = new UnityCatalogClient(BASE_URL, TOKEN, clientWith(stub), 0);

        List<UnityCatalogTypes.Schema> schemas = client.listSchemas("main");
        Assertions.assertEquals(2, schemas.size());
        Assertions.assertEquals(2, stub.getRequestCount());
        // Second call must include the page_token query parameter.
        Assertions.assertTrue(stub.getRequests().get(1).url().queryParameterNames().contains("page_token"));
        Assertions.assertEquals("tkn1", stub.getRequests().get(1).url().queryParameter("page_token"));
    }

    @Test
    public void testListTablesFiltersUnknownFormatsAreStillReturned() {
        // The client itself does not filter by data_source_format (UnityMetastore does).
        StubInterceptor stub = new StubInterceptor()
                .addResponse(200,
                        "{\"tables\": [" +
                                "{\"name\": \"t_delta\", \"data_source_format\": \"DELTA\"}," +
                                "{\"name\": \"t_iceberg\", \"data_source_format\": \"ICEBERG\"}" +
                                "]}");
        UnityCatalogClient client = new UnityCatalogClient(BASE_URL, TOKEN, clientWith(stub), 0);

        List<UnityCatalogTypes.TableSummary> tables = client.listTables("main", "sales");
        Assertions.assertEquals(2, tables.size());
    }

    @Test
    public void testGetTableParsesStorageLocation() {
        StubInterceptor stub = new StubInterceptor()
                .addResponse(200,
                        "{\"name\": \"orders\", \"full_name\": \"main.sales.orders\"," +
                                "\"table_id\": \"abc-123\"," +
                                "\"data_source_format\": \"DELTA\"," +
                                "\"storage_location\": \"s3://bucket/prefix/orders\"}");
        UnityCatalogClient client = new UnityCatalogClient(BASE_URL, TOKEN, clientWith(stub), 0);

        UnityCatalogTypes.TableInfo info = client.getTable("main.sales.orders");
        Assertions.assertEquals("abc-123", info.tableId);
        Assertions.assertEquals("s3://bucket/prefix/orders", info.storageLocation);
        Assertions.assertEquals("DELTA", info.dataSourceFormat);
        // URL must contain the full_name as a path segment.
        Assertions.assertTrue(stub.getRequests().get(0).url().encodedPath()
                .endsWith("/api/2.1/unity-catalog/tables/main.sales.orders"));
    }

    @Test
    public void testTableExistsFalseOn404() {
        StubInterceptor stub = new StubInterceptor()
                .addResponse(404, "{\"error_code\": \"TABLE_DOES_NOT_EXIST\"}");
        UnityCatalogClient client = new UnityCatalogClient(BASE_URL, TOKEN, clientWith(stub), 0);

        Assertions.assertFalse(client.tableExists("main.sales.missing"));
    }

    @Test
    public void testTableExistsTrueOn200() {
        StubInterceptor stub = new StubInterceptor()
                .addResponse(200, "{\"name\": \"orders\"}");
        UnityCatalogClient client = new UnityCatalogClient(BASE_URL, TOKEN, clientWith(stub), 0);

        Assertions.assertTrue(client.tableExists("main.sales.orders"));
    }

    @Test
    public void testGetTemporaryTableCredentialsAws() {
        StubInterceptor stub = new StubInterceptor()
                .addResponse(200,
                        "{\"aws_temp_credentials\":{" +
                                "\"access_key_id\":\"AKIA\"," +
                                "\"secret_access_key\":\"s\"," +
                                "\"session_token\":\"t\"}," +
                                "\"expiration_time\": 1700000000000}");
        UnityCatalogClient client = new UnityCatalogClient(BASE_URL, TOKEN, clientWith(stub), 0);

        UnityCatalogTypes.TemporaryTableCredentials creds = client.getTemporaryTableCredentials("abc", "READ");
        Assertions.assertNotNull(creds.awsTempCredentials);
        Assertions.assertEquals("AKIA", creds.awsTempCredentials.accessKeyId);
        Assertions.assertEquals("s", creds.awsTempCredentials.secretAccessKey);
        Assertions.assertEquals("t", creds.awsTempCredentials.sessionToken);
        Assertions.assertEquals(Long.valueOf(1700000000000L), creds.expirationTime);
    }

    @Test
    public void testRetriesOn500ThenSucceeds() {
        StubInterceptor stub = new StubInterceptor()
                .addResponse(500, "{\"error\": \"boom\"}")
                .addResponse(200, "{\"schemas\": [{\"name\": \"x\"}]}");
        UnityCatalogClient client = new UnityCatalogClient(BASE_URL, TOKEN, clientWith(stub), 1);

        List<UnityCatalogTypes.Schema> schemas = client.listSchemas("main");
        Assertions.assertEquals(1, schemas.size());
        Assertions.assertEquals(2, stub.getRequestCount());
    }

    @Test
    public void testFailsAfterExhaustingRetries() {
        StubInterceptor stub = new StubInterceptor()
                .addResponse(500, "{\"error\": \"boom\"}")
                .addResponse(500, "{\"error\": \"boom\"}");
        UnityCatalogClient client = new UnityCatalogClient(BASE_URL, TOKEN, clientWith(stub), 1);

        Assertions.assertThrows(StarRocksConnectorException.class, () -> client.listSchemas("main"));
        Assertions.assertEquals(2, stub.getRequestCount());
    }

    @Test
    public void testNon500NonRetryableErrorFailsImmediately() {
        StubInterceptor stub = new StubInterceptor()
                .addResponse(401, "{\"error\": \"unauthorized\"}");
        UnityCatalogClient client = new UnityCatalogClient(BASE_URL, TOKEN, clientWith(stub), 3);

        Assertions.assertThrows(StarRocksConnectorException.class, () -> client.listSchemas("main"));
        Assertions.assertEquals(1, stub.getRequestCount(),
                "401 is not retryable; only one request should be made");
    }

    /**
     * An OkHttp interceptor that returns canned responses in order, without ever touching the
     * network. Tracks the actual requests so tests can assert on URL / headers.
     */
    private static final class StubInterceptor implements Interceptor {
        private final List<String> bodies = new ArrayList<>();
        private final List<Integer> statuses = new ArrayList<>();
        private final List<okhttp3.Request> seenRequests = new ArrayList<>();
        private final AtomicInteger index = new AtomicInteger();

        StubInterceptor addResponse(int status, String body) {
            statuses.add(status);
            bodies.add(body);
            return this;
        }

        List<okhttp3.Request> getRequests() {
            return seenRequests;
        }

        int getRequestCount() {
            return seenRequests.size();
        }

        @Override
        public Response intercept(Chain chain) {
            int i = index.getAndIncrement();
            if (i >= statuses.size()) {
                throw new AssertionError("Unexpected extra HTTP request: " + chain.request().url());
            }
            seenRequests.add(chain.request());
            return new Response.Builder()
                    .request(chain.request())
                    .protocol(Protocol.HTTP_1_1)
                    .code(statuses.get(i))
                    .message(statuses.get(i) >= 200 && statuses.get(i) < 300 ? "OK" : "ERR")
                    .body(ResponseBody.create(bodies.get(i),
                            MediaType.parse("application/json; charset=utf-8")))
                    .build();
        }
    }
}
