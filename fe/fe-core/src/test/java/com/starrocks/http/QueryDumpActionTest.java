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

import com.starrocks.http.rest.QueryDumpAction;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.ExecuteEnv;
import io.netty.handler.codec.http.HttpResponseStatus;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryDumpActionTest extends StarRocksHttpTestCase {
    @Before
    @Override
    public void setUp() throws Exception {
        setUpWithCatalog();
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> GlobalStateMgr.getCurrentState().getMetadataMgr()
                        .getDb(new ConnectContext(), "default_catalog", DB_NAME) != null);
    }

    @Override
    protected void doSetUp() {
        MetricRepo.init();
        ExecuteEnv.setup();
    }

    @Test
    public void testSuccess() throws Exception {
        try (Response response = postQueryDump(DB_NAME, true, "select * from testTbl")) {
            assertThat(response.isSuccessful()).describedAs(response.toString()).isTrue();
            String body = response.body().string();
            assertThat(body)
                    .contains("\"statement\":\"SELECT *\\nFROM db_mock_000.tbl_mock_001\"")
                    .containsPattern("\"column_statistics\":\\{.+\\}")
                    .containsPattern("\"table_row_count\":\\{.+\\}");
        }

        try (Response response = postQueryDump(DB_NAME, false, "select * from testTbl")) {
            assertThat(response.isSuccessful()).isTrue();
            String body = response.body().string();
            assertThat(body)
                    .containsPattern("\"column_statistics\":\\{.+\\}")
                    .containsPattern("\"table_row_count\":\\{.+\\}");
        }

        try (Response response = postQueryDump(DB_NAME, null, "select * from testTbl")) {
            assertThat(response.isSuccessful()).isTrue();
            String body = response.body().string();
            assertThat(body)
                    .containsPattern("\"column_statistics\":\\{.+\\}")
                    .containsPattern("\"table_row_count\":\\{.+\\}");
        }

        try (Response response = postQueryDump("default_catalog." + DB_NAME, false, "select * from testTbl")) {
            assertThat(response.isSuccessful()).isTrue();
            String body = response.body().string();
            assertThat(body)
                    .containsPattern("\"column_statistics\":\\{.+\\}")
                    .containsPattern("\"table_row_count\":\\{.+\\}");
        }
    }

    @Test
    public void testInvalidQuery() throws Exception {
        try (Response response = postQueryDump(DB_NAME, false, "")) {
            String body = response.body().string();
            assertThat(response.code()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
            assertThat(body).contains("query is empty");
        }

        try (Response response = postQueryDump("default_catalog." + DB_NAME, false, "invalid-query")) {
            String body = response.body().string();
            assertThat(response.code()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
            assertThat(body).contains("execute query failed. Getting syntax error");
        }
    }

    @Test
    public void testInvalidDatabase() throws Exception {
        try (Response response = postQueryDump("default_catalog.no_db", false, "select * from testTbl")) {
            String body = response.body().string();
            assertThat(response.code()).isEqualTo(HttpResponseStatus.NOT_FOUND.code());
            assertThat(body).contains("Database [default_catalog.no_db] does not exists");
        }
        try (Response response = postQueryDump("", false, "select * from testTbl")) {
            String body = response.body().string();
            assertThat(response.isSuccessful()).isTrue();
            assertThat(body).contains("Getting analyzing error. Detail message: No database selected.");
        }
        try (Response response = postQueryDump(null, false, "select * from testTbl")) {
            String body = response.body().string();
            assertThat(response.isSuccessful()).isTrue();
            assertThat(body).contains("Getting analyzing error. Detail message: No database selected.");
        }
    }

    private Response postQueryDump(String db, Boolean enableMock, String query) throws IOException {
        RequestBody body = RequestBody.create(query, null);

        HttpUrl.Builder builder = HttpUrl.parse(BASE_URL + QueryDumpAction.URL).newBuilder();
        if (db != null) {
            builder.addQueryParameter("db", db);
        }
        if (enableMock != null) {
            builder.addQueryParameter("mock", enableMock.toString());
        }
        HttpUrl url = builder.build();

        Request request = new Request.Builder()
                .addHeader("Authorization", rootAuth)
                .url(url)
                .post(body)
                .build();

        return networkClient.newCall(request).execute();
    }

}
