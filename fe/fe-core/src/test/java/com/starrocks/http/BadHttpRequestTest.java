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

import com.starrocks.common.conf.Config;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class BadHttpRequestTest extends StarRocksHttpTestCase {
    private static final String STREAM_LOAD_URI_FORMAT = "/api/%s/%s/_stream_load";

    private static String STREAM_LOAD_URL_FORMAT;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        STREAM_LOAD_URL_FORMAT = "http://localhost:" + HTTP_PORT + STREAM_LOAD_URI_FORMAT;
    }

    /**
     * netty http server default http_header_max_length=8192
     *
     * @throws IOException
     */
    @Test
    public void testStreamLoadWithBadRequest() throws IOException {
        Request request = createRequest(Config.http_max_header_size);
        Response response = networkClient.newCall(request).execute();
        Assert.assertEquals(400, response.code());
        String respStr = Objects.requireNonNull(response.body()).string();
        Assert.assertTrue(respStr.startsWith("Bad Request"));
    }

    @Test
    public void testStreamLoadWithNormalRequest() throws IOException {
        Request request = createRequest(Config.http_max_header_size / 2);
        Response response = networkClient.newCall(request).execute();
        Assert.assertEquals(200, response.code());
    }

    @NotNull
    private Request createRequest(int columnTotalLength) {
        RequestBody body = RequestBody.create(JSON, "{}");
        Request.Builder requestBuilder = new Request.Builder()
                .url(String.format(STREAM_LOAD_URL_FORMAT, DB_NAME, TABLE_NAME))
                .put(body)
                .addHeader("Authorization", rootAuth)
                .addHeader("columns", genColumnNameList(columnTotalLength))
                .addHeader("label", UUID.randomUUID().toString())
                .addHeader("Expect", "100-continue");
        return requestBuilder.build();
    }

    private static String genColumnNameList(int max) {
        List<String> columnList = new ArrayList<>();
        String delimited = ",";
        int delimitedLength = delimited.length();
        int length = 0;
        int columnIndex = 1;
        while (length < max) {
            StringBuilder sb = new StringBuilder("column_").append(columnIndex++).append("_");
            String uuid = UUID.randomUUID().toString().replace("-", "");
            do {
                sb.append(uuid);
            } while (sb.length() < 64);
            String c = sb.substring(0, 64);
            length += c.length() + delimitedLength;
            columnList.add(c.substring(0, 64));
        }
        return String.join(delimited, columnList);
    }
}
