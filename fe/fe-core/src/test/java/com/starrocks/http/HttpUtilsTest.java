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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectProcessor.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
        StringEntity entity = new StringEntity(
                "{ \"sql\" :  \" select k1 as alias_1,k2 from " + DB_NAME + "." + TABLE_NAME + " \" }", 
                StandardCharsets.UTF_8);
        String result = HttpUtils.post(url, entity, header);
        Assertions.assertTrue(true);
        Assertions.assertNull(result);
    }
}
