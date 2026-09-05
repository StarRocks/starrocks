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

package com.starrocks.connector.elasticsearch;

import com.starrocks.connector.exception.StarRocksConnectorException;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EsRestClientTest {

    @Test
    public void testGetRowCount() {
        new MockUp<EsRestClient>() {
            @Mock
            String execute(String path) {
                return "[{\"docs.count\":\"1234567\"}]";
            }
        };
        EsRestClient client = new EsRestClient(new String[] {"http://localhost:9200"}, "", "");
        Assertions.assertEquals(1234567L, client.getRowCount("my_index"));
    }

    @Test
    public void testGetRowCountNullResponse() {
        new MockUp<EsRestClient>() {
            @Mock
            String execute(String path) {
                return null;
            }
        };
        EsRestClient client = new EsRestClient(new String[] {"http://localhost:9200"}, "", "");
        Assertions.assertEquals(-1L, client.getRowCount("my_index"));
    }

    @Test
    public void testGetRowCountOnException() {
        new MockUp<EsRestClient>() {
            @Mock
            String execute(String path) {
                throw new StarRocksConnectorException("connection failed");
            }
        };
        EsRestClient client = new EsRestClient(new String[] {"http://localhost:9200"}, "", "");
        Assertions.assertEquals(-1L, client.getRowCount("my_index"));
    }

    @Test
    public void testGetRowCountEmptyArray() {
        new MockUp<EsRestClient>() {
            @Mock
            String execute(String path) {
                return "[]";
            }
        };
        EsRestClient client = new EsRestClient(new String[] {"http://localhost:9200"}, "", "");
        Assertions.assertEquals(-1L, client.getRowCount("my_index"));
    }

    @Test
    public void testGetRowCountMultipleIndices() {
        new MockUp<EsRestClient>() {
            @Mock
            String execute(String path) {
                return "[{\"docs.count\":\"1000000\"},{\"docs.count\":\"2000000\"},{\"docs.count\":\"500000\"}]";
            }
        };
        EsRestClient client = new EsRestClient(new String[] {"http://localhost:9200"}, "", "");
        Assertions.assertEquals(3500000L, client.getRowCount("logs-*"));
    }
}
