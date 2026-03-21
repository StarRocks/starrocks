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

package com.starrocks.connector.opensearch;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

public class OpenSearchRestClientTest {

    @Test
    public void testConstructorWithSingleHost() {
        String[] hosts = {"localhost:9200"};
        
        OpenSearchRestClient client = new OpenSearchRestClient(hosts);
        
        assertNotNull(client, "Client should be created");
    }

    @Test
    public void testConstructorWithMultipleHosts() {
        String[] hosts = {"host1:9200", "host2:9200", "host3:9200"};
        
        OpenSearchRestClient client = new OpenSearchRestClient(hosts);
        
        assertNotNull(client, "Client should be created with multiple hosts");
    }

    @Test
    public void testConstructorWithHttpPrefix() {
        String[] hosts = {"http://localhost:9200"};
        
        OpenSearchRestClient client = new OpenSearchRestClient(hosts);
        
        assertNotNull(client, "Client should handle http:// prefix");
    }

    @Test
    public void testNoAuthConstructor() {
        // Verify constructor only takes hosts (no auth parameters)
        String[] hosts = {"localhost:9200"};
        
        // This should work - no auth needed for Phase 1
        OpenSearchRestClient client = new OpenSearchRestClient(hosts);
        
        assertNotNull(client);
    }

    @Test
    public void testNoSslInClient() {
        // Verify that OpenSearchRestClient does not use SSL
        String[] hosts = {"localhost:9200"};
        OpenSearchRestClient client = new OpenSearchRestClient(hosts);
        
        // The client should be HTTP-only
        assertNotNull(client);
        
        // Check no SSL-related methods exist
        try {
            OpenSearchRestClient.class.getMethod("setSslEnabled", boolean.class);
            fail("Should not have SSL configuration method");
        } catch (NoSuchMethodException e) {
            // Expected
        }
    }
}
