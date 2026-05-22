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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class OpenSearchConfigTest {

    @Test
    public void testDefaultValues() {
        OpenSearchConfig config = new OpenSearchConfig();
        
        assertTrue(config.isEnableWanOnly(), "wan_only should default to true");
        assertTrue(config.isEnableDocValueScan(), "doc_value_scan should default to true");
        assertTrue(config.isEnableKeywordSniff(), "keyword_sniff should default to true");
    }

    @Test
    public void testHostsConfiguration() {
        OpenSearchConfig config = new OpenSearchConfig();
        String[] hosts = {"localhost:9200", "localhost:9201"};
        
        config.setNodes(hosts);
        
        assertArrayEquals(hosts, config.getNodes(), "Hosts should be set correctly");
    }

    @Test
    public void testWanOnlyConfiguration() {
        OpenSearchConfig config = new OpenSearchConfig();
        
        config.setEnableWanOnly(false);
        
        assertFalse(config.isEnableWanOnly(), "wan_only should be configurable");
    }

    @Test
    public void testDocValueScanConfiguration() {
        OpenSearchConfig config = new OpenSearchConfig();
        
        config.setEnableDocValueScan(false);
        
        assertFalse(config.isEnableDocValueScan(), "doc_value_scan should be configurable");
    }

    @Test
    public void testKeywordSniffConfiguration() {
        OpenSearchConfig config = new OpenSearchConfig();
        
        config.setEnableKeywordSniff(false);
        
        assertFalse(config.isEnableKeywordSniff(), "keyword_sniff should be configurable");
    }

    @Test
    public void testNoSslConfiguration() {
        // Verify that OpenSearchConfig does not have SSL-related methods
        // This is a design test - SSL should not be present in Phase 1
        OpenSearchConfig config = new OpenSearchConfig();
        
        // Config should have no SSL-related state
        assertNotNull(config.getNodes(), "Config should be initialized");
        
        // Verify no SSL methods exist through reflection check
        try {
            OpenSearchConfig.class.getMethod("isEnableSsl");
            fail("OpenSearchConfig should not have SSL configuration in Phase 1");
        } catch (NoSuchMethodException e) {
            // Expected - SSL method should not exist
        }
    }

    @Test
    public void testNoAuthConfiguration() {
        // Verify that OpenSearchConfig does not have auth-related methods
        OpenSearchConfig config = new OpenSearchConfig();
        
        // Verify no username/password methods exist
        try {
            OpenSearchConfig.class.getMethod("getUserName");
            fail("OpenSearchConfig should not have username auth in Phase 1");
        } catch (NoSuchMethodException e) {
            // Expected - auth method should not exist
        }
        
        try {
            OpenSearchConfig.class.getMethod("getPassword");
            fail("OpenSearchConfig should not have password auth in Phase 1");
        } catch (NoSuchMethodException e) {
            // Expected - auth method should not exist
        }
    }
}
