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

package com.starrocks.common.util;

import com.starrocks.common.AnalysisException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class PropertyAnalyzerTest {

    @Test
    public void testAnalyzeDataCacheInfo() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");
        properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_ASYNC_WRITE_BACK, "true");

        try {
            PropertyAnalyzer.analyzeDataCacheInfo(properties);
            Assertions.assertTrue(false);
        } catch (AnalysisException e) {
            Assertions.assertEquals("enable_async_write_back is disabled since version 3.1.4", e.getMessage());
        }
    }

    @Test
    public void testAnalyzeDataCachePartitionDuration() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION, "7 day");

        try {
            PropertyAnalyzer.analyzeDataCachePartitionDuration(properties);
        } catch (AnalysisException e) {
            Assertions.assertTrue(false);
        }

        Assertions.assertTrue(properties.size() == 0);
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION, "abcd");
        try {
            PropertyAnalyzer.analyzeDataCachePartitionDuration(properties);
            Assertions.assertTrue(false);
        } catch (AnalysisException e) {
            Assertions.assertEquals("Cannot parse text to Duration", e.getMessage());
        }
    }

    @Test
    public void testAnalyzeTableQueryTimeout() throws AnalysisException {
        // Test 1: properties is null - should return -1
        Assertions.assertEquals(-1, PropertyAnalyzer.analyzeTableQueryTimeout(null));
        
        // Test 2: properties does not contain PROPERTIES_TABLE_QUERY_TIMEOUT - should return -1
        Map<String, String> properties1 = new HashMap<>();
        properties1.put("other_property", "value");
        Assertions.assertEquals(-1, PropertyAnalyzer.analyzeTableQueryTimeout(properties1));
        
        // Test 3: valid positive value
        Map<String, String> properties2 = new HashMap<>();
        properties2.put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "120");
        int result = PropertyAnalyzer.analyzeTableQueryTimeout(properties2);
        Assertions.assertEquals(120, result);
        // Property should be removed after analysis
        Assertions.assertFalse(properties2.containsKey(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT));
        
        // Test 4: value is 0 - should throw exception
        Map<String, String> properties3 = new HashMap<>();
        properties3.put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "0");
        try {
            PropertyAnalyzer.analyzeTableQueryTimeout(properties3);
            Assertions.fail("Should throw AnalysisException for timeout <= 0");
        } catch (AnalysisException e) {
            Assertions.assertTrue(e.getMessage().contains("must be greater than 0"));
        }
        
        // Test 5: value is negative - should throw exception
        Map<String, String> properties4 = new HashMap<>();
        properties4.put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "-10");
        try {
            PropertyAnalyzer.analyzeTableQueryTimeout(properties4);
            Assertions.fail("Should throw AnalysisException for timeout <= 0");
        } catch (AnalysisException e) {
            Assertions.assertTrue(e.getMessage().contains("must be greater than 0"));
        }
        
        // Test 6: value is -1 - should be treated as "unset" and return -1
        Map<String, String> propertiesUnset = new HashMap<>();
        propertiesUnset.put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "-1");
        int unset = PropertyAnalyzer.analyzeTableQueryTimeout(propertiesUnset);
        Assertions.assertEquals(-1, unset);
        Assertions.assertFalse(propertiesUnset.containsKey(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT));

        // Test 7: invalid format (not a number) - should throw NumberFormatException wrapped in AnalysisException
        Map<String, String> properties5 = new HashMap<>();
        properties5.put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "invalid_number");
        try {
            PropertyAnalyzer.analyzeTableQueryTimeout(properties5);
            Assertions.fail("Should throw AnalysisException for invalid number format");
        } catch (AnalysisException e) {
            Assertions.assertTrue(e.getMessage().contains("must be a valid integer"));
        }
        
        // Test 8: value greater than cluster timeout (should be allowed)
        Map<String, String> properties6 = new HashMap<>();
        properties6.put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "600");
        int result2 = PropertyAnalyzer.analyzeTableQueryTimeout(properties6);
        Assertions.assertEquals(600, result2);
    }

    @Test
    public void testAnalyzeDataCacheEnable() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");

        try {
            Assertions.assertTrue(PropertyAnalyzer.analyzeDataCacheEnable(properties));
        } catch (AnalysisException e) {
            Assertions.fail("Should not throw exception");
        }

        Assertions.assertEquals(0, properties.size());
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "false");
        try {
            Assertions.assertFalse(PropertyAnalyzer.analyzeDataCacheEnable(properties));
        } catch (AnalysisException e) {
            Assertions.fail("Should not throw exception");
        }

        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "abcd");
        // If the string passed in is not "true" (ignoring case),
        // analyzeDataCacheEnable will return false instead of throwing an exception
        try {
            Assertions.assertFalse(PropertyAnalyzer.analyzeDataCacheEnable(properties));
        } catch (AnalysisException e) {
            Assertions.fail("Should not throw exception");
        }
    }
}
