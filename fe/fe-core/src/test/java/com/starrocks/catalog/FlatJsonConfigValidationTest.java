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

package com.starrocks.catalog;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.PropertyAnalyzer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit test for flat JSON configuration validation logic.
 * This test focuses on the validation methods that were fixed in the bug fix.
 */
public class FlatJsonConfigValidationTest {

    @Test
    public void testTablePropertyBuildFlatJsonConfig() {
        // Test that TableProperty.buildFlatJsonConfig() works correctly with the fix
        
        // Test 1: Only flat_json.enable = false (should work)
        Map<String, String> properties1 = new HashMap<>();
        properties1.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "false");
        
        TableProperty tableProperty1 = new TableProperty(properties1);
        tableProperty1.buildFlatJsonConfig();
        
        FlatJsonConfig config1 = tableProperty1.getFlatJsonConfig();
        Assertions.assertNotNull(config1);
        Assertions.assertFalse(config1.getFlatJsonEnable());
        // Should use default values for other properties
        Assertions.assertEquals(Config.flat_json_null_factor, config1.getFlatJsonNullFactor(), 0.001);
        Assertions.assertEquals(Config.flat_json_sparsity_factory, config1.getFlatJsonSparsityFactor(), 0.001);
        Assertions.assertEquals(Config.flat_json_column_max, config1.getFlatJsonColumnMax());

        // Test 2: flat_json.enable = false with other properties (should throw exception)
        Map<String, String> properties2 = new HashMap<>();
        properties2.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "false");
        properties2.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, "0.1");
        
        TableProperty tableProperty2 = new TableProperty(properties2);
        
        Exception exception = Assertions.assertThrows(RuntimeException.class, () -> {
            tableProperty2.buildFlatJsonConfig();
        });
        
        // The exception should be our validation error, not an AnalysisException
        Assertions.assertTrue(exception.getMessage().contains("flat JSON configuration must be set after enabling flat JSON"));

        // Test 3: flat_json.enable = true with other properties (should work)
        Map<String, String> properties3 = new HashMap<>();
        properties3.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "true");
        properties3.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, "0.1");
        properties3.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR, "0.8");
        properties3.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX, "50");
        
        TableProperty tableProperty3 = new TableProperty(properties3);
        tableProperty3.buildFlatJsonConfig();
        
        FlatJsonConfig config3 = tableProperty3.getFlatJsonConfig();
        Assertions.assertNotNull(config3);
        Assertions.assertTrue(config3.getFlatJsonEnable());
        Assertions.assertEquals(0.1, config3.getFlatJsonNullFactor(), 0.001);
        Assertions.assertEquals(0.8, config3.getFlatJsonSparsityFactor(), 0.001);
        Assertions.assertEquals(50, config3.getFlatJsonColumnMax());

        // Test 4: flat_json.enable = true without other properties (should work with defaults)
        Map<String, String> properties4 = new HashMap<>();
        properties4.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "true");
        
        TableProperty tableProperty4 = new TableProperty(properties4);
        tableProperty4.buildFlatJsonConfig();
        
        FlatJsonConfig config4 = tableProperty4.getFlatJsonConfig();
        Assertions.assertNotNull(config4);
        Assertions.assertTrue(config4.getFlatJsonEnable());
        Assertions.assertEquals(Config.flat_json_null_factor, config4.getFlatJsonNullFactor(), 0.001);
        Assertions.assertEquals(Config.flat_json_sparsity_factory, config4.getFlatJsonSparsityFactor(), 0.001);
        Assertions.assertEquals(Config.flat_json_column_max, config4.getFlatJsonColumnMax());
    }

    @Test
    public void testPropertyAnalyzerMethods() throws AnalysisException {
        // Test PropertyAnalyzer methods that were affected by the fix
        
        // Test 1: analyzeFlatJsonEnabled with false
        Map<String, String> properties1 = new HashMap<>();
        properties1.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "false");
        boolean result1 = PropertyAnalyzer.analyzeFlatJsonEnabled(properties1);
        Assertions.assertFalse(result1);

        // Test 2: analyzeFlatJsonEnabled with true
        Map<String, String> properties2 = new HashMap<>();
        properties2.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "true");
        boolean result2 = PropertyAnalyzer.analyzeFlatJsonEnabled(properties2);
        Assertions.assertTrue(result2);

        // Test 3: analyzeFlatJsonEnabled with missing property (should return false)
        Map<String, String> properties3 = new HashMap<>();
        boolean result3 = PropertyAnalyzer.analyzeFlatJsonEnabled(properties3);
        Assertions.assertFalse(result3);

        // Test 4: analyzerDoubleProp with default value
        Map<String, String> properties4 = new HashMap<>();
        properties4.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "true");
        double nullFactor = PropertyAnalyzer.analyzerDoubleProp(properties4,
                PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, Config.flat_json_null_factor);
        Assertions.assertEquals(Config.flat_json_null_factor, nullFactor, 0.001);

        // Test 5: analyzeIntProp with default value
        int columnMax = PropertyAnalyzer.analyzeIntProp(properties4,
                PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX, Config.flat_json_column_max);
        Assertions.assertEquals(Config.flat_json_column_max, columnMax);
    }

    @Test
    public void testFlatJsonConfigConstructor() {
        // Test FlatJsonConfig constructor and methods
        
        // Test 1: Constructor with all parameters
        FlatJsonConfig config1 = new FlatJsonConfig(true, 0.1, 0.8, 50);
        Assertions.assertTrue(config1.getFlatJsonEnable());
        Assertions.assertEquals(0.1, config1.getFlatJsonNullFactor(), 0.001);
        Assertions.assertEquals(0.8, config1.getFlatJsonSparsityFactor(), 0.001);
        Assertions.assertEquals(50, config1.getFlatJsonColumnMax());

        // Test 2: Constructor with false enable
        FlatJsonConfig config2 = new FlatJsonConfig(false, 0.2, 0.9, 100);
        Assertions.assertFalse(config2.getFlatJsonEnable());
        Assertions.assertEquals(0.2, config2.getFlatJsonNullFactor(), 0.001);
        Assertions.assertEquals(0.9, config2.getFlatJsonSparsityFactor(), 0.001);
        Assertions.assertEquals(100, config2.getFlatJsonColumnMax());

        // Test 3: Default constructor
        FlatJsonConfig config3 = new FlatJsonConfig();
        Assertions.assertFalse(config3.getFlatJsonEnable());
        Assertions.assertEquals(Config.flat_json_null_factor, config3.getFlatJsonNullFactor(), 0.001);
        Assertions.assertEquals(Config.flat_json_sparsity_factory, config3.getFlatJsonSparsityFactor(), 0.001);
        Assertions.assertEquals(Config.flat_json_column_max, config3.getFlatJsonColumnMax());

        // Test 4: Copy constructor
        FlatJsonConfig config4 = new FlatJsonConfig(config1);
        Assertions.assertTrue(config4.getFlatJsonEnable());
        Assertions.assertEquals(0.1, config4.getFlatJsonNullFactor(), 0.001);
        Assertions.assertEquals(0.8, config4.getFlatJsonSparsityFactor(), 0.001);
        Assertions.assertEquals(50, config4.getFlatJsonColumnMax());
    }

    @Test
    public void testFlatJsonConfigBuildFromProperties() {
        // Test FlatJsonConfig.buildFromProperties method
        
        FlatJsonConfig config = new FlatJsonConfig();
        
        // Test 1: Build with flat_json.enable = false
        Map<String, String> properties1 = new HashMap<>();
        properties1.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "false");
        config.buildFromProperties(properties1);
        Assertions.assertFalse(config.getFlatJsonEnable());

        // Test 2: Build with flat_json.enable = true and other properties
        Map<String, String> properties2 = new HashMap<>();
        properties2.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "true");
        properties2.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, "0.1");
        properties2.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR, "0.8");
        properties2.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX, "50");
        config.buildFromProperties(properties2);
        
        Assertions.assertTrue(config.getFlatJsonEnable());
        Assertions.assertEquals(0.1, config.getFlatJsonNullFactor(), 0.001);
        Assertions.assertEquals(0.8, config.getFlatJsonSparsityFactor(), 0.001);
        Assertions.assertEquals(50, config.getFlatJsonColumnMax());
    }

    @Test
    public void testFlatJsonConfigSetters() {
        // Test FlatJsonConfig setter methods
        
        FlatJsonConfig config = new FlatJsonConfig();
        
        // Test setters
        config.setFlatJsonEnable(true);
        config.setFlatJsonNullFactor(0.1);
        config.setFlatJsonSparsityFactor(0.8);
        config.setFlatJsonColumnMax(50);
        
        Assertions.assertTrue(config.getFlatJsonEnable());
        Assertions.assertEquals(0.1, config.getFlatJsonNullFactor(), 0.001);
        Assertions.assertEquals(0.8, config.getFlatJsonSparsityFactor(), 0.001);
        Assertions.assertEquals(50, config.getFlatJsonColumnMax());
    }

    @Test
    public void testFlatJsonConfigToProperties() {
        // Test FlatJsonConfig.toProperties method

        // Test 1: When flat_json.enable is true, all properties should be present
        FlatJsonConfig config1 = new FlatJsonConfig(true, 0.1, 0.8, 50);
        Map<String, String> properties1 = config1.toProperties();

        Assertions.assertEquals("true", properties1.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE));
        Assertions.assertEquals("0.1", properties1.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR));
        Assertions.assertEquals("0.8", properties1.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR));
        Assertions.assertEquals("50", properties1.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX));

        // Test 2: When flat_json.enable is false, only the enable property should be present
        FlatJsonConfig config2 = new FlatJsonConfig(false, 0.1, 0.8, 50);
        Map<String, String> properties2 = config2.toProperties();

        Assertions.assertEquals("false", properties2.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE));
        Assertions.assertNull(properties2.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR));
        Assertions.assertNull(properties2.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR));
        Assertions.assertNull(properties2.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX));
    }
}
