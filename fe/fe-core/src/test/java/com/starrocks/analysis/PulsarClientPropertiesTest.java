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

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.RoutineLoadDataSourceProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PulsarClientPropertiesTest {
    private void assertAccepted(Map<String, String> properties) throws AnalysisException {
        Map<String, String> expected = new HashMap<>();
        properties.forEach((key, value) -> expected.put(key.substring("property.".length()), value));
        Map<String, String> createProperties = new HashMap<>();
        CreateRoutineLoadStmt.analyzePulsarCustomProperties(properties, createProperties);
        Assertions.assertEquals(expected, createProperties);

        RoutineLoadDataSourceProperties alterProperties = new RoutineLoadDataSourceProperties("PULSAR", properties);
        alterProperties.analyze();
        Assertions.assertEquals(expected, alterProperties.getCustomPulsarProperties());
    }

    @Test
    public void testCreateAndAlterTimeouts() throws AnalysisException {
        assertAccepted(Map.of("property.connection_timeout_ms", "25000",
                "property.operation_timeout_seconds", "60", "property.auth.token", "test-token"));
        for (String key : List.of("property.connection_timeout_ms", "property.operation_timeout_seconds")) {
            int maximum = key.equals("property.connection_timeout_ms") ? Integer.MAX_VALUE : Integer.MAX_VALUE / 1000;
            assertAccepted(Map.of(key, "1"));
            assertAccepted(Map.of(key, Integer.toString(maximum)));
        }
    }

    @Test
    public void testOmittedTimeoutsDoNotAddOverrides() throws AnalysisException {
        assertAccepted(Map.of("property.auth.token", "test-token"));
        Map<String, String> properties = new HashMap<>();
        CreateRoutineLoadStmt.analyzePulsarCustomProperties(Map.of(), properties);
        Assertions.assertTrue(properties.isEmpty());
    }

    @Test
    public void testInvalidTimeoutsRejectedForCreateAndAlter() {
        for (String key : List.of("property.connection_timeout_ms", "property.operation_timeout_seconds")) {
            for (String value : List.of("", "0", "-1", "+1", " 1", "1 ", "1ms", "1.5", "2147483648")) {
                assertRejected(key, value);
            }
        }
        assertRejected("property.operation_timeout_seconds", "2147484");
        assertRejected("property.connection_timeout_ms", null);
    }

    private void assertRejected(String key, String value) {
        Map<String, String> properties = new HashMap<>();
        properties.put(key, value);
        Assertions.assertThrows(AnalysisException.class,
                () -> CreateRoutineLoadStmt.analyzePulsarCustomProperties(properties, new HashMap<>()), key + "=" + value);
        RoutineLoadDataSourceProperties alterProperties = new RoutineLoadDataSourceProperties("PULSAR", properties);
        Assertions.assertThrows(AnalysisException.class, alterProperties::analyze, key + "=" + value);
    }
}
