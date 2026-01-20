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

package com.starrocks.connector.hive.glue.util;

import com.starrocks.connector.hive.glue.metastore.AWSGlueMetastore;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

public class HiveTableValidatorTest {

    private AWSGlueMetastore mockMetastore = mock(AWSGlueMetastore.class);

    @Test
    public void testPartitionProjectionEnabledSkipsValidation() {
        // Table with projection.enabled = true should skip metastore partition validation
        Map<String, String> parameters = new HashMap<>();
        parameters.put("projection.enabled", "true");
        parameters.put("projection.region.type", "enum");
        parameters.put("projection.region.values", "us-east,us-west");

        Table table = Table.builder()
                .name("projection_table")
                .tableType("EXTERNAL_TABLE")
                .parameters(parameters)
                .storageDescriptor(StorageDescriptor.builder()
                        .location("s3://bucket/path")
                        .build())
                .build();

        assertDoesNotThrow(() ->
                HiveTableValidator.REQUIRED_PROPERTIES_VALIDATOR.validate(table, mockMetastore));
    }

    @Test
    public void testPartitionProjectionEnableLegacySkipsValidation() {
        // Table with projection.enable = true (legacy) should also skip validation
        Map<String, String> parameters = new HashMap<>();
        parameters.put("projection.enable", "true");
        parameters.put("projection.dt.type", "date");

        Table table = Table.builder()
                .name("projection_table_legacy")
                .tableType("EXTERNAL_TABLE")
                .parameters(parameters)
                .storageDescriptor(StorageDescriptor.builder()
                        .location("s3://bucket/path")
                        .build())
                .build();

        assertDoesNotThrow(() ->
                HiveTableValidator.REQUIRED_PROPERTIES_VALIDATOR.validate(table, mockMetastore));
    }

    @Test
    public void testPartitionProjectionEnabledCaseInsensitive() {
        // projection.enabled should be case-insensitive
        Map<String, String> parameters = new HashMap<>();
        parameters.put("PROJECTION.ENABLED", "TRUE");

        Table table = Table.builder()
                .name("projection_table_upper")
                .tableType("EXTERNAL_TABLE")
                .parameters(parameters)
                .storageDescriptor(StorageDescriptor.builder()
                        .location("s3://bucket/path")
                        .build())
                .build();

        assertDoesNotThrow(() ->
                HiveTableValidator.REQUIRED_PROPERTIES_VALIDATOR.validate(table, mockMetastore));
    }

    @Test
    public void testPartitionProjectionDisabledDoesNotSkip() {
        // Table with projection.enabled = false should NOT skip validation
        Map<String, String> parameters = new HashMap<>();
        parameters.put("projection.enabled", "false");

        Table table = Table.builder()
                .name("non_projection_table")
                .tableType("EXTERNAL_TABLE")
                .parameters(parameters)
                .storageDescriptor(StorageDescriptor.builder()
                        .location("s3://bucket/path")
                        .build())
                .build();

        // Should not throw because it has storageDescriptor
        assertDoesNotThrow(() ->
                HiveTableValidator.REQUIRED_PROPERTIES_VALIDATOR.validate(table, mockMetastore));
    }

    @Test
    public void testNullParametersDoesNotThrowNPE() {
        // Table with null parameters should not cause NPE
        Table table = Table.builder()
                .name("null_params_table")
                .tableType("EXTERNAL_TABLE")
                .storageDescriptor(StorageDescriptor.builder()
                        .location("s3://bucket/path")
                        .build())
                .build();

        assertDoesNotThrow(() ->
                HiveTableValidator.REQUIRED_PROPERTIES_VALIDATOR.validate(table, mockMetastore));
    }

    @Test
    public void testEmptyParametersDoesNotThrowNPE() {
        // Table with empty parameters should not cause NPE
        Map<String, String> parameters = new HashMap<>();

        Table table = Table.builder()
                .name("empty_params_table")
                .tableType("EXTERNAL_TABLE")
                .parameters(parameters)
                .storageDescriptor(StorageDescriptor.builder()
                        .location("s3://bucket/path")
                        .build())
                .build();

        assertDoesNotThrow(() ->
                HiveTableValidator.REQUIRED_PROPERTIES_VALIDATOR.validate(table, mockMetastore));
    }
}
