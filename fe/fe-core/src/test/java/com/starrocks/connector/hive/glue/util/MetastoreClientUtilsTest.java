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

import com.starrocks.connector.hive.glue.metastore.DefaultAWSGlueMetastore;
import com.starrocks.connector.hive.glue.metastore.GlueMetastoreClientDelegate;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.ResourceShareType;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;

import java.util.Map;
import java.util.Optional;


public class MetastoreClientUtilsTest {

    @Test
    public void testGetGlueCatalogId() {
        Configuration conf = new Configuration();
        Assertions.assertNull(MetastoreClientUtils.getCatalogId(conf));
        conf.set(GlueMetastoreClientDelegate.CATALOG_ID_CONF, "123");
        Assertions.assertEquals("123", MetastoreClientUtils.getCatalogId(conf));
        conf = new Configuration();
        conf.set(CloudConfigurationConstants.AWS_GLUE_CATALOG_ID, "1234");
        Assertions.assertEquals("1234", MetastoreClientUtils.getCatalogId(conf));
    }

    @Mocked
    private DefaultAWSGlueMetastore metastore;

    @Test
    public void testGluePartitionProjectionTablesAreValid() {
        software.amazon.awssdk.services.glue.model.Table.Builder tableBuilder = 
                software.amazon.awssdk.services.glue.model.Table.builder();
        tableBuilder
                .name("test_table")
                .databaseName("test_db")
                .owner("owner")
                .tableType("EXTERNAL_TABLE")
                .storageDescriptor(StorageDescriptor.builder().build())
                .parameters(Map.of(
                        "Projection.enable", "TRUE",
                        "projection.year.type", "integer",
                        "projection.year.range", "2014,2016"));
        // Partition projection is now supported - validation should pass without throwing exception
        // even when metastore partitions are empty, because PartitionProjectionService will
        // dynamically generate partitions based on table properties
        try {
            MetastoreClientUtils.validateGlueTable(tableBuilder.build(), metastore);
        } catch (Exception e) {
            Assertions.fail("Partition projection table should be valid: " + e.getMessage());
        }
    }

    @Test
    public void testGetResourceShareTypeDefaultValue() {
        Configuration conf = new Configuration();
        // When not set, should return empty Optional (AWS defaults to local databases only)
        Optional<ResourceShareType> result = MetastoreClientUtils.getResourceShareType(conf);
        Assertions.assertFalse(result.isPresent(), "When not set, should return empty Optional");
    }

    @Test
    public void testGetResourceShareTypeValidValues() {
        Configuration conf = new Configuration();

        // Test ALL
        conf.set(CloudConfigurationConstants.AWS_GLUE_RESOURCE_SHARE_TYPE, "ALL");
        Optional<ResourceShareType> result = MetastoreClientUtils.getResourceShareType(conf);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(ResourceShareType.ALL, result.get());

        // Test FOREIGN
        conf = new Configuration();
        conf.set(CloudConfigurationConstants.AWS_GLUE_RESOURCE_SHARE_TYPE, "FOREIGN");
        result = MetastoreClientUtils.getResourceShareType(conf);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(ResourceShareType.FOREIGN, result.get());

        // Test FEDERATED
        conf = new Configuration();
        conf.set(CloudConfigurationConstants.AWS_GLUE_RESOURCE_SHARE_TYPE, "FEDERATED");
        result = MetastoreClientUtils.getResourceShareType(conf);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(ResourceShareType.FEDERATED, result.get());
    }

    @Test
    public void testGetResourceShareTypeCaseInsensitive() {
        Configuration conf = new Configuration();

        // Test lowercase
        conf.set(CloudConfigurationConstants.AWS_GLUE_RESOURCE_SHARE_TYPE, "all");
        Optional<ResourceShareType> result = MetastoreClientUtils.getResourceShareType(conf);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(ResourceShareType.ALL, result.get());

        // Test mixed case
        conf = new Configuration();
        conf.set(CloudConfigurationConstants.AWS_GLUE_RESOURCE_SHARE_TYPE, "Foreign");
        result = MetastoreClientUtils.getResourceShareType(conf);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(ResourceShareType.FOREIGN, result.get());

        conf = new Configuration();
        conf.set(CloudConfigurationConstants.AWS_GLUE_RESOURCE_SHARE_TYPE, "federated");
        result = MetastoreClientUtils.getResourceShareType(conf);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(ResourceShareType.FEDERATED, result.get());
    }

    @Test
    public void testGetResourceShareTypeInvalidValue() {
        Configuration conf = new Configuration();

        // Test invalid value - should return empty Optional (AWS defaults to local databases)
        conf.set(CloudConfigurationConstants.AWS_GLUE_RESOURCE_SHARE_TYPE, "INVALID");
        Optional<ResourceShareType> result = MetastoreClientUtils.getResourceShareType(conf);
        Assertions.assertFalse(result.isPresent(), "Invalid value should return empty Optional");

        // Test empty string - should return empty Optional
        conf = new Configuration();
        conf.set(CloudConfigurationConstants.AWS_GLUE_RESOURCE_SHARE_TYPE, "");
        result = MetastoreClientUtils.getResourceShareType(conf);
        Assertions.assertFalse(result.isPresent(), "Empty string should return empty Optional");

        // Test whitespace - should return empty Optional
        conf = new Configuration();
        conf.set(CloudConfigurationConstants.AWS_GLUE_RESOURCE_SHARE_TYPE, "   ");
        result = MetastoreClientUtils.getResourceShareType(conf);
        Assertions.assertFalse(result.isPresent(), "Whitespace should return empty Optional");
    }
}
