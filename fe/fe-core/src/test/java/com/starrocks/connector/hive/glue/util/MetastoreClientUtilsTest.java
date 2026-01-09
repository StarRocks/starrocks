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
import software.amazon.awssdk.services.glue.model.StorageDescriptor;

import java.util.Map;


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
}
