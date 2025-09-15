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
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;

import java.util.ArrayList;
import java.util.Map;

import static org.mockito.Mockito.mock;

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
    public void testGluePartitionProjection() {
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
        new Expectations(metastore) {
            {
                try {
                    metastore.getPartitions("test_db", "test_table", null, 1);
                    result = new ArrayList<software.amazon.awssdk.services.glue.model.Partition>();
                    minTimes = 0;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> {
                MetastoreClientUtils.validateGlueTable(tableBuilder.build(),  metastore);
            }
        );
        Assertions.assertEquals(
                "Partition projection table may not readable",
                exception.getMessage()); 

        software.amazon.awssdk.services.glue.model.Partition partition = 
                mock(software.amazon.awssdk.services.glue.model.Partition.class);
        ArrayList<software.amazon.awssdk.services.glue.model.Partition> partitions = new ArrayList<>();
        partitions.add(partition);
        new Expectations(metastore) {
            {
                try {
                    metastore.getPartitions("test_db", "test_table2", null, 1);
                    result = partitions;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        tableBuilder.name("test_table2");
        try {
            MetastoreClientUtils.validateGlueTable(tableBuilder.build(), metastore);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }
}
