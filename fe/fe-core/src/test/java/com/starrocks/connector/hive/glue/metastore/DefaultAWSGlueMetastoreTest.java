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

package com.starrocks.connector.hive.glue.metastore;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForTableResponse;

public class DefaultAWSGlueMetastoreTest {
    private static final String CATALOG_ID = "123456789012";

    private HiveConf confWithCatalogId() {
        HiveConf conf = new HiveConf();
        conf.set("aws.glue.catalog_id", CATALOG_ID);
        return conf;
    }

    @Test
    public void testGetTableColumnStatisticsCarriesCatalogId() {
        GlueClient glueClient = Mockito.mock(GlueClient.class);
        Mockito.when(glueClient.getColumnStatisticsForTable(Mockito.any(GetColumnStatisticsForTableRequest.class)))
                .thenReturn(GetColumnStatisticsForTableResponse.builder().build());

        DefaultAWSGlueMetastore metastore = new DefaultAWSGlueMetastore(confWithCatalogId(), glueClient);
        metastore.getTableColumnStatistics("db", "tbl", ImmutableList.of("c1"));

        ArgumentCaptor<GetColumnStatisticsForTableRequest> captor =
                ArgumentCaptor.forClass(GetColumnStatisticsForTableRequest.class);
        Mockito.verify(glueClient).getColumnStatisticsForTable(captor.capture());
        Assertions.assertEquals(CATALOG_ID, captor.getValue().catalogId());
    }

    @Test
    public void testGetPartitionColumnStatisticsCarriesCatalogId() {
        GlueClient glueClient = Mockito.mock(GlueClient.class);
        Mockito.when(glueClient
                        .getColumnStatisticsForPartition(Mockito.any(GetColumnStatisticsForPartitionRequest.class)))
                .thenReturn(GetColumnStatisticsForPartitionResponse.builder().build());

        DefaultAWSGlueMetastore metastore = new DefaultAWSGlueMetastore(confWithCatalogId(), glueClient);
        metastore.getPartitionColumnStatistics("db", "tbl", ImmutableList.of("p=1"), ImmutableList.of("c1"));

        ArgumentCaptor<GetColumnStatisticsForPartitionRequest> captor =
                ArgumentCaptor.forClass(GetColumnStatisticsForPartitionRequest.class);
        Mockito.verify(glueClient).getColumnStatisticsForPartition(captor.capture());
        Assertions.assertEquals(CATALOG_ID, captor.getValue().catalogId());
    }

    @Test
    public void testCatalogIdIsNullWhenNotConfigured() {
        GlueClient glueClient = Mockito.mock(GlueClient.class);
        Mockito.when(glueClient.getColumnStatisticsForTable(Mockito.any(GetColumnStatisticsForTableRequest.class)))
                .thenReturn(GetColumnStatisticsForTableResponse.builder().build());

        DefaultAWSGlueMetastore metastore = new DefaultAWSGlueMetastore(new HiveConf(), glueClient);
        metastore.getTableColumnStatistics("db", "tbl", ImmutableList.of("c1"));

        ArgumentCaptor<GetColumnStatisticsForTableRequest> captor =
                ArgumentCaptor.forClass(GetColumnStatisticsForTableRequest.class);
        Mockito.verify(glueClient).getColumnStatisticsForTable(captor.capture());
        Assertions.assertNull(captor.getValue().catalogId());
    }
}
