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

package com.starrocks.epack.connector.lakeformation;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetUnfilteredPartitionsMetadataRequest;
import software.amazon.awssdk.services.glue.model.GetUnfilteredPartitionsMetadataResponse;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.UnfilteredPartition;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LakeFormationPartitionReaderTest {

    private static final LakeFormationTableIdentity IDENTITY =
            new LakeFormationTableIdentity("lf_catalog", "123456789012", "us-west-2", "db", "tbl");
    private static final LakeFormationQuerySession SESSION =
            new LakeFormationQuerySession("query-1", Instant.parse("2026-08-27T00:00:00Z"), "cluster-1");

    private static LakeFormationCatalogProperties properties() {
        Map<String, String> map = Maps.newHashMap();
        map.put("hive.metastore.type", "glue");
        map.put("catalog.access.control", "lakeformation");
        map.put("aws.lakeformation.session_tag_value", "starrocks");
        map.put("aws.glue.region", "us-west-2");
        map.put("aws.glue.catalog_id", "123456789012");
        return LakeFormationCatalogProperties.from("hive", map);
    }

    private static UnfilteredPartition partition(String value) {
        return UnfilteredPartition.builder()
                .partition(Partition.builder().values(value).build())
                .isRegisteredWithLakeFormation(true)
                .build();
    }

    private LakeFormationPartitionReader reader(GlueClient glueClient) {
        return new LakeFormationPartitionReader(glueClient, properties());
    }

    @Test
    public void testFollowsNextTokenToTheEnd(@Mocked GlueClient glueClient) {
        new Expectations() {
            {
                glueClient.getUnfilteredPartitionsMetadata((GetUnfilteredPartitionsMetadataRequest) any);
                returns(GetUnfilteredPartitionsMetadataResponse.builder()
                                .unfilteredPartitions(partition("p1"))
                                .nextToken("token-1")
                                .build(),
                        GetUnfilteredPartitionsMetadataResponse.builder()
                                .unfilteredPartitions(partition("p2"))
                                .nextToken("")
                                .build());
                times = 2;
            }
        };

        List<UnfilteredPartition> partitions = reader(glueClient).readAll(IDENTITY, SESSION, null);
        assertEquals(2, partitions.size());
        assertEquals(List.of("p1"), partitions.get(0).partition().values());
        assertEquals(List.of("p2"), partitions.get(1).partition().values());
    }

    @Test
    public void testSinglePageStopsAtOneCall(@Mocked GlueClient glueClient) {
        new Expectations() {
            {
                glueClient.getUnfilteredPartitionsMetadata((GetUnfilteredPartitionsMetadataRequest) any);
                result = GetUnfilteredPartitionsMetadataResponse.builder()
                        .unfilteredPartitions(partition("p1"))
                        .build();
                times = 1;
            }
        };

        assertEquals(1, reader(glueClient).readAll(IDENTITY, SESSION, null).size());
    }

    @Test
    public void testRequestCarriesPageSizeAndPredicate(@Mocked GlueClient glueClient) {
        List<GetUnfilteredPartitionsMetadataRequest> captured = new ArrayList<>();
        new Expectations() {
            {
                glueClient.getUnfilteredPartitionsMetadata(withCapture(captured));
                result = GetUnfilteredPartitionsMetadataResponse.builder().build();
            }
        };

        reader(glueClient).readAll(IDENTITY, SESSION, "dt = '2026-01-01'");

        GetUnfilteredPartitionsMetadataRequest request = captured.get(0);
        assertEquals(LakeFormationPartitionReader.MAX_RESULTS_PER_PAGE, request.maxResults());
        assertEquals("dt = '2026-01-01'", request.expression());
        assertEquals("query-1", request.querySessionContext().queryId());
    }

    /**
     * A page that fails must fail the whole read: returning what arrived so far is indistinguishable
     * from a table that genuinely has fewer partitions, and would silently narrow the scan.
     */
    @Test
    public void testAFailingPageFailsTheWholeRead(@Mocked GlueClient glueClient) {
        new Expectations() {
            {
                glueClient.getUnfilteredPartitionsMetadata((GetUnfilteredPartitionsMetadataRequest) any);
                result = GetUnfilteredPartitionsMetadataResponse.builder()
                        .unfilteredPartitions(partition("p1"))
                        .nextToken("token-1")
                        .build();
                result = EntityNotFoundException.builder().message("gone").build();
            }
        };

        LakeFormationTableAccessException e = assertThrows(LakeFormationTableAccessException.class,
                () -> reader(glueClient).readAll(IDENTITY, SESSION, null));
        assertTrue(e.getMessage().contains("lf_catalog.db.tbl"));
    }
}
