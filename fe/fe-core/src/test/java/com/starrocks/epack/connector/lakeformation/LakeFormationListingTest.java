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

import com.starrocks.connector.hive.HiveMetastoreOperations;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Enumeration asks Lake Formation's identity, not the catalog's own.
 *
 * <p>Every listing entry point - SHOW TABLES, information_schema.tables, the JDBC metadata calls - reads
 * listTableNames, and it used to answer from the machine identity configured in aws.glue.*. Lake Formation
 * authorizes a different identity, so a table registered with Lake Formation and granted to nobody was
 * still listed by name. Plain GetTables through the tagged client is filtered by Lake Formation for the
 * caller, which is why this costs one call rather than one per table.
 */
public class LakeFormationListingTest {

    private static final String CATALOG = "lf";
    private static final String AWS_CATALOG_ID = "123456789012";
    private static final String DB = "db";

    @Mocked
    private GlueClient glueClient;
    @Mocked
    private LakeFormationClient lakeFormationClient;

    private static LakeFormationCatalogProperties properties() {
        Map<String, String> raw = new HashMap<>();
        raw.put("hive.metastore.type", "glue");
        raw.put("catalog.access.control", "lakeformation");
        raw.put("aws.lakeformation.session_tag_value", "starrocks");
        raw.put("aws.glue.region", "us-west-2");
        raw.put("aws.glue.catalog_id", AWS_CATALOG_ID);
        return LakeFormationCatalogProperties.from("hive", raw);
    }

    private LakeFormationMetadataGateway gateway() {
        return new LakeFormationMetadataGateway(glueClient, lakeFormationClient, properties());
    }

    private static GetTablesResponse page(String nextToken, String... names) {
        GetTablesResponse.Builder builder = GetTablesResponse.builder()
                .tableList(List.of(names).stream().map(n -> Table.builder().name(n).build()).toList());
        return nextToken == null ? builder.build() : builder.nextToken(nextToken).build();
    }

    @Test
    public void testEveryPageIsFollowedAndTheRequestNamesTheCatalogAndDatabase() {
        List<GetTablesRequest> captured = new ArrayList<>();
        new Expectations() {
            {
                glueClient.getTables(withCapture(captured));
                returns(page("more", "t1", "t2"), page(null, "t3"));
                times = 2;
            }
        };

        assertEquals(List.of("t1", "t2", "t3"), gateway().listTableNames(DB));

        assertEquals(2, captured.size());
        for (GetTablesRequest request : captured) {
            assertEquals(AWS_CATALOG_ID, request.catalogId());
            assertEquals(DB, request.databaseName());
        }
        // The second request has to carry the first response's token, or paging silently restarts and the
        // same page is returned forever.
        assertEquals(null, captured.get(0).nextToken());
        assertEquals("more", captured.get(1).nextToken());
    }

    /**
     * A truncated list is worse than an error: the caller cannot tell it from a database that really holds
     * only those tables, and would read "the rest are not there" out of a failed request.
     */
    @Test
    public void testAFailedPageFailsTheWholeEnumeration() {
        new Expectations() {
            {
                glueClient.getTables((GetTablesRequest) any);
                // Two separate results, not returns(...): the second is a throwable, and returns() would
                // hand it back as a value instead of throwing it.
                result = page("more", "t1");
                result = new RuntimeException("throttled");
            }
        };

        LakeFormationTableAccessException failure = assertThrows(LakeFormationTableAccessException.class,
                () -> gateway().listTableNames(DB));
        assertTrue(failure.getMessage().contains(DB), failure.getMessage());
        assertTrue(failure.getMessage().contains("throttled"), failure.getMessage());
    }

    /** A token that never terminates must not page forever. */
    @Test
    public void testAnEndlessTokenIsRefused() {
        new Expectations() {
            {
                glueClient.getTables((GetTablesRequest) any);
                result = page("always-more", "t1");
                minTimes = 0;
            }
        };

        LakeFormationTableAccessException failure = assertThrows(LakeFormationTableAccessException.class,
                () -> gateway().listTableNames(DB));
        assertTrue(failure.getMessage().contains(String.valueOf(
                LakeFormationMetadataGateway.MAX_LISTING_PAGES)), failure.getMessage());
    }

    /**
     * The Hive metadata must route through the gateway rather than the metastore operations the base class
     * holds - those run as the catalog's own identity, which is the whole thing this replaces.
     */
    @Test
    public void testHiveMetadataListsThroughTheGatewayAndNotTheMetastore(
            @Mocked HiveMetastoreOperations hmsOps) {
        new Expectations() {
            {
                glueClient.getTables((GetTablesRequest) any);
                result = page(null, "governed");
                hmsOps.getAllTableNames(anyString);
                times = 0;
            }
        };

        LakeFormationHiveMetadata metadata = new LakeFormationHiveMetadata(CATALOG, null, hmsOps, null, null,
                Optional.empty(), null, null, gateway(), properties(), null, Map.of(), null, null, false);

        assertEquals(List.of("governed"), metadata.listTableNames(null, DB));
    }
}
