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

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetUnfilteredPartitionsMetadataRequest;
import software.amazon.awssdk.services.glue.model.GetUnfilteredPartitionsMetadataResponse;
import software.amazon.awssdk.services.glue.model.UnfilteredPartition;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Reads every authorized partition of one table, following NextToken to the end.
 *
 * Partial results are never returned: a truncated partition list looks exactly like a table with
 * fewer partitions, so a page that fails mid-way has to fail the whole read rather than quietly
 * narrow the scan.
 */
public class LakeFormationPartitionReader {
    /** The API's own ceiling; asking for more is rejected rather than clamped. */
    static final int MAX_RESULTS_PER_PAGE = 1000;

    /**
     * A page count no legitimate table reaches: at 1000 partitions per page this allows a million
     * partitions, so hitting it means the service is handing back a token that never terminates.
     */
    static final int MAX_PAGES = 1000;

    private final GlueClient glueClient;
    private final LakeFormationCatalogProperties properties;

    public LakeFormationPartitionReader(GlueClient glueClient,
                                        LakeFormationCatalogProperties properties) {
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.properties = requireNonNull(properties, "properties is null");
    }

    /**
     * @param expression a Glue partition predicate to push down, or null for all partitions
     */
    public List<UnfilteredPartition> readAll(LakeFormationTableIdentity identity,
                                             LakeFormationQuerySession session,
                                             String expression) {
        List<UnfilteredPartition> partitions = new ArrayList<>();
        String nextToken = null;
        int pages = 0;

        do {
            // Checked before the request, not after adding its results: a token that never terminates
            // would otherwise pull a million partitions into memory before anyone objected.
            if (++pages > MAX_PAGES) {
                throw new LakeFormationTableAccessException("Lake Formation kept returning more partition "
                        + "pages for " + identity + " after " + MAX_PAGES + " requests; refusing to keep paging");
            }

            GetUnfilteredPartitionsMetadataRequest request = GetUnfilteredPartitionsMetadataRequest.builder()
                    .catalogId(properties.awsCatalogId())
                    .region(properties.region())
                    .databaseName(identity.dbName())
                    .tableName(identity.tableName())
                    .expression(expression)
                    .maxResults(MAX_RESULTS_PER_PAGE)
                    .nextToken(nextToken)
                    .supportedPermissionTypesWithStrings(
                            software.amazon.awssdk.services.glue.model.PermissionType.COLUMN_PERMISSION.toString())
                    .querySessionContext(software.amazon.awssdk.services.glue.model.QuerySessionContext.builder()
                            .queryId(session.queryId())
                            .queryStartTime(session.queryStartTime())
                            .clusterId(session.clusterId())
                            .build())
                    .build();

            GetUnfilteredPartitionsMetadataResponse response;
            try {
                response = glueClient.getUnfilteredPartitionsMetadata(request);
            } catch (RuntimeException e) {
                throw LakeFormationErrors.metadataFailure(identity, e);
            }

            partitions.addAll(response.unfilteredPartitions());
            nextToken = response.nextToken();
        } while (nextToken != null && !nextToken.isEmpty());

        return ImmutableList.copyOf(partitions);
    }
}
