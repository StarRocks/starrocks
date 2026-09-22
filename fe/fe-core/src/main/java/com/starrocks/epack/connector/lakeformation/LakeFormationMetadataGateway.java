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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GetUnfilteredTableMetadataRequest;
import software.amazon.awssdk.services.glue.model.GetUnfilteredTableMetadataResponse;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsRequest;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsResponse;
import software.amazon.awssdk.services.lakeformation.model.Permission;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * The two Lake Formation calls, exposed separately.
 *
 * Deliberately not one atomic "authorize and vend" method: callers that only need metadata - DESC,
 * information_schema, statistics discovery - must be able to skip vending entirely, and bundling the
 * two would spend a credential request on every one of them.
 */
public class LakeFormationMetadataGateway {
    private static final Logger LOG = LogManager.getLogger(LakeFormationMetadataGateway.class);

    /**
     * Phase 1 supports column-level permissions only. Declaring CELL_FILTER_PERMISSION as well would
     * invite Lake Formation to answer with row and cell filters that we cannot enforce, and an
     * unenforced filter is a data leak rather than a missing feature.
     */
    private static final List<String> SUPPORTED_PERMISSION_TYPES =
            List.of(software.amazon.awssdk.services.glue.model.PermissionType.COLUMN_PERMISSION.toString());

    /** Same shape as the partition reader's cap: a token that never terminates must not page forever. */
    static final int MAX_LISTING_PAGES = 1000;
    static final int MAX_LISTING_RESULTS_PER_PAGE = 1000;

    private final GlueClient glueClient;
    private final LakeFormationClient lakeFormationClient;
    private final LakeFormationCatalogProperties properties;

    public LakeFormationMetadataGateway(GlueClient glueClient,
                                        LakeFormationClient lakeFormationClient,
                                        LakeFormationCatalogProperties properties) {
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.lakeFormationClient = requireNonNull(lakeFormationClient, "lakeFormationClient is null");
        this.properties = requireNonNull(properties, "properties is null");
    }

    public AuthorizedTableMetadata getTableMetadata(LakeFormationTableIdentity identity,
                                                    LakeFormationQuerySession session) {
        checkIdentityMatchesThisCatalog(identity);
        GetUnfilteredTableMetadataRequest request = GetUnfilteredTableMetadataRequest.builder()
                .catalogId(properties.awsCatalogId())
                .region(properties.region())
                .databaseName(identity.dbName())
                .name(identity.tableName())
                .supportedPermissionTypesWithStrings(SUPPORTED_PERMISSION_TYPES)
                .querySessionContext(querySessionContext(session))
                .build();

        GetUnfilteredTableMetadataResponse response;
        try {
            response = glueClient.getUnfilteredTableMetadata(request);
        } catch (RuntimeException e) {
            throw LakeFormationErrors.metadataFailure(identity, e);
        }
        refuseAnswerAboutAnotherTable(identity, response);
        return AuthorizedTableMetadata.from(response);
    }

    /**
     * The tables this catalog's Lake Formation identity may see in a database.
     *
     * <p>Plain GetTables, not the unfiltered variant: Lake Formation filters the response for the caller, so
     * one call answers for the whole database with no per-table fan out. ⚠️ That filtering comes from the
     * account's Lake Formation settings, not from this code - a database left on IAM-only access control is
     * not filtered at all.
     *
     * <p>A failed page fails the whole enumeration: a truncated list reads exactly like a database that
     * really holds only those tables.
     */
    public List<String> listTableNames(String dbName) {
        requireNonNull(dbName, "dbName is null");
        List<String> names = new ArrayList<>();
        String nextToken = null;
        int pages = 0;
        do {
            if (++pages > MAX_LISTING_PAGES) {
                throw new LakeFormationTableAccessException("Glue kept returning more table pages for "
                        + describe(dbName) + " after " + MAX_LISTING_PAGES + " requests; refusing to keep paging");
            }
            GetTablesRequest request = GetTablesRequest.builder()
                    .catalogId(properties.awsCatalogId())
                    .databaseName(dbName)
                    .maxResults(MAX_LISTING_RESULTS_PER_PAGE)
                    .nextToken(nextToken)
                    .build();
            GetTablesResponse response;
            try {
                response = glueClient.getTables(request);
            } catch (RuntimeException e) {
                throw new LakeFormationTableAccessException("Cannot list the tables of " + describe(dbName)
                        + " that Lake Formation authorizes for this catalog's role. Cause: " + e.getMessage(), e);
            }
            response.tableList().forEach(table -> names.add(table.name()));
            nextToken = response.nextToken();
        } while (nextToken != null && !nextToken.isEmpty());
        return ImmutableList.copyOf(names);
    }

    /**
     * The response has to be about the table that was asked for.
     *
     * Nothing downstream would notice if it were not: the converter takes the database name from the
     * request and everything else - table name, schema, location - from the response, so a mismatched
     * answer would be published under the requested table's name with another table's columns.
     */
    private static void refuseAnswerAboutAnotherTable(LakeFormationTableIdentity identity,
                                                      GetUnfilteredTableMetadataResponse response) {
        Table table = response.table();
        if (table == null) {
            throw new LakeFormationTableAccessException(
                    "Lake Formation returned no table for " + identity);
        }
        // Database name and catalog id are compared only when both sides carry one: they are optional in
        // the model, and an absent one is not a disagreement. A null catalog id on the identity means the
        // request named no account, so there is nothing to compare the answer against.
        boolean sameTable = identity.tableName().equalsIgnoreCase(table.name());
        boolean sameDatabase = table.databaseName() == null
                || identity.dbName().equalsIgnoreCase(table.databaseName());
        boolean sameCatalog = table.catalogId() == null || identity.awsCatalogId() == null
                || identity.awsCatalogId().equals(table.catalogId());
        if (!sameTable || !sameDatabase || !sameCatalog) {
            throw new LakeFormationTableAccessException(
                    "Lake Formation answered about a different table than " + identity
                            + " was asked for. Refusing to serve that answer.");
        }
    }

    /**
     * @param queryAuthorizationId passed back exactly as the metadata call returned it - a fresh or
     *                             rebuilt id would authorize a different request than the one the
     *                             caller was told about
     */
    public LakeFormationTableAccess vendTableCredentials(LakeFormationTableIdentity identity,
                                                         String tableArn,
                                                         String queryAuthorizationId,
                                                         LakeFormationQuerySession session) {
        int requestedDuration = properties.credentialDurationSeconds();
        GetTemporaryGlueTableCredentialsRequest request = GetTemporaryGlueTableCredentialsRequest.builder()
                .tableArn(tableArn)
                .permissions(Permission.SELECT)
                .durationSeconds(requestedDuration)
                .supportedPermissionTypesWithStrings(SUPPORTED_PERMISSION_TYPES)
                .querySessionContext(credentialQuerySessionContext(session, queryAuthorizationId))
                .build();

        GetTemporaryGlueTableCredentialsResponse response;
        try {
            response = lakeFormationClient.getTemporaryGlueTableCredentials(request);
        } catch (RuntimeException e) {
            // Fail fast rather than retry with a shorter duration: an InvalidInputException is not
            // necessarily about the duration at all, so stepping down would both hide the real cause
            // and make one catalog's behavior depend on whichever table was read first.
            throw LakeFormationErrors.vendingFailure(identity, requestedDuration, e);
        }

        LakeFormationTableAccess access =
                LakeFormationTableAccess.from(identity, queryAuthorizationId, response);
        checkLease(identity, requestedDuration, access.expiresAt());
        return access;
    }

    /** Names the database the way a refusal has to name it: with the account and region actually used. */
    private String describe(String dbName) {
        String catalogId = properties.awsCatalogId();
        return dbName + " (catalogId=" + (catalogId == null ? "the caller's own AWS account" : catalogId)
                + ", region=" + properties.region() + ")";
    }

    /**
     * The request is built from this gateway's catalog id and region while errors name the identity, so a
     * disagreement would report an account or region that was never consulted.
     *
     * Neither branch can fire today - the only producer of an identity builds it from these same properties
     * - so both are a structural assertion for the day a second producer appears.
     */
    private void checkIdentityMatchesThisCatalog(LakeFormationTableIdentity identity) {
        String catalogId = properties.awsCatalogId();
        if (!Objects.equals(catalogId, identity.awsCatalogId())) {
            throw new LakeFormationTableAccessException("Refusing to query " + identity
                    + " through a Lake Formation gateway bound to AWS catalog id "
                    + (catalogId == null ? "the caller's own AWS account" : catalogId));
        }
        if (!properties.region().equalsIgnoreCase(identity.region())) {
            throw new LakeFormationTableAccessException("Refusing to query " + identity
                    + " through a Lake Formation gateway bound to region " + properties.region());
        }
    }

    /**
     * A shorter lease than asked for is the service answering honestly, not an error: take what was
     * granted and let the admission check decide whether it is enough. Retrying or stepping down here
     * would just spend more calls to arrive at the same answer.
     *
     * A lease that has already run out is different - it can never be used - so that one fails here
     * rather than being handed on to fail somewhere less obvious.
     */
    private void checkLease(LakeFormationTableIdentity identity, int requestedDurationSeconds,
                            Instant expiresAt) {
        long granted = Duration.between(Instant.now(), expiresAt).getSeconds();
        if (granted <= 0) {
            throw new LakeFormationTableAccessException("Lake Formation returned credentials for " + identity
                    + " that expired at " + expiresAt + ", before they could be used. Check for clock skew "
                    + "between this FE and AWS.");
        }
        // A minute of slack absorbs clock skew and the round trip; anything beyond that is the
        // account really capping us, which operators need to know before a long query fails.
        if (granted < requestedDurationSeconds - 60) {
            LOG.warn("Lake Formation granted a {}s lease for {} although {}={}s was requested; "
                            + "long-running queries may be refused at admission",
                    granted, identity, LakeFormationCatalogProperties.CREDENTIAL_DURATION_SECONDS,
                    requestedDurationSeconds);
        }
    }

    private software.amazon.awssdk.services.glue.model.QuerySessionContext querySessionContext(
            LakeFormationQuerySession session) {
        return software.amazon.awssdk.services.glue.model.QuerySessionContext.builder()
                .queryId(session.queryId())
                .queryStartTime(session.queryStartTime())
                .clusterId(session.clusterId())
                .build();
    }

    private software.amazon.awssdk.services.lakeformation.model.QuerySessionContext credentialQuerySessionContext(
            LakeFormationQuerySession session, String queryAuthorizationId) {
        return software.amazon.awssdk.services.lakeformation.model.QuerySessionContext.builder()
                .queryId(session.queryId())
                .queryStartTime(session.queryStartTime())
                .clusterId(session.clusterId())
                .queryAuthorizationId(queryAuthorizationId)
                .build();
    }
}
