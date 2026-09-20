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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetUnfilteredTableMetadataRequest;
import software.amazon.awssdk.services.glue.model.GetUnfilteredTableMetadataResponse;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsRequest;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsResponse;
import software.amazon.awssdk.services.lakeformation.model.Permission;

import java.time.Duration;
import java.time.Instant;
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
        return AuthorizedTableMetadata.from(response);
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

    /**
     * The request is built from this gateway's catalog id and region, but errors are reported against
     * the identity. If the two disagree, a refusal would name an account or region that was never
     * consulted - so they have to agree before a single call goes out.
     *
     * Today the only producer of an identity builds it from these same properties, so neither branch can
     * fire; both stay as a structural assertion for the day a second producer appears. That is also why
     * the catalog id is compared with equals() in both directions rather than only when this gateway has
     * one: a gateway using the caller's own account paired with an identity naming an explicit account is
     * exactly the mismatch the paragraph above says must be impossible.
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
