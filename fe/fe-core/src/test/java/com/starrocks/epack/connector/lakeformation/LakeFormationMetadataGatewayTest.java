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
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetUnfilteredTableMetadataRequest;
import software.amazon.awssdk.services.glue.model.GetUnfilteredTableMetadataResponse;
import software.amazon.awssdk.services.glue.model.PermissionType;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsRequest;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsResponse;
import software.amazon.awssdk.services.lakeformation.model.InvalidInputException;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LakeFormationMetadataGatewayTest {

    private static final LakeFormationTableIdentity IDENTITY =
            new LakeFormationTableIdentity("lf_catalog", "123456789012", "us-west-2", "db", "tbl");
    private static final LakeFormationQuerySession SESSION =
            new LakeFormationQuerySession("query-1", Instant.parse("2026-08-27T00:00:00Z"), "cluster-1");
    private static final String TABLE_ARN = "arn:aws:glue:us-west-2:123456789012:table/db/tbl";

    private static LakeFormationCatalogProperties properties() {
        Map<String, String> map = Maps.newHashMap();
        map.put("hive.metastore.type", "glue");
        map.put("catalog.access.control", "lakeformation");
        map.put("aws.lakeformation.session_tag_value", "starrocks");
        map.put("aws.glue.region", "us-west-2");
        map.put("aws.glue.catalog_id", "123456789012");
        return LakeFormationCatalogProperties.from("hive", map);
    }

    private static GetUnfilteredTableMetadataResponse metadataResponse() {
        return GetUnfilteredTableMetadataResponse.builder()
                .table(Table.builder().name("tbl")
                        .storageDescriptor(StorageDescriptor.builder().location("s3://bucket/tbl").build())
                        .build())
                .isRegisteredWithLakeFormation(true)
                .authorizedColumns("c1", "c2")
                .queryAuthorizationId("auth-1")
                .build();
    }

    @Test
    public void testMetadataRequestDeclaresColumnPermissionsOnly(@Mocked GlueClient glueClient,
                                                                 @Mocked LakeFormationClient lfClient) {
        new Expectations() {
            {
                glueClient.getUnfilteredTableMetadata((GetUnfilteredTableMetadataRequest) any);
                result = metadataResponse();
                times = 1;
            }
        };

        AuthorizedTableMetadata metadata =
                new LakeFormationMetadataGateway(glueClient, lfClient, properties())
                        .getTableMetadata(IDENTITY, SESSION);

        assertTrue(metadata.isRegistered(IDENTITY));
        assertEquals(List.of("c1", "c2"), metadata.authorizedColumns());
        assertEquals("auth-1", metadata.queryAuthorizationId());
        assertFalse(metadata.hasFilters());
        assertEquals("s3://bucket/tbl", metadata.table().storageDescriptor().location());
    }

    /**
     * Declaring CELL_FILTER_PERMISSION would invite filters phase 1 cannot enforce, and an
     * unenforced filter is a leak rather than a missing feature.
     */
    @Test
    public void testOnlyColumnPermissionTypeIsDeclared(@Mocked GlueClient glueClient,
                                                       @Mocked LakeFormationClient lfClient) {
        List<GetUnfilteredTableMetadataRequest> captured = new ArrayList<>();
        new Expectations() {
            {
                glueClient.getUnfilteredTableMetadata(withCapture(captured));
                result = metadataResponse();
            }
        };

        new LakeFormationMetadataGateway(glueClient, lfClient, properties())
                .getTableMetadata(IDENTITY, SESSION);

        GetUnfilteredTableMetadataRequest request = captured.get(0);
        assertEquals(List.of(PermissionType.COLUMN_PERMISSION), request.supportedPermissionTypes());
        assertEquals("query-1", request.querySessionContext().queryId());
        assertEquals("db", request.databaseName());
        assertEquals("tbl", request.name());
    }

    @Test
    public void testMissingRegisteredFlagIsAnError(@Mocked GlueClient glueClient,
                                                   @Mocked LakeFormationClient lfClient) {
        new Expectations() {
            {
                glueClient.getUnfilteredTableMetadata((GetUnfilteredTableMetadataRequest) any);
                result = GetUnfilteredTableMetadataResponse.builder()
                        .table(Table.builder().name("tbl").build())
                        .queryAuthorizationId("auth-1")
                        .build();
            }
        };

        AuthorizedTableMetadata metadata =
                new LakeFormationMetadataGateway(glueClient, lfClient, properties())
                        .getTableMetadata(IDENTITY, SESSION);

        LakeFormationTableAccessException e =
                assertThrows(LakeFormationTableAccessException.class, () -> metadata.isRegistered(IDENTITY));
        assertTrue(e.getMessage().contains("refusing to guess"));
    }

    @Test
    public void testVendPassesTheAuthorizationIdBackVerbatim(@Mocked GlueClient glueClient,
                                                             @Mocked LakeFormationClient lfClient) {
        List<GetTemporaryGlueTableCredentialsRequest> captured = new ArrayList<>();
        new Expectations() {
            {
                lfClient.getTemporaryGlueTableCredentials(withCapture(captured));
                result = GetTemporaryGlueTableCredentialsResponse.builder()
                        .accessKeyId("AK").secretAccessKey("SK").sessionToken("ST")
                        .expiration(Instant.now().plus(1, ChronoUnit.HOURS))
                        .build();
            }
        };

        LakeFormationTableAccess access =
                new LakeFormationMetadataGateway(glueClient, lfClient, properties())
                        .vendTableCredentials(IDENTITY, TABLE_ARN, "auth-1", SESSION);

        GetTemporaryGlueTableCredentialsRequest request = captured.get(0);
        assertEquals("auth-1", request.querySessionContext().queryAuthorizationId());
        assertEquals(TABLE_ARN, request.tableArn());
        assertEquals(3600, request.durationSeconds());
        assertEquals("auth-1", access.queryAuthorizationId());
        assertEquals("AK", access.accessKeyId());
    }

    /**
     * A rejected duration must fail the query with the configured value named, not silently retry
     * with a shorter one: stepping down hides the account's real limit and makes a catalog behave
     * differently depending on which table was read first.
     */
    @Test
    public void testRejectedDurationFailsFastWithDiagnostics(@Mocked GlueClient glueClient,
                                                             @Mocked LakeFormationClient lfClient) {
        new Expectations() {
            {
                lfClient.getTemporaryGlueTableCredentials((GetTemporaryGlueTableCredentialsRequest) any);
                result = InvalidInputException.builder()
                        .message("duration too long")
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode("InvalidInputException")
                                .errorMessage("duration too long")
                                .build())
                        .requestId("req-42")
                        .build();
                times = 1;
            }
        };

        LakeFormationTableAccessException e = assertThrows(LakeFormationTableAccessException.class, () ->
                new LakeFormationMetadataGateway(glueClient, lfClient, properties())
                        .vendTableCredentials(IDENTITY, TABLE_ARN, "auth-1", SESSION));

        assertTrue(e.getMessage().contains("credential_duration_seconds=3600"));
        assertTrue(e.getMessage().contains("ALTER CATALOG"));
        assertTrue(e.getMessage().contains("req-42"));
        assertTrue(e.getMessage().contains("never retries"));
    }

    /**
     * The request is built from the gateway's catalog id and region while errors name the identity's,
     * so a mismatch would report an account or region that was never consulted.
     */
    @Test
    public void testIdentityFromAnotherAccountOrRegionIsRefused(@Mocked GlueClient glueClient,
                                                                @Mocked LakeFormationClient lfClient) {
        LakeFormationMetadataGateway gateway =
                new LakeFormationMetadataGateway(glueClient, lfClient, properties());

        LakeFormationTableIdentity otherAccount =
                new LakeFormationTableIdentity("lf_catalog", "999999999999", "us-west-2", "db", "tbl");
        assertTrue(assertThrows(LakeFormationTableAccessException.class,
                () -> gateway.getTableMetadata(otherAccount, SESSION)).getMessage().contains("123456789012"));

        LakeFormationTableIdentity otherRegion =
                new LakeFormationTableIdentity("lf_catalog", "123456789012", "us-east-1", "db", "tbl");
        assertTrue(assertThrows(LakeFormationTableAccessException.class,
                () -> gateway.getTableMetadata(otherRegion, SESSION)).getMessage().contains("us-west-2"));
    }

    /** An already-expired lease can never be used, so it fails here rather than somewhere less obvious. */
    @Test
    public void testAlreadyExpiredCredentialsAreRefused(@Mocked GlueClient glueClient,
                                                        @Mocked LakeFormationClient lfClient) {
        new Expectations() {
            {
                lfClient.getTemporaryGlueTableCredentials((GetTemporaryGlueTableCredentialsRequest) any);
                result = GetTemporaryGlueTableCredentialsResponse.builder()
                        .accessKeyId("AK").secretAccessKey("SK").sessionToken("ST")
                        .expiration(Instant.now().minus(1, ChronoUnit.MINUTES))
                        .build();
            }
        };

        LakeFormationTableAccessException e = assertThrows(LakeFormationTableAccessException.class, () ->
                new LakeFormationMetadataGateway(glueClient, lfClient, properties())
                        .vendTableCredentials(IDENTITY, TABLE_ARN, "auth-1", SESSION));
        assertTrue(e.getMessage().contains("clock skew"));
    }

    @Test
    public void testCredentialsWithoutAnExpirationAreRefused(@Mocked GlueClient glueClient,
                                                             @Mocked LakeFormationClient lfClient) {
        new Expectations() {
            {
                lfClient.getTemporaryGlueTableCredentials((GetTemporaryGlueTableCredentialsRequest) any);
                result = GetTemporaryGlueTableCredentialsResponse.builder()
                        .accessKeyId("AK").secretAccessKey("SK").sessionToken("ST")
                        .build();
            }
        };

        LakeFormationTableAccessException e = assertThrows(LakeFormationTableAccessException.class, () ->
                new LakeFormationMetadataGateway(glueClient, lfClient, properties())
                        .vendTableCredentials(IDENTITY, TABLE_ARN, "auth-1", SESSION));
        assertTrue(e.getMessage().contains("without an expiration"));
    }

    @Test
    public void testCredentialMaterialIsNotInToString(@Mocked GlueClient glueClient,
                                                      @Mocked LakeFormationClient lfClient) {
        new Expectations() {
            {
                lfClient.getTemporaryGlueTableCredentials((GetTemporaryGlueTableCredentialsRequest) any);
                result = GetTemporaryGlueTableCredentialsResponse.builder()
                        .accessKeyId("AKIAsecret").secretAccessKey("SKsecret").sessionToken("STsecret")
                        .expiration(Instant.now().plus(1, ChronoUnit.HOURS))
                        .build();
            }
        };

        LakeFormationTableAccess access =
                new LakeFormationMetadataGateway(glueClient, lfClient, properties())
                        .vendTableCredentials(IDENTITY, TABLE_ARN, "auth-1", SESSION);

        String text = access.toString();
        assertFalse(text.contains("AKIAsecret"));
        assertFalse(text.contains("SKsecret"));
        assertFalse(text.contains("STsecret"));
        assertFalse(text.contains("auth-1"));
    }

    /**
     * A failing Glue call must not surface as a raw SDK exception: it names neither the table nor what
     * to do about it, and this is the layer that still knows both.
     */
    @Test
    public void testAFailedMetadataCallIsReportedAgainstTheTable(@Mocked GlueClient glueClient,
                                                                 @Mocked LakeFormationClient lfClient) {
        new Expectations() {
            {
                glueClient.getUnfilteredTableMetadata((GetUnfilteredTableMetadataRequest) any);
                result = new RuntimeException("connection reset");
            }
        };

        LakeFormationTableAccessException failure = assertThrows(LakeFormationTableAccessException.class,
                () -> new LakeFormationMetadataGateway(glueClient, lfClient, properties())
                        .getTableMetadata(IDENTITY, SESSION));
        assertTrue(failure.getMessage().contains("db.tbl"), failure.getMessage());
        assertTrue(failure.getMessage().contains("connection reset"), failure.getMessage());
    }

    /**
     * The account may cap the lease below what the catalog asked for. That is not a failure - the
     * credentials work - but it decides whether a long query survives admission, so it is recorded as
     * the lease the service actually granted rather than the one that was requested.
     */
    @Test
    public void testAShortenedLeaseIsAcceptedAtItsRealExpiry(@Mocked GlueClient glueClient,
                                                             @Mocked LakeFormationClient lfClient) {
        Instant shortened = Instant.now().plus(100, ChronoUnit.SECONDS);
        new Expectations() {
            {
                lfClient.getTemporaryGlueTableCredentials((GetTemporaryGlueTableCredentialsRequest) any);
                result = GetTemporaryGlueTableCredentialsResponse.builder()
                        .accessKeyId("AK").secretAccessKey("SK").sessionToken("ST")
                        .expiration(shortened)
                        .build();
            }
        };

        LakeFormationTableAccess access =
                new LakeFormationMetadataGateway(glueClient, lfClient, properties())
                        .vendTableCredentials(IDENTITY, TABLE_ARN, "auth-1", SESSION);

        assertEquals(shortened, access.expiresAt());
    }
}
