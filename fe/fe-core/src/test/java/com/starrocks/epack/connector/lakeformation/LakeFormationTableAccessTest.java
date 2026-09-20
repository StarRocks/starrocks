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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsResponse;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What the vending response is allowed to be missing. A half-built credential must not become a
 * usable object: the parts a scan reads later are not re-checked there, so the check has to be here.
 */
public class LakeFormationTableAccessTest {

    private static final LakeFormationTableIdentity IDENTITY =
            new LakeFormationTableIdentity("lf_catalog", "123456789012", "us-west-2", "db", "tbl");
    private static final Instant EXPIRY = Instant.parse("2026-01-01T00:00:00Z");

    private static GetTemporaryGlueTableCredentialsResponse.Builder response() {
        return GetTemporaryGlueTableCredentialsResponse.builder()
                .accessKeyId("AK")
                .secretAccessKey("SK")
                .sessionToken("ST")
                .expiration(EXPIRY);
    }

    @Test
    public void testACompleteResponseIsReadBackVerbatim() {
        LakeFormationTableAccess access = LakeFormationTableAccess.from(IDENTITY, "auth-1",
                response().vendedS3Path("s3://bucket/db/tbl", "s3://bucket/db/tbl/part=1").build());

        assertEquals(IDENTITY, access.identity());
        assertEquals("auth-1", access.queryAuthorizationId());
        assertEquals("AK", access.accessKeyId());
        assertEquals("SK", access.secretAccessKey());
        assertEquals("ST", access.sessionToken());
        assertEquals(EXPIRY, access.expiresAt());
        assertEquals(List.of("s3://bucket/db/tbl", "s3://bucket/db/tbl/part=1"), access.vendedS3Paths());
    }

    @Test
    public void testVendedPathsAreImmutable() {
        LakeFormationTableAccess access = LakeFormationTableAccess.from(IDENTITY, "auth-1",
                response().vendedS3Path("s3://bucket/db/tbl").build());
        assertThrows(UnsupportedOperationException.class,
                () -> access.vendedS3Paths().add("s3://bucket/elsewhere"));
    }

    @Test
    public void testEveryMissingCredentialPartIsRefused() {
        for (GetTemporaryGlueTableCredentialsResponse incomplete : List.of(
                response().accessKeyId(null).build(),
                response().secretAccessKey(null).build(),
                response().sessionToken(null).build())) {
            LakeFormationTableAccessException failure = assertThrows(LakeFormationTableAccessException.class,
                    () -> LakeFormationTableAccess.from(IDENTITY, "auth-1", incomplete));
            assertTrue(failure.getMessage().contains("incomplete credentials"), failure.getMessage());
            assertTrue(failure.getMessage().contains("db.tbl"), failure.getMessage());
        }
    }

    /** Treating an unknown expiry as "forever" is the unsafe direction, so it is refused instead. */
    @Test
    public void testAnAbsentExpirationIsRefused() {
        LakeFormationTableAccessException failure = assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationTableAccess.from(IDENTITY, "auth-1", response().expiration(null).build()));
        assertTrue(failure.getMessage().contains("without an expiration"), failure.getMessage());
    }

    /** The secrets must not reach a log through the object's own rendering. */
    @Test
    public void testSecretsAreNotInToString() {
        String rendered = LakeFormationTableAccess.from(IDENTITY, "auth-1", response().build()).toString();
        assertTrue(rendered.contains("db.tbl"), rendered);
        assertTrue(!rendered.contains("SK") && !rendered.contains("ST"), rendered);
    }
}
