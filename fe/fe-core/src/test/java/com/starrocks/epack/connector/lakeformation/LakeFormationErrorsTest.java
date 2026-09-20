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
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * These messages are the whole point of this class: the raw SDK exceptions name neither the table nor
 * what to do about it, and two of the cases below read as generic invalid-input errors. A test that only
 * checked "an exception is thrown" would let the text rot, which is the part an operator acts on.
 */
public class LakeFormationErrorsTest {

    private static final LakeFormationTableIdentity IDENTITY =
            new LakeFormationTableIdentity("lf", "123456789012", "us-east-2", "db", "t");

    private static String metadataMessage(Exception cause) {
        LakeFormationTableAccessException failure = LakeFormationErrors.metadataFailure(IDENTITY, cause);
        assertSame(cause, failure.getCause(), "the original failure must stay reachable");
        return failure.getMessage();
    }

    private static String vendingMessage(int requestedDurationSeconds, Exception cause) {
        LakeFormationTableAccessException failure =
                LakeFormationErrors.vendingFailure(IDENTITY, requestedDurationSeconds, cause);
        assertSame(cause, failure.getCause());
        return failure.getMessage();
    }

    @Test
    public void testAPermissionTypeMismatchSaysWhichPermissionTypesAreSupported() {
        String message = metadataMessage(
                software.amazon.awssdk.services.glue.model.PermissionTypeMismatchException.builder().build());
        assertTrue(message.contains("db.t"), message);
        assertTrue(message.contains("column-level permissions only"), message);
        // Naming them is what keeps this from being read as a transient failure worth retrying.
        assertTrue(message.contains("row filters and cell filters are not supported yet"), message);
    }

    @Test
    public void testAGlueRefusalPointsAtTheGrantAndTheRegistration() {
        String message = metadataMessage(
                software.amazon.awssdk.services.glue.model.AccessDeniedException.builder().build());
        assertTrue(message.contains("no Lake Formation grant"), message);
        assertTrue(message.contains("authorized caller"), message);
    }

    @Test
    public void testAMissingTableSaysItMayBeUnregisteredRatherThanAbsent() {
        String message = metadataMessage(
                software.amazon.awssdk.services.glue.model.EntityNotFoundException.builder().build());
        assertTrue(message.contains("does not exist in this Glue catalog"), message);
        assertTrue(message.contains("not registered with Lake Formation"), message);
    }

    /** Anything unrecognized still has to name the table and carry the cause's own text. */
    @Test
    public void testAnUnrecognizedMetadataFailureStillNamesTheTable() {
        String message = metadataMessage(new IllegalStateException("socket closed"));
        assertTrue(message.contains("db.t"), message);
        assertTrue(message.contains("the Lake Formation metadata request failed"), message);
        assertTrue(message.contains("Cause: socket closed"), message);
        assertFalse(message.contains("AWS error code"), message);
    }

    /**
     * The duration is the input most likely to be refused and the AWS message never says so, so the
     * refusal has to carry the property name, the value that was sent, and why the ceiling is lower
     * than the API's own maximum.
     */
    @Test
    public void testARejectedDurationNamesThePropertyAndTheValueSent() {
        String message = vendingMessage(21600,
                software.amazon.awssdk.services.lakeformation.model.InvalidInputException.builder().build());
        assertTrue(message.contains(LakeFormationCatalogProperties.CREDENTIAL_DURATION_SECONDS + "=21600"),
                message);
        assertTrue(message.contains("ALTER CATALOG"), message);
        assertTrue(message.contains("role chaining"), message);
        // Saying this out loud matters: a silent retry with a shorter duration would hide the real limit.
        assertTrue(message.contains("never retries with a shorter duration"), message);
    }

    @Test
    public void testAVendingRefusalPointsAtTheDataLocationRegistration() {
        String message = vendingMessage(3600,
                software.amazon.awssdk.services.lakeformation.model.AccessDeniedException.builder().build());
        assertTrue(message.contains("data location must be"), message);
        assertTrue(message.contains("registered with Lake Formation"), message);
    }

    @Test
    public void testAnUnrecognizedVendingFailureStillNamesTheTable() {
        String message = vendingMessage(900, new IllegalStateException("no route to host"));
        assertTrue(message.contains("db.t"), message);
        assertTrue(message.contains("the credential request failed"), message);
        assertTrue(message.contains("Cause: no route to host"), message);
    }

    /**
     * Region, error code and request id are what an operator opens a support case with, and what tells
     * two identically-worded refusals apart.
     */
    @Test
    public void testAnAwsFailureCarriesTheErrorCodeAndRequestId() {
        String message = metadataMessage(
                software.amazon.awssdk.services.glue.model.AccessDeniedException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode("AccessDeniedException")
                                .errorMessage("Insufficient Lake Formation permissions")
                                .build())
                        .requestId("req-42")
                        .build());
        assertTrue(message.contains("AWS error code: AccessDeniedException."), message);
        assertTrue(message.contains("AWS message: Insufficient Lake Formation permissions."), message);
        assertTrue(message.contains("Request ID: req-42."), message);
    }
}
