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

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Which role the tagged session assumes. Lake Formation authorizes that role, so picking it wrong is
 * not a performance detail - every request is refused, and for a reason the message never states.
 *
 * <p>The provider itself is lazy: building one issues no AssumeRole call, which is why these tests
 * can assert the choice without a live STS.
 */
public class LakeFormationSessionProviderTest {

    private static final String ROLE = "arn:aws:iam::123456789012:role/Engine";

    @Test
    public void testAConfiguredRoleIsUsedWithoutAskingSts(@Mocked StsClient stsClient) {
        new Expectations() {
            {
                // The point of configuring it: no GetCallerIdentity round trip, and no dependency on
                // the FE's own identity being an assumed role at all.
                stsClient.getCallerIdentity();
                times = 0;
            }
        };

        AwsCredentialsProvider provider =
                LakeFormationSession.taggedCredentialsProvider(stsClient, ROLE, "starrocks");
        assertNotNull(provider);
    }

    /** Blank counts as absent: a property left as whitespace must not become a role name. */
    @Test
    public void testABlankRoleFallsBackToTheCallersOwnRole(@Mocked StsClient stsClient) {
        new Expectations() {
            {
                stsClient.getCallerIdentity();
                result = GetCallerIdentityResponse.builder()
                        .arn("arn:aws:sts::123456789012:assumed-role/Engine/i-0123456789abcdef")
                        .build();
                times = 2;
            }
        };

        assertNotNull(LakeFormationSession.taggedCredentialsProvider(stsClient, null, "starrocks"));
        assertNotNull(LakeFormationSession.taggedCredentialsProvider(stsClient, "   ", "starrocks"));
    }

    /**
     * Without the caller's identity there is no role to assume. Failing here names the property that
     * removes the dependency; failing later would surface as an opaque Lake Formation refusal.
     */
    @Test
    public void testAFailingCallerIdentityNamesTheEscapeHatch(@Mocked StsClient stsClient) {
        new Expectations() {
            {
                stsClient.getCallerIdentity();
                result = new RuntimeException("no credentials in the chain");
            }
        };

        LakeFormationTableAccessException failure = assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationSession.taggedCredentialsProvider(stsClient, null, "starrocks"));
        assertTrue(failure.getMessage().contains(LakeFormationCatalogProperties.IAM_ROLE_ARN),
                failure.getMessage());
        assertTrue(failure.getMessage().contains("no credentials in the chain"), failure.getMessage());
    }

    /** An IAM user has no role to assume, so running untagged would be refused for an opaque reason. */
    @Test
    public void testAPlainUserIdentityIsRefused(@Mocked StsClient stsClient) {
        new Expectations() {
            {
                stsClient.getCallerIdentity();
                result = GetCallerIdentityResponse.builder()
                        .arn("arn:aws:iam::123456789012:user/gavin")
                        .build();
            }
        };

        assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationSession.taggedCredentialsProvider(stsClient, null, "starrocks"));
    }
}
