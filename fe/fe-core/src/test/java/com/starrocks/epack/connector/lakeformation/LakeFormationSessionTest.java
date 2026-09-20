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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LakeFormationSessionTest {

    /**
     * GetCallerIdentity answers with an assumed-role ARN, which AssumeRole will not accept as a
     * target. Self-assuming requires converting it back to the role's own ARN first.
     */
    @Test
    public void testAssumedRoleArnBecomesTheRoleArn() {
        assertEquals("arn:aws:iam::123456789012:role/StarRocksFE",
                LakeFormationSession.roleArnOfAssumedRole(
                        "arn:aws:sts::123456789012:assumed-role/StarRocksFE/i-0abc123"));
    }

    @Test
    public void testRoleArnIsAlreadyUsable() {
        assertEquals("arn:aws:iam::123456789012:role/StarRocksFE",
                LakeFormationSession.roleArnOfAssumedRole("arn:aws:iam::123456789012:role/StarRocksFE"));
    }

    @Test
    public void testNonCommercialPartitionIsPreserved() {
        // A hardcoded "aws" partition would break every GovCloud and China deployment.
        assertEquals("arn:aws-cn:iam::123456789012:role/StarRocksFE",
                LakeFormationSession.roleArnOfAssumedRole(
                        "arn:aws-cn:sts::123456789012:assumed-role/StarRocksFE/session"));
        assertEquals("arn:aws-us-gov:iam::123456789012:role/StarRocksFE",
                LakeFormationSession.roleArnOfAssumedRole(
                        "arn:aws-us-gov:sts::123456789012:assumed-role/StarRocksFE/session"));
    }

    /**
     * An IAM user has no role to assume. Continuing untagged would make Lake Formation refuse every
     * later request for a reason that names neither the tag nor the identity.
     */
    @Test
    public void testPlainUserIsRejectedWithAnActionableMessage() {
        LakeFormationTableAccessException e = assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationSession.roleArnOfAssumedRole("arn:aws:iam::123456789012:user/alice"));
        assertTrue(e.getMessage().contains("aws.lakeformation.iam_role_arn"));
    }

    @Test
    public void testMalformedArnsAreRejected() {
        assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationSession.roleArnOfAssumedRole(null));
        assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationSession.roleArnOfAssumedRole("not-an-arn"));
        assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationSession.roleArnOfAssumedRole("arn:aws:sts::123456789012:assumed-role"));
        // The prefix is there but the role name is not: splitting yields one part, and guessing the
        // role from a truncated ARN would assume a role nobody named.
        assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationSession.roleArnOfAssumedRole("arn:aws:sts::123456789012:assumed-role/"));
    }

    @Test
    public void testSessionTagKeyIsThePoCVerifiedOne() {
        // AWS documentation also mentions "TrustedCaller"; the PoC against the target account only
        // worked with this key, and the observed behavior is what we ship.
        assertEquals("LakeFormationAuthorizedCaller", LakeFormationSession.SESSION_TAG_KEY);
    }
}
