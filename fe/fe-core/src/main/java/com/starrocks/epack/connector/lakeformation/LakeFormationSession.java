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

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.Tag;

import java.util.UUID;

/**
 * Builds the tagged session Lake Formation requires of a caller acting on behalf of a user.
 *
 * The role is assumed with a session tag rather than switched: the caller keeps the same identity it
 * already had and only gains the tag that marks it as an authorized Lake Formation caller. When no
 * role is configured this is a self-assume - the process assumes the very role it is already running
 * as, purely to attach the tag.
 *
 * ⚠️ A tagged session is role chaining, which AWS caps at one hour regardless of the role's
 * MaxSessionDuration. Whether that cap also limits the lease Lake Formation vends on top of it is an
 * open question for deployment preflight, not something this class can decide.
 */
public final class LakeFormationSession {
    /**
     * Verified against the target account during the PoC. AWS documentation also mentions
     * "TrustedCaller" for this purpose; the two disagree, and the observed behavior wins.
     */
    public static final String SESSION_TAG_KEY = "LakeFormationAuthorizedCaller";

    private LakeFormationSession() {
    }

    /**
     * @param stsClient         already carries the base credentials and the region, so neither is
     *                          passed separately
     * @param configuredRoleArn the role from the catalog properties, or null/blank to self-assume
     */
    public static AwsCredentialsProvider taggedCredentialsProvider(StsClient stsClient,
                                                                   String configuredRoleArn,
                                                                   String sessionTagValue) {
        String roleArn = configuredRoleArn != null && !configuredRoleArn.trim().isEmpty()
                ? configuredRoleArn.trim()
                : callerRoleArn(stsClient);

        AssumeRoleRequest request = AssumeRoleRequest.builder()
                .roleArn(roleArn)
                .roleSessionName("starrocks-lf-" + UUID.randomUUID())
                .tags(Tag.builder().key(SESSION_TAG_KEY).value(sessionTagValue).build())
                .build();

        return StsAssumeRoleCredentialsProvider.builder()
                .stsClient(stsClient)
                .refreshRequest(request)
                .build();
    }

    private static String callerRoleArn(StsClient stsClient) {
        String callerArn;
        try {
            callerArn = stsClient.getCallerIdentity().arn();
        } catch (RuntimeException e) {
            throw new LakeFormationTableAccessException(
                    "Cannot determine the role to assume for Lake Formation: sts:GetCallerIdentity failed. "
                            + "Set " + LakeFormationCatalogProperties.IAM_ROLE_ARN + " to name it explicitly. "
                            + "Cause: " + e.getMessage(), e);
        }
        return roleArnOfAssumedRole(callerArn);
    }

    /**
     * Turns the assumed-role ARN STS reports back into the role ARN that can be assumed again.
     *
     * GetCallerIdentity answers with arn:aws:sts::123456789012:assumed-role/RoleName/session-name,
     * which is not a valid target for AssumeRole; the role itself is
     * arn:aws:iam::123456789012:role/RoleName.
     */
    static String roleArnOfAssumedRole(String callerArn) {
        if (callerArn == null) {
            throw new LakeFormationTableAccessException(
                    "sts:GetCallerIdentity returned no ARN, so the role to assume for Lake Formation is unknown");
        }
        String[] parts = callerArn.split(":");
        if (parts.length < 6) {
            throw cannotConvert(callerArn);
        }
        String partition = parts[1];
        String account = parts[4];
        String resource = parts[5];

        if (resource.startsWith("assumed-role/")) {
            String[] resourceParts = resource.split("/");
            if (resourceParts.length < 2) {
                throw cannotConvert(callerArn);
            }
            return "arn:" + partition + ":iam::" + account + ":role/" + resourceParts[1];
        }
        if (resource.startsWith("role/")) {
            return callerArn;
        }
        // A plain IAM user has no role to assume, and silently running untagged would mean Lake
        // Formation refuses every request for an opaque reason.
        throw cannotConvert(callerArn);
    }

    private static LakeFormationTableAccessException cannotConvert(String callerArn) {
        return new LakeFormationTableAccessException(
                "Cannot derive a role to assume for Lake Formation from the current identity '" + callerArn
                        + "'. Set " + LakeFormationCatalogProperties.IAM_ROLE_ARN + " to name the role explicitly.");
    }
}
