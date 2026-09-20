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
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsResponse;

import java.time.Instant;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * One vended set of table credentials, taken straight off GetTemporaryGlueTableCredentials.
 *
 * expiresAt is whatever the API returned, never the duration we asked for: the service is free to
 * hand back a shorter lease, and every admission decision downstream has to be made against the
 * lease we actually hold.
 *
 * The credential material stays raw here. Turning it into a CloudConfiguration is connector wiring
 * and belongs with the code that puts it on a scan node.
 */
public final class LakeFormationTableAccess {
    private final LakeFormationTableIdentity identity;
    private final String queryAuthorizationId;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;
    private final Instant expiresAt;
    private final List<String> vendedS3Paths;

    private LakeFormationTableAccess(LakeFormationTableIdentity identity, String queryAuthorizationId,
                                     String accessKeyId, String secretAccessKey, String sessionToken,
                                     Instant expiresAt, List<String> vendedS3Paths) {
        this.identity = identity;
        this.queryAuthorizationId = queryAuthorizationId;
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.sessionToken = sessionToken;
        this.expiresAt = expiresAt;
        this.vendedS3Paths = ImmutableList.copyOf(vendedS3Paths);
    }

    public static LakeFormationTableAccess from(LakeFormationTableIdentity identity,
                                                String queryAuthorizationId,
                                                GetTemporaryGlueTableCredentialsResponse response) {
        requireNonNull(identity, "identity is null");
        requireNonNull(response, "response is null");
        if (response.accessKeyId() == null || response.secretAccessKey() == null
                || response.sessionToken() == null) {
            throw new LakeFormationTableAccessException(
                    "Lake Formation returned incomplete credentials for " + identity);
        }
        if (response.expiration() == null) {
            // Without an expiry there is no way to decide whether the lease outlives the query, and
            // treating "unknown" as "forever" is the unsafe direction.
            throw new LakeFormationTableAccessException(
                    "Lake Formation returned credentials without an expiration for " + identity);
        }
        return new LakeFormationTableAccess(identity, queryAuthorizationId, response.accessKeyId(),
                response.secretAccessKey(), response.sessionToken(), response.expiration(),
                response.vendedS3Path());
    }

    public LakeFormationTableIdentity identity() {
        return identity;
    }

    /** Kept so the lease can be renewed later with the same authorization rather than a fresh one. */
    public String queryAuthorizationId() {
        return queryAuthorizationId;
    }

    public String accessKeyId() {
        return accessKeyId;
    }

    public String secretAccessKey() {
        return secretAccessKey;
    }

    public String sessionToken() {
        return sessionToken;
    }

    public Instant expiresAt() {
        return expiresAt;
    }

    /** The S3 prefixes the vended credentials were scoped down to, when the service reported them. */
    public List<String> vendedS3Paths() {
        return vendedS3Paths;
    }

    @Override
    public String toString() {
        return "LakeFormationTableAccess{identity=" + identity
                + ", expiresAt=" + expiresAt
                + ", vendedS3PathCount=" + vendedS3Paths.size()
                + ", credentials=<redacted>"
                + ", queryAuthorizationId=<redacted>}";
    }
}
