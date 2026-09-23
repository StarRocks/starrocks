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

import com.starrocks.connector.TableLoadPurpose;

import static java.util.Objects.requireNonNull;

/**
 * What a LakeFormationHiveTable carries instead of its credentials: enough to find them, nothing that is
 * worth stealing.
 *
 * A Table outlives its query on five paths - the MV rewrite plan cache, a prepared statement's AST, a
 * statistics job, MV pinned refresh, retry reusing the physical plan - so a credential parked on it would
 * sit there in plaintext. A stale handle simply stops resolving and the query fails closed.
 *
 * Staleness comes from attemptId: every planning attempt opens a scope with a new one.
 */
public record LakeFormationTableHandle(LakeFormationTableIdentity identity,
                                       TableLoadPurpose purpose,
                                       String attemptId) {

    public LakeFormationTableHandle {
        requireNonNull(identity, "identity is null");
        requireNonNull(purpose, "purpose is null");
        requireNonNull(attemptId, "attemptId is null");
    }

    /**
     * False for anything from another planning attempt: a retry must re-resolve to get fresh credentials,
     * and a prepared statement's second EXECUTE must re-resolve to notice a revoked grant.
     */
    public boolean isReusableIn(LakeFormationQueryScope scope) {
        return scope != null && attemptId.equals(scope.attemptId());
    }
}
