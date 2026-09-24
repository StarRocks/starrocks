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
 * A Table outlives its query in several caches (MV plan cache, prepared statements, retry), so it carries
 * this handle rather than a credential; a stale handle stops resolving and the query fails closed.
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
     * Whether a table carrying this handle may be reused as already resolved.
     *
     * False across attempts, so a retry gets fresh credentials and a re-EXECUTE notices a revoked grant.
     */
    public boolean isReusableIn(LakeFormationQueryScope scope) {
        return scope != null && attemptId.equals(scope.attemptId());
    }
}
