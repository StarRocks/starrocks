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

import com.starrocks.catalog.Table;

/**
 * What one attempt decided about one table, including the decision to refuse it.
 *
 * A failure is a result, not an exception, so it is remembered: one statement never gets two answers.
 *
 * No METADATA_AUTHORIZED to ACCESS_READY transition: data access resolves again as a separate scope entry.
 */
public final class LakeFormationTableResolution {

    private enum State {
        /** Lake Formation answered IsRegisteredWithLakeFormation = false; the ordinary path applies. */
        UNREGISTERED,
        /** Authorized for metadata. Carries no credentials and cannot be scanned. */
        METADATA_AUTHORIZED,
        /** Authorized and vended. Carries the lease a scan node will capture. */
        ACCESS_READY,
        /** Refused. Sticky for the rest of the attempt. */
        FAILED
    }

    private final State state;
    private final Table authorizedTable;
    private final AuthorizedTableMetadata metadata;
    private final LakeFormationLease lease;
    private final LakeFormationTableAccessException failure;

    private LakeFormationTableResolution(State state, Table authorizedTable, AuthorizedTableMetadata metadata,
                                         LakeFormationLease lease,
                                         LakeFormationTableAccessException failure) {
        this.state = state;
        this.authorizedTable = authorizedTable;
        this.metadata = metadata;
        this.lease = lease;
        this.failure = failure;
    }

    public static LakeFormationTableResolution unregistered() {
        return new LakeFormationTableResolution(State.UNREGISTERED, null, null, null, null);
    }

    static LakeFormationTableResolution metadataAuthorized(Table table, AuthorizedTableMetadata metadata) {
        return new LakeFormationTableResolution(State.METADATA_AUTHORIZED, table, metadata, null, null);
    }

    static LakeFormationTableResolution accessReady(Table table, AuthorizedTableMetadata metadata,
                                                    LakeFormationLease lease) {
        return new LakeFormationTableResolution(State.ACCESS_READY, table, metadata, lease, null);
    }

    public static LakeFormationTableResolution failed(LakeFormationTableAccessException failure) {
        return new LakeFormationTableResolution(State.FAILED, null, null, null, failure);
    }

    public boolean isUnregistered() {
        return state == State.UNREGISTERED;
    }

    public boolean isAccessReady() {
        return state == State.ACCESS_READY;
    }

    public boolean isFailed() {
        return state == State.FAILED;
    }

    /**
     * Rethrows wrapped, so each caller gets its own stack trace.
     */
    public void rethrowIfFailed() {
        if (failure != null) {
            throw new LakeFormationTableAccessException(failure.getMessage(), failure);
        }
    }

    public Table authorizedTable() {
        return authorizedTable;
    }

    AuthorizedTableMetadata metadata() {
        return metadata;
    }

    LakeFormationLease lease() {
        return lease;
    }

    // No partition state: partitions are read after the attempt ends, so they travel with the lease.
}
