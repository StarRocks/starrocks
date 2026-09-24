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

import com.starrocks.catalog.UserIdentity;
import com.starrocks.connector.hive.Partition;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.thrift.TUniqueId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * Everything about a table's credentials that a renewal does not change: who they are for, which table,
 * and what is needed to ask for them again.
 *
 * Split out from the lease itself so that renewing is "same context, new response". Keeping these on the
 * lease meant either a twelve-argument constructor or fields assigned after construction, and the second
 * one had already cost this design its immutability once.
 *
 * <p>Also carries the partitions checked while planning: the listing runs afterwards and only has the lease.
 *
 * @param holder the cell every scan node on this table reads through, so one renewal serves all of them
 * @param partitions what the partition listing decided for this table, success or failure, once
 * @param tablePartition the single partition an unpartitioned table is listed through
 */
record LakeFormationLeaseContext(LakeFormationTableIdentity identity,
                                 UserIdentity principal,
                                 String attemptId,
                                 String queryId,
                                 TUniqueId executionId,
                                 LakeFormationMetadataGateway gateway,
                                 LakeFormationQuerySession session,
                                 String tableArn,
                                 String tableRoot,
                                 LakeFormationCatalogProperties lakeFormationProperties,
                                 Map<String, String> catalogProperties,
                                 long cacheScopeSeed,
                                 AtomicReference<LakeFormationLease> holder,
                                 AtomicReference<PartitionListing> partitions,
                                 AtomicReference<Partition> tablePartition,
                                 AtomicReference<AdmissionRefresh> admissionRefresh) {

    /**
     * Whether this table's credentials were already refreshed at admission, and what came of it.
     *
     * <p>Kept per table rather than per scan node because a self-join puts several scan nodes on the same
     * table, and each of them is admitted separately. Without this, every one of them would re-vend.
     *
     * <p>A failure is recorded and replayed rather than retried: asking Lake Formation again after it just
     * refused would give the same answer while doubling the calls, and the two scan nodes of one self-join
     * must not disagree about whether their shared credentials were renewable.
     */
    record AdmissionRefresh(LakeFormationLease renewed, LakeFormationTableAccessException failure) {

        static AdmissionRefresh of(LakeFormationLease renewed) {
            return new AdmissionRefresh(renewed, null);
        }

        static AdmissionRefresh failed(LakeFormationTableAccessException failure) {
            return new AdmissionRefresh(null, failure);
        }

        LakeFormationLease getOrRethrow() {
            if (failure != null) {
                throw failure;
            }
            return renewed;
        }
    }

    /** The partition listing's outcome, failure included, so a second call gets the same answer. */
    record PartitionListing(LakeFormationPartitionSnapshot snapshot,
                            LakeFormationTableAccessException failure) {

        static PartitionListing of(LakeFormationPartitionSnapshot snapshot) {
            return new PartitionListing(snapshot, null);
        }

        static PartitionListing failed(LakeFormationTableAccessException failure) {
            return new PartitionListing(null, failure);
        }

        LakeFormationPartitionSnapshot getOrRethrow() {
            if (failure != null) {
                throw new LakeFormationTableAccessException(failure.getMessage(), failure);
            }
            return snapshot;
        }
    }

    /**
     * Convenience for the common case: fresh holders for a table nothing has decided anything about yet.
     *
     * @param cacheScopeSeed carried here rather than read off the gateway, because a lease that cannot be
     *                       renewed has no gateway - and a scope that defaulted to zero there would leave
     *                       exactly those tables sharing their cached bytes with every other catalog.
     */
    static LakeFormationLeaseContext create(LakeFormationTableIdentity identity, UserIdentity principal,
                                            String attemptId, String queryId, TUniqueId executionId,
                                            LakeFormationMetadataGateway gateway,
                                            LakeFormationQuerySession session, String tableArn,
                                            String tableRoot,
                                            LakeFormationCatalogProperties lakeFormationProperties,
                                            Map<String, String> catalogProperties,
                                            long cacheScopeSeed) {
        return new LakeFormationLeaseContext(identity, principal, attemptId, queryId, executionId, gateway,
                session, tableArn, tableRoot, lakeFormationProperties, catalogProperties, cacheScopeSeed,
                new AtomicReference<>(), new AtomicReference<>(), new AtomicReference<>(),
                new AtomicReference<>());
    }

    LakeFormationLeaseContext {
        requireNonNull(identity, "identity is null");
        requireNonNull(attemptId, "attemptId is null");
        requireNonNull(queryId, "queryId is null");
        // Required for the same reason principal is: the execution attempt is what distinguishes a
        // credential issued for this run from one issued before a retry, and a lease that cannot name its
        // attempt makes that check disappear rather than fail. Better to refuse to build the lease at all.
        requireNonNull(executionId, "executionId is null");
        requireNonNull(lakeFormationProperties, "lakeFormationProperties is null");
        requireNonNull(holder, "holder is null");

        // Keep only the two addressing options LakeFormationCredentials actually reads. The catalog's own
        // property map carries aws.glue.secret_key and friends, and this is a record - its generated
        // toString() would print every one of them the first time anybody logs a lease context.
        catalogProperties = addressingOptionsOnly(catalogProperties);
    }

    /** The catalog properties that say *where* the bucket is; never the ones that say who is asking. */
    private static Map<String, String> addressingOptionsOnly(Map<String, String> catalogProperties) {
        if (catalogProperties == null || catalogProperties.isEmpty()) {
            return Map.of();
        }
        Map<String, String> addressing = new HashMap<>();
        for (String key : List.of(CloudConfigurationConstants.AWS_S3_ENDPOINT,
                CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS)) {
            String value = catalogProperties.get(key);
            if (value != null) {
                addressing.put(key, value);
            }
        }
        return Map.copyOf(addressing);
    }

    boolean canRenew() {
        return gateway != null && session != null && tableArn != null;
    }
}
