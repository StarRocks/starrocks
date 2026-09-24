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
import com.starrocks.connector.QueryScopedCredentials;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudType;
import com.starrocks.qe.ConnectContext;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * One table's credentials for one planning attempt: what Lake Formation vended, what it turned into, and
 * who it was vended for.
 *
 * Immutable, and deliberately not reachable from the Table - no cache keeps a reference, so nothing can
 * hand a later query a credential it did not earn. Renewal produces a new instance sharing the same
 * context; scan nodes read through the holder, so a renewal is visible to all of them at once.
 */
public final class LakeFormationLease implements QueryScopedCredentials {

    /**
     * Accept a lease that is about to expire rather than one that already has: FE's clock and the clock
     * that stamped the expiry are not synchronized, and treating "expires in two seconds" as usable pushes
     * the failure into the BE, where it reads as a permissions problem instead of a timing one.
     */
    static final Duration CLOCK_SKEW_GUARD = Duration.ofSeconds(60);

    private final LakeFormationLeaseContext context;
    private final LakeFormationTableAccess access;
    private final CloudConfiguration cloudConfiguration;

    private LakeFormationLease(LakeFormationLeaseContext context, LakeFormationTableAccess access,
                               CloudConfiguration cloudConfiguration) {
        this.context = requireNonNull(context, "context is null");
        this.access = requireNonNull(access, "access is null");
        this.cloudConfiguration = requireNonNull(cloudConfiguration, "cloudConfiguration is null");
    }

    /**
     * The only way a lease comes into existence, for the first vend and for every renewal alike.
     *
     * Sharing one factory is the point: an earlier draft validated the vended prefix only on the first
     * call, which left a renewal free to come back covering a different location and have it handed
     * straight to the BE. Anything that can differ between two responses is re-checked on both.
     */
    static LakeFormationLease validated(LakeFormationLeaseContext context, LakeFormationTableAccess access) {
        if (!context.identity().equals(access.identity())) {
            throw new LakeFormationTableAccessException("Lake Formation returned credentials for "
                    + access.identity() + " while " + context.identity() + " was requested.");
        }
        checkVendedPathsCoverTableRoot(access, context.tableRoot());
        CloudConfiguration cloudConfiguration = LakeFormationCredentials.toCloudConfiguration(
                access, context.lakeFormationProperties(), context.catalogProperties());
        return new LakeFormationLease(context, access, cloudConfiguration);
    }

    /**
     * A table credential covers a subtree, and the response says which one. If it does not contain the
     * table's own location then this credential cannot read the table, and continuing would surface as an
     * access denied from S3 rather than as a message naming what actually went wrong.
     *
     * An empty list is not a failure: the field is optional and Lake Formation does not always fill it in.
     * The per-partition containment check in the partition snapshot is the one that must hold.
     */
    private static void checkVendedPathsCoverTableRoot(LakeFormationTableAccess access, String tableRoot) {
        if (tableRoot == null || tableRoot.isEmpty()) {
            // A registered table without a location cannot be read at all, and a credential said to cover
            // "nothing in particular" is not something to hand on.
            throw new LakeFormationTableAccessException("Lake Formation returned no location for "
                    + access.identity() + ", so there is nothing its credentials could be checked against.");
        }
        // Parsed before anything else: a vended credential only governs S3, so a root on any other scheme
        // would be listed with whatever identity the catalog configures for it.
        S3Location root = S3Location.parse(tableRoot);
        List<String> vendedPaths = access.vendedS3Paths();
        if (vendedPaths == null || vendedPaths.isEmpty()) {
            // Optional in the response; the per-partition containment check is the one that has to hold.
            return;
        }
        for (String vendedPath : vendedPaths) {
            if (root.isSameOrDescendantOf(S3Location.parse(vendedPath))) {
                return;
            }
        }
        throw new LakeFormationTableAccessException("The credentials Lake Formation vended for "
                + access.identity() + " do not cover the table's own location, so they cannot read it.");
    }

    public LakeFormationTableIdentity identity() {
        return context.identity();
    }

    public Instant expiresAt() {
        return access.expiresAt();
    }

    AtomicReference<LakeFormationLease> holder() {
        return context.holder();
    }


    /**
     * The partitions this table was authorized for, or null if they were never listed.
     *
     * Rethrows a failed listing, so it stays a failure.
     */
    LakeFormationPartitionSnapshot partitions() {
        LakeFormationLeaseContext.PartitionListing listing = context.partitions().get();
        return listing == null ? null : listing.getOrRethrow();
    }

    /** The single partition an unpartitioned table is listed through, built when it was authorized. */
    com.starrocks.connector.hive.Partition tablePartition() {
        return context.tablePartition().get();
    }


    LakeFormationLeaseContext context() {
        return context;
    }

    /** The lease the holder currently points at - this one, unless a renewal has already replaced it. */
    private LakeFormationLease currentOrSelf() {
        LakeFormationLease current = context.holder().get();
        return current == null ? this : current;
    }

    /**
     * Why this lease does not belong to what is running now - same query, same execution attempt, same
     * user - or null if it does.
     *
     * <p>Ownership only; {@link #cloudConfiguration} adds "and they still work". The expiry check is
     * deliberately not here: the descriptor table is serialized before the coordinator admits the query,
     * and admission is what renews a lease that is running out.
     */
    private String bindingMismatch(ConnectContext connectContext) {
        if (connectContext == null || connectContext.getQueryId() == null) {
            return "it is being serialized outside a query context";
        }
        if (!context.queryId().equals(connectContext.getQueryId().toString())) {
            return "these credentials belong to query " + context.queryId()
                    + ", but the plan is running as " + connectContext.getQueryId();
        }
        // Both sides present and equal, or refuse. Null is a mismatch rather than a wildcard: a path that
        // lost the execution id or the user is a path that cannot show these credentials belong to whoever
        // is running the query, and answering "yes" there is how a stale credential gets reused.
        if (context.executionId() == null || !context.executionId().equals(connectContext.getExecutionId())) {
            return "these credentials belong to an earlier execution attempt;"
                    + " the plan has to be rebuilt before it can run again";
        }
        UserIdentity currentUser = connectContext.getCurrentUserIdentity();
        if (context.principal() == null || currentUser == null || !context.principal().equals(currentUser)) {
            return "these credentials cannot be shown to belong to the user running this query";
        }
        return null;
    }

    /**
     * The same question {@link #validateBinding} asks, for the caller that has somewhere else to go rather
     * than an error to raise: it gates whether an already resolved table may be reused, and that table
     * carries one user's authorized columns.
     */
    @Override
    public boolean matches(ConnectContext connectContext) {
        return currentOrSelf().bindingMismatch(connectContext) == null;
    }

    /**
     * @return the lease the holder currently points at, which a renewal may have replaced
     * @throws LakeFormationTableAccessException if it does not belong to what is running now
     */
    LakeFormationLease validateBinding(ConnectContext connectContext) {
        LakeFormationLease current = currentOrSelf();
        String mismatch = current.bindingMismatch(connectContext);
        if (mismatch != null) {
            throw current.refuse(mismatch);
        }
        return current;
    }

    /**
     * The last gate before credentials reach the wire. Every clause is a way a plan can be reused that the
     * planner cannot prevent, which is why this runs on every serialization rather than once per plan.
     */
    @Override
    public CloudConfiguration cloudConfiguration(ConnectContext connectContext) {
        LakeFormationLease current = validateBinding(connectContext);
        // Structurally impossible - the factory refuses anything else - but this is the last gate and the
        // cost of repeating it is one enum comparison.
        if (current.cloudConfiguration.getCloudType() != CloudType.AWS) {
            throw current.refuse("the credentials are not an AWS configuration");
        }
        // The same slack admission uses. Serializing a credential that expires in the next few seconds
        // means the BE receives one that is already dead, which surfaces as a permissions error rather
        // than as the timing problem it is.
        if (!current.expiresAt().isAfter(Instant.now().plus(CLOCK_SKEW_GUARD))) {
            throw current.refuse("the credentials expire at " + current.expiresAt()
                    + ", too soon to start a scan with");
        }
        return current.cloudConfiguration;
    }

    /**
     * The catalog's own value, not this lease's: two statements on the same catalog must reach the same
     * cached bytes, and a catalog whose role changed must not reach what the old role cached.
     */
    @Override
    public long cacheScopeSeed() {
        return context.cacheScopeSeed();
    }

    /**
     * Admission: how long this table may actually be scanned for.
     *
     * <p>Renews once if the current lease cannot reach the requested deadline, then answers with the window
     * it really has - the earlier of what was asked for and what the credentials can safely cover.
     *
     * <p>Deliberately not "outlive the deadline or refuse": that deadline is a statement timeout, which
     * says how long a query is <i>allowed</i> to run, so with {@code insert_timeout} at four hours and a
     * one-hour lease every {@code INSERT ... SELECT} would be refused before it started. Refusal remains
     * the answer for an empty window, where no work is possible at all.
     */
    @Override
    public CredentialAdmission admitForExecution(Instant requestedDeadline) {
        LakeFormationLease current = currentOrSelf();
        if (current.expiresAt().isAfter(requestedDeadline.plus(CLOCK_SKEW_GUARD))) {
            return new CredentialAdmission(requestedDeadline);
        }
        // One refresh per table, whatever the outcome - see AdmissionRefresh. A self-join admits the same
        // table once per scan node, and each of them landing its own re-vend would multiply the calls while
        // letting the two halves of the join disagree about what happened.
        //
        // A lease that cannot be renewed at all is not a refusal: whatever time it has left is still a real
        // window, and the query is held to it like any other. Refusing here would throw away work that fits.
        LakeFormationLease best = current.context.canRenew()
                ? context.admissionRefresh()
                        .updateAndGet(existing -> existing != null ? existing : refreshOnce(current))
                        .getOrRethrow()
                : current;

        Instant safeUntil = best.expiresAt().minus(CLOCK_SKEW_GUARD);
        if (!safeUntil.isAfter(Instant.now())) {
            throw new LakeFormationTableAccessException("Cannot start the Lake Formation scan for "
                    + current.identity() + ": after an admission-time credential refresh, no safe execution"
                    + " window remains - the credential expires at " + best.expiresAt() + " and "
                    + CLOCK_SKEW_GUARD.toSeconds() + " seconds are held back for clock skew. Retry to obtain"
                    + " a fresh credential. If this repeats, check "
                    + LakeFormationCatalogProperties.CREDENTIAL_DURATION_SECONDS + " and the AWS"
                    + " session-duration limit. Raising query_timeout or insert_timeout will not help.");
        }
        // The earlier of the two, which is the whole point: a short credential shortens the query rather
        // than refusing it.
        return new CredentialAdmission(
                safeUntil.isBefore(requestedDeadline) ? safeUntil : requestedDeadline);
    }

    /**
     * Re-vends once for admission and installs the result, recording a failure so it is replayed rather
     * than retried.
     *
     * <p>The renewed lease is installed even when it still cannot cover the requested deadline. The previous
     * version threw first and so never reached the install, which meant a perfectly good longer credential
     * was fetched and then dropped on the floor.
     */
    private LakeFormationLeaseContext.AdmissionRefresh refreshOnce(LakeFormationLease current) {
        LakeFormationLease renewed;
        try {
            renewed = current.renew();
        } catch (LakeFormationTableAccessException e) {
            // Recorded, not swallowed: the next scan node on this table gets the same answer, and no path
            // falls back to the catalog's own credentials.
            return LakeFormationLeaseContext.AdmissionRefresh.failed(e);
        }
        // Whoever loses this race is looking at a lease from the same renewal window, so it is at least as
        // good as the one they would have installed. Nothing to retry.
        current.context.holder().compareAndSet(current, renewed);
        return LakeFormationLeaseContext.AdmissionRefresh.of(renewed);
    }

    /**
     * Re-vends with the authorization already in hand: the QueryAuthorizationId is passed back verbatim so
     * this stays the same authorization decision, only with a later expiry.
     */
    private LakeFormationLease renew() {
        LakeFormationTableAccess renewedAccess = context.gateway().vendTableCredentials(
                context.identity(), context.tableArn(), access.queryAuthorizationId(), context.session());
        return validated(context, renewedAccess);
    }

    private LakeFormationTableAccessException refuse(String why) {
        return new LakeFormationTableAccessException(
                "Refusing to scan " + context.identity() + ": " + why + ".");
    }

    /**
     * Names the table and when the lease runs out, nothing else. This object holds plaintext credentials,
     * so it has to stay unprintable even when something logs it by accident.
     */
    @Override
    public String toString() {
        return "LakeFormationLease{identity=" + context.identity() + ", attemptId=" + context.attemptId()
                + ", queryId=" + context.queryId() + ", expiresAt=" + access.expiresAt()
                + ", credentials=<redacted>}";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof LakeFormationLease that)) {
            return false;
        }
        return context.identity().equals(that.context.identity())
                && context.attemptId().equals(that.context.attemptId())
                && context.queryId().equals(that.context.queryId())
                && access.equals(that.access);
    }

    @Override
    public int hashCode() {
        return Objects.hash(context.identity(), context.attemptId(), context.queryId(), access);
    }
}
