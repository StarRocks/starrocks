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


package com.starrocks.sql.optimizer.base;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Distribution spec for lake tables that use range distribution with a
 * colocate group. Scan-local only: carries the colocate columns and an
 * {@link EquivalentDescriptor} so later optimizer phases can decide
 * whether a range-colocate join, aggregation or window avoids a shuffle.
 *
 * <h3>Satisfaction contract</h3>
 * <ul>
 * <li>{@link DistributionType#ANY} always satisfied.
 * <li>Another {@code RangeDistributionSpec} — delegate to {@link #canColocate}
 *     (structural check: same group, both stable, non-empty partitions,
 *     equal colocate column count).
 * <li>{@link HashDistributionSpec} with {@code SourceType.SHUFFLE_JOIN} or
 *     {@code SourceType.SHUFFLE_AGG} — satisfied when every colocate column
 *     is covered (set-based, via
 *     {@code EquivalentDescriptor.isConnected(required, existing)}) by some
 *     required shuffle column; for {@code SHUFFLE_AGG} the cover must be
 *     null-strict (a NULL-padded key has NULL rows in every ColocateRange),
 *     the spec's selected partitions non-empty and the table's colocate
 *     group stable. Covering every colocate column keeps each join key / group
 *     inside one ColocateRange, which a colocate-dispatched fragment reads
 *     in one instance. A consumer whose fragment is not colocate-dispatched,
 *     such as a table sink that requires {@code SHUFFLE_AGG}, gets no
 *     one-instance-per-key guarantee from this, as with a hash-local spec.
 *     {@code SHUFFLE_ENFORCE} and {@code LOCAL} hash specs are rejected.
 * <li>Anything else not satisfied.
 * </ul>
 *
 * <h3>Invariants</h3>
 * <ul>
 * <li>{@link DistributionType#RANGE} never reaches thrift serialization;
 *     {@code toThrift()} throws.
 * <li>{@link DistributionProperty#appendEnforcers} refuses range —
 *     {@code RangeDistributionSpec} must not be the required property of
 *     a distribution enforcer. Use
 *     {@code ChildOutputPropertyGuarantor.convertRangeToHashShuffle} to
 *     materialize a hash shuffle exchange when needed.
 * <li>{@code equals}/{@code hashCode} keyed on
 *     {@code (DistributionType.RANGE_LOCAL, tableId, colocateColumns)}. The
 *     mutable {@link EquivalentDescriptor} is intentionally excluded to
 *     keep memo hash-key stable, mirroring {@code HashDistributionSpec}.
 * </ul>
 *
 * <h3>Non-join defenses</h3>
 * {@code SHUFFLE_JOIN} is emitted by set operations too
 * ({@code PropertyDeriverBase.computeShuffleSetRequiredProperties}), so the
 * source-type filter alone is not sufficient to restrict activation to
 * joins. The set-op guarantor in
 * {@code ChildOutputPropertyGuarantor.visitPhysicalSetOperation} runs a
 * top-of-method range-normalization pass that puts every range child
 * behind a hash {@code SHUFFLE_ENFORCE} exchange — a property no range
 * spec satisfies, so the exchange cannot be dropped in favour of the
 * bare range child — before any {@code isShuffle()} precondition or
 * {@code HashDistributionSpec} cast runs.
 */
public final class RangeDistributionSpec extends DistributionSpec {
    private final List<DistributionCol> colocateColumns;
    private final EquivalentDescriptor equivalentDescriptor;

    public RangeDistributionSpec(List<DistributionCol> colocateColumns,
                                 EquivalentDescriptor equivalentDescriptor) {
        super(DistributionType.RANGE_LOCAL);
        Preconditions.checkArgument(colocateColumns != null && !colocateColumns.isEmpty(),
                "colocateColumns must be non-empty");
        this.colocateColumns = ImmutableList.copyOf(colocateColumns);
        this.equivalentDescriptor = Objects.requireNonNull(equivalentDescriptor);
    }

    public List<DistributionCol> getColocateColumns() {
        return colocateColumns;
    }

    public EquivalentDescriptor getEquivalentDescriptor() {
        return equivalentDescriptor;
    }

    /**
     * Cross-type satisfaction.
     *
     * <ul>
     * <li>ANY → always satisfied.
     * <li>Another RangeDistributionSpec → {@link #canColocate}.
     * <li>HashDistributionSpec with source type {@code SHUFFLE_JOIN} or
     *     {@code SHUFFLE_AGG} and every colocate column covered by some
     *     required column via {@link EquivalentDescriptor#isConnected} →
     *     satisfied; for {@code SHUFFLE_AGG} the cover must be null-strict, the
     *     selected partitions non-empty and the group stable. {@code SHUFFLE_ENFORCE}
     *     and {@code LOCAL} hash specs are rejected.
     * <li>Anything else → not satisfied.
     * </ul>
     *
     * <p>Set operations also emit {@code SHUFFLE_JOIN}
     * ({@code PropertyDeriverBase.computeShuffleSetRequiredProperties}), so
     * this method alone does not keep a range child of a set operation off
     * the colocate path — the second defense is the top-of-method
     * range-normalization pass in
     * {@code ChildOutputPropertyGuarantor.visitPhysicalSetOperation}.
     */
    @Override
    public boolean isSatisfy(DistributionSpec spec) {
        if (spec.getType() == DistributionType.ANY) {
            return true;
        }
        if (spec instanceof RangeDistributionSpec) {
            return canColocate((RangeDistributionSpec) spec);
        }
        if (spec instanceof HashDistributionSpec) {
            return isSatisfyHashShuffle((HashDistributionSpec) spec);
        }
        return false;
    }

    /**
     * Structural compatibility check. Each spec owns its own
     * {@link EquivalentDescriptor} and the two are never merged, so
     * cross-descriptor {@link EquivalentDescriptor#isConnected} would
     * always return false and cannot be used here. Per-side
     * colocate-vs-shuffle equivalence is checked in
     * {@code ChildOutputPropertyGuarantor.canRangeColocateJoin}, which
     * uses each side's own descriptor.
     */
    public boolean canColocate(RangeDistributionSpec other) {
        ColocateTableIndex index = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long leftTableId = equivalentDescriptor.getTableId();
        long rightTableId = other.equivalentDescriptor.getTableId();
        if (!index.isSameGroup(leftTableId, rightTableId)) {
            return false;
        }
        ColocateTableIndex.GroupId leftGroupId = index.getGroup(leftTableId);
        ColocateTableIndex.GroupId rightGroupId = index.getGroup(rightTableId);
        if (leftGroupId == null || rightGroupId == null) {
            return false;
        }
        if (index.isGroupUnstable(leftGroupId) || index.isGroupUnstable(rightGroupId)) {
            return false;
        }
        if (equivalentDescriptor.isEmptyPartition() || other.equivalentDescriptor.isEmptyPartition()) {
            return false;
        }
        // Same ColocateGroupSchema guarantees identical colocate column count
        // and types. The position-preserving pairing check in the join
        // guarantor is what proves each pair is bound to the same equijoin.
        Preconditions.checkState(colocateColumns.size() == other.colocateColumns.size(),
                "colocate column count mismatch: %s vs %s",
                colocateColumns.size(), other.colocateColumns.size());
        return true;
    }

    /**
     * Set-based cover check, serving both join ({@code SHUFFLE_JOIN}) and aggregation / window
     * partitioning ({@code SHUFFLE_AGG}) callers. Every colocate column must be equivalent to
     * at least one required shuffle column; extras on the required side are fine.
     * Order-independent. Matches hash precedent at
     * {@code HashDistributionSpec.isJoinEqColumnsCompatible} with
     * {@code isConnected(requiredCol, existingCol)} argument order.
     *
     * <p>A join re-checks group stability and non-empty partitions itself in {@link #canColocate};
     * nothing downstream re-checks them for an aggregation, so {@code SHUFFLE_AGG} requires both
     * here.
     */
    private boolean isSatisfyHashShuffle(HashDistributionSpec hashSpec) {
        HashDistributionDesc.SourceType sourceType =
                hashSpec.getHashDistributionDesc().getSourceType();
        if (sourceType != HashDistributionDesc.SourceType.SHUFFLE_JOIN
                && sourceType != HashDistributionDesc.SourceType.SHUFFLE_AGG) {
            return false;
        }
        // NULL keys form a group, and a NULL-padded key has NULL rows in every ColocateRange. An aggregation over it
        // needs a null-strict cover even when its requirement was relaxed (an anti join above relaxes it, yet keeps
        // the NULL groups).
        boolean nullStrictCover = sourceType == HashDistributionDesc.SourceType.SHUFFLE_AGG;
        List<DistributionCol> requiredCols =
                hashSpec.getHashDistributionDesc().getDistributionCols();
        for (DistributionCol colocateCol : colocateColumns) {
            boolean covered = requiredCols.stream()
                    .map(req -> nullStrictCover ? req.getNullStrictCol() : req)
                    .anyMatch(req -> equivalentDescriptor.isConnected(req, colocateCol));
            if (!covered) {
                return false;
            }
        }
        if (sourceType == HashDistributionDesc.SourceType.SHUFFLE_AGG) {
            return !equivalentDescriptor.isEmptyPartition() && isGroupStable();
        }
        return true;
    }

    private boolean isGroupStable() {
        ColocateTableIndex index = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId groupId = index.getGroup(equivalentDescriptor.getTableId());
        return groupId != null && !index.isGroupUnstable(groupId);
    }

    /**
     * Null-relaxed variant (colocate columns rebuilt with {@code nullStrict=false}),
     * mirroring {@code HashDistributionSpec.getNullRelaxSpec}. Used to derive the
     * output distribution of a full-outer range-colocate join, where either side
     * may be NULL-padded.
     */
    public RangeDistributionSpec getNullRelaxSpec(EquivalentDescriptor descriptor) {
        List<DistributionCol> relaxed = colocateColumns.stream()
                .map(c -> new DistributionCol(c.getColId(), false))
                .collect(Collectors.toUnmodifiableList());
        return new RangeDistributionSpec(relaxed, descriptor);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RangeDistributionSpec)) {
            return false;
        }
        RangeDistributionSpec other = (RangeDistributionSpec) obj;
        // equivalentDescriptor is mutable; exclude from equality to keep memo hash-key stable.
        // Identity is (DistributionType.RANGE_LOCAL, tableId, colocateColumns) — mirrors HashDistributionSpec.
        return equivalentDescriptor.getTableId() == other.equivalentDescriptor.getTableId()
                && colocateColumns.equals(other.colocateColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(DistributionType.RANGE_LOCAL,
                equivalentDescriptor.getTableId(), colocateColumns);
    }

    @Override
    public String toString() {
        return "RANGE(" + colocateColumns + ")";
    }
}
