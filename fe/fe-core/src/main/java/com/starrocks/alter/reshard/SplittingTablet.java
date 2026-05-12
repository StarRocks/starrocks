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

package com.starrocks.alter.reshard;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.TabletRange;
import com.starrocks.proto.ReshardingTabletInfoPB;
import com.starrocks.proto.SplittingTabletInfoPB;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/*
 * SplittingTablet saves the old tablet id and the new tablets during a tablet splitting.
 *
 * When `newTabletRanges` is empty the BE follows the data-driven split path
 * (segment distribution computes the K-1 boundaries). When non-empty, the
 * external boundaries path applies on the BE: the FE-supplied
 * boundaries are honored verbatim and `newTabletRanges.size()` must equal
 * `newTabletIds.size()`. BE re-validates structural and schema-aware
 * invariants before committing.
 */
public class SplittingTablet implements ReshardingTablet {

    @SerializedName(value = "oldTabletId")
    protected final long oldTabletId;

    @SerializedName(value = "newTabletIds")
    protected final List<Long> newTabletIds;

    // Defaulted to empty list in the no-arg Gson constructor below so legacy
    // SplittingTablet records persisted before this field existed deserialize
    // to an empty list instead of null. Gson invokes the no-arg constructor
    // (when one exists, as here) and then reflection-sets only the fields
    // present in the JSON, so toProto / fallbackToIdenticalTablet never see
    // null even on an in-flight reshard job replayed from a pre-PR snapshot.
    @SerializedName(value = "newTabletRanges")
    protected final List<TabletRange> newTabletRanges;

    // Used only by Gson deserialization; delegates to the validated 3-arg
    // constructor so the same wrapping / preconditions apply. Gson then
    // reflection-sets each field present in the JSON, leaving newTabletRanges
    // at the empty default for legacy records that predate the field.
    private SplittingTablet() {
        this(0, Collections.emptyList(), Collections.emptyList());
    }

    public SplittingTablet(long oldTabletId, List<Long> newTabletIds) {
        this(oldTabletId, newTabletIds, Collections.emptyList());
    }

    public SplittingTablet(long oldTabletId, List<Long> newTabletIds, List<TabletRange> newTabletRanges) {
        this.oldTabletId = oldTabletId;
        this.newTabletIds = newTabletIds;
        Preconditions.checkNotNull(newTabletRanges, "newTabletRanges must not be null");
        Preconditions.checkArgument(newTabletRanges.stream().allMatch(Objects::nonNull),
                "newTabletRanges must not contain null elements");
        // Always wrap in a mutable ArrayList: fallbackToIdenticalTablet() clears
        // this list after a BE-side fallback, mirroring how newTabletIds is
        // mutated via subList(...).clear().
        this.newTabletRanges = new ArrayList<>(newTabletRanges);
        Preconditions.checkState(this.newTabletRanges.isEmpty()
                        || this.newTabletRanges.size() == newTabletIds.size(),
                "newTabletRanges.size=%s must equal newTabletIds.size=%s when set",
                this.newTabletRanges.size(), newTabletIds.size());
    }

    @Override
    public SplittingTablet getSplittingTablet() {
        return isIdenticalTablet() ? null : this;
    }

    @Override
    public MergingTablet getMergingTablet() {
        return null;
    }

    @Override
    public IdenticalTablet getIdenticalTablet() {
        return isIdenticalTablet() ? new IdenticalTablet(oldTabletId, newTabletIds.get(0)) : null;
    }

    public long getOldTabletId() {
        return oldTabletId;
    }

    @Override
    public long getFirstOldTabletId() {
        return oldTabletId;
    }

    @Override
    public long getFirstNewTabletId() {
        return newTabletIds.get(0);
    }

    @Override
    public List<Long> getOldTabletIds() {
        return List.of(oldTabletId);
    }

    @Override
    public List<Long> getNewTabletIds() {
        return newTabletIds;
    }

    public List<TabletRange> getNewTabletRanges() {
        return newTabletRanges;
    }

    @Override
    public long getParallelTablets() {
        return newTabletIds.size();
    }

    @Override
    public ReshardingTabletInfoPB toProto() {
        ReshardingTabletInfoPB reshardingTabletInfoPB = new ReshardingTabletInfoPB();
        reshardingTabletInfoPB.splittingTabletInfo = new SplittingTabletInfoPB();
        reshardingTabletInfoPB.splittingTabletInfo.oldTabletId = oldTabletId;
        reshardingTabletInfoPB.splittingTabletInfo.newTabletIds = newTabletIds;
        if (!newTabletRanges.isEmpty()) {
            reshardingTabletInfoPB.splittingTabletInfo.newTabletRanges =
                    newTabletRanges.stream().map(TabletRange::toProto).collect(Collectors.toList());
        }
        return reshardingTabletInfoPB;
    }

    public void fallbackToIdenticalTablet() {
        Preconditions.checkState(!newTabletIds.isEmpty());
        newTabletIds.subList(1, newTabletIds.size()).clear();
        // Drop stale FE-supplied ranges. After BE's identical fallback there is
        // a single inherited new tablet, not the K originally requested, so
        // the external-boundaries-style range list would be both mis-sized and meaningless.
        newTabletRanges.clear();
    }

    public boolean isIdenticalTablet() {
        return newTabletIds.size() == 1;
    }
}
