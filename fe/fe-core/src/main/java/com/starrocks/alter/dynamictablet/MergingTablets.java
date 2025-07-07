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

package com.starrocks.alter.dynamictablet;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

/*
 * MergingTablets saves the old and new tablets during tablet merging for a materialized index
 */
public class MergingTablets implements DynamicTablets {

    @SerializedName(value = "mergingTablets")
    private List<Pair<List<Long>, Tablet>> mergingTablets = new ArrayList<>();

    public MergingTablets() {
    }

    @Override
    public void addMergingTablet(List<Long> oldTabletIds, Tablet newTablet) {
        // Old tablet size is usaully 2, but we allow a power of 2
        Preconditions.checkState(
                oldTabletIds.size() > 0 && (oldTabletIds.size() & (oldTabletIds.size() - 1)) == 0,
                "Old tablet size must be a power of 2, actual: " + oldTabletIds.size());

        mergingTablets.add(Pair.create(oldTabletIds, newTablet));
    }

    @Override
    public List<Pair<List<Long>, Tablet>> getMergingTablets() {
        return mergingTablets;
    }

    @Override
    public Set<Long> getOldTabletIds() {
        Set<Long> oldTabletsIds = new HashSet<>();
        for (Pair<List<Long>, Tablet> mergingTablet : mergingTablets) {
            for (Long id : mergingTablet.first) {
                Preconditions.checkState(
                        oldTabletsIds.add(id),
                        "Duplicated old tablet: " + id);
            }
        }
        return oldTabletsIds;
    }

    @Override
    public List<Tablet> getNewTablets() {
        List<Tablet> newTablets = new ArrayList<>();
        for (Pair<List<Long>, Tablet> mergingTablet : mergingTablets) {
            newTablets.add(mergingTablet.second);
        }
        return newTablets;
    }

    @Override
    public boolean isEmpty() {
        return mergingTablets.isEmpty();
    }

    @Override
    public void clear() {
        mergingTablets.clear();
    }

    @Override
    public List<Long> calcNewVirtualBuckets(List<Long> oldVirtualBuckets) {
        Map<Long, Tablet> oldToNewTablets = new HashMap<>();
        for (Pair<List<Long>, Tablet> mergingTablet : mergingTablets) {
            for (Long id : mergingTablet.first) {
                Preconditions.checkState(
                        oldToNewTablets.put(id, mergingTablet.second) == null,
                        "Duplicated old tablet: " + id);
            }
        }

        List<Long> newVirtualBuckets = new ArrayList<>(oldVirtualBuckets);

        // Replace old tablet id with new tablet id in new virtual buckets
        for (ListIterator<Long> it = newVirtualBuckets.listIterator(); it.hasNext(); /* */) {
            Tablet newTablet = oldToNewTablets.get(it.next());
            if (newTablet == null) {
                continue;
            }

            it.set(newTablet.getId());
        }

        // Try to half new virtual buckets
        while ((newVirtualBuckets.size() & 1) == 0) {
            int mid = newVirtualBuckets.size() / 2;
            List<Long> frontHalfList = newVirtualBuckets.subList(0, mid);
            List<Long> backHalfList = newVirtualBuckets.subList(mid, newVirtualBuckets.size());
            if (frontHalfList.equals(backHalfList)) {
                newVirtualBuckets = frontHalfList;
            } else {
                break;
            }
        }

        return newVirtualBuckets;
    }

    @Override
    public void addSplittingTablet(long oldTabletId, List<Tablet> newTablets) {
        throw new UnsupportedOperationException("Unimplemented method 'addSplittingTablet'");
    }

    @Override
    public Map<Long, List<Tablet>> getSplittingTablets() {
        return null;
    }
}
