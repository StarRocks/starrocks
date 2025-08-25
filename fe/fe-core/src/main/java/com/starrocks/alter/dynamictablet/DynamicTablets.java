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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/*
 * DynamicTablets saves the old and new tabletIds during tablet splitting or merging for a materialized index
 */
public class DynamicTablets {

    @SerializedName(value = "splittingTablets")
    protected final List<SplittingTablet> splittingTablets;

    @SerializedName(value = "mergingTablets")
    protected final List<MergingTablet> mergingTablets;

    @SerializedName(value = "identicalTablets")
    protected final List<IdenticalTablet> identicalTablets;

    public DynamicTablets(
            List<SplittingTablet> splittingTablets,
            List<MergingTablet> mergingTablets,
            List<IdenticalTablet> identicalTablets) {
        this.splittingTablets = splittingTablets;
        this.mergingTablets = mergingTablets;
        this.identicalTablets = identicalTablets;
    }

    public List<SplittingTablet> getSplittingTablets() {
        return splittingTablets;
    }

    public List<MergingTablet> getMergingTablets() {
        return mergingTablets;
    }

    public List<IdenticalTablet> getIdenticalTablets() {
        return identicalTablets;
    }

    public List<Long> getOldTabletIds() {
        List<Long> oldTabletsIds = new ArrayList<>(
                splittingTablets.size() + mergingTablets.size() * 2 + identicalTablets.size());
        for (SplittingTablet splittingTablet : splittingTablets) {
            oldTabletsIds.add(splittingTablet.getOldTabletId());
        }
        for (MergingTablet mergingTablet : mergingTablets) {
            oldTabletsIds.addAll(mergingTablet.getOldTabletIds());
        }
        for (IdenticalTablet identicalTablet : identicalTablets) {
            oldTabletsIds.add(identicalTablet.getOldTabletId());
        }
        return oldTabletsIds;
    }

    public List<Long> getNewTabletIds() {
        List<Long> newTabletIds = new ArrayList<>(
                splittingTablets.size() * 2 + mergingTablets.size() + identicalTablets.size());
        for (SplittingTablet splittingTablet : splittingTablets) {
            newTabletIds.addAll(splittingTablet.getNewTabletIds());
        }
        for (MergingTablet mergingTablet : mergingTablets) {
            newTabletIds.add(mergingTablet.getNewTabletId());
        }
        for (IdenticalTablet identicalTablet : identicalTablets) {
            newTabletIds.add(identicalTablet.getNewTabletId());
        }
        return newTabletIds;
    }

    public long getParallelTablets() {
        long parallelTablets = 0;
        for (SplittingTablet splittingTablet : splittingTablets) {
            parallelTablets += splittingTablet.getParallelTablets();
        }
        for (MergingTablet mergingTablet : mergingTablets) {
            parallelTablets += mergingTablet.getParallelTablets();
        }
        for (IdenticalTablet identicalTablet : identicalTablets) {
            parallelTablets += identicalTablet.getParallelTablets();
        }
        return parallelTablets;
    }

    boolean isEmpty() {
        return splittingTablets.isEmpty() && mergingTablets.isEmpty() && identicalTablets.isEmpty();
    }

    public List<Long> calcNewVirtualBuckets(List<Long> oldVirtualBuckets) {
        // Temporary class
        class Context {
            int counter = 0;
            final List<Long> tabletIds;

            Context(List<Long> tabletIds) {
                this.tabletIds = tabletIds;
            }
        }

        // Prepare splitting tablet context
        Map<Long, Context> tabletIdToContext = new HashMap<>();
        for (SplittingTablet splittingTablet : splittingTablets) {
            tabletIdToContext.put(splittingTablet.getOldTabletId(), new Context(splittingTablet.getNewTabletIds()));
        }
        for (MergingTablet mergingTablet : mergingTablets) {
            for (Long tabletId : mergingTablet.getOldTabletIds()) {
                Preconditions.checkState(
                        tabletIdToContext.put(tabletId, new Context(List.of(mergingTablet.getNewTabletId()))) == null,
                        "Duplicated old tablet: " + tabletId);
            }
        }
        for (IdenticalTablet identicalTablet : identicalTablets) {
            long tabletId = identicalTablet.getOldTabletId();
            Preconditions.checkState(
                    tabletIdToContext.put(tabletId, new Context(List.of(identicalTablet.getNewTabletId()))) == null,
                    "Duplicated old tablet: " + tabletId);
        }

        // Calculate old virtual bucket number for each splitting tablet
        for (Long id : oldVirtualBuckets) {
            Context context = tabletIdToContext.get(id);
            Preconditions.checkState(context != null,
                    "Cannot find tablet " + id + " in virtual buckets: " + oldVirtualBuckets);

            ++context.counter;
        }

        // Calculate the times that old virtual bucket number needs to be multiplied
        int maxTimes = 0;
        for (Map.Entry<Long, Context> entry : tabletIdToContext.entrySet()) {
            Context context = entry.getValue();
            Preconditions.checkState(
                    context.counter > 0,
                    "Cannot find tablet " + entry.getKey() + " in virtual buckets: " + oldVirtualBuckets);
            int times = context.tabletIds.size() / context.counter;
            if (times > maxTimes) {
                maxTimes = times;
            }
            // Reset counter
            context.counter = 0;
        }

        // Caculate new virtual buckets
        List<Long> newVirtualBuckets = new ArrayList<>(oldVirtualBuckets);
        for (int i = 2; i <= maxTimes; i *= 2) {
            // Double
            newVirtualBuckets.addAll(newVirtualBuckets);
        }

        // Replace old tablet id with new tablet id in new virtual buckets
        for (ListIterator<Long> it = newVirtualBuckets.listIterator(); it.hasNext(); /* */) {
            Context context = tabletIdToContext.get(it.next());

            it.set(context.tabletIds.get(context.counter));
            // Round robin
            if (++context.counter >= context.tabletIds.size()) {
                context.counter = 0;
            }
        }

        // Try to half new virtual buckets
        if (!mergingTablets.isEmpty()) {
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
        }

        return newVirtualBuckets;
    }
}
