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
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

/*
 * SplittingTablets saves the old and new tablets during tablet splitting for a materialized index
 */
public class SplittingTablets implements DynamicTablets {
    @SerializedName(value = "splittingTablets")
    private final Map<Long, List<Tablet>> splittingTablets = new HashMap<>();

    public SplittingTablets() {
    }

    @Override
    public void addSplittingTablet(long oldTabletId, List<Tablet> newTablets) {
        // New tablet size is usaully 2, but we allow a power of 2
        Preconditions.checkState(
                newTablets.size() > 0 && (newTablets.size() & (newTablets.size() - 1)) == 0,
                "New tablet size must be a power of 2, actual: " + newTablets.size());

        Preconditions.checkState(
                splittingTablets.put(oldTabletId, newTablets) == null,
                "Duplicated old tablet: " + oldTabletId);
    }

    @Override
    public Map<Long, List<Tablet>> getSplittingTablets() {
        return splittingTablets;
    }

    @Override
    public Set<Long> getOldTabletIds() {
        return splittingTablets.keySet();
    }

    @Override
    public List<Tablet> getNewTablets() {
        List<Tablet> newTablets = new ArrayList<>();
        for (List<Tablet> tablets : splittingTablets.values()) {
            newTablets.addAll(tablets);
        }
        return newTablets;
    }

    @Override
    public boolean isEmpty() {
        return splittingTablets.isEmpty();
    }

    @Override
    public void clear() {
        splittingTablets.clear();
    }

    @Override
    public List<Long> calcNewVirtualBuckets(List<Long> oldVirtualBuckets) {
        // Temporary class
        class Context {
            int counter = 0;
            final List<Tablet> tablets;

            Context(List<Tablet> tablets) {
                this.tablets = tablets;
            }
        }

        // Prepare splitting tablet context
        Map<Long, Context> idToContext = new HashMap<>();
        for (Map.Entry<Long, List<Tablet>> entry : splittingTablets.entrySet()) {
            idToContext.put(entry.getKey(), new Context(entry.getValue()));
        }

        // Calculate old virtual bucket number for each splitting tablet
        for (Long id : oldVirtualBuckets) {
            Context context = idToContext.get(id);
            if (context == null) {
                continue;
            }

            ++context.counter;
        }

        // Calculate the times that old virtual bucket number needs to be multiplied
        long maxTimes = 0;
        for (Map.Entry<Long, Context> entry : idToContext.entrySet()) {
            Context context = entry.getValue();
            Preconditions.checkState(
                    context.counter > 0,
                    "Cannot find tablet " + entry.getKey() + " in virtual buckets: " + oldVirtualBuckets);
            long times = context.tablets.size() / context.counter;
            if (times > maxTimes) {
                maxTimes = times;
            }
            // Reset counter
            context.counter = 0;
        }

        // Caculate new virtual buckets
        List<Long> newVirtualBuckets = new ArrayList<>(oldVirtualBuckets);
        for (long i = 2; i <= maxTimes; i *= 2) {
            // Double
            newVirtualBuckets.addAll(newVirtualBuckets);
        }

        // Replace old tablet id with new tablet id in new virtual buckets
        for (ListIterator<Long> it = newVirtualBuckets.listIterator(); it.hasNext(); /* */) {
            Context context = idToContext.get(it.next());
            if (context == null) {
                continue;
            }

            it.set(context.tablets.get(context.counter).getId());
            // Round robin
            if (++context.counter >= context.tablets.size()) {
                context.counter = 0;
            }
        }

        return newVirtualBuckets;
    }

    @Override
    public void addMergingTablet(List<Long> oldTabletIds, Tablet newTablet) {
        throw new UnsupportedOperationException("Unimplemented method 'addMergingTablet'");
    }

    @Override
    public List<Pair<List<Long>, Tablet>> getMergingTablets() {
        return null;
    }
}
