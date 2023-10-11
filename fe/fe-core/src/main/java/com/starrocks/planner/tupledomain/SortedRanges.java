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

package com.starrocks.planner.tupledomain;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

import java.util.Collection;
import java.util.TreeSet;

public class SortedRanges<C extends Comparable<C>> extends TreeSet<Range<C>> {
    public SortedRanges() {
        super((o1, o2) -> {
            if (!o1.hasLowerBound() && !o2.hasLowerBound()) {
                return 0;
            }
            if (!o1.hasLowerBound()) {
                return -1;
            }
            if (!o2.hasLowerBound()) {
                return 1;
            }
            int comp = o1.lowerEndpoint().compareTo(o2.lowerEndpoint());
            if (comp != 0) {
                return comp;
            }
            if (o1.lowerBoundType().equals(o2.lowerBoundType())) {
                return 0;
            }
            return (o1.lowerBoundType().equals(BoundType.CLOSED)) ? -1 : 1;
        });
    }

    public SortedRanges(Collection<Range<C>> c) {
        this();
        addAll(c);
    }

    public SortedRanges(Range<C> c) {
        this();
        add(c);
    }

    @Override
    public boolean add(Range<C> c) {
        if (contains(c)) {
            return false;
        }
        Range<C> floor = floor(c);
        if (floor != null && c.isConnected(floor)) {
            throw new IllegalArgumentException("sorted set cannot hold overlapping ranges: " + floor + " and " + c);
        }
        Range<C> ceil = ceiling(c);
        if (ceil != null && c.isConnected(ceil)) {
            throw new IllegalArgumentException("sorted set cannot hold overlapping ranges: " + ceil + " and " + c);
        }
        return super.add(c);
    }
}
