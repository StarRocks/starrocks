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

package com.starrocks.alter.reshard.presplit;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Output of a boundary planner. The boundary list is collapsed (adjacent
 * duplicates removed) and immutable at the outer level; {@link Tuple} value
 * contents are NOT deep-copied, so the caller must pass stable Tuple instances.
 *
 * <p>{@link #getEffectiveTabletCount} can be smaller than the originally
 * requested count when duplicate cuts collapse; {@link #isNoSplit} means
 * "skip pre-split".
 */
public final class BoundaryPlannerResult {

    public static final BoundaryPlannerResult NO_SPLIT = new BoundaryPlannerResult(Collections.emptyList());

    private final List<Tuple> boundaries;

    /**
     * @param sortedCuts cuts in lex-sorted order. Duplicates are allowed and
     *                   will be collapsed; the input is not mutated.
     */
    public BoundaryPlannerResult(List<Tuple> sortedCuts) {
        Objects.requireNonNull(sortedCuts, "sortedCuts");
        this.boundaries = collapseAdjacentDuplicates(sortedCuts);
    }

    public List<Tuple> getBoundaries() {
        return boundaries;
    }

    public int getEffectiveTabletCount() {
        return boundaries.size() + 1;
    }

    public boolean isNoSplit() {
        return boundaries.isEmpty();
    }

    private static List<Tuple> collapseAdjacentDuplicates(List<Tuple> sortedCuts) {
        if (sortedCuts.isEmpty()) {
            return ImmutableList.of();
        }
        List<Tuple> collapsed = new ArrayList<>(sortedCuts.size());
        Tuple previous = null;
        for (Tuple cut : sortedCuts) {
            if (previous == null || cut.compareTo(previous) != 0) {
                collapsed.add(cut);
                previous = cut;
            }
        }
        return ImmutableList.copyOf(collapsed);
    }
}
