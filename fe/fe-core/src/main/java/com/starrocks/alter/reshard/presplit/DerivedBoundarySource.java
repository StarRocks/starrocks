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

/**
 * Produces split boundaries from what is already known about the sort key's own value domain,
 * reading no data at all — neither file statistics like the meta tier nor a row sample like the
 * data tier. {@link DefaultPreSplitPipeline#forDerivedBoundaries} builds a pipeline around one,
 * and that pipeline never reaches a sampler.
 */
@FunctionalInterface
interface DerivedBoundarySource {

    /**
     * @param indexTarget          the index whose single tablet is being carved; the cuts must be
     *                             expressed in its sort key
     * @param requestedTabletCount tablet count the caller's byte-based sizing asked for; a source may
     *                             derive fewer cuts than that when the key domain cannot carry them
     */
    Result plan(IndexPreSplitTarget indexTarget, int requestedTabletCount);

    /**
     * Either the derived cuts or the reason there are none: exactly one of the two components is
     * non-null.
     *
     * <p>A bare {@link BoundaryPlannerResult} cannot carry that reason — it only knows whether cuts
     * exist. Reporting "no cuts" by returning an empty {@code Optional} from the pipeline instead
     * makes {@link TabletPreSplitCoordinator} record the generic {@link SkipReason#NO_USEFUL_CUTS},
     * which would hide what actually stopped the derivation (an id space already in use, an estimate
     * too small to carve usefully, ...). Carrying the reason here is what keeps the skip metric
     * diagnosable.
     */
    record Result(BoundaryPlannerResult boundaries, SkipReason skipReason) {

        static Result of(BoundaryPlannerResult boundaries) {
            return new Result(boundaries, null);
        }

        static Result skipped(SkipReason skipReason) {
            return new Result(null, skipReason);
        }
    }
}
