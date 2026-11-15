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

import com.starrocks.planner.ScanNode;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Reads a statement's estimated output size out of the plan the optimizer already built.
 *
 * <p>The sampling tiers learn their input size from the sample itself (file footers, an Iceberg
 * snapshot, a source table's data size). A derived-tier boundary source reads no data at all, so it
 * has no sample to learn from and needs the estimate from somewhere else. The plan root's statistics
 * are that somewhere: they are already computed, cost nothing to read, and describe the rows this
 * statement will actually write rather than the rows some source happens to hold.
 *
 * <p>Accuracy only steers how many tablets to carve, so a rough estimate is fine and a missing one is
 * not fatal — the caller skips pre-split and the load runs exactly as it does today.
 */
public final class PreSplitEstimates {

    private static final Logger LOG = LogManager.getLogger(PreSplitEstimates.class);

    private PreSplitEstimates() {
    }

    /**
     * @return the statement's estimated output rows and bytes, or {@link Estimates#ZERO} when the plan
     *         carries no usable estimate.
     */
    public static Estimates fromExecPlan(ExecPlan execPlan) {
        if (execPlan == null) {
            return Estimates.ZERO;
        }
        // Every other pre-split entry point is wrapped in a catch-all because the feature must never
        // turn a load that would have succeeded into one that fails. This one is read as an argument at
        // its call site, outside any such wrapper, so it carries its own: a missing estimate costs a
        // skipped pre-split, while a throw escaping here would cost the whole INSERT OVERWRITE.
        try {
            Estimates fromRoot = fromRootStatistics(execPlan.getPhysicalPlan());
            return fromRoot != null ? fromRoot : fromScanNodes(execPlan);
        } catch (Throwable unusable) {
            LOG.warn("Sample-Based Tablet Pre-Split: could not read an output estimate from the plan; "
                    + "the load proceeds without pre-split", unusable);
            return Estimates.ZERO;
        }
    }

    /**
     * The estimate for what the statement writes. Both numbers come from the same {@link Statistics}
     * object so the implied average row size is self-consistent.
     *
     * <p>{@code Statistics} clamps its row count to at least 1 (see {@code clampOutputRowCount}), so a
     * present root always yields a usable estimate and there is no "root says zero" case to handle. A
     * statement whose real output is tiny therefore estimates as tiny, which the caller's own tablet
     * sizing already declines to split.
     *
     * @return {@code null} only when there is no root or no statistics on it, so the caller can fall
     *         back.
     */
    private static Estimates fromRootStatistics(OptExpression root) {
        if (root == null || root.getStatistics() == null) {
            return null;
        }
        Statistics statistics = root.getStatistics();
        // Both are clamped: Estimates rejects a negative component outright, and a plan is not worth
        // failing a load over.
        return new Estimates(Math.max(0L, (long) statistics.getComputeSize()),
                Math.max(0L, (long) statistics.getOutputRowCount()));
    }

    /**
     * Fallback for a plan whose root statistics are missing: sum what the scans expect to read. It
     * over-estimates whatever the statement filters out, which is the safe direction — an
     * over-estimate leaves some pre-split tablets empty for the background merge to reclaim, while an
     * under-estimate concentrates rows in one of them.
     *
     * <p>{@code getCardinality()} is -1 until the planner sets it, so a non-positive total yields
     * {@link Estimates#ZERO} rather than a bogus estimate.
     */
    private static Estimates fromScanNodes(ExecPlan execPlan) {
        long rows = 0;
        long bytes = 0;
        for (ScanNode scanNode : execPlan.getScanNodes()) {
            long cardinality = scanNode.getCardinality();
            if (cardinality <= 0) {
                continue;
            }
            rows += cardinality;
            bytes += (long) (cardinality * Math.max(1.0f, scanNode.getAvgRowSize()));
        }
        return rows > 0 ? new Estimates(bytes, rows) : Estimates.ZERO;
    }
}
