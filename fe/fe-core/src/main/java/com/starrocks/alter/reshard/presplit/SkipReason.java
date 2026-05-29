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
 * Reason {@link TabletPreSplitCoordinator#maybeAct} declined to pre-split.
 * One value per eligibility branch; each value is a stable label for the
 * skip-metric bvar so operators can tell at a glance why pre-split was not
 * exercised.
 */
public enum SkipReason {
    /** Operator-side feature gates ({@code enable_tablet_pre_split_for_*}) are off. */
    DISABLED_BY_CONFIG,
    /** Per-session opt-out ({@code SET enable_tablet_pre_split = false}). */
    DISABLED_BY_SESSION,
    /**
     * Target is not a cloud-native (shared-data) table. The reshard substrate
     * requires cloud-native, so this must be gated at the table level before the
     * multi-partition path pre-creates any partition that the factory would then
     * reject.
     */
    NOT_CLOUD_NATIVE,
    NOT_RANGE_DISTRIBUTION,
    TABLE_NOT_NORMAL,
    HAS_MATERIALIZED_VIEW_OR_ROLLUP,
    MULTIPLE_BASE_INDEX_TABLETS,
    PARTITION_NOT_EMPTY,
    UNSUPPORTED_SORT_KEY,
    /** Catalog snapshot did not resolve the partition or base index — load surfaces this error. */
    METADATA_NOT_RESOLVED,
    /** Pre-submit phase (sample + plan + build job) exceeded its timeout. */
    TIMEOUT_PRE_SUBMIT,
    /** Sampler or boundary planner failed for a non-timeout reason — load proceeds without pre-split. */
    SAMPLE_FAILED,
    /** Planner concluded no useful split exists (no distinct cuts above the minimum). */
    NO_USEFUL_CUTS,
    /** Submission of the reshard job to {@code TabletReshardJobMgr} failed (state check, journal, etc.). */
    SUBMIT_FAILED,
    /** Admitted reshard job entered a terminal-error state (CANCELLED, etc.) before reaching FINISHED. */
    JOB_FAILED_BEFORE_FINISH,
    /** A partition source column has a type the grouper cannot project into a SQL-parseable string. */
    UNSUPPORTED_PARTITION_COLUMN_TYPE,
    /** A sampled partition tuple could not be turned into a usable AddPartitionClause (formatter null or analyzer threw). */
    INVALID_PARTITION_VALUE,
    /** Grouping produced no usable target partitions (every row dropped). */
    GROUPER_EMPTY,
    /**
     * Catalog raced under the grouper: an existing partition was found by name
     * but its physical partition or base index was no longer available between
     * the {@code getPartition} hit and the index/tablet lookup. Distinct from
     * {@link #INVALID_PARTITION_VALUE} (which signals bad data); this signals
     * a stale catalog snapshot the load can retry against.
     */
    STALE_CATALOG_STATE,
    /**
     * Multi-partition path: {@link com.starrocks.server.LocalMetastore#addPartitions}
     * threw while pre-creating a target partition. Just this partition is
     * dropped from the combined submit; sibling partitions continue.
     */
    PRE_CREATE_FAILED,
    /**
     * Multi-partition path: after pre-create (or for an existing partition
     * re-resolved under the coordinator's READ lock), the partition no longer
     * meets per-partition eligibility — physical partition or base index
     * missing, multiple tablets, or non-empty. Caused by concurrent ALTER /
     * load between the grouper snapshot and the coordinator's re-resolve.
     */
    PARTITION_NOT_ELIGIBLE_POST_CREATE,
}
