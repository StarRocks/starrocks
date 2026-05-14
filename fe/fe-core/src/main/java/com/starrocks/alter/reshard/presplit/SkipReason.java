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
    NOT_RANGE_DISTRIBUTION,
    TABLE_NOT_NORMAL,
    HAS_MATERIALIZED_VIEW_OR_ROLLUP,
    MULTIPLE_BASE_INDEX_TABLETS,
    PARTITION_NOT_EMPTY,
    UNSUPPORTED_SORT_KEY,
    /** Catalog snapshot did not resolve the partition or base index — load surfaces this error. */
    METADATA_NOT_RESOLVED,
}
