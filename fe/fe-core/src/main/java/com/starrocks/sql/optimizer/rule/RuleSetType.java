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

package com.starrocks.sql.optimizer.rule;

public enum RuleSetType {
    LOGICAL_TRANSFORMATION,
    PHYSICAL_IMPLEMENTATION,
    MERGE_LIMIT,
    PRUNE_COLUMNS,
    PARTITION_PRUNE,
    PUSH_DOWN_PREDICATE,
    SUBQUERY_REWRITE_COMMON,
    SUBQUERY_REWRITE_TO_WINDOW,
    SUBQUERY_REWRITE_TO_JOIN,
    PUSH_DOWN_SUBQUERY,
    PRUNE_ASSERT_ROW,
    MULTI_DISTINCT_REWRITE,
    AGGREGATE_REWRITE,
    PRUNE_PROJECT,
    PRUNE_UKFK_JOIN,
    COLLECT_CTE,
    INLINE_CTE,
    INTERSECT_REWRITE,
    SINGLE_TABLE_MV_REWRITE,
    MULTI_TABLE_MV_REWRITE,
    ALL_MV_REWRITE,
    PRUNE_EMPTY_OPERATOR,
    SHORT_CIRCUIT_SET,
    META_SCAN_REWRITE,

    FINE_GRAINED_RANGE_PREDICATE,

    ELIMINATE_OP_WITH_CONSTANT,
    NUM_RULE_SET
}
