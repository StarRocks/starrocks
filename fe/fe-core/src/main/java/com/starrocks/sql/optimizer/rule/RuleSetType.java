// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.rule;

public enum RuleSetType {
    LOGICAL_TRANSFORMATION,
    PHYSICAL_IMPLEMENTATION,
    MERGE_LIMIT,
    PRUNE_COLUMNS,
    PARTITION_PRUNE,
    PUSH_DOWN_PREDICATE,
    SUBQUERY_REWRITE,
    PRUNE_ASSERT_ROW,
    MULTI_DISTINCT_REWRITE,
    AGGREGATE_REWRITE,
    PRUNE_SET_OPERATOR,
    PRUNE_PROJECT,
    COLLECT_CTE,
    INLINE_CTE,
    INLINE_ONE_CTE,
}
