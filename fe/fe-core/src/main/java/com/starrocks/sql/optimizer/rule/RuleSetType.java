// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
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
    PRUNE_SET_OPERATOR,
    PRUNE_PROJECT,
    COLLECT_CTE,
    INLINE_CTE,
    INTERSECT_REWRITE,
    SINGLE_TABLE_MV_REWRITE,
    MULTI_TABLE_MV_REWRITE,
<<<<<<< HEAD
    NUM_RULE_SET;
=======
    PRUNE_EMPTY_OPERATOR,
    NUM_RULE_SET
>>>>>>> fc74a4dd60 ([Enhancement] Fix the checkstyle of semicolons (#33130))
}
