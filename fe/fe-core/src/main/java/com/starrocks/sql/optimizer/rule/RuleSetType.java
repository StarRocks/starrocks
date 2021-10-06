// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
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
}
