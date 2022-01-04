// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

public class StatisticsEstimateCoefficient {
    // Estimated parameters for multiple join on predicates when predicate correlation is not known
    public static final double UNKNOWN_AUXILIARY_FILTER_COEFFICIENT = 0.9;
    // Group by columns correlation in estimate aggregates row count
    public static final double UNKNOWN_GROUP_BY_CORRELATION_COEFFICIENT = 0.75;
    // estimate aggregates row count with default group by columns statistics
    public static final double DEFAULT_GROUP_BY_CORRELATION_COEFFICIENT = 0.5;
    // expand estimate aggregates row count with default group by columns statistics
    public static final double DEFAULT_GROUP_BY_EXPAND_COEFFICIENT = 1.05;
    // IN predicate default filter rate
    public static final double IN_PREDICATE_DEFAULT_FILTER_COEFFICIENT = 0.5;
    // Is null predicate default filter rate
    public static final double IS_NULL_PREDICATE_DEFAULT_FILTER_COEFFICIENT = 0.1;
    // unknown filter coefficient for now
    public static final double PREDICATE_UNKNOWN_FILTER_COEFFICIENT = 0.25;
    // constant value compare constant value filter coefficient
    public static final double CONSTANT_TO_CONSTANT_PREDICATE_COEFFICIENT = 0.5;
    // coefficient of overlap percent which overlap range is infinite
    public static final double OVERLAP_INFINITE_RANGE_FILTER_COEFFICIENT = 0.5;
    // used in compute extra cost for multi distinct function, estimate whether to trigger streaming
    public static final double STREAMING_EXTRA_COST_THRESHOLD_COEFFICIENT = 0.8;
}
