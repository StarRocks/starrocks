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
    // default mysql external table output rows
    public static final int DEFAULT_MYSQL_OUTPUT_ROWS = 10000;
    // default es external table output rows
    public static final int DEFAULT_ES_OUTPUT_ROWS = 5000;
    // default JDBC external table output rows, JDBC maybe is a distribute system
    public static final int DEFAULT_JDBC_OUTPUT_ROWS = 20000;
    // if after aggregate row count * DEFAULT_AGGREGATE_EFFECT_COEFFICIENT < input row count,
    // the aggregate has good effect.
    public static final double LOW_AGGREGATE_EFFECT_COEFFICIENT = 1000;
    public static final double MEDIUM_AGGREGATE_EFFECT_COEFFICIENT = 100;
    // default selectivity for anti join
    public static final double DEFAULT_ANTI_JOIN_SELECTIVITY_COEFFICIENT = 0.4;
    // default shuffle column row count limit
    public static final double DEFAULT_PRUNE_SHUFFLE_COLUMN_ROWS_LIMIT = 200000;

    public static final long TINY_SCALE_ROWS_LIMIT = 100000;

    // a small scale rows, such as default push down aggregate row count limit, 100w
    public static final long SMALL_SCALE_ROWS_LIMIT = 1000000;
    // default or predicate limit
    public static final int DEFAULT_OR_OPERATOR_LIMIT = 16;


    public static final double EXECUTE_COST_PENALTY = 2;
    public static final int BROADCAST_JOIN_MEM_EXCEED_PENALTY = 1000;

    public static final double MAXIMUM_COST = Double.MAX_VALUE / Math.pow(10, 50);

    public static final double MAXIMUM_ROW_COUNT = Double.MAX_VALUE / Math.pow(10, 100);

    public static final double MAXIMUM_OUTPUT_SIZE = Double.MAX_VALUE / Math.pow(10, 80);
}
