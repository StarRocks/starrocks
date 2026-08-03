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

package com.starrocks.sql.optimizer;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.connector.BucketProperty;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/**
 * Decides whether an Iceberg scan should refuse to advertise its bucket distribution for a
 * shuffle-aggregation requirement because too few buckets survive pruning to feed the cluster.
 * <p>
 * Bucket-aware execution derives the scan's distribution purely from table metadata, so a
 * predicate that prunes to a handful of buckets (e.g. an equality filter on the bucket source
 * column) silently caps the one-stage aggregation's parallelism at the number of surviving
 * buckets. When that number is small relative to the cluster and the aggregation output is
 * non-trivial, a shuffle (multi-stage) aggregation is preferable.
 * <p>
 * See OutputPropertyDeriver#visitPhysicalIcebergScan.
 */
public final class LakeBucketAwareAggFallback {

    private LakeBucketAwareAggFallback() {
    }

    public static boolean shouldFallbackToShuffle(HashDistributionDesc requiredDesc,
                                                  List<BucketProperty> bucketProperties,
                                                  Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                                  ScalarOperator scanPredicate,
                                                  Statistics statistics,
                                                  SessionVariable sv,
                                                  int aliveWorkerNum) {
        if (requiredDesc.getSourceType() != HashDistributionDesc.SourceType.SHUFFLE_AGG) {
            return false;
        }
        double minBucketsPerWorker = sv.getLakeBucketAwareMinBucketsPerWorker();
        if (minBucketsPerWorker <= 0 || aliveWorkerNum <= 0 || statistics == null || bucketProperties.isEmpty()) {
            return false;
        }
        double survivingBuckets = estimateSurvivingBuckets(
                bucketProperties, colRefToColumnMetaMap, scanPredicate, statistics);
        if (survivingBuckets >= minBucketsPerWorker * aliveWorkerNum) {
            return false;
        }
        // Few buckets survive: the one-stage plan caps aggregation parallelism at survivingBuckets
        // serial streams. Keep it only when the aggregation output is small enough that those
        // streams stay cheap; with unknown grouping statistics assume the worst.
        int totalDop = aliveWorkerNum * Math.max(1, sv.getPipelineDop());
        OptionalDouble groupCount = estimateGroupCount(requiredDesc, colRefToColumnMetaMap, scanPredicate, statistics);
        return groupCount.isEmpty() || groupCount.getAsDouble() > totalDop;
    }

    private static double estimateSurvivingBuckets(List<BucketProperty> bucketProperties,
                                                   Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                                   ScalarOperator scanPredicate,
                                                   Statistics statistics) {
        double total = 1;
        for (BucketProperty bp : bucketProperties) {
            double bound = bp.getBucketNum();
            ColumnRefOperator ref = findColumnRef(bp.getColumn(), colRefToColumnMetaMap);
            if (ref != null) {
                ColumnStatistic cs = statistics.getColumnStatistics().get(ref);
                if (cs != null && !cs.isUnknown()) {
                    bound = Math.min(bound, Math.max(1, Math.ceil(cs.getDistinctValuesCount())));
                }
                OptionalLong predicateBound = boundFromPredicate(ref, scanPredicate);
                if (predicateBound.isPresent()) {
                    bound = Math.min(bound, Math.max(1, predicateBound.getAsLong()));
                }
            }
            total *= bound;
            if (total >= Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
        }
        return total;
    }

    // Same by-name matching as OutputPropertyDeriver#computeLakeHashDistributionDesc.
    private static ColumnRefOperator findColumnRef(Column column,
                                                   Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        for (Map.Entry<ColumnRefOperator, Column> entry : colRefToColumnMetaMap.entrySet()) {
            if (entry.getKey().getName().equals(column.getName())) {
                return entry.getKey();
            }
        }
        return null;
    }

    // Deterministic bucket bound from `col = const` / `col IN (const, ...)` conjuncts; usable
    // when statistics are missing entirely. Empty when the predicate does not constrain col.
    private static OptionalLong boundFromPredicate(ColumnRefOperator ref, ScalarOperator predicate) {
        if (predicate == null) {
            return OptionalLong.empty();
        }
        long best = Long.MAX_VALUE;
        for (ScalarOperator conjunct : Utils.extractConjuncts(predicate)) {
            if (conjunct instanceof BinaryPredicateOperator binary
                    && binary.getBinaryType() == BinaryType.EQ
                    && binary.getChild(0).equals(ref)
                    && binary.getChild(1).isConstantRef()) {
                best = Math.min(best, 1);
            } else if (conjunct instanceof InPredicateOperator in
                    && !in.isNotIn()
                    && in.getChild(0).equals(ref)
                    && in.allValuesMatch(ScalarOperator::isConstantRef)) {
                best = Math.min(best, in.getChildren().size() - 1L);
            }
        }
        return best == Long.MAX_VALUE ? OptionalLong.empty() : OptionalLong.of(best);
    }

    // Group count of the required (group-by) columns; empty (treated as high) when a column has
    // neither a known statistic nor a deterministic predicate bound. Predicates on partition
    // columns are stripped before scan-statistics estimation (StatisticsCalculator
    // #removePartitionPredicate), so an equality on the bucket source column does not reduce its
    // NDV in the scan statistics — re-apply the deterministic bound here.
    private static OptionalDouble estimateGroupCount(HashDistributionDesc requiredDesc,
                                                     Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                                     ScalarOperator scanPredicate,
                                                     Statistics statistics) {
        List<ColumnRefOperator> refs = new ArrayList<>();
        Statistics.Builder adjusted = Statistics.buildFrom(statistics);
        for (DistributionCol col : requiredDesc.getDistributionCols()) {
            ColumnRefOperator ref = colRefToColumnMetaMap.keySet().stream()
                    .filter(r -> r.getId() == col.getColId()).findFirst().orElse(null);
            if (ref == null) {
                return OptionalDouble.empty();
            }
            ColumnStatistic cs = statistics.getColumnStatistics().get(ref);
            OptionalLong predicateBound = boundFromPredicate(ref, scanPredicate);
            if (cs == null || cs.isUnknown()) {
                if (predicateBound.isEmpty()) {
                    return OptionalDouble.empty();
                }
                adjusted.addColumnStatistic(ref, ColumnStatistic.builder()
                        .setDistinctValuesCount(predicateBound.getAsLong()).build());
            } else if (predicateBound.isPresent()) {
                adjusted.addColumnStatistic(ref, ColumnStatistic.buildFrom(cs)
                        .setDistinctValuesCount(Math.min(cs.getDistinctValuesCount(), predicateBound.getAsLong()))
                        .build());
            }
            refs.add(ref);
        }
        if (refs.isEmpty()) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(
                StatisticsCalculator.computeGroupByStatistics(refs, adjusted.build(), Maps.newHashMap()));
    }
}
