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

import com.starrocks.catalog.Column;
import com.starrocks.connector.BucketProperty;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.thrift.TBucketFunction;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LakeBucketAwareAggFallbackTest {
    private final Column projectIdCol = new Column("project_id", IntegerType.BIGINT);
    private final ColumnRefOperator projectIdRef = new ColumnRefOperator(1, IntegerType.BIGINT, "project_id", true);
    private final ColumnRefOperator emailRef = new ColumnRefOperator(2, VarcharType.VARCHAR, "email", true);
    private final List<BucketProperty> buckets16 =
            List.of(new BucketProperty(TBucketFunction.MURMUR3_X86_32, 16, projectIdCol));
    private final Map<ColumnRefOperator, Column> colMap = Map.of(
            projectIdRef, projectIdCol, emailRef, new Column("email", VarcharType.VARCHAR));
    private final HashDistributionDesc aggRequire = new HashDistributionDesc(
            List.of(new DistributionCol(1, true), new DistributionCol(2, true)),
            HashDistributionDesc.SourceType.SHUFFLE_AGG);

    private Statistics stats(double rows, double projectNdv, double emailNdv) {
        return Statistics.builder().setOutputRowCount(rows)
                .addColumnStatistic(projectIdRef, ColumnStatistic.builder()
                        .setDistinctValuesCount(projectNdv).setAverageRowSize(8).setNullsFraction(0).build())
                .addColumnStatistic(emailRef, ColumnStatistic.builder()
                        .setDistinctValuesCount(emailNdv).setAverageRowSize(20).setNullsFraction(0).build())
                .build();
    }

    private Statistics unknownStats() {
        return Statistics.builder().setOutputRowCount(1_000_000)
                .addColumnStatistic(projectIdRef, ColumnStatistic.unknown())
                .addColumnStatistic(emailRef, ColumnStatistic.unknown())
                .build();
    }

    private SessionVariable sv() {
        return new SessionVariable();
    }

    @Test
    public void fallbackWhenOneBucketAndHighNdv() {
        // post-predicate statistics: equality already reduced project_id NDV to 1
        assertTrue(LakeBucketAwareAggFallback.shouldFallbackToShuffle(
                aggRequire, buckets16, colMap, null, stats(1_000_000, 1, 500_000), sv(), 4, 1));
    }

    @Test
    public void keepWhenManyBucketsSurvive() {
        assertFalse(LakeBucketAwareAggFallback.shouldFallbackToShuffle(
                aggRequire, buckets16, colMap, null, stats(10_000_000, 1000, 500_000), sv(), 4, 1));
    }

    @Test
    public void keepWhenLowGroupCount() {
        HashDistributionDesc groupByBucketColOnly = new HashDistributionDesc(
                List.of(new DistributionCol(1, true)), HashDistributionDesc.SourceType.SHUFFLE_AGG);
        assertFalse(LakeBucketAwareAggFallback.shouldFallbackToShuffle(
                groupByBucketColOnly, buckets16, colMap, null, stats(1_000_000, 1, 500_000), sv(), 4, 1));
    }

    @Test
    public void keepWhenPredicatePinsGroupingColumn() {
        // Statistics keep the table-level NDV (partition predicates are stripped before scan
        // stats estimation), but the equality predicate pins the grouping column to one value.
        // The aggregation is tiny, so the one-stage plan stays.
        HashDistributionDesc groupByBucketColOnly = new HashDistributionDesc(
                List.of(new DistributionCol(1, true)), HashDistributionDesc.SourceType.SHUFFLE_AGG);
        ScalarOperator eq = new BinaryPredicateOperator(BinaryType.EQ,
                projectIdRef, ConstantOperator.createBigint(100));
        assertFalse(LakeBucketAwareAggFallback.shouldFallbackToShuffle(
                groupByBucketColOnly, buckets16, colMap, eq, stats(10_000_000, 1000, 500_000), sv(), 4, 1));
    }

    @Test
    public void fallbackOnPinnedBucketWithHighNdvGrouping() {
        // Production shape of the bug: table-level NDV survives on the bucket column, the
        // equality predicate proves a single surviving bucket, grouping includes high-NDV email.
        ScalarOperator eq = new BinaryPredicateOperator(BinaryType.EQ,
                projectIdRef, ConstantOperator.createBigint(100));
        assertTrue(LakeBucketAwareAggFallback.shouldFallbackToShuffle(
                aggRequire, buckets16, colMap, eq, stats(10_000_000, 1000, 500_000), sv(), 4, 1));
    }

    @Test
    public void fallbackOnEqualityPredicateWithoutStats() {
        ScalarOperator eq = new BinaryPredicateOperator(BinaryType.EQ,
                projectIdRef, ConstantOperator.createBigint(100));
        assertTrue(LakeBucketAwareAggFallback.shouldFallbackToShuffle(
                aggRequire, buckets16, colMap, eq, unknownStats(), sv(), 4, 1));
    }

    @Test
    public void keepWithoutStatsAndWithoutPruningPredicate() {
        assertFalse(LakeBucketAwareAggFallback.shouldFallbackToShuffle(
                aggRequire, buckets16, colMap, null, unknownStats(), sv(), 4, 1));
    }

    @Test
    public void inPredicateBoundsSurvivingBuckets() {
        ScalarOperator in = new InPredicateOperator(false, projectIdRef,
                ConstantOperator.createBigint(1), ConstantOperator.createBigint(2));
        // B = 2 < 4 workers, grouping NDV unknown -> conservative fallback
        assertTrue(LakeBucketAwareAggFallback.shouldFallbackToShuffle(
                aggRequire, buckets16, colMap, in, unknownStats(), sv(), 4, 1));
    }

    @Test
    public void joinRequirementNeverFallsBack() {
        HashDistributionDesc joinRequire = new HashDistributionDesc(
                List.of(new DistributionCol(1, true)), HashDistributionDesc.SourceType.SHUFFLE_JOIN);
        assertFalse(LakeBucketAwareAggFallback.shouldFallbackToShuffle(
                joinRequire, buckets16, colMap, null, stats(1_000_000, 1, 500_000), sv(), 4, 1));
    }

    @Test
    public void ratioZeroDisablesFallback() {
        SessionVariable sv = sv();
        sv.setLakeBucketAwareMinBucketsPerWorker(0);
        assertFalse(LakeBucketAwareAggFallback.shouldFallbackToShuffle(
                aggRequire, buckets16, colMap, null, stats(1_000_000, 1, 500_000), sv, 4, 1));
    }

    @Test
    public void highEffectiveDopKeepsModerateGroupCountsOneStage() {
        // groups (~1000 for the bucket column after damping) exceed the worker count but fit
        // within workers * effective DOP, so the one-stage plan stays
        HashDistributionDesc groupByBucketColOnly = new HashDistributionDesc(
                List.of(new DistributionCol(1, true)), HashDistributionDesc.SourceType.SHUFFLE_AGG);
        Statistics oneBucketStats = stats(10_000_000, 1000, 500_000);
        assertTrue(LakeBucketAwareAggFallback.shouldFallbackToShuffle(
                groupByBucketColOnly,
                List.of(new BucketProperty(TBucketFunction.MURMUR3_X86_32, 2, projectIdCol)),
                colMap, null, oneBucketStats, sv(), 4, 1));
        assertFalse(LakeBucketAwareAggFallback.shouldFallbackToShuffle(
                groupByBucketColOnly,
                List.of(new BucketProperty(TBucketFunction.MURMUR3_X86_32, 2, projectIdCol)),
                colMap, null, oneBucketStats, sv(), 4, 512));
    }

    @Test
    public void singleWorkerNeverFallsBack() {
        assertFalse(LakeBucketAwareAggFallback.shouldFallbackToShuffle(
                aggRequire, buckets16, colMap, null, stats(1_000_000, 1, 500_000), sv(), 1, 1));
    }
}
