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

package com.starrocks.sql.automv.options;

import com.starrocks.qe.SessionVariable;

public class AutoMVOptions {
    private final int partialRollupMinAggPieces;
    private final boolean useCardinalityEstimation;
    private final boolean pruneRollupAbleWithConjuncts;
    private final boolean pushDownAggBelowSemiAntiJoin;
    private final boolean enableSemiAntiJoin;
    private final int maxOrderByColumns;
    private final boolean useArrayAggCountDistinct;
    private final boolean useBitmapCountDistinct;
    private final boolean useHllCountDistinct;
    private final double samplingRatioLowBound;
    private final long minSamplingRows;
    private final double relativeErrorBound;
    private final int samplingBuckets;
    private final long samplingTimeout;
    private final double cardRowCountRatioLWM;
    private final double cardRowCountRatioHWM;
    private final int maxCalculateSteps;
    private final boolean enableComplexDerivedMetrics;
    private final boolean enableComplexDerivedDimensions;
    private final String defaultPartitionByTimeGranule;

    private AutoMVOptions(
            int partialRollupMinAggregatePieces,
            boolean useCardinalityEstimation,
            boolean pruneRollupAbleWithConjuncts,
            boolean pushDownAggBelowSemiAntiJoin,
            boolean enableSemiAntiJoin,
            int maxOrderByColumns,
            boolean useArrayAggCountDistinct,
            boolean useBitmapCountDistinct,
            boolean useHllCountDistinct,
            double samplingRatioLowBound,
            long minSamplingRows,
            double relativeErrorBound,
            int samplingBuckets,
            long samplingTimeout,
            double cardRowCountRatioLWM, double cardRowCountRatioHWM, int maxCalculateSteps,
            boolean enableComplexDerivedMetrics,
            boolean enableComplexDerivedDimensions,
            String defaultPartitionByTimeGranule
    ) {
        this.partialRollupMinAggPieces = partialRollupMinAggregatePieces;
        this.useCardinalityEstimation = useCardinalityEstimation;
        this.pruneRollupAbleWithConjuncts = pruneRollupAbleWithConjuncts;
        this.pushDownAggBelowSemiAntiJoin = pushDownAggBelowSemiAntiJoin;
        this.enableSemiAntiJoin = enableSemiAntiJoin;
        this.maxOrderByColumns = maxOrderByColumns;
        this.useArrayAggCountDistinct = useArrayAggCountDistinct;
        this.useBitmapCountDistinct = useBitmapCountDistinct;
        this.useHllCountDistinct = useHllCountDistinct;
        this.samplingRatioLowBound = samplingRatioLowBound;
        this.minSamplingRows = minSamplingRows;
        this.relativeErrorBound = relativeErrorBound;
        this.samplingBuckets = samplingBuckets;
        this.samplingTimeout = samplingTimeout;
        this.cardRowCountRatioLWM = cardRowCountRatioLWM;
        this.cardRowCountRatioHWM = cardRowCountRatioHWM;
        this.maxCalculateSteps = maxCalculateSteps;
        this.enableComplexDerivedMetrics = enableComplexDerivedMetrics;
        this.enableComplexDerivedDimensions = enableComplexDerivedDimensions;
        this.defaultPartitionByTimeGranule = defaultPartitionByTimeGranule;
    }

    public static AutoMVOptions of(SessionVariable sv) {
        return new AutoMVOptions(
                sv.getAutoMVPartialRollupMinAggPieces(),
                sv.isAutoMVUseCardinalityEstimation(),
                sv.isAutoMVPruneRollupUnableAggregateWithConjuncts(),
                sv.isAutoMVPushDownAggBelowSemiAntiJoin(),
                sv.isAutoMVEnableSemiAntiJoin(),
                sv.getAutoMVMaxOrderByColumns(),
                sv.getAutoMVUseArrayAggCountDistinct(),
                sv.getAutoMVUseBitmapCountDistinct(),
                sv.getAutoMVUseHllCountDistinct(),
                sv.getAutoMVSamplingRatioLowBound(),
                sv.getAutoMVMinSamplingRows(),
                sv.getAutoMVRelativeErrorBound(),
                sv.getAutoMVSamplingBuckets(),
                sv.getAutoMVSamplingTimeout(),
                sv.getAutoMVCardRowCountRatioLWM(),
                sv.getAutoMVCardRowCountRatioHWM(),
                sv.getAutoMVMaxCalculateSteps(),
                sv.isAutoMVEnableComplexDerivedMetrics(),
                sv.isAutoMVEnableComplexDerivedDimensions(),
                sv.getAutoMVDefaultPartitionByTimeGranule());
    }

    public String getDefaultPartitionByTimeGranule() {
        return defaultPartitionByTimeGranule;
    }

    public boolean isEnableSemiAntiJoin() {
        return enableSemiAntiJoin;
    }

    public boolean isPushDownAggBelowSemiAntiJoin() {
        return pushDownAggBelowSemiAntiJoin;
    }

    public boolean isEnableComplexDerivedMetrics() {
        return enableComplexDerivedMetrics;
    }

    public boolean isEnableComplexDerivedDimensions() {
        return enableComplexDerivedDimensions;
    }

    public int getMaxOrderByColumns() {
        return maxOrderByColumns;
    }

    public boolean isPruneRollupAbleWithConjuncts() {
        return pruneRollupAbleWithConjuncts;
    }

    public boolean isUseArrayAggCountDistinct() {
        return useArrayAggCountDistinct;
    }

    public boolean isUseBitmapCountDistinct() {
        return useBitmapCountDistinct;
    }

    public boolean isUseHllCountDistinct() {
        return useHllCountDistinct;
    }

    public double getSamplingRatioLowBound() {
        return samplingRatioLowBound;
    }

    public long getMinSamplingRows() {
        return minSamplingRows;
    }

    public int getPartialRollupMinAggPieces() {
        return partialRollupMinAggPieces;
    }

    public boolean isUseCardinalityEstimation() {
        return useCardinalityEstimation;
    }

    public double getRelativeErrorBound() {
        return relativeErrorBound;
    }

    public int getSamplingBuckets() {
        return samplingBuckets;
    }

    public long getSamplingTimeout() {
        return samplingTimeout;
    }

    public double getCardRowCountRatioLWM() {
        return cardRowCountRatioLWM;
    }

    public double getCardRowCountRatioHWM() {
        return cardRowCountRatioHWM;
    }

    public int getMaxCalculateSteps() {
        return maxCalculateSteps;
    }
}
