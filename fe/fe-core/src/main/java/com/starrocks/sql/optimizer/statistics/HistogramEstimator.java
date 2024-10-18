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

import com.google.common.base.Preconditions;

/**
 * Use histogram to estimate cardinality
 */
public class HistogramEstimator {
    
    /**
     * Estimate the selectivity of two columns with EqualTo operator
     * Return null if fail to do the estimation
     */
    public static Double estimateEqualToSelectivity(ColumnStatistic left, ColumnStatistic right) {
        // Check if input parameters are valid
        if (left == null || right == null) {
            return null;
        }

        // Get histograms
        Histogram leftHistogram = left.getHistogram();
        Histogram rightHistogram = right.getHistogram();

        // If either histogram is empty, estimation is not possible
        if (leftHistogram == null || rightHistogram == null) {
            return null;
        }

        // Calculate the overlapping area of the two histograms
        double overlapArea = 0.0;
        double totalArea = 0.0;

        for (Bucket leftBucket : leftHistogram.getBuckets()) {
            for (Bucket rightBucket : rightHistogram.getBuckets()) {
                double overlap = calculateBucketOverlap(leftBucket, rightBucket);
                overlapArea += overlap;
            }
            totalArea += leftBucket.getCount();
        }

        // Calculate selectivity
        if (totalArea > 0) {
            double selectivity = overlapArea / totalArea;
            Preconditions.checkState(0.0 <= selectivity && selectivity <= 1.0,
                    "exceptional selectivity: " + selectivity);
            return overlapArea / totalArea;
        } else {
            return null;
        }
    }

    private static double calculateBucketOverlap(Bucket leftBucket, Bucket rightBucket) {
        double leftLower = leftBucket.getLower();
        double leftUpper = leftBucket.getUpper();
        double rightLower = rightBucket.getLower();
        double rightUpper = rightBucket.getUpper();

        // Calculate overlap interval
        double overlapLower = Math.max(leftLower, rightLower);
        double overlapUpper = Math.min(leftUpper, rightUpper);

        // If there's no overlap, return 0
        if (overlapLower >= overlapUpper) {
            return 0;
        }

        // Calculate overlap ratio
        double leftRange = leftUpper - leftLower;
        double rightRange = rightUpper - rightLower;
        double overlapRange = overlapUpper - overlapLower;

        double leftOverlapRatio = overlapRange / leftRange;
        double rightOverlapRatio = overlapRange / rightRange;

        // Estimate the count of overlapping elements
        double overlapCount =
                Math.min(leftBucket.getCount() * leftOverlapRatio, rightBucket.getCount() * rightOverlapRatio);

        return overlapCount;
    }
}
