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

package com.starrocks.sql.automv.lifecycle;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.qe.GlobalVariable;

public class MVRecommendationSelectOptions {
    private final double unpartitionedMVCardinalityMax;
    private final double partitionedMVCardinalityMax;
    private final int perLatticeMVLimit;
    private final double perLatticeMVSelectivityRatio;

    public MVRecommendationSelectOptions() {
        this(GlobalVariable.getAutoMVUnpartitionedMVCardMax(),
                GlobalVariable.getAutoMVPartitionedMVCardMax(),
                GlobalVariable.getAutoMVPerLatticeMVLimit(),
                GlobalVariable.getAutoMVPerLatticeMVSelectivityRatio());
    }

    @VisibleForTesting
    public MVRecommendationSelectOptions(

            double unpartitionedMVCardinalityMax,
            double partitionedMVCardinalityMax,
            int perLatticeMVNumLWM,
            double perLatticeMVSelectivityRatio) {
        this.unpartitionedMVCardinalityMax = unpartitionedMVCardinalityMax;
        this.partitionedMVCardinalityMax = partitionedMVCardinalityMax;
        this.perLatticeMVLimit = perLatticeMVNumLWM;
        this.perLatticeMVSelectivityRatio = perLatticeMVSelectivityRatio;
    }

    public double getUnpartitionedMVCardinalityMax() {
        return unpartitionedMVCardinalityMax;
    }

    public double getPartitionedMVCardinalityMax() {
        return partitionedMVCardinalityMax;
    }

    public int getPerLatticeMVLimit() {
        return perLatticeMVLimit;
    }

    public double getPerLatticeMVSelectivityRatio() {
        return perLatticeMVSelectivityRatio;
    }
}
