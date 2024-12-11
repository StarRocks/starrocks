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

package com.starrocks.statistic.sample;

import com.starrocks.catalog.Type;

public class DistributionColumnStats extends PrimitiveTypeColumnStats {

    private final SampleInfo sampleInfo;

    public DistributionColumnStats(String columnName, Type columnType, SampleInfo sampleInfo) {
        super(columnName, columnType);
        this.sampleInfo = sampleInfo;
    }

    @Override
    public String getDistinctCount(double rowSampleRatio) {
        long tabletAmplification = (long) Math.floor(1.0 / sampleInfo.getTabletSampleRatio());
        long rowsAmplification = (long) Math.floor(1.0 / rowSampleRatio);
        if (rowsAmplification > 500) {
            rowsAmplification = (long) Math.max(500.0, Math.pow(10L, Math.floor(Math.log10(rowsAmplification))));
        }
        return String.format("IF(SUM(t1.count) / %d > 0.75, COUNT(1) * %d, COUNT(1) * %d)",
                sampleInfo.getSampleRowCount(), rowsAmplification, tabletAmplification);
    }
}
