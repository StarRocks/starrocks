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

package com.starrocks.statistic.base;

import com.google.common.collect.Lists;
import com.starrocks.statistic.sample.TabletStats;

import java.util.List;
import java.util.stream.Collectors;

public class TabletSampler {
    private final double tabletsSampleRatio;

    private final double tabletReadRatio;

    private final int maxSize;

    private final List<TabletStats> tablets = Lists.newArrayList();

    private final long sampleRowsLimit;

    private long totalRows;

    public TabletSampler(double tabletsSampleRatio, double tabletReadRatio, int maxSize, long sampleRowsLimit) {
        this.tabletsSampleRatio = tabletsSampleRatio;
        this.tabletReadRatio = tabletReadRatio;
        this.maxSize = maxSize;
        this.sampleRowsLimit = sampleRowsLimit;
    }

    public void addTabletStats(TabletStats tablet) {
        this.tablets.add(tablet);
        this.totalRows += tablet.getRowCount();
    }

    public List<TabletStats> sample() {
        int number = (int) Math.ceil(tablets.size() * tabletsSampleRatio);
        number = Math.min(number, maxSize);
        // sort by row count in descending order
        return tablets.stream().sorted((o1, o2) -> Long.compare(o2.getRowCount(), o1.getRowCount()))
                .limit(number).collect(Collectors.toList());
    }

    public long getTotalRows() {
        return totalRows;
    }

    public long getSampleRows() {
        return (long) Math.min(sampleRowsLimit, Math.max(totalRows * tabletReadRatio, 1L));
    }

    public long getTotalTablets() {
        return tablets.size();
    }

    public void clear() {

    }
}
