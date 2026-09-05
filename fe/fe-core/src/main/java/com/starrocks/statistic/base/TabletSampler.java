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
    private double tabletsSampleRatio;

    private final int maxSize;

    private final List<TabletStats> tablets = Lists.newArrayList();


    private long totalRows;

    public TabletSampler(double tabletsSampleRatio, int maxSize) {
        this.tabletsSampleRatio = tabletsSampleRatio;
        this.maxSize = maxSize;
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

    public void adjustTabletSampleRatio() {
        this.tabletsSampleRatio *= 2;
    }

    public long getTotalRows() {
        return totalRows;
    }

    public long getTotalTablets() {
        return tablets.size();
    }

    public void clear() {

    }
}
