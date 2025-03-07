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

import com.google.common.collect.Lists;

import java.util.List;

public class SampleTabletSlot {

    private final List<TabletStats> tabletStats;

    private final double tabletsSampleRatio;

    private final double tabletReadRatio;

    private final int maxSize;

    public SampleTabletSlot(double tabletsSampleRatio, double tabletReadRatio, int maxSize) {
        this.tabletStats = Lists.newArrayList();
        this.tabletsSampleRatio = tabletsSampleRatio;
        this.tabletReadRatio = tabletReadRatio;
        this.maxSize = maxSize;
    }

    public void addTabletStats(TabletStats tabletStats) {
        this.tabletStats.add(tabletStats);
    }

    public List<TabletStats> sampleTabletStats() {
        int number = (int) Math.ceil(tabletStats.size() * tabletsSampleRatio);
        number = Math.min(number, maxSize);

        List<TabletStats> resultList = Lists.newArrayList(tabletStats);

        // sort by row count in descending order
        resultList.sort((o1, o2) -> {
            if (o1.getRowCount() > o2.getRowCount()) {
                return -1;
            } else if (o1.getRowCount() < o2.getRowCount()) {
                return 1;
            } else {
                return 0;
            }
        });

        return resultList.subList(0, number);
    }

    public double getTabletReadRatio() {
        return tabletReadRatio;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public long getNonEmptyTabletCount() {
        return tabletStats.stream().filter(e -> e.getRowCount() > 0).count();
    }

    public long getRowCount() {
        return tabletStats.stream().mapToLong(TabletStats::getRowCount).sum();
    }
}
