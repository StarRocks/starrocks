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

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.statistic.StatsConstants;

import java.util.List;
import java.util.Map;

public class TabletSampleManager {

    private static double HIGH_WEIGHT_READ_RATIO = 0.005;

    private static double MEDIUM_HIGH_WEIGHT_READ_RATIO = 0.1;

    private static double MEDIUM_LOW_WEIGHT_READ_RATIO = 0.2;

    private static double LOW_WEIGHT_READ_RATIO = 0.8;

    private static long HIGH_WEIGHT_ROWS_THRESHOLD = 10000000L;

    private static long MEDIUM_HIGH_WEIGHT_ROWS_THRESHOLD = 1000000L;

    private static long MEDIUM_LOW_WEIGHT_ROWS_THRESHOLD = 100000L;

    public static long MAX_ROW_COUNT_BY_BLOCK_SAMPLE = 1000000000;

    private final SampleTabletSlot highWeight;

    private final SampleTabletSlot mediumHighWeight;

    private final SampleTabletSlot mediumLowWeight;

    private final SampleTabletSlot lowWeight;

    private final long sampleRowsLimit;

    public static TabletSampleManager init(Map<String, String> properties, Table table) {
        double highSampleRatio = Double.parseDouble(properties.getOrDefault(StatsConstants.HIGH_WEIGHT_SAMPLE_RATIO,
                "0.5"));
        double mediumHighRatio = Double.parseDouble(properties.getOrDefault(StatsConstants.MEDIUM_HIGH_WEIGHT_SAMPLE_RATIO,
                "0.45"));
        double mediumLowRatio = Double.parseDouble(properties.getOrDefault(StatsConstants.MEDIUM_LOW_WEIGHT_SAMPLE_RATIO,
                "0.35"));
        double lowRatio = Double.parseDouble(properties.getOrDefault(StatsConstants.LOW_WEIGHT_SAMPLE_RATIO,
                "0.3"));
        int maxSize = Integer.parseInt(properties.getOrDefault(StatsConstants.MAX_SAMPLE_TABLET_NUM,
                "5000"));
        long sampleRowLimit = Long.parseLong(properties.getOrDefault(StatsConstants.STATISTIC_SAMPLE_COLLECT_ROWS,
                String.valueOf(Config.statistic_sample_collect_rows)));

        TabletSampleManager manager = new TabletSampleManager(highSampleRatio, mediumHighRatio, mediumLowRatio, lowRatio,
                maxSize, sampleRowLimit);
        manager.classifyTablet(table);
        return manager;
    }

    private TabletSampleManager(double highSampleRatio, double mediumHighRatio, double mediumLowRatio, double lowRatio,
                                int maxSize, long sampleRowsLimit) {
        this.highWeight = new SampleTabletSlot(highSampleRatio, HIGH_WEIGHT_READ_RATIO, maxSize);
        this.mediumHighWeight = new SampleTabletSlot(mediumHighRatio, MEDIUM_HIGH_WEIGHT_READ_RATIO, maxSize);
        this.mediumLowWeight = new SampleTabletSlot(mediumLowRatio, MEDIUM_LOW_WEIGHT_READ_RATIO, maxSize);
        this.lowWeight = new SampleTabletSlot(lowRatio, LOW_WEIGHT_READ_RATIO, maxSize);
        this.sampleRowsLimit = sampleRowsLimit;
    }

    private void classifyTablet(Table table) {
        if (table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            for (Partition logicalPartition : olapTable.getPartitions()) {
                if (!logicalPartition.hasData()) {
                    continue;
                }
                for (PhysicalPartition physicalPartition : logicalPartition.getSubPartitions()) {
                    for (Tablet tablet : physicalPartition.getBaseIndex().getTablets()) {
                        long tabletId = tablet.getId();
                        long rowCount = tablet.getFuzzyRowCount();
                        TabletStats tabletStats = new TabletStats(tabletId, physicalPartition.getId(), rowCount);
                        if (rowCount >= HIGH_WEIGHT_ROWS_THRESHOLD) {
                            highWeight.addTabletStats(tabletStats);
                        } else if (rowCount >= MEDIUM_HIGH_WEIGHT_ROWS_THRESHOLD) {
                            mediumHighWeight.addTabletStats(tabletStats);
                        } else if (rowCount >= MEDIUM_LOW_WEIGHT_ROWS_THRESHOLD) {
                            mediumLowWeight.addTabletStats(tabletStats);
                        } else {
                            lowWeight.addTabletStats(tabletStats);
                        }
                    }
                }
            }
        }
    }

    public SampleInfo generateSampleInfo() {
        List<TabletStats> highWeightTablets = highWeight.sampleTabletStats();
        List<TabletStats> mediumHighWeightTablets = mediumHighWeight.sampleTabletStats();
        List<TabletStats> mediumLowWeightTablets = mediumLowWeight.sampleTabletStats();
        List<TabletStats> lowWeightTablets = lowWeight.sampleTabletStats();

        long sampleTablets = highWeightTablets.stream().filter(e -> e.getRowCount() > 0).count() +
                mediumHighWeightTablets.stream().filter(e -> e.getRowCount() > 0).count() +
                mediumLowWeightTablets.stream().filter(e -> e.getRowCount() > 0).count() +
                lowWeightTablets.stream().filter(e -> e.getRowCount() > 0).count();
        sampleTablets = Math.max(1, sampleTablets);

        long totalTablets = highWeight.getNonEmptyTabletCount() + mediumHighWeight.getNonEmptyTabletCount()
                + mediumLowWeight.getNonEmptyTabletCount() + lowWeight.getNonEmptyTabletCount();
        totalTablets = Math.max(1, totalTablets);

        long sampleRows = Math.min(sampleRowsLimit, highWeightTablets.stream()
                .mapToLong(e -> getReadRowCount(e.getRowCount(), highWeight.getTabletReadRatio()))
                .sum());
        sampleRows += Math.min(sampleRowsLimit, mediumHighWeightTablets.stream()
                .mapToLong(e -> getReadRowCount(e.getRowCount(), mediumHighWeight.getTabletReadRatio()))
                .sum());
        sampleRows += Math.min(sampleRowsLimit, mediumLowWeightTablets.stream()
                .mapToLong(e -> getReadRowCount(e.getRowCount(), mediumLowWeight.getTabletReadRatio()))
                .sum());
        sampleRows += Math.min(sampleRowsLimit, lowWeightTablets.stream()
                .mapToLong(e -> getReadRowCount(e.getRowCount(), lowWeight.getTabletReadRatio()))
                .sum());
        sampleRows = Math.max(1, sampleRows);
        long totalRows = Math.max(highWeight.getRowCount() + mediumHighWeight.getRowCount()
                + mediumLowWeight.getRowCount() + lowWeight.getRowCount(), 1);

        return new SampleInfo(
                sampleTablets * 1.0 / totalTablets,
                sampleRows, totalRows,
                highWeightTablets,
                mediumHighWeightTablets,
                mediumLowWeightTablets,
                lowWeightTablets);
    }

    public SampleTabletSlot getHighWeight() {
        return highWeight;
    }

    public SampleTabletSlot getMediumHighWeight() {
        return mediumHighWeight;
    }

    public SampleTabletSlot getMediumLowWeight() {
        return mediumLowWeight;
    }

    public SampleTabletSlot getLowWeight() {
        return lowWeight;
    }

    public long getSampleRowsLimit() {
        return sampleRowsLimit;
    }

    private long getReadRowCount(long totalRowCount, double readRatio) {
        return (long) Math.max(totalRowCount * readRatio, 1L);
    }
}
