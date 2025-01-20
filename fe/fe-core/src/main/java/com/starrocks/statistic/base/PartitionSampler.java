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

import com.google.common.collect.Maps;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.statistic.sample.SampleInfo;
import com.starrocks.statistic.sample.TabletStats;

import java.util.List;
import java.util.Map;

public class PartitionSampler {
    public static final double HIGH_WEIGHT_READ_RATIO = 0.001;
    public static final double MEDIUM_HIGH_WEIGHT_READ_RATIO = 0.01;
    public static final double MEDIUM_LOW_WEIGHT_READ_RATIO = 0.1;
    public static final double LOW_WEIGHT_READ_RATIO = 0.8;
    private static final long HIGH_WEIGHT_ROWS_THRESHOLD = 10000000L;
    private static final long MEDIUM_HIGH_WEIGHT_ROWS_THRESHOLD = 1000000L;
    private static final long MEDIUM_LOW_WEIGHT_ROWS_THRESHOLD = 100000L;

    private final double highRatio;
    private final double mediumHighRatio;
    private final double mediumLowRatio;
    private final double lowRatio;
    private final int maxSize;

    private final long sampleRowsLimit;

    private final Map<Long, SampleInfo> partitionSampleMaps = Maps.newHashMap();

    public PartitionSampler(double highSampleRatio, double mediumHighRatio, double mediumLowRatio, double lowRatio,
                            int maxSize, long sampleRowLimit) {
        this.highRatio = highSampleRatio;
        this.mediumHighRatio = mediumHighRatio;
        this.mediumLowRatio = mediumLowRatio;
        this.lowRatio = lowRatio;
        this.maxSize = maxSize;

        this.sampleRowsLimit = sampleRowLimit;
    }

    public long getSampleRowsLimit() {
        return sampleRowsLimit;
    }

    public SampleInfo getSampleInfo(long pid) {
        return partitionSampleMaps.get(pid);
    }

    public void classifyPartitions(Table table, List<Long> partitions) {
        for (Long partitionId : partitions) {
            Partition p = table.getPartition(partitionId);
            if (p == null || !p.hasData()) {
                continue;
            }

            TabletSampler high = new TabletSampler(highRatio, HIGH_WEIGHT_READ_RATIO, maxSize, sampleRowsLimit);
            TabletSampler mediumHigh =
                    new TabletSampler(mediumHighRatio, MEDIUM_HIGH_WEIGHT_READ_RATIO, maxSize, sampleRowsLimit);
            TabletSampler mediumLow =
                    new TabletSampler(mediumLowRatio, MEDIUM_LOW_WEIGHT_READ_RATIO, maxSize, sampleRowsLimit);
            TabletSampler low = new TabletSampler(lowRatio, LOW_WEIGHT_READ_RATIO, maxSize, sampleRowsLimit);

            for (Tablet tablet : p.getDefaultPhysicalPartition().getBaseIndex().getTablets()) {
                long rowCount = tablet.getFuzzyRowCount();
                if (rowCount <= 0) {
                    continue;
                }
                if (rowCount >= HIGH_WEIGHT_ROWS_THRESHOLD) {
                    high.addTabletStats(new TabletStats(tablet.getId(), partitionId, rowCount));
                } else if (rowCount >= MEDIUM_HIGH_WEIGHT_ROWS_THRESHOLD) {
                    mediumHigh.addTabletStats(new TabletStats(tablet.getId(), partitionId, rowCount));
                } else if (rowCount >= MEDIUM_LOW_WEIGHT_ROWS_THRESHOLD) {
                    mediumLow.addTabletStats(new TabletStats(tablet.getId(), partitionId, rowCount));
                } else {
                    low.addTabletStats(new TabletStats(tablet.getId(), partitionId, rowCount));
                }
            }

            long totalRows = high.getTotalRows() + mediumHigh.getTotalRows() + mediumLow.getTotalRows()
                    + low.getTotalRows();

            List<TabletStats> highSampleTablets = high.sample();
            List<TabletStats> mediumHighSampleTablets = mediumHigh.sample();
            List<TabletStats> mediumLowSampleTablets = mediumLow.sample();
            List<TabletStats> lowSampleTablets = low.sample();

            long sampleRows = Math.min(sampleRowsLimit, highSampleTablets.stream()
                    .mapToLong(e -> getReadRowCount(e.getRowCount(), highRatio))
                    .sum());
            sampleRows += Math.min(sampleRowsLimit, mediumHighSampleTablets.stream()
                    .mapToLong(e -> getReadRowCount(e.getRowCount(), mediumHighRatio))
                    .sum());
            sampleRows += Math.min(sampleRowsLimit, mediumLowSampleTablets.stream()
                    .mapToLong(e -> getReadRowCount(e.getRowCount(), mediumLowRatio))
                    .sum());
            sampleRows += Math.min(sampleRowsLimit, lowSampleTablets.stream()
                    .mapToLong(e -> getReadRowCount(e.getRowCount(), lowRatio))
                    .sum());
            sampleRows = Math.max(1, sampleRows);

            long totalTablets = high.getTotalTablets() + mediumHigh.getTotalTablets() + mediumLow.getTotalTablets() +
                    low.getTotalTablets();
            long sampleTablets = highSampleTablets.size() + mediumHighSampleTablets.size()
                    + mediumLowSampleTablets.size() + lowSampleTablets.size();

            partitionSampleMaps.put(partitionId,
                    new SampleInfo(null, null,
                            sampleTablets * 1.0 / totalTablets, sampleRows, totalRows, highSampleTablets,
                            mediumHighSampleTablets, mediumLowSampleTablets, lowSampleTablets));
        }
    }

    public static PartitionSampler create(Table table, List<Long> partitions, Map<String, String> properties) {
        double highSampleRatio = Double.parseDouble(properties.getOrDefault(StatsConstants.HIGH_WEIGHT_SAMPLE_RATIO,
                "0.5"));
        double mediumHighRatio =
                Double.parseDouble(properties.getOrDefault(StatsConstants.MEDIUM_HIGH_WEIGHT_SAMPLE_RATIO,
                        "0.45"));
        double mediumLowRatio =
                Double.parseDouble(properties.getOrDefault(StatsConstants.MEDIUM_LOW_WEIGHT_SAMPLE_RATIO,
                        "0.35"));
        double lowRatio = Double.parseDouble(properties.getOrDefault(StatsConstants.LOW_WEIGHT_SAMPLE_RATIO,
                "0.3"));
        int maxSize = Integer.parseInt(properties.getOrDefault(StatsConstants.MAX_SAMPLE_TABLET_NUM,
                "5000"));
        long sampleRowLimit = Long.parseLong(properties.getOrDefault(StatsConstants.STATISTIC_SAMPLE_COLLECT_ROWS,
                String.valueOf(Config.statistic_sample_collect_rows)));

        PartitionSampler sampler = new PartitionSampler(highSampleRatio, mediumHighRatio, mediumLowRatio, lowRatio,
                maxSize, sampleRowLimit);
        sampler.classifyPartitions(table, partitions);
        return sampler;
    }

    private long getReadRowCount(long totalRowCount, double readRatio) {
        return (long) Math.max(totalRowCount * readRatio, 1L);
    }
}
