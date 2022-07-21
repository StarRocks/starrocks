// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.lake.compaction;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CompactionPickerByCountTest {
    private static final long MIN_COMPACTION_VERSIONS = 3;

    private CompactionPicker picker;

    public CompactionPickerByCountTest() {
    }

    @Before
    public void init() {
        picker = new CompactionPickerByCount(MIN_COMPACTION_VERSIONS);
    }

    @Test
    public void testEmpty() {
        List<PartitionStatistics> statisticsList = new ArrayList<>();
        Assert.assertNull(picker.pick(statisticsList));
    }

    @Test
    public void testVersionCountNotReached() {
        List<PartitionStatistics> statisticsList = new ArrayList<>();

        final PartitionIdentifier partitionIdentifier = new PartitionIdentifier(1, 2, 3);
        PartitionStatistics statistics = new PartitionStatistics(partitionIdentifier, 0, 1, MIN_COMPACTION_VERSIONS - 1);
        statisticsList.add(statistics);

        Assert.assertNull(picker.pick(statisticsList));
    }

    @Test
    public void testVersionCountReached() {
        List<PartitionStatistics> statisticsList = new ArrayList<>();

        final PartitionIdentifier partitionIdentifier1 = new PartitionIdentifier(1, 2, 3);
        PartitionStatistics statistics1 = new PartitionStatistics(partitionIdentifier1, 0, 1, MIN_COMPACTION_VERSIONS);
        statisticsList.add(statistics1);

        final PartitionIdentifier partitionIdentifier2 = new PartitionIdentifier(1, 2, 4);
        PartitionStatistics statistics2 = new PartitionStatistics(partitionIdentifier2, 0, 1, MIN_COMPACTION_VERSIONS + 1);
        statisticsList.add(statistics2);

        Assert.assertSame(statistics2, picker.pick(statisticsList));
    }

    @Test
    public void testDoingCompaction() {
        List<PartitionStatistics> statisticsList = new ArrayList<>();

        final PartitionIdentifier partitionIdentifier = new PartitionIdentifier(1, 2, 4);
        PartitionStatistics statistics = new PartitionStatistics(partitionIdentifier, 0, 1, MIN_COMPACTION_VERSIONS + 1);
        statisticsList.add(statistics);

        statistics.setDoingCompaction(true);

        Assert.assertNull(picker.pick(statisticsList));

        statistics.setDoingCompaction(false);

        Assert.assertSame(statistics, picker.pick(statisticsList));
    }

    @Test
    public void testCompactionTimeNotReached() {
        List<PartitionStatistics> statisticsList = new ArrayList<>();

        final PartitionIdentifier partitionIdentifier = new PartitionIdentifier(1, 2, 4);
        PartitionStatistics statistics = new PartitionStatistics(partitionIdentifier, 0, 1, MIN_COMPACTION_VERSIONS + 1);
        statisticsList.add(statistics);

        statistics.setNextCompactionTime(System.currentTimeMillis() + 60 * 1000);

        Assert.assertNull(picker.pick(statisticsList));

        statistics.setNextCompactionTime(System.currentTimeMillis() - 10);

        Assert.assertSame(statistics, picker.pick(statisticsList));
    }
}
