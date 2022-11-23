// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ScoreSorterTest {

    @Test
    public void test() {
        List<PartitionStatistics> statisticsList = new ArrayList<>();
        PartitionStatistics statistics = new PartitionStatistics(new PartitionIdentifier(1, 2, 3));
        statistics.setCompactionScore(Quantiles.compute(Arrays.asList(0.0, 0.0, 0.0)));
        statisticsList.add(statistics);

        statistics = new PartitionStatistics(new PartitionIdentifier(1, 2, 6));
        statistics.setCompactionScore(Quantiles.compute(Arrays.asList(1.1, 1.1, 1.2)));
        statisticsList.add(statistics);

        statistics = new PartitionStatistics(new PartitionIdentifier(1, 2, 4));
        statistics.setCompactionScore(Quantiles.compute(Arrays.asList(0.99, 0.99, 0.99)));
        statisticsList.add(statistics);

        statistics = new PartitionStatistics(new PartitionIdentifier(1, 2, 5));
        statistics.setCompactionScore(Quantiles.compute(Arrays.asList(1.0, 1.0)));
        statisticsList.add(statistics);

        ScoreSorter sorter = new ScoreSorter();

        List<PartitionStatistics> sortedList = sorter.sort(statisticsList);
        Assert.assertEquals(4, sortedList.size());
        Assert.assertEquals(6, sortedList.get(0).getPartition().getPartitionId());
        Assert.assertEquals(5, sortedList.get(1).getPartition().getPartitionId());
        Assert.assertEquals(4, sortedList.get(2).getPartition().getPartitionId());
        Assert.assertEquals(3, sortedList.get(3).getPartition().getPartitionId());
    }
}
