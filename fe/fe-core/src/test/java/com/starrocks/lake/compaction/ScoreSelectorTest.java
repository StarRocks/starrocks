// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

import com.starrocks.common.Config;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ScoreSelectorTest {
    private ScoreSelector selector;

    @Before
    public void setUp() {
        Config.lake_compaction_score_selector_min_score = 1.0;
        selector = new ScoreSelector();
    }

    @Test
    public void test() {
        List<PartitionStatistics> statisticsList = new ArrayList<>();
        PartitionStatistics statistics = new PartitionStatistics(new PartitionIdentifier(1, 2, 3));
        statistics.setCompactionScore(Quantiles.compute(Collections.singleton(0.0)));
        statisticsList.add(statistics);

        statistics = new PartitionStatistics(new PartitionIdentifier(1, 2, 4));
        statistics.setCompactionScore(Quantiles.compute(Collections.singleton(0.99)));
        statisticsList.add(statistics);

        statistics = new PartitionStatistics(new PartitionIdentifier(1, 2, 5));
        statistics.setCompactionScore(Quantiles.compute(Collections.singleton(1.0)));
        statisticsList.add(statistics);

        statistics = new PartitionStatistics(new PartitionIdentifier(1, 2, 6));
        statistics.setCompactionScore(Quantiles.compute(Collections.singleton(1.1)));
        statisticsList.add(statistics);

        List<PartitionStatistics> targetList = selector.select(statisticsList);
        Assert.assertEquals(2, targetList.size());
        Assert.assertEquals(5, targetList.get(0).getPartition().getPartitionId());
        Assert.assertEquals(6, targetList.get(1).getPartition().getPartitionId());
    }
}
