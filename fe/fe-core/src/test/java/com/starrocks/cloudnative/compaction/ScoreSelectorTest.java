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


package com.starrocks.cloudnative.compaction;

import com.starrocks.common.conf.Config;
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
