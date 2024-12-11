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


package com.starrocks.lake.compaction;

import com.starrocks.common.Config;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
<<<<<<< HEAD
=======
import java.util.HashSet;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import java.util.List;

public class SimpleSelectorTest {
    private static final long MIN_COMPACTION_VERSIONS = 3;

    private Selector selector;

    public SimpleSelectorTest() {
    }

    @Before
    public void init() {
        Config.lake_compaction_simple_selector_threshold_versions = MIN_COMPACTION_VERSIONS;
        selector = new SimpleSelector();
    }

    @Test
    public void testEmpty() {
        List<PartitionStatistics> statisticsList = new ArrayList<>();
<<<<<<< HEAD
        Assert.assertEquals(0, selector.select(statisticsList).size());
=======
        Assert.assertEquals(0, selector.select(statisticsList, new HashSet<Long>()).size());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Test
    public void testVersionCountNotReached() {
        List<PartitionStatistics> statisticsList = new ArrayList<>();

        final PartitionIdentifier partitionIdentifier = new PartitionIdentifier(1, 2, 3);
        PartitionStatistics statistics = new PartitionStatistics(partitionIdentifier);
        statistics.setCurrentVersion(new PartitionVersion(MIN_COMPACTION_VERSIONS - 1, System.currentTimeMillis()));
        statistics.setCompactionVersion(new PartitionVersion(1, 0));
        statisticsList.add(statistics);

<<<<<<< HEAD
        Assert.assertEquals(0, selector.select(statisticsList).size());
=======
        Assert.assertEquals(0, selector.select(statisticsList, new HashSet<Long>()).size());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Test
    public void testVersionCountReached() {
        List<PartitionStatistics> statisticsList = new ArrayList<>();

        final PartitionIdentifier partitionIdentifier1 = new PartitionIdentifier(1, 2, 3);
        PartitionStatistics statistics1 = new PartitionStatistics(partitionIdentifier1);
        statistics1.setCompactionVersion(new PartitionVersion(1, 0));
<<<<<<< HEAD
=======
        statistics1.setCompactionScore(new Quantiles(0.0, 0.0, 0.0));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        statistics1.setCurrentVersion(new PartitionVersion(MIN_COMPACTION_VERSIONS, System.currentTimeMillis()));
        statisticsList.add(statistics1);

        final PartitionIdentifier partitionIdentifier2 = new PartitionIdentifier(1, 2, 4);
        PartitionStatistics statistics2 = new PartitionStatistics(partitionIdentifier2);
        statistics2.setCompactionVersion(new PartitionVersion(1, 0));
<<<<<<< HEAD
        statistics2.setCurrentVersion(new PartitionVersion(MIN_COMPACTION_VERSIONS + 1, System.currentTimeMillis()));
        statisticsList.add(statistics2);

        Assert.assertSame(statistics2, selector.select(statisticsList).get(0));
=======
        statistics2.setCompactionScore(new Quantiles(0.0, 0.0, 0.0));
        statistics2.setCurrentVersion(new PartitionVersion(MIN_COMPACTION_VERSIONS + 1, System.currentTimeMillis()));
        statisticsList.add(statistics2);

        PartitionStatisticsSnapshot stat = new PartitionStatisticsSnapshot(statistics2);
        Assert.assertEquals(stat.getPartition(),
                            selector.select(statisticsList, new HashSet<Long>()).get(0).getPartition());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Test
    public void testCompactionTimeNotReached() {
        List<PartitionStatistics> statisticsList = new ArrayList<>();

        final PartitionIdentifier partitionIdentifier = new PartitionIdentifier(1, 2, 4);
        PartitionStatistics statistics = new PartitionStatistics(partitionIdentifier);
        statistics.setCompactionVersion(new PartitionVersion(1, 0));
<<<<<<< HEAD
=======
        statistics.setCompactionScore(new Quantiles(0.0, 0.0, 0.0));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        statistics.setCurrentVersion(new PartitionVersion(MIN_COMPACTION_VERSIONS + 1, System.currentTimeMillis()));
        statisticsList.add(statistics);

        statistics.setNextCompactionTime(System.currentTimeMillis() + 60 * 1000);
<<<<<<< HEAD

        Assert.assertEquals(0, selector.select(statisticsList).size());

        statistics.setNextCompactionTime(System.currentTimeMillis() - 10);

        Assert.assertSame(statistics, selector.select(statisticsList).get(0));
=======
        Assert.assertEquals(0, selector.select(statisticsList, new HashSet<Long>()).size());

        statistics.setNextCompactionTime(System.currentTimeMillis() - 10);
        PartitionStatisticsSnapshot stat = new PartitionStatisticsSnapshot(statistics);
        Assert.assertEquals(stat.getPartition(),
                            selector.select(statisticsList, new HashSet<Long>()).get(0).getPartition());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }
}
