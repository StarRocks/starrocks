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

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class CompactionMgrTest {

    @Test
    public void testChoosePartitionsToCompact() {
        Config.lake_compaction_selector = "SimpleSelector";
        Config.lake_compaction_sorter = "RandomSorter";
        CompactionMgr compactionManager = new CompactionMgr();

        PartitionIdentifier partition1 = new PartitionIdentifier(1, 2, 3);
        PartitionIdentifier partition2 = new PartitionIdentifier(1, 2, 4);

        for (int i = 1; i <= Config.lake_compaction_simple_selector_threshold_versions - 1; i++) {
            compactionManager.handleLoadingFinished(partition1, i, System.currentTimeMillis(), null);
            compactionManager.handleLoadingFinished(partition2, i, System.currentTimeMillis(), null);
            Assert.assertEquals(0, compactionManager.choosePartitionsToCompact().size());
        }
        compactionManager.handleLoadingFinished(partition1, Config.lake_compaction_simple_selector_threshold_versions,
                System.currentTimeMillis(), null);
        List<PartitionIdentifier> compactionList = compactionManager.choosePartitionsToCompact();
        Assert.assertEquals(1, compactionList.size());
        Assert.assertSame(partition1, compactionList.get(0));

        Assert.assertEquals(compactionList, compactionManager.choosePartitionsToCompact());

        compactionManager.handleLoadingFinished(partition2, Config.lake_compaction_simple_selector_threshold_versions,
                System.currentTimeMillis(), null);

        compactionList = compactionManager.choosePartitionsToCompact();
        Assert.assertEquals(2, compactionList.size());
        Assert.assertTrue(compactionList.contains(partition1));
        Assert.assertTrue(compactionList.contains(partition2));

        compactionList = compactionManager.choosePartitionsToCompact(Collections.singleton(partition1));
        Assert.assertEquals(1, compactionList.size());
        Assert.assertSame(partition2, compactionList.get(0));

        compactionManager.enableCompactionAfter(partition1, 5000);
        compactionManager.enableCompactionAfter(partition2, 5000);
        compactionList = compactionManager.choosePartitionsToCompact();
        Assert.assertEquals(0, compactionList.size());

        compactionManager.enableCompactionAfter(partition1, 0);
        compactionManager.enableCompactionAfter(partition2, 0);
        compactionManager.removePartition(partition1);
        compactionList = compactionManager.choosePartitionsToCompact();
        Assert.assertEquals(1, compactionList.size());
        Assert.assertSame(partition2, compactionList.get(0));
    }

    @Test
    public void testGetMaxCompactionScore() {
        double delta = 0.001;

        CompactionMgr compactionMgr = new CompactionMgr();
        PartitionIdentifier partition1 = new PartitionIdentifier(1, 2, 3);
        PartitionIdentifier partition2 = new PartitionIdentifier(1, 2, 4);
        Assert.assertEquals(0, compactionMgr.getMaxCompactionScore(), delta);

        compactionMgr.handleLoadingFinished(partition1, 2, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(1d)));
        Assert.assertEquals(1, compactionMgr.getMaxCompactionScore(), delta);
        compactionMgr.handleCompactionFinished(partition1, 3, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(2d)));
        Assert.assertEquals(2, compactionMgr.getMaxCompactionScore(), delta);

        compactionMgr.handleLoadingFinished(partition2, 2, System.currentTimeMillis(),
                Quantiles.compute(Lists.newArrayList(3d)));
        Assert.assertEquals(3, compactionMgr.getMaxCompactionScore(), delta);

        compactionMgr.removePartition(partition2);
        Assert.assertEquals(2, compactionMgr.getMaxCompactionScore(), delta);
    }
}
