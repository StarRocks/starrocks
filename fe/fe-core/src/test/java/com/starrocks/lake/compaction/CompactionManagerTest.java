// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

import com.starrocks.common.Config;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class CompactionManagerTest {
    private CompactionManager compactionManager;

    @Before
    public void init() {
        compactionManager = new CompactionManager();
    }

    @Test
    public void testChoosePartitionsToCompact() {
        PartitionIdentifier partition1 = new PartitionIdentifier(1, 2, 3);
        PartitionIdentifier partition2 = new PartitionIdentifier(1, 2, 4);

        for (int i = 1; i <= Config.lake_compaction_simple_selector_threshold_versions - 1; i++) {
            compactionManager.handleLoadingFinished(partition1, i, System.currentTimeMillis());
            compactionManager.handleLoadingFinished(partition2, i, System.currentTimeMillis());
            Assert.assertEquals(0, compactionManager.choosePartitionsToCompact().size());
        }
        compactionManager.handleLoadingFinished(partition1, Config.lake_compaction_simple_selector_threshold_versions,
                System.currentTimeMillis());
        List<PartitionIdentifier> compactionList = compactionManager.choosePartitionsToCompact();
        Assert.assertEquals(1, compactionList.size());
        Assert.assertSame(partition1, compactionList.get(0));

        Assert.assertEquals(compactionList, compactionManager.choosePartitionsToCompact());

        compactionManager.handleLoadingFinished(partition2, Config.lake_compaction_simple_selector_threshold_versions,
                System.currentTimeMillis());

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
}
