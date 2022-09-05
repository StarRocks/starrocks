// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

import com.starrocks.common.Config;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CompactionManagerTest {
    private CompactionManager compactionManager;

    @Before
    public void init() {
        compactionManager = new CompactionManager();
    }

    @Test
    public void test() throws InterruptedException {
        PartitionIdentifier partition1 = new PartitionIdentifier(1, 2, 3);
        PartitionIdentifier partition2 = new PartitionIdentifier(1, 2, 4);

        for (int i = 1; i <= Config.experimental_lake_compaction_max_version_count - 1; i++) {
            compactionManager.handleLoadingFinished(partition1, i);
            compactionManager.handleLoadingFinished(partition2, i);

            Assert.assertNull(compactionManager.choosePartitionToCompact());
        }
        compactionManager.handleLoadingFinished(partition1, Config.experimental_lake_compaction_max_version_count);
        Assert.assertSame(partition1, compactionManager.choosePartitionToCompact());
        Assert.assertNull(compactionManager.choosePartitionToCompact());

        compactionManager.handleLoadingFinished(partition1, Config.experimental_lake_compaction_max_version_count + 1);
        Assert.assertNull(compactionManager.choosePartitionToCompact());

        compactionManager.enableCompactionAfter(partition1, 5000);
        Assert.assertNull(compactionManager.choosePartitionToCompact());

        compactionManager.handleLoadingFinished(partition2, Config.experimental_lake_compaction_max_version_count);
        Assert.assertSame(partition2, compactionManager.choosePartitionToCompact());
        Assert.assertNull(compactionManager.choosePartitionToCompact());
        compactionManager.enableCompactionAfter(partition2, 0);
        compactionManager.removePartition(partition2);
        Assert.assertNull(compactionManager.choosePartitionToCompact());
    }
}
