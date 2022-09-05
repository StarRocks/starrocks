// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.starrocks.sql.ast.InsertStmt;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class InsertOverwriteJobTest {
    @Test
    public void testBasic(@Injectable InsertStmt insertStmt) {
        new Expectations() {
            {
                insertStmt.getTargetPartitionIds();
                result = Lists.newArrayList(10L, 20L, 30L);
            }
        };
        InsertOverwriteJob insertOverwriteJob1 = new InsertOverwriteJob(100L, insertStmt, 110L, 120L);
        Assert.assertEquals(100L, insertOverwriteJob1.getJobId());
        Assert.assertEquals(110L, insertOverwriteJob1.getTargetDbId());
        Assert.assertEquals(120L, insertOverwriteJob1.getTargetTableId());
        Assert.assertEquals(InsertOverwriteJobState.OVERWRITE_PENDING, insertOverwriteJob1.getJobState());
        Assert.assertEquals(Lists.newArrayList(10L, 20L, 30L), insertOverwriteJob1.getSourcePartitionIds());
        Assert.assertFalse(insertOverwriteJob1.isFinished());
        insertOverwriteJob1.setJobState(InsertOverwriteJobState.OVERWRITE_SUCCESS);
        Assert.assertTrue(insertOverwriteJob1.isFinished());

        List<Long> targetPartitionIds = Lists.newArrayList(10L, 20L, 30L);
        InsertOverwriteJob insertOverwriteJob2 = new InsertOverwriteJob(100L, 110L, 120L, targetPartitionIds);
        Assert.assertEquals(100L, insertOverwriteJob2.getJobId());
        Assert.assertEquals(110L, insertOverwriteJob2.getTargetDbId());
        Assert.assertEquals(120L, insertOverwriteJob2.getTargetTableId());
        Assert.assertEquals(InsertOverwriteJobState.OVERWRITE_PENDING, insertOverwriteJob2.getJobState());
        Assert.assertEquals(Lists.newArrayList(10L, 20L, 30L), insertOverwriteJob2.getSourcePartitionIds());
        Assert.assertFalse(insertOverwriteJob2.isFinished());
    }
}
