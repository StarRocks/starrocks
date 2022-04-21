// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.persist.CreateInsertOverwriteJobInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class InsertOverwriteJobManagerTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private ConnectContext context;

    @Mocked
    private StmtExecutor stmtExecutor;

    @Mocked
    private InsertStmt insertStmt;

    @Mocked
    private Database db;

    @Mocked
    private OlapTable table1;

    @Mocked
    private InsertOverwriteJob insertOverwriteJob2;

    private InsertOverwriteJobManager insertOverwriteJobManager;
    private InsertOverwriteJob insertOverwriteJob1;
    private List<Long> targetPartitionIds;

    @Before
    public void setUp() {
        insertOverwriteJobManager = new InsertOverwriteJobManager();
        targetPartitionIds = Lists.newArrayList(10L, 20L, 30L, 40L);
    }

    @Test
    public void testBasic() throws Exception {
        new Expectations() {
            {
                insertOverwriteJob2.getJobId();
                result = 1100L;

                insertOverwriteJob2.getTargetTableId();
                result = 110L;

                insertOverwriteJob2.run();
                result = InsertOverwriteJob.OverwriteJobState.OVERWRITE_SUCCESS;

                insertOverwriteJob2.getOriginalTargetPartitionIds();
                result = targetPartitionIds;

            }
        };

        insertOverwriteJobManager.registerOverwriteJob(insertOverwriteJob2);
        Assert.assertEquals(1, insertOverwriteJobManager.getJobNum());

        insertOverwriteJobManager.deregisterOverwriteJob(1100L);
        Assert.assertEquals(0, insertOverwriteJobManager.getJobNum());

        insertOverwriteJobManager.submitJob(insertOverwriteJob2);

        insertOverwriteJobManager.registerOverwriteJob(insertOverwriteJob2);
        Assert.assertEquals(1, insertOverwriteJobManager.getJobNum());
        insertOverwriteJobManager.registerOverwriteJobTxn(1100L, 1L);

        Assert.assertFalse(insertOverwriteJobManager.hasRunningOverwriteJob(1l, 110L, targetPartitionIds));
        Assert.assertTrue(insertOverwriteJobManager.hasRunningOverwriteJob(2l, 110L, targetPartitionIds));
        insertOverwriteJobManager.deregisterOverwriteJob(1100L);
        Assert.assertFalse(insertOverwriteJobManager.hasRunningOverwriteJob(2l, 110L, targetPartitionIds));
    }

    @Test
    public void testReplay() throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getServingState();
                result = globalStateMgr;

                GlobalStateMgr.isCheckpointThread();
                result = false;

                globalStateMgr.getServingState();
                result = globalStateMgr;

                globalStateMgr.isReady();
                result = true;

                insertOverwriteJob2.getJobId();
                result = 1100L;

                insertOverwriteJob2.getTargetTableId();
                result = 110L;

                insertOverwriteJob2.cancel();
                result = true;

                insertOverwriteJob2.getOriginalTargetPartitionIds();
                result = targetPartitionIds;

            }
        };
        CreateInsertOverwriteJobInfo jobInfo = new CreateInsertOverwriteJobInfo(
                1100L, 100L, 110L, "table_1", targetPartitionIds);
        Assert.assertEquals(1100L, jobInfo.getJobId());
        Assert.assertEquals(100L, jobInfo.getDbId());
        Assert.assertEquals(110L, jobInfo.getTableId());
        Assert.assertEquals("table_1", jobInfo.getTableName());
        Assert.assertEquals(targetPartitionIds, jobInfo.getTargetPartitionIds());

        insertOverwriteJobManager.replayCreateInsertOverwrite(jobInfo);
        Assert.assertEquals(1, insertOverwriteJobManager.getRunningJobSize());
        insertOverwriteJobManager.cancelRunningJobs();
        Thread.sleep(5000);
        Assert.assertEquals(0, insertOverwriteJobManager.getRunningJobSize());
    }

    @Test
    public void testSerialization() throws IOException {
        new Expectations() {
            {
                db.getId();
                result = 100L;

                table1.getId();
                result = 110L;

                table1.getName();
                result = "table_1";
            }
        };

        insertOverwriteJobManager.registerOverwriteJob(insertOverwriteJob1);
        Assert.assertEquals(1, insertOverwriteJobManager.getJobNum());

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        insertOverwriteJobManager.write(dataOutputStream);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        InsertOverwriteJobManager newInsertOverwriteJobManager = InsertOverwriteJobManager.read(dataInputStream);
        Assert.assertEquals(1, newInsertOverwriteJobManager.getJobNum());
        InsertOverwriteJob newJob = insertOverwriteJobManager.getInsertOverwriteJob(1000L);
        Assert.assertEquals(insertOverwriteJob1, newJob);
        Assert.assertEquals(1000L, newJob.getJobId());
        Assert.assertEquals(100L, newJob.getTargetDbId());
        Assert.assertEquals(110L, newJob.getTargetTableId());
        Assert.assertEquals("table_1", newJob.getTargetTableName());
        Assert.assertEquals(targetPartitionIds, newJob.getOriginalTargetPartitionIds());
    }
}
