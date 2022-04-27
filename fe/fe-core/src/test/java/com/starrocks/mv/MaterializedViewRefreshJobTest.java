package com.starrocks.mv;

import com.clearspring.analytics.util.Lists;
import com.starrocks.statistic.Constants;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class MaterializedViewRefreshJobTest {

    @Test
    public void testUpdateStatusAfterDone() {
        MaterializedViewRefreshJob job = new MaterializedViewRefreshJob();
        job.setStatus(Constants.MaterializedViewJobStatus.RUNNING);
        job.updateStatusAfterDone();
        Assert.assertEquals(Constants.MaterializedViewJobStatus.FAILED, job.getStatus());

        List<IMaterializedViewRefreshTask> tasks = Lists.newArrayList();
        job.setStatus(Constants.MaterializedViewJobStatus.RUNNING);
        job.setTasks(tasks);
        job.updateStatusAfterDone();
        Assert.assertEquals(Constants.MaterializedViewJobStatus.SUCCESS, job.getStatus());

        job = new MaterializedViewRefreshJob();
        job.setStatus(Constants.MaterializedViewJobStatus.RUNNING);
        tasks = Lists.newArrayList();
        IMaterializedViewRefreshTask task1 = new MaterializedViewMockRefreshTask(1L, "mv1");
        task1.setStatus(Constants.MaterializedViewTaskStatus.FAILED);
        tasks.add(task1);
        IMaterializedViewRefreshTask task2 = new MaterializedViewMockRefreshTask(1L, "mv1");
        task2.setStatus(Constants.MaterializedViewTaskStatus.FAILED);
        tasks.add(task2);
        job.setTasks(tasks);
        job.updateStatusAfterDone();
        Assert.assertEquals(Constants.MaterializedViewJobStatus.FAILED, job.getStatus());

        job = new MaterializedViewRefreshJob();
        job.setStatus(Constants.MaterializedViewJobStatus.RUNNING);
        tasks = Lists.newArrayList();
        task1 = new MaterializedViewMockRefreshTask(1L, "mv1");
        task1.setStatus(Constants.MaterializedViewTaskStatus.SUCCESS);
        tasks.add(task1);
        task2 = new MaterializedViewMockRefreshTask(1L, "mv1");
        task2.setStatus(Constants.MaterializedViewTaskStatus.FAILED);
        tasks.add(task2);
        job.setTasks(tasks);
        job.updateStatusAfterDone();
        Assert.assertEquals(Constants.MaterializedViewJobStatus.PARTIAL_SUCCESS, job.getStatus());

        job = new MaterializedViewRefreshJob();
        job.setStatus(Constants.MaterializedViewJobStatus.RUNNING);
        tasks = Lists.newArrayList();
        task1 = new MaterializedViewMockRefreshTask(1L, "mv1");
        task1.setStatus(Constants.MaterializedViewTaskStatus.SUCCESS);
        tasks.add(task1);
        task2 = new MaterializedViewMockRefreshTask(1L, "mv1");
        task2.setStatus(Constants.MaterializedViewTaskStatus.SUCCESS);
        tasks.add(task2);
        job.setTasks(tasks);
        job.updateStatusAfterDone();
        Assert.assertEquals(Constants.MaterializedViewJobStatus.SUCCESS, job.getStatus());
    }

}
