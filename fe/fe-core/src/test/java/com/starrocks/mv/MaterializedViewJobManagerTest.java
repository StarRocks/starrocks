// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.mv;

import com.clearspring.analytics.util.Lists;
import com.starrocks.catalog.Catalog;
import com.starrocks.statistic.Constants;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MaterializedViewJobManagerTest {

    private static final Logger LOG = LogManager.getLogger(MaterializedViewJobManagerTest.class);

    @Mocked
    private Catalog catalog;

    @Before
    public void setUp() {

        new Expectations() {
            {
                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getNextId();
                minTimes = 0;
                returns(100L, 101L, 102L, 103L, 104L, 105L);
            }
        };
    }

    @Test
    public void ScheduleScenariosRegularlyTest() {

        LocalDateTime now = LocalDateTime.now();

        MaterializedViewRefreshJobBuilder builder = MaterializedViewRefreshJobBuilder
                .newBuilder(1, 2)
                .mode(Constants.MaterializedViewRefreshMode.ASYNC)
                .triggerType(Constants.MaterializedViewTriggerType.AUTO);


        List<IMaterializedViewRefreshTask> tasks = Lists.newArrayList();

        builder.setTasksAhead(tasks);
        tasks.add(new MaterializedViewMockRefreshTask(1000L, "mv1"));
        tasks.add(new MaterializedViewMockRefreshTask(1000L, "mv1"));
        tasks.add(new MaterializedViewMockRefreshTask(1000L, "mv1"));

        new MockUp<MaterializedViewRefreshJob>(MaterializedViewRefreshJob.class) {
            @Mock
            public void generateTasks() {
            }
        };

        MaterializedViewJobManager scheduler = new MaterializedViewJobManager();
        Long scheduledId = scheduler.registerScheduledJob(builder, now.plusSeconds(3), 7L, TimeUnit.SECONDS);
        LOG.info("register scheduled job: " + scheduledId + ", " + LocalDateTime.now());
        ThreadUtil.sleepAtLeastIgnoreInterrupts(15000L);

        scheduler.deregisterScheduledJob(scheduledId);
        List<MaterializedViewRefreshJob> jobList = scheduler.listJob();
        for (MaterializedViewRefreshJob materializedViewRefreshJob : jobList) {
            LOG.info(materializedViewRefreshJob);
        }

        Assert.assertEquals(2, jobList.size());

        MaterializedViewRefreshJob job1 = jobList.get(1);
        Assert.assertEquals(101, job1.getId());
        Assert.assertEquals(Constants.MaterializedViewJobStatus.SUCCESS, job1.getStatus());
        Assert.assertEquals(3, job1.getTasks().size());

        MaterializedViewRefreshJob job2 = jobList.get(0);

        Assert.assertEquals(102, job2.getId());
        Assert.assertEquals(Constants.MaterializedViewJobStatus.SUCCESS, job2.getStatus());
        Assert.assertEquals(3, job2.getTasks().size());
    }

    @Test
    public void TriggerScenariosRegularlyTest() {
        List<IMaterializedViewRefreshTask> tasks = Lists.newArrayList();
        tasks.add(new MaterializedViewMockRefreshTask(1000L, "mv1"));
        tasks.add(new MaterializedViewMockRefreshTask(1000L, "mv1"));
        tasks.add(new MaterializedViewMockRefreshTask(1000L, "mv1"));

        MaterializedViewRefreshJob job = MaterializedViewRefreshJobBuilder
                .newBuilder(1, 2)
                .mode(Constants.MaterializedViewRefreshMode.ASYNC)
                .triggerType(Constants.MaterializedViewTriggerType.AUTO)
                .setTasksAhead(tasks)
                .build();

        new MockUp<MaterializedViewRefreshJob>(MaterializedViewRefreshJob.class) {
            @Mock
            public void generateTasks() {
            }
        };
        MaterializedViewJobManager scheduler = new MaterializedViewJobManager();
        scheduler.addPendingJob(job);
        ThreadUtil.sleepAtLeastIgnoreInterrupts(5000L);
        List<MaterializedViewRefreshJob> jobList = scheduler.listJob();
        for (MaterializedViewRefreshJob materializedViewRefreshJob : jobList) {
            LOG.info(materializedViewRefreshJob);
        }

        Assert.assertEquals(Constants.MaterializedViewJobStatus.SUCCESS, jobList.get(0).getStatus());
    }

    @Test
    public void ManualScenariosRegularlyTest() {
        List<IMaterializedViewRefreshTask> tasks = Lists.newArrayList();
        tasks.add(new MaterializedViewMockRefreshTask(1000L, "mv1"));
        tasks.add(new MaterializedViewMockRefreshTask(1000L, "mv1"));
        tasks.add(new MaterializedViewMockRefreshTask(1000L, "mv1"));

        MaterializedViewRefreshJob job = MaterializedViewRefreshJobBuilder
                .newBuilder(1, 2)
                .mode(Constants.MaterializedViewRefreshMode.ASYNC)
                .triggerType(Constants.MaterializedViewTriggerType.MANUAL)
                .withRefreshDatePartitionRange("2020-01-01", "2020-04-24")
                .setTasksAhead(tasks)
                .build();

        new MockUp<MaterializedViewRefreshJob>(MaterializedViewRefreshJob.class) {
            @Mock
            public void generateTasks() {
            }
        };
        MaterializedViewJobManager scheduler = new MaterializedViewJobManager();
        scheduler.addPendingJob(job);
        ThreadUtil.sleepAtLeastIgnoreInterrupts(5000L);
        List<MaterializedViewRefreshJob> jobList = scheduler.listJob();
        for (MaterializedViewRefreshJob materializedViewRefreshJob : jobList) {
            LOG.info(materializedViewRefreshJob);
        }

        Assert.assertEquals(Constants.MaterializedViewJobStatus.SUCCESS, jobList.get(0).getStatus());
    }

    @Test
    public void TriggerScenariosCancelRunningJobTest() {
        List<IMaterializedViewRefreshTask> tasks = Lists.newArrayList();
        tasks.add(new MaterializedViewMockRefreshTask(2000L, "mv1"));
        tasks.add(new MaterializedViewMockRefreshTask(4000L, "mv1"));
        tasks.add(new MaterializedViewMockRefreshTask(2000L, "mv1"));

        MaterializedViewRefreshJob job = MaterializedViewRefreshJobBuilder
                .newBuilder(1, 2)
                .mode(Constants.MaterializedViewRefreshMode.ASYNC)
                .triggerType(Constants.MaterializedViewTriggerType.AUTO)
                .setTasksAhead(tasks)
                .build();

        new MockUp<MaterializedViewRefreshJob>(MaterializedViewRefreshJob.class) {
            @Mock
            public void generateTasks() {
            }
        };
        MaterializedViewJobManager scheduler = new MaterializedViewJobManager();
        boolean isRegister = scheduler.addPendingJob(job);
        ThreadUtil.sleepAtLeastIgnoreInterrupts(3000L);
        boolean isCancel = scheduler.cancelJob(job.getMvTableId(), job.getId());
        List<MaterializedViewRefreshJob> jobList = scheduler.listJob();
        for (MaterializedViewRefreshJob materializedViewRefreshJob : jobList) {
            LOG.info(materializedViewRefreshJob);
        }

        Assert.assertEquals(true, isRegister);
        Assert.assertEquals(true, isCancel);
    }

    @Test
    public void TriggerScenariosFailedJobTest() {
        List<IMaterializedViewRefreshTask> tasks = Lists.newArrayList();
        tasks.add(new MaterializedViewMockRefreshTask(1000L, "mv1"));
        MaterializedViewMockRefreshTask failedTask = new MaterializedViewMockRefreshTask(1000L, "mv1");
        failedTask.setMockFailed(true);
        tasks.add(failedTask);
        tasks.add(new MaterializedViewMockRefreshTask(1000L, "mv1"));

        MaterializedViewRefreshJob job = MaterializedViewRefreshJobBuilder
                .newBuilder(1, 2)
                .mode(Constants.MaterializedViewRefreshMode.ASYNC)
                .triggerType(Constants.MaterializedViewTriggerType.AUTO)
                .setTasksAhead(tasks)
                .build();

        new MockUp<MaterializedViewRefreshJob>(MaterializedViewRefreshJob.class) {
            @Mock
            public void generateTasks() {
            }
        };
        MaterializedViewJobManager scheduler = new MaterializedViewJobManager();
        boolean isRegister = scheduler.addPendingJob(job);
        ThreadUtil.sleepAtLeastIgnoreInterrupts(5000L);
        List<MaterializedViewRefreshJob> jobList = scheduler.listJob();
        for (MaterializedViewRefreshJob materializedViewRefreshJob : jobList) {
            LOG.info(materializedViewRefreshJob);
        }

        Assert.assertEquals(true, isRegister);
        Assert.assertEquals(Constants.MaterializedViewJobStatus.PARTIAL_SUCCESS, job.getStatus());
        Assert.assertEquals(Constants.MaterializedViewTaskStatus.SUCCESS, job.getTasks().get(0).getStatus());
        Assert.assertEquals(Constants.MaterializedViewTaskStatus.FAILED, job.getTasks().get(1).getStatus());
        Assert.assertEquals(Constants.MaterializedViewTaskStatus.SUCCESS, job.getTasks().get(2).getStatus());

        MaterializedViewMockRefreshTask filedMockTask = (MaterializedViewMockRefreshTask)job.getTasks().get(1);
        filedMockTask.setMockFailed(false);

        scheduler.retryJob(job.getId());
        ThreadUtil.sleepAtLeastIgnoreInterrupts(3000L);
        for (MaterializedViewRefreshJob materializedViewRefreshJob : jobList) {
            LOG.info(materializedViewRefreshJob);
        }
        Assert.assertEquals(Constants.MaterializedViewTaskStatus.SUCCESS, job.getTasks().get(1).getStatus());
        Assert.assertEquals(Constants.MaterializedViewJobStatus.SUCCESS, job.getStatus());
        Assert.assertEquals(1, job.getRetryTime());
    }

    @Test
    public void TriggerScenariosCancelPendingJobTest() {
        List<IMaterializedViewRefreshTask> tasks = Lists.newArrayList();
        tasks.add(new MaterializedViewMockRefreshTask(4000L, "mv1"));
        tasks.add(new MaterializedViewMockRefreshTask(1000L, "mv1"));

        MaterializedViewRefreshJob job1 = MaterializedViewRefreshJobBuilder
                .newBuilder(1, 2)
                .mode(Constants.MaterializedViewRefreshMode.ASYNC)
                .triggerType(Constants.MaterializedViewTriggerType.AUTO)
                .setTasksAhead(tasks)
                .build();

        MaterializedViewRefreshJob job2 = MaterializedViewRefreshJobBuilder
                .newBuilder(1, 2)
                .mode(Constants.MaterializedViewRefreshMode.ASYNC)
                .triggerType(Constants.MaterializedViewTriggerType.MANUAL)
                .build();
        new MockUp<MaterializedViewRefreshJob>(MaterializedViewRefreshJob.class) {
            @Mock
            public void generateTasks() {
            }
        };
        MaterializedViewJobManager scheduler = new MaterializedViewJobManager();
        boolean isRegister1 = scheduler.addPendingJob(job1);
        boolean isRegister2 = scheduler.addPendingJob(job2);
        ThreadUtil.sleepAtLeastIgnoreInterrupts(5000L);
        boolean isCancel = scheduler.cancelJob(job2.getMvTableId(), job2.getId());
        List<MaterializedViewRefreshJob> jobList = scheduler.listJob();
        for (MaterializedViewRefreshJob materializedViewRefreshJob : jobList) {
            LOG.info(materializedViewRefreshJob);
        }
        Assert.assertEquals(2, jobList.size());
        Assert.assertEquals(Constants.MaterializedViewJobStatus.CANCELED, jobList.get(1).getStatus());
        Assert.assertEquals(Constants.MaterializedViewJobStatus.RUNNING, jobList.get(0).getStatus());
        Assert.assertEquals(true, isRegister1);
        Assert.assertEquals(true, isRegister2);
        Assert.assertEquals(true, isCancel);
    }

    @Test
    public void AutoTriggerPendingJobMergeTest() {
        List<IMaterializedViewRefreshTask> tasks = Lists.newArrayList();
        tasks.add(new MaterializedViewMockRefreshTask(4000L, "mv1"));
        tasks.add(new MaterializedViewMockRefreshTask(4000L, "mv1"));

        MaterializedViewRefreshJob job1 = MaterializedViewRefreshJobBuilder
                .newBuilder(1, 2)
                .mode(Constants.MaterializedViewRefreshMode.ASYNC)
                .triggerType(Constants.MaterializedViewTriggerType.AUTO)
                .setTasksAhead(tasks)
                .build();

        MaterializedViewRefreshJob job2 = MaterializedViewRefreshJobBuilder
                .newBuilder(1, 2)
                .mode(Constants.MaterializedViewRefreshMode.ASYNC)
                .triggerType(Constants.MaterializedViewTriggerType.AUTO)
                .build();

        MaterializedViewRefreshJob job3 = MaterializedViewRefreshJobBuilder
                .newBuilder(1, 2)
                .mode(Constants.MaterializedViewRefreshMode.ASYNC)
                .triggerType(Constants.MaterializedViewTriggerType.AUTO)
                .build();

        MaterializedViewRefreshJob job4 = MaterializedViewRefreshJobBuilder
                .newBuilder(1, 2)
                .mode(Constants.MaterializedViewRefreshMode.ASYNC)
                .triggerType(Constants.MaterializedViewTriggerType.AUTO)
                .build();
        new MockUp<MaterializedViewRefreshJob>(MaterializedViewRefreshJob.class) {
            @Mock
            public void generateTasks() {
            }
        };

        MaterializedViewJobManager scheduler = new MaterializedViewJobManager();
        boolean isRegister1 = scheduler.addPendingJob(job1);
        ThreadUtil.sleepAtLeastIgnoreInterrupts(1000L);
        boolean isRegister2 = scheduler.addPendingJob(job2);
        boolean isRegister3 = scheduler.addPendingJob(job3);
        boolean isRegister4 = scheduler.addPendingJob(job4);
        ThreadUtil.sleepAtLeastIgnoreInterrupts(3000L);
        List<MaterializedViewRefreshJob> jobList = scheduler.listJob();
        for (MaterializedViewRefreshJob materializedViewRefreshJob : jobList) {
            LOG.info(materializedViewRefreshJob);
        }

        Assert.assertEquals(true, isRegister1);
        Assert.assertEquals(true, isRegister2);
        Assert.assertEquals(true, isRegister3);
        Assert.assertEquals(true, isRegister4);

        Assert.assertTrue(2 == jobList.get(0).getMergeCount());

        scheduler.cancelJob(job1.getMvTableId(), job1.getId());
        scheduler.cancelJob(job2.getMvTableId(), job2.getId());
        scheduler.cancelJob(job3.getMvTableId(), job3.getId());
        scheduler.cancelJob(job4.getMvTableId(), job4.getId());
    }

    @Test
    public void ManualTriggerPendingJobMergeTest() {
        List<IMaterializedViewRefreshTask> tasks = Lists.newArrayList();
        tasks.add(new MaterializedViewMockRefreshTask(4000L, "mv1"));
        tasks.add(new MaterializedViewMockRefreshTask(4000L, "mv1"));

        MaterializedViewRefreshJob job1 = MaterializedViewRefreshJobBuilder
                .newBuilder(1, 2)
                .mode(Constants.MaterializedViewRefreshMode.ASYNC)
                .triggerType(Constants.MaterializedViewTriggerType.MANUAL)
                .setTasksAhead(tasks)
                .build();

        MaterializedViewRefreshJob job2 = MaterializedViewRefreshJobBuilder
                .newBuilder(1, 2)
                .mode(Constants.MaterializedViewRefreshMode.ASYNC)
                .triggerType(Constants.MaterializedViewTriggerType.MANUAL)
                .build();

        MaterializedViewRefreshJob job3 = MaterializedViewRefreshJobBuilder
                .newBuilder(1, 2)
                .mode(Constants.MaterializedViewRefreshMode.ASYNC)
                .triggerType(Constants.MaterializedViewTriggerType.MANUAL)
                .build();

        MaterializedViewRefreshJob job4 = MaterializedViewRefreshJobBuilder
                .newBuilder(1, 2)
                .mode(Constants.MaterializedViewRefreshMode.ASYNC)
                .triggerType(Constants.MaterializedViewTriggerType.MANUAL)
                .build();
        new MockUp<MaterializedViewRefreshJob>(MaterializedViewRefreshJob.class) {
            @Mock
            public void generateTasks() {
            }
        };
        MaterializedViewJobManager scheduler = new MaterializedViewJobManager();
        boolean isRegister1 = scheduler.addPendingJob(job1);
        ThreadUtil.sleepAtLeastIgnoreInterrupts(1000L);
        boolean isRegister2 = scheduler.addPendingJob(job2);
        boolean isRegister3 = scheduler.addPendingJob(job3);
        boolean isRegister4 = scheduler.addPendingJob(job4);
        ThreadUtil.sleepAtLeastIgnoreInterrupts(3000L);
        List<MaterializedViewRefreshJob> jobList = scheduler.listJob();
        for (MaterializedViewRefreshJob materializedViewRefreshJob : jobList) {
            LOG.info(materializedViewRefreshJob);
        }

        Assert.assertEquals(true, isRegister1);
        Assert.assertEquals(true, isRegister2);
        Assert.assertEquals(true, isRegister3);
        Assert.assertEquals(true, isRegister4);

        Assert.assertTrue(4 == jobList.size());

        scheduler.cancelJob(job1.getMvTableId(), job1.getId());
        scheduler.cancelJob(job2.getMvTableId(), job2.getId());
        scheduler.cancelJob(job3.getMvTableId(), job3.getId());
        scheduler.cancelJob(job4.getMvTableId(), job4.getId());
    }

    @Test
    public void differentMaterializedViewConcurrencyTest() {
        List<IMaterializedViewRefreshTask> tasks1 = Lists.newArrayList();
        tasks1.add(new MaterializedViewMockRefreshTask(1000L, "mv1"));
        tasks1.add(new MaterializedViewMockRefreshTask(1000L, "mv1"));

        List<IMaterializedViewRefreshTask> tasks2 = Lists.newArrayList();
        tasks2.add(new MaterializedViewMockRefreshTask(1000L, "mv2"));
        tasks2.add(new MaterializedViewMockRefreshTask(1000L, "mv2"));

        MaterializedViewRefreshJob job1 = MaterializedViewRefreshJobBuilder
                .newBuilder(1, 2)
                .mode(Constants.MaterializedViewRefreshMode.ASYNC)
                .triggerType(Constants.MaterializedViewTriggerType.AUTO)
                .setTasksAhead(tasks1)
                .build();

        MaterializedViewRefreshJob job2 = MaterializedViewRefreshJobBuilder
                .newBuilder(1, 3)
                .mode(Constants.MaterializedViewRefreshMode.ASYNC)
                .triggerType(Constants.MaterializedViewTriggerType.MANUAL)
                .setTasksAhead(tasks2)
                .build();
        new MockUp<MaterializedViewRefreshJob>(MaterializedViewRefreshJob.class) {
            @Mock
            public void generateTasks() {
            }
        };
        MaterializedViewJobManager scheduler = new MaterializedViewJobManager();
        scheduler.addPendingJob(job1);
        scheduler.addPendingJob(job2);
        ThreadUtil.sleepAtLeastIgnoreInterrupts(4000L);
        List<MaterializedViewRefreshJob> jobList = scheduler.listJob();
        for (MaterializedViewRefreshJob materializedViewRefreshJob : jobList) {
            LOG.info(materializedViewRefreshJob);
        }

        Assert.assertEquals(Constants.MaterializedViewJobStatus.SUCCESS, job1.getStatus());
        Assert.assertEquals(Constants.MaterializedViewJobStatus.SUCCESS, job2.getStatus());

    }



}
