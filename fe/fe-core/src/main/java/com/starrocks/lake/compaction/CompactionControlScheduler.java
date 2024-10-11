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

import com.google.common.collect.Sets;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.CompactionControlTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.DateBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CompactionControlScheduler {
    private static final Logger LOG = LogManager.getLogger(CompactionControlScheduler.class);

    private Scheduler scheduler;
    private Map<Long, Long> tableCompactionMap = new ConcurrentHashMap<>();

    private boolean schedulerStarted = false;

    public synchronized void startScheduler() throws SchedulerException {
        if (schedulerStarted) {
            return;
        }
        scheduler = new StdSchedulerFactory().getScheduler();
        scheduler.start();
        addSyncTask();
        schedulerStarted = true;
    }

    public void updateTableForbiddenTimeRanges(Long tableId, String crontab) throws SchedulerException {
        startScheduler();
        // Generate JobKey and TriggerKey based on tableId
        String jobKeyString = "job_" + tableId;
        String groupKeyString = "group_" + tableId;
        JobKey jobKey = JobKey.jobKey(jobKeyString, groupKeyString);
        TriggerKey triggerKey = TriggerKey.triggerKey("trigger_" + tableId, groupKeyString);

        // Check if a job with the same JobKey already exists, and delete it if found
        if (scheduler.checkExists(jobKey)) {
            scheduler.deleteJob(jobKey);
        }

        // If crontab is empty, remove the job and return
        if (crontab == null || crontab.isEmpty()) {
            LOG.info("Table {} has been removed from compaction control", tableId);
            return;
        }

        // Validate crontab format and modify it for Quartz
        String[] cronParts = crontab.split("\\s+");
        if (cronParts.length != 5) {
            throw new IllegalArgumentException("Invalid crontab format. It must have 5 fields.");
        }

        // Parse each field and build the Quartz Cron expression
        String hour = cronParts[1];
        String dayOfMonth = cronParts[2];
        String month = cronParts[3];
        String dayOfWeek = cronParts[4];

        // If either dayOfMonth or dayOfWeek is *, set the other to ?
        if (!dayOfMonth.equals("*") && !dayOfWeek.equals("*")) {
            throw new IllegalArgumentException("For Quartz cron, either dayOfMonth or dayOfWeek should be '*', not both.");
        }
        if (dayOfMonth.equals("*")) {
            dayOfMonth = "?";
        } else {
            dayOfWeek = "?";
        }

        String modifiedCron = String.format("0 0/5 %s %s %s %s", hour, dayOfMonth, month, dayOfWeek);

        // Build the new JobDetail
        JobDetail jobDetail = JobBuilder.newJob(DisableCompactionJob.class)
                .withIdentity(jobKey)
                .usingJobData("tableId", tableId)
                .build();

        // Pass the tableCompactionMap to the job
        jobDetail.getJobDataMap().put("tableCompactionMap", tableCompactionMap);

        // Create a new CronTrigger based on the modified cron expression
        CronTrigger cronTrigger = TriggerBuilder.newTrigger()
                .withIdentity(triggerKey)
                .withSchedule(CronScheduleBuilder.cronSchedule(modifiedCron))
                .build();

        // Create a SimpleTrigger to execute the job immediately
        Trigger immediateTrigger = TriggerBuilder.newTrigger()
                .withIdentity("syncImmediateTrigger" + tableId, groupKeyString)
                .startNow() // Start immediately
                .build();

        // Schedule the job with both triggers
        scheduler.scheduleJob(jobDetail, Sets.newHashSet(cronTrigger, immediateTrigger), true);

        triggerSyncJobWithDelay();

        LOG.info("Table {} has been scheduled for compaction control with crontab {}", tableId, modifiedCron);
    }

    private void addSyncTask() throws SchedulerException {
        JobDetail jobDetail = JobBuilder.newJob(SyncJob.class)
                .withIdentity("syncJob", "defaultGroup")
                .build();

        jobDetail.getJobDataMap().put("tableCompactionMap", tableCompactionMap);

        CronTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("syncTrigger", "defaultGroup")
                .withSchedule(CronScheduleBuilder.cronSchedule("0 1/5 * * * ?"))
                .build();

        scheduler.scheduleJob(jobDetail, trigger);
    }

    public void triggerSyncJobWithDelay() throws SchedulerException {
        // Define the job key which identifies the job to be triggered
        JobKey jobKey = new JobKey("syncJob", "defaultGroup");
        TriggerKey triggerKey = TriggerKey.triggerKey("delayedTrigger" + System.currentTimeMillis(), "defaultGroup");

        // Check if the job exists
        if (scheduler.checkExists(jobKey)) {
            // Create a trigger that fires two second in the future
            Trigger trigger = TriggerBuilder.newTrigger()
                    .withIdentity(triggerKey)
                    .startAt(DateBuilder.futureDate(2, DateBuilder.IntervalUnit.SECOND))
                    .forJob(jobKey) // associate the trigger with the job
                    .build();

            // Schedule the job with the delayed trigger
            scheduler.scheduleJob(trigger);

            LOG.debug("Scheduled syncJob with a delay of 2 second");
        }
    }

    public void shutdown() throws SchedulerException {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    public static class DisableCompactionJob implements Job {
        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            Long tableId = context.getJobDetail().getJobDataMap().getLong("tableId");
            Map<Long, Long> tableCompactionMap = (Map<Long, Long>) context.getJobDetail()
                    .getJobDataMap().get("tableCompactionMap");

            long currentTimePlus5Min = System.currentTimeMillis() / 1000 + 7 * 60;

            tableCompactionMap.put(tableId, currentTimePlus5Min);

            LOG.debug("Table {} disabled tableCompactionMap {}", tableId, tableCompactionMap);
        }
    }

    public static class SyncJob implements Job {
        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            Map<Long, Long> tableCompactionMap = (Map<Long, Long>) context.getJobDetail()
                    .getJobDataMap().get("tableCompactionMap");
            AgentBatchTask batchTask = new AgentBatchTask();
            if (tableCompactionMap.isEmpty()) {
                return;
            }
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds().forEach(backendId -> {
                CompactionControlTask task = new CompactionControlTask(backendId, tableCompactionMap);
                // add task to send
                batchTask.addTask(task);
                LOG.debug("add compaction control task. backendId: {} data: {}", backendId, tableCompactionMap);
            });
            if (batchTask.getTaskNum() > 0) {
                for (AgentTask task : batchTask.getAllTasks()) {
                    AgentTaskQueue.addTask(task);
                }
                AgentTaskExecutor.submit(batchTask);
                LOG.debug("tablet[{}] send compaction control task. num: {}", batchTask.getTaskNum());
            }
        }
    }
}