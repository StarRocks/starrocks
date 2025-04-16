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
import org.quartz.CronExpression;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CompactionControlScheduler {
    private static final Logger LOG = LogManager.getLogger(CompactionControlScheduler.class);

    private Scheduler scheduler;
    private Map<Long, Long> tableCompactionMap = new ConcurrentHashMap<>();
    private Map<Long, Long> backendLastStartTimeMap = new ConcurrentHashMap<>();

    private boolean schedulerStarted = false;

    public synchronized void startScheduler() throws SchedulerException {
        if (schedulerStarted) {
            return;
        }
        scheduler = new StdSchedulerFactory().getScheduler();
        scheduler.start();
        addScheduleSyncTask();
        addRecoverSyncTask();
        schedulerStarted = true;
    }

    public void updateTableForbiddenTimeRanges(Long tableId, String crontab) throws SchedulerException {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        startScheduler();
        // Generate JobKey and TriggerKey based on tableId
        String jobKeyString = "job_" + tableId;
        String groupKeyString = "group_" + tableId;
        JobKey jobKey = JobKey.jobKey(jobKeyString, groupKeyString);
        TriggerKey triggerKey = TriggerKey.triggerKey("trigger_" + tableId, groupKeyString);

        // Check if a job with the same JobKey already exists, and delete it if found
        if (scheduler.checkExists(jobKey)) {
            scheduler.deleteJob(jobKey);
            tableCompactionMap.remove(tableId);
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
        if (!cronParts[0].equals("*")) {
            throw new IllegalArgumentException("Invalid crontab format. The minute field must be '*'.");
        }
        String hour = cronParts[1];
        String dayOfMonth = cronParts[2];
        String month = cronParts[3];
        String dayOfWeek = cronParts[4];

        // If either dayOfMonth or dayOfWeek is *, set the other to ?
        if (!dayOfMonth.equals("*") && !dayOfWeek.equals("*")) {
            throw new IllegalArgumentException("For Quartz cron, either day of month or day of week must be '*'.");
        }

        Set<Trigger> triggers = Sets.newHashSet();
        // fully disable should trigger immediately
        if (dayOfMonth.equals("*") && dayOfWeek.equals("*") && hour.equals("*") && month.equals("*")) {
            triggers.add(TriggerBuilder.newTrigger()
                    .withIdentity("syncImmediateTrigger" + tableId, groupKeyString)
                    .startNow() // Start immediately
                    .build());
        }

        if (dayOfMonth.equals("*")) {
            dayOfMonth = "?";
        } else {
            dayOfWeek = "?";
        }

        String modifiedCron = String.format("0 0/1 %s %s %s %s", hour, dayOfMonth, month, dayOfWeek);

        if (!CronExpression.isValidExpression(modifiedCron)) {
            throw new IllegalArgumentException("Invalid crontab format. You can check through "
                + "https://www.freeformatter.com/cron-expression-generator-quartz.html");
        }

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
        triggers.add(cronTrigger);

        // Schedule the job with both triggers
        scheduler.scheduleJob(jobDetail, triggers, true);

        triggerSyncJobWithDelay();

        LOG.info("Table {} has been scheduled for compaction control with crontab {}", tableId, modifiedCron);
    }

    private void addScheduleSyncTask() throws SchedulerException {
        JobDetail jobDetail = JobBuilder.newJob(ScheduleSyncJob.class)
                .withIdentity("scheduleSyncJob", "defaultGroup")
                .build();

        jobDetail.getJobDataMap().put("tableCompactionMap", tableCompactionMap);

        CronTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("syncTrigger", "defaultGroup")
                .withSchedule(CronScheduleBuilder.cronSchedule("10 1/3 * * * ?"))
                .build();

        scheduler.scheduleJob(jobDetail, trigger);
    }

    private void addRecoverSyncTask() throws SchedulerException {
        JobDetail jobDetail = JobBuilder.newJob(RecoverSyncJob.class)
                .withIdentity("recoverSyncJob", "defaultGroup")
                .build();

        jobDetail.getJobDataMap().put("tableCompactionMap", tableCompactionMap);
        jobDetail.getJobDataMap().put("backendLastStartTimeMap", backendLastStartTimeMap);

        CronTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("recoverTrigger", "defaultGroup")
                .withSchedule(CronScheduleBuilder.cronSchedule("5 * * * * ?"))
                .build();

        scheduler.scheduleJob(jobDetail, trigger);
    }

    public void triggerSyncJobWithDelay() throws SchedulerException {
        // Define the job key which identifies the job to be triggered
        JobKey jobKey = new JobKey("scheduleSyncJob", "defaultGroup");
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

            // 7min = 2 * 3min + 1min, 3min is the interval of sync job
            // crash before first sync, it will be recovered by the next sync
            long currentTimePlus7Min = System.currentTimeMillis() / 1000 + 7 * 60;

            tableCompactionMap.put(tableId, currentTimePlus7Min);

            LOG.debug("Table {} disabled tableCompactionMap {}", tableId, tableCompactionMap);
        }
    }

    public static class ScheduleSyncJob implements Job {
        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            Map<Long, Long> tableCompactionMap = (Map<Long, Long>) context.getJobDetail()
                    .getJobDataMap().get("tableCompactionMap");
            if (tableCompactionMap.isEmpty()) {
                return;
            }
            for (Map.Entry<Long, Long> entry : tableCompactionMap.entrySet()) {
                // remove table not in time range
                if (entry.getValue() < System.currentTimeMillis() / 1000) {
                    tableCompactionMap.remove(entry.getKey());
                }
            }
            AgentBatchTask batchTask = new AgentBatchTask();
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds().forEach(backendId -> {
                CompactionControlTask task = new CompactionControlTask(backendId, tableCompactionMap);
                batchTask.addTask(task);
            });
            if (batchTask.getTaskNum() > 0) {
                for (AgentTask task : batchTask.getAllTasks()) {
                    AgentTaskQueue.addTask(task);
                }
                AgentTaskExecutor.submit(batchTask);
                LOG.info("disable compaction: task num {} map {}", batchTask.getTaskNum(), tableCompactionMap);
            }
        }
    }

    public static class RecoverSyncJob implements Job {
        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            Map<Long, Long> tableCompactionMap = (Map<Long, Long>) context.getJobDetail()
                    .getJobDataMap().get("tableCompactionMap");
            if (tableCompactionMap.isEmpty()) {
                return;
            }
            Map<Long, Long> backendLastStartTimeMap = (Map<Long, Long>) context.getJobDetail()
                    .getJobDataMap().get("backendLastStartTimeMap");
            AgentBatchTask batchTask = new AgentBatchTask();
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends().stream().forEach(backend -> {
                long backendId = backend.getId();
                if (!backendLastStartTimeMap.containsKey(backendId)
                        || backend.getLastStartTime() != backendLastStartTimeMap.get(backendId)) {
                    // backend has restarted, need to notify all backends
                    backendLastStartTimeMap.put(backendId, backend.getLastStartTime());
                    CompactionControlTask task = new CompactionControlTask(backendId, tableCompactionMap);
                    batchTask.addTask(task);
                }
            });
            if (batchTask.getTaskNum() > 0) {
                for (AgentTask task : batchTask.getAllTasks()) {
                    AgentTaskQueue.addTask(task);
                }
                AgentTaskExecutor.submit(batchTask);
                LOG.info("disable compaction: task num {} map {}", batchTask.getTaskNum(), tableCompactionMap);
            }
        }
    }
}