// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.scheduler;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.scheduler.persist.TaskRunStatusChange;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Future;

public class TaskRunManager {

    private static final Logger LOG = LogManager.getLogger(TaskRunManager.class);

    // taskId -> pending TaskRun Queue, for each Task only support 1 running taskRun currently,
    // so the map value is FIFO queue
    private final Map<Long, Queue<TaskRun>> pendingTaskRunMap = Maps.newConcurrentMap();

    // taskId -> running TaskRun, for each Task only support 1 running taskRun currently,
    // so the map value is not queue
    private final Map<Long, TaskRun> runningTaskRunMap = Maps.newConcurrentMap();

    // include SUCCESS/FAILED/CANCEL taskRun
    private final TaskRunHistory taskRunHistory = new TaskRunHistory();

    // Use to execute actual TaskRun
    private final TaskRunExecutor taskRunExecutor = new TaskRunExecutor();

    public SubmitResult submitTaskRun(TaskRun taskRun) {
        // duplicate submit
        if (taskRun.getStatus() != null) {
            return new SubmitResult(taskRun.getStatus().getQueryId(), SubmitResult.SubmitStatus.FAILED);
        }

        int validPendingCount = 0;
        for (Long taskId : pendingTaskRunMap.keySet()) {
            Queue<TaskRun> taskRuns = pendingTaskRunMap.get(taskId);
            if (taskRuns != null && taskRuns.isEmpty()) {
                validPendingCount++;
            }
        }

        if (validPendingCount >= Config.task_runs_queue_length) {
            LOG.warn("pending TaskRun exceeds task_runs_queue_length:{}, reject the submit.",
                    Config.task_runs_queue_length);
            return new SubmitResult(null, SubmitResult.SubmitStatus.REJECTED);
        }

        String queryId = UUIDUtil.genUUID().toString();
        TaskRunStatus status = taskRun.initStatus(queryId, System.currentTimeMillis());
        GlobalStateMgr.getCurrentState().getEditLog().logTaskRunCreateStatus(status);
        long taskId = taskRun.getTaskId();

        Queue<TaskRun> taskRuns = pendingTaskRunMap.computeIfAbsent(taskId, u -> Queues.newConcurrentLinkedQueue());
        taskRuns.offer(taskRun);
        return new SubmitResult(queryId, SubmitResult.SubmitStatus.SUBMITTED);
    }

    // check if a running TaskRun is complete and remove it from running TaskRun map
    public void checkRunningTaskRun() {
        Iterator<Long> runningIterator = runningTaskRunMap.keySet().iterator();
        while (runningIterator.hasNext()) {
            Long taskId = runningIterator.next();
            TaskRun taskRun = runningTaskRunMap.get(taskId);
            if (taskRun == null) {
                LOG.warn("failed to get running TaskRun by taskId:{}", taskId);
                runningIterator.remove();
                return;
            }
            Future<?> future = taskRun.getFuture();
            if (future.isDone()) {
                runningIterator.remove();
                taskRunHistory.addHistory(taskRun.getStatus());
                TaskRunStatusChange statusChange = new TaskRunStatusChange(taskRun.getTaskId(), taskRun.getStatus(),
                        Constants.TaskRunState.RUNNING, taskRun.getStatus().getState());
                GlobalStateMgr.getCurrentState().getEditLog().logUpdateTaskRun(statusChange);
            }
        }
    }

    // schedule the pending TaskRun that can be run into running TaskRun map
    public void scheduledPendingTaskRun() {
        int currentRunning = runningTaskRunMap.size();

        Iterator<Long> pendingIterator = pendingTaskRunMap.keySet().iterator();
        while (pendingIterator.hasNext()) {
            Long taskId = pendingIterator.next();
            TaskRun runningTaskRun = runningTaskRunMap.get(taskId);
            if (runningTaskRun == null) {
                Queue<TaskRun> taskRunQueue = pendingTaskRunMap.get(taskId);
                if (taskRunQueue.size() == 0) {
                    pendingIterator.remove();
                } else {
                    if (currentRunning >= Config.task_runs_concurrency) {
                        break;
                    }
                    TaskRun pendingTaskRun = taskRunQueue.poll();
                    taskRunExecutor.executeTaskRun(pendingTaskRun);
                    runningTaskRunMap.put(taskId, pendingTaskRun);
                    // RUNNING state persistence is for FE FOLLOWER update state
                    TaskRunStatusChange statusChange = new TaskRunStatusChange(taskId, pendingTaskRun.getStatus(),
                            Constants.TaskRunState.PENDING, Constants.TaskRunState.RUNNING);
                    GlobalStateMgr.getCurrentState().getEditLog().logUpdateTaskRun(statusChange);
                    currentRunning++;
                }
            }
        }
    }

    public Map<Long, Queue<TaskRun>> getPendingTaskRunMap() {
        return pendingTaskRunMap;
    }

    public Map<Long, TaskRun> getRunningTaskRunMap() {
        return runningTaskRunMap;
    }

    public TaskRunHistory getTaskRunHistory() {
        return taskRunHistory;
    }
}
