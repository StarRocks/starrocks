// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.scheduler;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.statistic.Constants;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class TaskRunManager {

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

    public String addTaskRun(TaskRun taskRun) {
        // duplicate submit
        if (taskRun.getStatus() != null) {
            return null;
        }

        String queryId = UUIDUtil.genUUID().toString();
        TaskRunStatus status = taskRun.initStatus(queryId);
        // GlobalStateMgr.getCurrentState().getEditLog().logTaskRunCreateStatus(status);
        long taskId = taskRun.getTaskId();

        Queue<TaskRun> taskRuns = pendingTaskRunMap.computeIfAbsent(taskId, u -> Queues.newConcurrentLinkedQueue());
        taskRuns.offer(taskRun);
        return queryId;
    }

    public List<TaskRunStatus> showTaskRunStatus() {
        List<TaskRunStatus> taskRunList = Lists.newArrayList();
        for (Queue<TaskRun> pTaskRunQueue : pendingTaskRunMap.values()) {
            taskRunList.addAll(pTaskRunQueue.stream().map(TaskRun::getStatus).collect(Collectors.toList()));
        }
        taskRunList.addAll(runningTaskRunMap.values().stream().map(TaskRun::getStatus).collect(Collectors.toList()));
        taskRunList.addAll(taskRunHistory.getAllHistory());
        return taskRunList;
    }

    // check if a running TaskRun is complete and remove it from running TaskRun map
    public void checkRunningTaskRun() {
        Iterator<Long> runningIterator = runningTaskRunMap.keySet().iterator();
        while (runningIterator.hasNext()) {
            Long taskId = runningIterator.next();
            TaskRun taskRun = runningTaskRunMap.get(taskId);
            Future<?> future = taskRun.getFuture();
            if (future.isDone()) {
                runningIterator.remove();
                taskRunHistory.addHistory(taskRun.getStatus());
                // TaskRunStatusChange statusChange = new TaskRunStatusChange(taskRun,
                //        Constants.TaskRunStatus.RUNNING, taskRun.getStatus());
                // GlobalStateMgr.getCurrentState().getEditLog().logTaskRunStatusChange(statusChange);
            }
        }
    }

    // schedule the pending TaskRun that can be run into running TaskRun map
    public void scheduledPendingTaskRun() {
        Iterator<Long> pendingIterator = pendingTaskRunMap.keySet().iterator();
        while (pendingIterator.hasNext()) {
            Long taskId = pendingIterator.next();
            TaskRun runningTaskRun = runningTaskRunMap.get(taskId);
            if (runningTaskRun == null) {
                Queue<TaskRun> taskRunQueue = pendingTaskRunMap.get(taskId);
                if (taskRunQueue.size() == 0) {
                    pendingIterator.remove();
                } else {
                    TaskRun pendingTaskRun = taskRunQueue.poll();
                    pendingTaskRun.getStatus().setState(Constants.TaskRunState.RUNNING);
                    runningTaskRunMap.put(taskId, pendingTaskRun);
                    taskRunExecutor.executeTaskRun(pendingTaskRun);
                    // TaskRunStatusChange statusChange =
                    //        new TaskRunStatusChange(pendingTaskRun, Constants.TaskRunStatus.PENDING,
                    //                Constants.TaskRunStatus.RUNNING);
                    // GlobalStateMgr.getCurrentState().getEditLog().logTaskRunStatusChange(statusChange);
                }
            }
        }
    }

}
