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
        String oldQueryId = taskRun.getQueryId();
        if (oldQueryId != null) {
            return null;
        }
        String newQueryId = UUIDUtil.genUUID().toString();
        taskRun.setQueryId(newQueryId);
        long taskId = taskRun.getTaskId();
        Queue<TaskRun> taskRunQueue = pendingTaskRunMap.get(taskId);
        if (taskRunQueue == null) {
            taskRunQueue = Queues.newConcurrentLinkedQueue();
            taskRunQueue.offer(taskRun);
            pendingTaskRunMap.put(taskId, taskRunQueue);
        } else {
            taskRunQueue.offer(taskRun);
        }
        // GlobalStateMgr.getCurrentState().getEditLog().logAddTaskRun(taskRun);
        return newQueryId;
    }

    public List<TaskRun> showTaskRun() {
        List<TaskRun> taskRunList = Lists.newArrayList();
        for (Queue<TaskRun> pJobs : pendingTaskRunMap.values()) {
            taskRunList.addAll(pJobs);
        }
        taskRunList.addAll(runningTaskRunMap.values());
        taskRunList.addAll(taskRunHistory.getAllHistory());
        return taskRunList;
    }

    // check if a running TaskRun is complete and remove it from running TaskRun map
    public void checkRunningTaskRun() {
        Iterator<Long> runningIterator = runningTaskRunMap.keySet().iterator();
        while (runningIterator.hasNext()) {
            Long mvTableId = runningIterator.next();
            TaskRun taskRun = runningTaskRunMap.get(mvTableId);
            Future<?> future = taskRun.getFuture();
            if (future.isDone()) {
                runningIterator.remove();
                taskRunHistory.addHistory(taskRun);
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
            Long mvTableId = pendingIterator.next();
            TaskRun runningTaskRun = runningTaskRunMap.get(mvTableId);
            if (runningTaskRun == null) {
                Queue<TaskRun> taskRunQueue = pendingTaskRunMap.get(mvTableId);
                if (taskRunQueue.size() == 0) {
                    pendingIterator.remove();
                } else {
                    TaskRun pendingTaskRun = taskRunQueue.poll();
                    pendingTaskRun.setState(Constants.TaskRunState.RUNNING);
                    runningTaskRunMap.put(mvTableId, pendingTaskRun);
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
