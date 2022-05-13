// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.scheduler;

import com.starrocks.statistic.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class TaskRunExecutor {
    private static final Logger LOG = LogManager.getLogger(TaskRunExecutor.class);
    private final ExecutorService taskRunPool = Executors.newCachedThreadPool();

    public void executeTaskRun(TaskRun taskRun) {
        if (taskRun == null) {
            return;
        }

        if (taskRun.getState() == Constants.TaskRunState.SUCCESS ||
                taskRun.getState() == Constants.TaskRunState.FAILED) {
            LOG.warn("TaskRun {} is in final status {} ", taskRun.getQueryId(), taskRun.getState());
            return;
        }

        Future<?> future = taskRunPool.submit(() -> {
            taskRun.setState(Constants.TaskRunState.RUNNING);
            try {
                taskRun.setStartTime(System.currentTimeMillis());
                boolean isSuccess = taskRun.executeTaskRun();
                if (isSuccess) {
                    taskRun.setState(Constants.TaskRunState.SUCCESS);
                } else {
                    taskRun.setState(Constants.TaskRunState.FAILED);
                }
            } catch (Exception ex) {
                LOG.warn("failed to execute TaskRun.", ex);
                taskRun.setState(Constants.TaskRunState.FAILED);
                taskRun.setErrorCode(-1);
                taskRun.setErrorMsg(ex.toString());
            } finally {
                taskRun.setCompleteTime(System.currentTimeMillis());
            }
        });
        taskRun.setFuture(future);
    }

}
