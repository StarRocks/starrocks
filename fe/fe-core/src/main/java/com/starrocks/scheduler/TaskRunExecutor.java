// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.scheduler;

import com.starrocks.statistic.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
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

        if (taskRun.getStatus() == Constants.TaskRunState.SUCCESS ||
                taskRun.getStatus() == Constants.TaskRunState.FAILED ||
                taskRun.getStatus() == Constants.TaskRunState.CANCELED) {
            LOG.warn("TaskRun {} is in final status {} ", taskRun.getQueryId(), taskRun.getStatus());
            return;
        }

        Future<?> future = taskRunPool.submit(() -> {
            taskRun.setStatus(Constants.TaskRunState.RUNNING);
            try {
                taskRun.setStartTime(LocalDateTime.now());
                boolean isSuccess = taskRun.executeTaskRun();
                if (isSuccess) {
                    taskRun.setStatus(Constants.TaskRunState.SUCCESS);
                } else {
                    taskRun.setStatus(Constants.TaskRunState.FAILED);
                }
            } catch (Exception ex) {
                LOG.warn("failed to execute TaskRun.", ex);
                taskRun.setStatus(Constants.TaskRunState.FAILED);
                taskRun.setErrorCode(-1);
                taskRun.setErrorMsg(ex.toString());
            } finally {
                taskRun.setCompleteTime(LocalDateTime.now());
            }
        });
        taskRun.setFuture(future);
    }

}
