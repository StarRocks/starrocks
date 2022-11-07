// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

import com.starrocks.scheduler.persist.TaskRunStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TaskRunExecutor {
    private static final Logger LOG = LogManager.getLogger(TaskRunExecutor.class);
    private final ExecutorService taskRunPool = Executors.newCachedThreadPool();

    public void executeTaskRun(TaskRun taskRun) {
        if (taskRun == null) {
            return;
        }
        TaskRunStatus status = taskRun.getStatus();
        if (status == null) {
            return;
        }
        if (status.getState() == Constants.TaskRunState.SUCCESS ||
                status.getState() == Constants.TaskRunState.FAILED) {
            LOG.warn("TaskRun {} is in final status {} ", status.getQueryId(), status.getState());
            return;
        }

        CompletableFuture<Constants.TaskRunState> future = CompletableFuture.supplyAsync(() -> {
            status.setState(Constants.TaskRunState.RUNNING);
            try {
                boolean isSuccess = taskRun.executeTaskRun();
                if (isSuccess) {
                    status.setState(Constants.TaskRunState.SUCCESS);
                } else {
                    status.setState(Constants.TaskRunState.FAILED);
                }
            } catch (Exception ex) {
                LOG.warn("failed to execute TaskRun.", ex);
                status.setState(Constants.TaskRunState.FAILED);
                status.setErrorCode(-1);
                status.setErrorMessage(ex.toString());
            } finally {
                status.setFinishTime(System.currentTimeMillis());
            }
            return status.getState();
        }, taskRunPool);
        future.whenComplete((r, e) -> {
            if (e == null) {
                taskRun.getFuture().complete(r);
            } else {
                taskRun.getFuture().completeExceptionally(e);
            }
        });
    }

}
