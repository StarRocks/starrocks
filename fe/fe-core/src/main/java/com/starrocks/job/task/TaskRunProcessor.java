// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.job.task;

public interface TaskRunProcessor {
    void processTaskRun(TaskRunContext context) throws Exception;
}
