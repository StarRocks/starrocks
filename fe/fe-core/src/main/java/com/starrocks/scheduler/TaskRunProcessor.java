// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

public interface TaskRunProcessor {
    void processTaskRun(TaskRunContext context) throws Exception;
}
