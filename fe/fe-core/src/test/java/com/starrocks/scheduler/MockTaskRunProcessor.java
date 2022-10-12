// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;

public class MockTaskRunProcessor implements TaskRunProcessor {
    private static final Logger LOG = LogManager.getLogger(TaskManagerTest.class);

    @Override
    public void processTaskRun(TaskRunContext context) throws Exception {
        LOG.info("running a task. currentTime:" + LocalDateTime.now());
    }
}
