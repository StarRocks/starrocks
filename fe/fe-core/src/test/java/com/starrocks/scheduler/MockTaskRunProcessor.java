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


package com.starrocks.scheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;

public class MockTaskRunProcessor implements TaskRunProcessor {
    private static final Logger LOG = LogManager.getLogger(TaskManagerTest.class);
    private final long sleepTimeMs;

    public MockTaskRunProcessor() {
        this.sleepTimeMs = 0;
    }

    public MockTaskRunProcessor(long sleepTime) {
        this.sleepTimeMs = sleepTime;
    }

    @Override
    public void prepare(TaskRunContext context) throws Exception {
        // do nothing
    }

    @Override
    public void processTaskRun(TaskRunContext context) throws Exception {
        if (sleepTimeMs > 0) {
            Thread.sleep(sleepTimeMs);
        }
        LOG.info("running a task. currentTime:" + LocalDateTime.now());
    }

    @Override
    public void postTaskRun(TaskRunContext context) throws Exception {
    }
}
