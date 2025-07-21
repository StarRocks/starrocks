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

package com.starrocks.statistic;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CancelableAnalyzeTask implements Runnable {
    private static final Logger LOG = LogManager.getLogger(CancelableAnalyzeTask.class);

    private final Runnable originalTask;
    private final AnalyzeStatus analyzeStatus;
    private volatile boolean cancelled = false;

    public CancelableAnalyzeTask(Runnable originalTask, AnalyzeStatus analyzeStatus) {
        this.originalTask = originalTask;
        this.analyzeStatus = analyzeStatus;
    }

    @Override
    public void run() {
        if (cancelled) {
            analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
            LOG.info("Analyze task {} was cancelled before execution", analyzeStatus.getId());
            return;
        }
        originalTask.run();
    }

    public void cancel() {
        cancelled = true;
        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
    }
}

