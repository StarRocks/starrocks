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

package com.starrocks.qe.scheduler;

import com.google.common.base.Preconditions;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.qe.scheduler.dag.JobInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SchedulerTraceUtil {
    private static final Logger LOG = LogManager.getLogger(SchedulerTraceUtil.class);

    public static void log(JobInformation info, Object object) {
        if (info.isEnableTraceLog()) {
            doLog(info, "{}", object);
        }
    }

    public static void log(JobInformation info, String format, Object... params) {
        if (info.isEnableTraceLog()) {
            doLog(info, format, params);
        }
    }

    private static void doLog(JobInformation info, String format, Object... params) {
        Preconditions.checkState(info.isEnableTraceLog(), "Use schedulerTraceLog when it is disabled");
        String prefix;
        if (info.isSetLoadJobId()) {
            prefix = String.format("[TRACE SCHEDULER %s %d] ", DebugUtil.printId(info.getQueryId()), info.getLoadJobId());
        } else {
            prefix = String.format("[TRACE SCHEDULER %s] ", DebugUtil.printId(info.getQueryId()));
        }
        LOG.info(prefix + format, params);
    }
}
