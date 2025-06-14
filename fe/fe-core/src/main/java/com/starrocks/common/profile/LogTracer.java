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

package com.starrocks.common.profile;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public abstract class LogTracer {
    public void log(long time, String content) {
    }

    public void log(long time, String format, Object... objects) {
    }

    public void log(long time, Function<Object[], String> logFunc, Object... objects) {
    }

    public List<LogEvent> getLogs() {
        return Collections.emptyList();
    }

    protected static class LogEvent {
        private final long timePoint;
        private final String log;

        public LogEvent(long timePoint, String log) {
            this.timePoint = timePoint;
            this.log = log;
        }

        public long getTimePoint() {
            return timePoint;
        }

        public String getLog() {
            return log;
        }
    }
}
