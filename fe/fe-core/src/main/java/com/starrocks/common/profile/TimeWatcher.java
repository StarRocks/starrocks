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

import com.google.common.base.Stopwatch;
import com.starrocks.common.util.DebugUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TimeWatcher {
    private int levels = 0;

    private final Map<String, ScopedTimer> timers = new LinkedHashMap<>();

    public Timer scope(long time, String name) {
        ScopedTimer t;
        if (timers.containsKey(name)) {
            t = timers.get(name);
        } else {
            t = new ScopedTimer(time, name);
            timers.put(name, t);
        }
        t.start();
        return t;
    }

    public List<Timer> getAllTimerWithOrder() {
        return timers.values().stream().sorted(Comparator.comparingLong(o -> o.firstTimePoints))
                .collect(Collectors.toList());
    }

    private class ScopedTimer extends Timer {
        private final String name;
        private final int scopeLevel;
        private final long firstTimePoints;
        private final Stopwatch stopWatch = Stopwatch.createUnstarted();

        private int count = 0;
        private int reentrantCount = 0;

        public ScopedTimer(long time, String name) {
            this.firstTimePoints = time;
            this.name = name;
            this.scopeLevel = levels;
        }

        @Override
        public String name() {
            return name;
        }

        public void start() {
            if (reentrantCount == 0) {
                stopWatch.start();
            }
            reentrantCount++;
            levels++;
            count++;
        }

        public void close() {
            reentrantCount--;
            if (reentrantCount == 0) {
                levels--;
                stopWatch.stop();
            }
        }

        @Override
        public long getFirstTimePoint() {
            return firstTimePoints;
        }

        @Override
        public String toString() {
            return StringUtils.repeat("    ", scopeLevel) + "-- " + name + "[" + count + "] " +
                    DebugUtil.getPrettyStringMs(getTotalTime());
        }

        @Override
        public long getTotalTime() {
            return stopWatch.elapsed(TimeUnit.MILLISECONDS);
        }
    }
}
