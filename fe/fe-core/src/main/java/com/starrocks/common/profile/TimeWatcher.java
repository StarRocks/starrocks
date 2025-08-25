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
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.starrocks.common.util.DebugUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TimeWatcher {
    private final List<String> levels = Lists.newArrayList();
    private final Table<String, String, ScopedTimer> scopedTimers = HashBasedTable.create();

    public Timer scope(long time, String name) {
        ScopedTimer t;
        String prefix = String.join("/", levels);
        if (scopedTimers.row(name).containsKey(prefix)) {
            t = scopedTimers.row(name).get(prefix);
        } else {
            t = new ScopedTimer(time, name);
            scopedTimers.put(name, prefix, t);
        }
        t.start();
        return t;
    }

    public Optional<Timer> getTimer(String name) {
        if (!scopedTimers.containsRow(name)) {
            return Optional.empty();
        }
        Map<String, ScopedTimer> timers = scopedTimers.row(name);
        if (timers.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(timers.entrySet().stream()
                .min(Comparator.comparingInt(e -> e.getKey().length()))
                .map(Map.Entry::getValue)
                .orElse(null));
    }

    public List<Timer> getAllTimerWithOrder() {
        return scopedTimers.values().stream().sorted(Comparator.comparingLong(o -> o.firstTimePointNanoSecond))
                .collect(Collectors.toList());
    }

    private class ScopedTimer extends Timer {
        private final String name;
        private final int scopeLevel;
        private final long firstTimePointNanoSecond;
        private final Stopwatch stopWatch = Stopwatch.createUnstarted();

        private int count = 0;
        private int reentrantCount = 0;

        public ScopedTimer(long time, String name) {
            // The reason why here we want nanosecond is to make sure
            // `getAllTimerWithOrder` can sort times in correct order.
            this.firstTimePointNanoSecond = time;
            this.name = name;
            this.scopeLevel = levels.size();
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
            count++;
            levels.add(name);
        }

        public void close() {
            reentrantCount--;
            levels.remove(levels.size() - 1);
            if (reentrantCount == 0) {
                stopWatch.stop();
            }
        }

        @Override
        public long getFirstTimePoint() {
            return firstTimePointNanoSecond / 1000000;
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
