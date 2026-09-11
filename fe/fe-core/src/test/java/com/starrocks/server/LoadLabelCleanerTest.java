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

package com.starrocks.server;

import com.starrocks.common.Config;
import com.starrocks.common.ConfigBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class LoadLabelCleanerTest {
    private static final long T0 = 1_700_000_000_000L;
    private static final long MINUTE_MS = 60 * 1000L;

    @Test
    public void testConfigIsMutable() throws Exception {
        Assertions.assertTrue(Config.class.getField("label_clean_interval_second")
                .getAnnotation(ConfigBase.ConfField.class).mutable());
    }

    @Test
    public void testCleansPerIntervalAndFollowsRuntimeChanges() {
        int original = Config.label_clean_interval_second;
        try {
            Config.label_clean_interval_second = 4 * 3600;
            AtomicInteger rounds = new AtomicInteger();
            LoadLabelCleaner cleaner = new LoadLabelCleaner(rounds::incrementAndGet);
            // Wakes up every minute regardless of the configured interval.
            Assertions.assertEquals(LoadLabelCleaner.TICK_MS, cleaner.getInterval());

            // The first tick cleans immediately, then nothing until the interval elapses.
            cleaner.tick(T0);
            cleaner.tick(T0 + MINUTE_MS);
            Assertions.assertEquals(1, rounds.get());

            // Shrink to ten minutes at runtime: honored on the next tick, without waiting out the old four hours.
            Config.label_clean_interval_second = 10 * 60;
            cleaner.tick(T0 + 10 * MINUTE_MS - 1);
            Assertions.assertEquals(1, rounds.get());
            cleaner.tick(T0 + 10 * MINUTE_MS);
            Assertions.assertEquals(2, rounds.get());

            // Grow to one hour: the next clean waits for the new interval.
            Config.label_clean_interval_second = 3600;
            cleaner.tick(T0 + 20 * MINUTE_MS);
            Assertions.assertEquals(2, rounds.get());
            cleaner.tick(T0 + 70 * MINUTE_MS);
            Assertions.assertEquals(3, rounds.get());
        } finally {
            Config.label_clean_interval_second = original;
        }
    }
}
