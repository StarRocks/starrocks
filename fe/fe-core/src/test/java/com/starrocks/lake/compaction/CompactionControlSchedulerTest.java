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

package com.starrocks.lake.compaction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Calendar;

public class CompactionControlSchedulerTest {
    @Test
    public void testForbiddenTimeRangeUsesWholeHour() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2026, Calendar.SEPTEMBER, 28, 8, 30, 45);
        Assertions.assertTrue(CompactionControlScheduler.isBaseCompactionForbidden(
                "* 8-20 * * *", calendar.getTime()));

        calendar.set(Calendar.HOUR_OF_DAY, 21);
        Assertions.assertFalse(CompactionControlScheduler.isBaseCompactionForbidden(
                "* 8-20 * * *", calendar.getTime()));
        Assertions.assertFalse(CompactionControlScheduler.isBaseCompactionForbidden("", calendar.getTime()));
        Assertions.assertTrue(CompactionControlScheduler.isBaseCompactionForbidden(
                "* * * * *", calendar.getTime()));
    }

    @Test
    public void testInvalidTimeRange() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CompactionControlScheduler.toQuartzExpression("0 8-20 * * *"));
    }
}
