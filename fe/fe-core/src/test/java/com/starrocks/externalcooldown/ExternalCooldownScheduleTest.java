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

package com.starrocks.externalcooldown;

import org.junit.Assert;
import org.junit.Test;


public class ExternalCooldownScheduleTest {

    @Test
    public void testPartitionStartEnd() {
        String scheduleStr = "START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE";
        ExternalCooldownSchedule schedule = ExternalCooldownSchedule.fromString(scheduleStr);
        Assert.assertNotNull(schedule);
        Assert.assertEquals("01:00", schedule.getStart());
        Assert.assertEquals("07:59", schedule.getEnd());
        Assert.assertEquals(1L, schedule.getInterval());
        Assert.assertEquals("MINUTE", schedule.getUnit());
        Assert.assertEquals(60L, schedule.getIntervalSeconds());
    }
}