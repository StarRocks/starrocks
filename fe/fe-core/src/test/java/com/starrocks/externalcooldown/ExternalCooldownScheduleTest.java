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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.starrocks.externalcooldown.ExternalCooldownSchedule.TIME_FORMAT;


public class ExternalCooldownScheduleTest {
    public void baseCheck(String scheduleStr, String start, String end, long interval, String unit, long seconds) {
        ExternalCooldownSchedule schedule = ExternalCooldownSchedule.fromString(scheduleStr);
        Assert.assertNotNull(schedule);
        Assert.assertEquals(start, schedule.getStart());
        Assert.assertEquals(end, schedule.getEnd());
        Assert.assertEquals(interval, schedule.getInterval());
        Assert.assertEquals(unit, schedule.getUnit());
        Assert.assertEquals(seconds, schedule.getIntervalSeconds());
    }

    @Test
    public void testPartitionStartEnd() {
        baseCheck("START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE", "01:00", "07:59", 1L, "MINUTE", 60L);
        baseCheck("START 01:00 END 07:59 EVERY INTERVAL 1 HOUR", "01:00", "07:59", 1L, "HOUR", 3600L);
        baseCheck("START 01:00 END 07:59 EVERY INTERVAL 1 SECOND", "01:00", "07:59", 1L, "SECOND", 1L);
    }

    @Test
    public void testPartitionStartEndDefault() {
        String scheduleStr = "START 01:00 END 07:59 EVERY INTERVAL 23";
        ExternalCooldownSchedule schedule = ExternalCooldownSchedule.fromString(scheduleStr);
        Assert.assertNull(schedule);
    }

    @Test
    public void testValidateScheduleString() {
        Assert.assertFalse(ExternalCooldownSchedule.validateScheduleString("START 01:00 END 07:59"));
        Assert.assertFalse(ExternalCooldownSchedule.validateScheduleString("START 25:00 END 07:59 EVERY INTERVAL 23"));
        Assert.assertFalse(ExternalCooldownSchedule.validateScheduleString("START 25:00 END 07:59 EVERY INTERVAL 23 SECOND"));
        Assert.assertFalse(ExternalCooldownSchedule.validateScheduleString("START 01:00 EVERY INTERVAL 23"));
        Assert.assertFalse(ExternalCooldownSchedule.validateScheduleString("START 01:00 END 07:59 EVERY INTERVAL 23"));
        Assert.assertTrue(ExternalCooldownSchedule.validateScheduleString("START 01:00 END 07:59 EVERY INTERVAL 23 SECOND"));
    }

    @Test
    public void testToString() {
        String scheduleStr = "START 01:00 END 07:59 EVERY INTERVAL 23 SECOND";
        ExternalCooldownSchedule schedule = ExternalCooldownSchedule.fromString(scheduleStr);
        Assert.assertNotNull(schedule);
        Assert.assertEquals(scheduleStr, schedule.toString());
    }

    public boolean runTrySchedule(long now, long startMs, long endMs) {
        String start = TIME_FORMAT.get().format(startMs);
        String end = TIME_FORMAT.get().format(endMs);
        String scheduleStr = String.format("START %s END %s EVERY INTERVAL 23 SECOND", start, end);
        ExternalCooldownSchedule schedule = ExternalCooldownSchedule.fromString(scheduleStr);
        return schedule.trySchedule(now);
    }

    @Test
    public void testTrySchedule() throws ParseException {
        long now = System.currentTimeMillis();
        Assert.assertTrue(runTrySchedule(now, now - 60 * 1000, now + 60 * 1000));
        Assert.assertFalse(runTrySchedule(now, now + 60 * 1000, now + 2 * 60 * 1000));
        Assert.assertFalse(runTrySchedule(now, now - 2 * 60 * 1000, now - 60 * 1000));
        Assert.assertFalse(runTrySchedule(now, now, now));

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date nowDate = dateFormat.parse("2024-10-30 22:00:00");
        Date startDate = dateFormat.parse("2024-10-30 23:00:00");
        Date endDate = dateFormat.parse("2024-10-30 07:00:00");
        Assert.assertFalse(runTrySchedule(nowDate.getTime(), startDate.getTime(), endDate.getTime()));
    }
}