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

import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ExternalCooldownSchedule {
    private static final Pattern SCHEDULE_PATTERN = Pattern.compile(
            "\\s*START\\s+(?<start>\\d{2}:\\d{2})\\s+END\\s+(?<end>\\d{2}:\\d{2})\\s+EVERY\\s+" +
                    "INTERVAL\\s+(?<interval>\\d+)\\s+(?<unit>(HOUR|MINUTE|SECOND))\\s*",
            Pattern.CASE_INSENSITIVE);
    public static final ThreadLocal<SimpleDateFormat> TIME_FORMAT = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("HH:mm"));

    private final String start;
    private final String end;
    private final long interval;
    private final String unit;
    private long lastScheduleMs = 0L;


    public ExternalCooldownSchedule(String start, String end, long interval, String unit) {
        this.start = start;
        this.end = end;
        this.interval = interval;
        this.unit = unit;
    }

    public static boolean validateScheduleString(String schedule) {
        ExternalCooldownSchedule externalCooldownSchedule = fromString(schedule);
        if (externalCooldownSchedule == null) {
            return false;
        }
        String start = externalCooldownSchedule.getStart();
        if (start.compareTo("23:59") > 0 || start.compareTo("00:00") < 0) {
            return false;
        }
        String end = externalCooldownSchedule.getEnd();
        return end.compareTo("23:59") <= 0 && end.compareTo("00:00") >= 0;
    }

    public long getIntervalSeconds() {
        long intervalSeconds;
        switch (unit.toUpperCase()) {
            case "HOUR":
                intervalSeconds = interval * 3600;
                break;
            case "MINUTE":
                intervalSeconds = interval * 60;
                break;
            default:
                intervalSeconds = interval;
                break;
        }
        return intervalSeconds;
    }

    public static ExternalCooldownSchedule fromString(String schedule) {
        if (schedule == null || schedule.isEmpty()) {
            return null;
        }
        Matcher matcher = SCHEDULE_PATTERN.matcher(schedule);
        if (!matcher.matches()) {
            return null;
        }
        String start = matcher.group("start");
        String end = matcher.group("end");
        String interval = matcher.group("interval");
        String unit = matcher.group("unit");
        long intervalValue = Long.parseLong(interval);
        return new ExternalCooldownSchedule(start, end, intervalValue, unit);
    }

    public String getStart() {
        return start;
    }

    public String getEnd() {
        return end;
    }

    public long getInterval() {
        return interval;
    }

    public String getUnit() {
        return unit;
    }

    @Override
    public String toString() {
        return String.format("START %s END %s EVERY INTERVAL %s %s", start, end, interval, unit);
    }

    public long getLastScheduleMs() {
        return lastScheduleMs;
    }

    public void setLastScheduleMs(long lastScheduleMs) {
        this.lastScheduleMs = lastScheduleMs;
    }

    public boolean trySchedule(long currentMs) {
        String s = TIME_FORMAT.get().format(currentMs);
        if (end.compareTo(start) < 0) {
            // ex: [start=23:00, end=07:00)
            if (!(s.compareTo(start) >= 0 || s.compareTo(end) < 0)) {
                return false;
            }
        } else if (end.compareTo(start) > 0) {
            // ex: [start=01:00, end=07:00)
            if (s.compareTo(start) < 0) {
                return false;
            }
            if (s.compareTo(end) >= 0) {
                return false;
            }
        } else {
            // never schedule if start == end
            return false;
        }
        if ((currentMs - getLastScheduleMs()) / 1000 < getIntervalSeconds()) {
            return false;
        }
        lastScheduleMs = currentMs;
        return true;
    }
}
