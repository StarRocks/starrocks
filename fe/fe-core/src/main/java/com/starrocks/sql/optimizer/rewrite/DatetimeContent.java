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

package com.starrocks.sql.optimizer.rewrite;

/**
 * @program: starrocks_online
 * @author: evenhuang
 * @description:
 * @create: 2024-09-29 19:16
 **/
public class DatetimeContent {

    private long year;
    private long month;
    private long day;
    private long hour;
    private long minute;
    private long second;
    private long microsecond;
    private DatetimeContentType type;

    public DatetimeContent() {
        // if there not is year/month/day ,it will parse fail.
        // if there not is hour/minute/second, it will fill with 0
        year =  0;
        month = 0;
        day = 0;
        hour = 0;
        minute = 0;
        second = 0;
        microsecond = 0;
    }

    public long getYear() {
        return year;
    }

    public void setYear(long year) {
        this.year = year;
    }

    public long getMonth() {
        return month;
    }

    public void setMonth(long month) {
        this.month = month;
    }

    public long getDay() {
        return day;
    }

    public void setDay(long day) {
        this.day = day;
    }

    public long getHour() {
        return hour;
    }

    public void setHour(long hour) {
        this.hour = hour;
    }

    public long getMinute() {
        return minute;
    }

    public void setMinute(long minute) {
        this.minute = minute;
    }

    public long getSecond() {
        return second;
    }

    public void setSecond(long second) {
        this.second = second;
    }

    public long getMicrosecond() {
        return microsecond;
    }

    public void setMicrosecond(long microsecond) {
        this.microsecond = microsecond;
    }

    public DatetimeContentType getType() {
        return type;
    }

    public void setType(DatetimeContentType type) {
        this.type = type;
    }
}
