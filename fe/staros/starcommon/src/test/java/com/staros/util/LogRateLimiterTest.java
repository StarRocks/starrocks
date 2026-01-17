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

package com.staros.util;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.simple.SimpleLogger;
import org.apache.logging.log4j.util.PropertiesUtil;
import org.junit.Assert;
import org.junit.Test;

public class LogRateLimiterTest {

    // A mock Logger just for counting the logMessage()
    private static class MyLogger extends SimpleLogger {
        private long logCount = 0;

        public MyLogger() {
            super("", Level.INFO, false, false, false, false, "",
                    null, PropertiesUtil.getProperties(), null);
        }

        @Override
        public void logMessage(final String fqcn, final Level mgsLevel, final Marker marker, final Message msg,
                               final Throwable throwable) {
            ++logCount;
        }

        public long getLogCount() {
            return logCount;
        }
    }

    @Test
    public void testLoggerLimit() throws InterruptedException {
        MyLogger log = new MyLogger();
        // allow limit one log line/seconds
        LogRateLimiter limiter = new LogRateLimiter(log, 1);
        String msg = "hello world";
        long expectRunMs = 5 * 1000; // 5s
        long sleepIntervalMs = 100; // 100ms
        long repeat = expectRunMs / sleepIntervalMs;
        long begin = System.currentTimeMillis();
        for (int i = 0; i < repeat; ++i) {
            limiter.info(msg);
            Thread.sleep(sleepIntervalMs);
        }
        long end = System.currentTimeMillis();
        long elapse = end - begin;
        long count = log.getLogCount();
        // elapse / 1000 ~~ count with delta: 1
        Assert.assertEquals("LogCount", count, (float) elapse / 1000, 1);
        // elapse / 1000 != repeat with delta: 1
        Assert.assertNotEquals("RepeatCount", repeat, (float) elapse / 1000, 1);
    }
}
