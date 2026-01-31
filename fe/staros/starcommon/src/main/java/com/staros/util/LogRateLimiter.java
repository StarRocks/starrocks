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

import com.google.common.util.concurrent.RateLimiter;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import java.util.concurrent.atomic.AtomicLong;

public class LogRateLimiter {
    private final RateLimiter limiter;
    private final AtomicLong omitCounter;
    private final Logger logger;

    /**
     * @param log the logger to write to
     * @param limits counts per second, can be decimal number
     */
    public LogRateLimiter(Logger log, double limits) {
        this.logger = log;
        this.limiter = RateLimiter.create(limits);
        this.omitCounter = new AtomicLong(0);
    }

    public void info(String messagePattern, Object ... params) {
        String msg = ParameterizedMessage.format(messagePattern, params);
        if (limiter.tryAcquire()) {
            long omits = omitCounter.getAndSet(0);
            if (omits == 0) {
                logger.info(msg);
            } else {
                logger.info("{}. [{} similar messages omitted]", msg, omits);
            }
        } else {
            omitCounter.incrementAndGet();
        }
    }
}
