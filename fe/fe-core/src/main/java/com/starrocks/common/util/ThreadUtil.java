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

package com.starrocks.common.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ThreadUtil {
    private static final Logger LOG = LogManager.getLogger(ThreadUtil.class);

    /**
     * Borrowed from hadoop-common and fix negative sleep time
     * https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-common-project/hadoop-common/
     * src/main/java/org/apache/hadoop/util/ThreadUtil.java#L39
     *
     * Cause the current thread to sleep as close as possible to the provided
     * number of milliseconds. This method will log and ignore any
     * {@link InterruptedException} encountered.
     *
     * @param millis the number of milliseconds for the current thread to sleep
     */
    public static void sleepAtLeastIgnoreInterrupts(long millis) {
        long start = System.currentTimeMillis();
        while (true) {
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed >= millis) {
                break;
            }

            long timeToSleep = millis - elapsed;
            try {
                Thread.sleep(timeToSleep);
            } catch (InterruptedException ie) {
                LOG.warn("interrupted while sleeping", ie);
            }
        }
    }

}
