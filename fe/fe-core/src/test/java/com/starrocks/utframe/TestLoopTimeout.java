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

package com.starrocks.utframe;

import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Utility class to help prevent dead loops in test cases by providing timeout protection.
 */
public class TestLoopTimeout {
    private static final Logger LOG = LogManager.getLogger(TestLoopTimeout.class);
    
    private final long startTime;
    private final long timeoutMs;
    private final String operationName;
    
    /**
     * Creates a timeout protector for test loops.
     * 
     * @param operationName Name of the operation for logging purposes
     */
    public TestLoopTimeout(String operationName) {
        this.operationName = operationName;
        this.startTime = System.currentTimeMillis();
        this.timeoutMs = Config.test_loop_max_timeout_seconds * 1000L;
    }
    
    /**
     * Creates a timeout protector for test loops with custom timeout.
     * 
     * @param operationName Name of the operation for logging purposes
     * @param timeoutSeconds Custom timeout in seconds
     */
    public TestLoopTimeout(String operationName, int timeoutSeconds) {
        this.operationName = operationName;
        this.startTime = System.currentTimeMillis();
        this.timeoutMs = timeoutSeconds * 1000L;
    }
    
    /**
     * Checks if the timeout has been exceeded.
     * 
     * @return true if timeout exceeded, false otherwise
     */
    public boolean isTimeoutExceeded() {
        return System.currentTimeMillis() - startTime > timeoutMs;
    }
    
    /**
     * Checks if the timeout has been exceeded and logs a warning if so.
     * 
     * @return true if timeout exceeded, false otherwise
     */
    public boolean checkTimeout() {
        if (isTimeoutExceeded()) {
            LOG.warn("{} timeout after {} seconds, operation: {}", 
                    operationName, Config.test_loop_max_timeout_seconds, operationName);
            return true;
        }
        return false;
    }
    
    /**
     * Throws an exception if timeout is exceeded.
     * 
     * @throws RuntimeException if timeout exceeded
     */
    public void throwIfTimeout() {
        if (isTimeoutExceeded()) {
            throw new RuntimeException(operationName + " timeout after " + 
                    Config.test_loop_max_timeout_seconds + " seconds!");
        }
    }
    
    /**
     * Gets the elapsed time in milliseconds.
     * 
     * @return elapsed time in milliseconds
     */
    public long getElapsedTimeMs() {
        return System.currentTimeMillis() - startTime;
    }
    
    /**
     * Gets the remaining time in milliseconds.
     * 
     * @return remaining time in milliseconds, 0 if timeout exceeded
     */
    public long getRemainingTimeMs() {
        long elapsed = getElapsedTimeMs();
        return Math.max(0, timeoutMs - elapsed);
    }
}
