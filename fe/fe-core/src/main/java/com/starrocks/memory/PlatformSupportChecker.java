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

package com.starrocks.memory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Utility class to check platform support for AsyncProfiler.
 * AsyncProfiler has limited platform support and may not work on all operating systems.
 */
public class PlatformSupportChecker {
    private static final Logger LOG = LogManager.getLogger(PlatformSupportChecker.class);
    
    private static final String OS_NAME = System.getProperty("os.name", "").toLowerCase();
    private static final String OS_ARCH = System.getProperty("os.arch", "").toLowerCase();
    
    // AsyncProfiler is known to work on Linux and macOS
    private static final boolean IS_SUPPORTED_PLATFORM = isLinux() || isMacOS();
    
    /**
     * Check if the current platform supports AsyncProfiler.
     * 
     * @return true if AsyncProfiler is supported on this platform, false otherwise
     */
    public static boolean isAsyncProfilerSupported() {
        return IS_SUPPORTED_PLATFORM;
    }
    
    /**
     * Get a human-readable description of the current platform.
     * 
     * @return platform description string
     */
    public static String getPlatformDescription() {
        return String.format("%s-%s", OS_NAME, OS_ARCH);
    }
    
    /**
     * Log platform compatibility information.
     */
    public static void logPlatformInfo() {
        if (isAsyncProfilerSupported()) {
            LOG.info("AsyncProfiler is supported on platform: {}", getPlatformDescription());
        } else {
            LOG.warn("AsyncProfiler is not supported on platform: {}. Profiling will be disabled.", 
                    getPlatformDescription());
        }
    }
    
    private static boolean isLinux() {
        return OS_NAME.contains("linux");
    }
    
    private static boolean isMacOS() {
        return OS_NAME.contains("mac") || OS_NAME.contains("darwin");
    }
}