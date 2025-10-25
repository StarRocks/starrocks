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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

public class PlatformSupportCheckerTest {

    @Test
    public void testPlatformDetection() {
        // Test that platform detection methods work
        boolean isSupported = PlatformSupportChecker.isAsyncProfilerSupported();
        String platformDesc = PlatformSupportChecker.getPlatformDescription();
        
        // These should not throw exceptions
        Assertions.assertNotNull(platformDesc);
        Assertions.assertFalse(platformDesc.isEmpty());
        
        // Log platform info should not throw exceptions
        Assertions.assertDoesNotThrow(() -> PlatformSupportChecker.logPlatformInfo());
    }

    @Test
    public void testPlatformDescription() {
        String platformDesc = PlatformSupportChecker.getPlatformDescription();
        
        // Should contain OS name and architecture
        Assertions.assertTrue(platformDesc.contains("-"));
        Assertions.assertFalse(platformDesc.isEmpty());
    }
}