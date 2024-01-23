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

import com.starrocks.common.AnalysisException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PropertyAnalyzerTest {

    @Test
    public void testAnalyzeDataCacheInfo() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");
        properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_ASYNC_WRITE_BACK, "true");

        try {
            PropertyAnalyzer.analyzeDataCacheInfo(properties);
            Assert.assertTrue(false);
        } catch (AnalysisException e) {
            Assert.assertEquals("enable_async_write_back is disabled since version 3.1.4", e.getMessage());
        }
    }

    @Test
    public void testAnalyzeDataCachePartitionDuration() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION, "7 day");

        try {
            PropertyAnalyzer.analyzeDataCachePartitionDuration(properties);
        } catch (AnalysisException e) {
            Assert.assertTrue(false);
        }

        Assert.assertTrue(properties.size() == 0);
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION, "abcd");
        try {
            PropertyAnalyzer.analyzeDataCachePartitionDuration(properties);
            Assert.assertTrue(false);
        } catch (AnalysisException e) {
            Assert.assertEquals("Cannot parse text to Duration", e.getMessage());
        }
    }
}
