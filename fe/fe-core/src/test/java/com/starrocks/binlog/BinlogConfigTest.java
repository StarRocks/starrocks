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

package com.starrocks.binlog;

import com.starrocks.common.util.PropertyAnalyzer;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class BinlogConfigTest {
    private static BinlogConfig binlogConfig;

    @BeforeClass
    public static void breforeClass() throws Exception {
        binlogConfig = new BinlogConfig(1, true, 2, 3);
    }

    @Test
    public void testBuildFromProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_VERSION, "10");
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE, "false");
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_TTL, "20");
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE, "30");
        binlogConfig.buildFromProperties(properties);
        Assert.assertEquals(10, binlogConfig.getVersion());
        Assert.assertFalse(binlogConfig.getBinlogEnable());
        Assert.assertEquals(20, binlogConfig.getBinlogTtlSecond());
        Assert.assertEquals(30, binlogConfig.getBinlogMaxSize());
    }

}
