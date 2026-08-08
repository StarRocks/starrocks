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

package com.starrocks.catalog;

import com.starrocks.common.Config;
import com.starrocks.sql.optimizer.statistics.CacheDictManager;
import mockit.Expectations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class CacheDictManagerTest {
    private final long savedMaxBytes = Config.low_cardinality_dict_cache_max_bytes;
    private final double savedMaxMemRatio = Config.low_cardinality_dict_cache_max_mem_ratio;

    @AfterEach
    public void restoreConfig() {
        Config.low_cardinality_dict_cache_max_bytes = savedMaxBytes;
        Config.low_cardinality_dict_cache_max_mem_ratio = savedMaxMemRatio;
    }

    @Test
    public void testCacheMaxBytesUsesRatioOfHeapWhenSet() {
        Config.low_cardinality_dict_cache_max_bytes = 1024;
        Config.low_cardinality_dict_cache_max_mem_ratio = 0.25;
        Assertions.assertEquals(Math.round(Runtime.getRuntime().maxMemory() * 0.25),
                CacheDictManager.resolveCacheMaxBytes());
    }

    @Test
    public void testCacheMaxBytesUsesAbsoluteConfigWhenRatioIsZero() {
        Config.low_cardinality_dict_cache_max_bytes = 12345;
        Config.low_cardinality_dict_cache_max_mem_ratio = 0;
        Assertions.assertEquals(12345, CacheDictManager.resolveCacheMaxBytes());
    }

    @Test
    public void test() {
        CacheDictManager manager = new CacheDictManager();
        new Expectations(manager) {
            {
                manager.getGlobalDict(anyLong, ColumnId.create("val"));
                result = Optional.empty();
            }
        };
        manager.getGlobalDict(1, ColumnId.create("val"));
    }
}
