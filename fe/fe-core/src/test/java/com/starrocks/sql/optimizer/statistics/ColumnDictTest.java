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

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.ImmutableMap;
import com.starrocks.common.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ColumnDictTest {
    private int previousLowCardinalityThreshold = 0;

    @BeforeEach
    void setUP() {
        previousLowCardinalityThreshold = Config.low_cardinality_threshold;
        Config.low_cardinality_threshold = 512;
    }

    @AfterEach
    void tearDown() {
        Config.low_cardinality_threshold = previousLowCardinalityThreshold;
    }

    @Test
    void checkLowCardinalityConfigAvailable() {
        ImmutableMap.Builder<ByteBuffer, Integer> builder = ImmutableMap.builder();

        for (int i = 0; i < 300; i++) {
            String key = "string-" + i;
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            ByteBuffer keyBuffer = ByteBuffer.allocate(keyBytes.length);
            keyBuffer.put(keyBytes);
            keyBuffer.flip();

            builder.put(keyBuffer, i);
        }
        ImmutableMap<ByteBuffer, Integer> dictMap = builder.build();

        ColumnDict dict = new ColumnDict(dictMap, 1);
        Assertions.assertEquals(300, dict.getDictSize());
    }
}
