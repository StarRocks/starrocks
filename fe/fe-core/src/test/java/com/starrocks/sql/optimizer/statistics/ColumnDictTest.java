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
import com.starrocks.common.Pair;
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

    @Test
    public void testMergeDictSuccess() {
        ImmutableMap.Builder<ByteBuffer, Integer> builder1 = ImmutableMap.builder();
        ImmutableMap.Builder<ByteBuffer, Integer> builder2 = ImmutableMap.builder();

        addWords(builder1, 1, 101, "common-");
        addWords(builder1, 101, 121, "unique1-");
        addWords(builder2, 1, 101, "common-");
        addWords(builder2, 101, 131, "unique2-");

        ImmutableMap<ByteBuffer, Integer> dictMap1 = builder1.build();
        ImmutableMap<ByteBuffer, Integer> dictMap2 = builder2.build();
        ColumnDict dict1 = new ColumnDict(dictMap1, 1);
        ColumnDict dict2 = new ColumnDict(dictMap2, 2);

        Pair<ColumnDict, ColumnDict> res = ColumnDict.merge(dict1, dict2);

        Assertions.assertNotNull(res);
        ColumnDict newDict1 = res.first;
        ColumnDict newDict2 = res.second;
        Assertions.assertEquals(newDict1.getVersion(), dict1.getVersion());
        Assertions.assertEquals(newDict2.getVersion(), dict2.getVersion());

        Assertions.assertEquals(150, newDict1.getDictSize());
        for (int i = 1; i < 101; i++) {
            String key = "common-" + i;
            ByteBuffer keyBuffer = toByteBuffer(key);
            Assertions.assertEquals(i, newDict1.getDict().get(keyBuffer));
        }
        for (int i = 101; i < 121; i++) {
            String key = "unique1-" + i;
            ByteBuffer keyBuffer = toByteBuffer(key);
            Assertions.assertEquals(i, newDict1.getDict().get(keyBuffer));
        }
        for (int i = 101; i < 131; i++) {
            String key = "unique2-" + i;
            ByteBuffer keyBuffer = toByteBuffer(key);
            Assertions.assertEquals(i + 20, newDict1.getDict().get(keyBuffer));
        }

        Assertions.assertEquals(newDict1.getDict(), newDict2.getDict());

        res = ColumnDict.merge(newDict1, newDict2);

        Assertions.assertNotNull(res);
        ColumnDict newDict11 = res.first;
        ColumnDict newDict21 = res.second;
        Assertions.assertEquals(newDict11.getDict(), newDict1.getDict());
        Assertions.assertEquals(newDict11.getDict(), newDict21.getDict());
    }

    @Test
    public void testMergeDictWithHighBitBytesUtf8() {
        // Regression test: BE sorts dict strings via memcmp (unsigned bytes) and assigns ids in
        // that order. ByteBuffer.compareTo on JDK 8 compares bytes as signed, which inverts the
        // order of any UTF-8 string with a high-bit byte. Walking the merge with a signed
        // comparator over a BE-sorted (unsigned) array put d2's high-byte key at newIdx=1, then
        // drained d1 and put the same key again at a later newIdx, failing ImmutableMap.build()
        // with "Multiple entries with same key".
        // Minimal repro: ASCII '7' (0x37) vs Cyrillic 'В' (UTF-8 first byte 0xD0). Signed: 0x37 -
        // (signed 0xD0) = +103. Unsigned: 0x37 - 0xD0 = -153.
        ImmutableMap.Builder<ByteBuffer, Integer> builder1 = ImmutableMap.builder();
        ImmutableMap.Builder<ByteBuffer, Integer> builder2 = ImmutableMap.builder();

        builder1.put(toByteBuffer("78"), 1);
        builder1.put(toByteBuffer("В"), 2);

        builder2.put(toByteBuffer("В"), 1);

        ColumnDict dict1 = new ColumnDict(builder1.build(), 1);
        ColumnDict dict2 = new ColumnDict(builder2.build(), 2);

        Pair<ColumnDict, ColumnDict> res = ColumnDict.merge(dict1, dict2);

        Assertions.assertNotNull(res);
        Assertions.assertEquals(2, res.first.getDictSize());
        Assertions.assertEquals(1, res.first.getDict().get(toByteBuffer("78")));
        Assertions.assertEquals(2, res.first.getDict().get(toByteBuffer("В")));
        Assertions.assertEquals(res.first.getDict(), res.second.getDict());
    }

    @Test
    public void testMergeDictFail() {
        ImmutableMap.Builder<ByteBuffer, Integer> builder1 = ImmutableMap.builder();
        ImmutableMap.Builder<ByteBuffer, Integer> builder2 = ImmutableMap.builder();

        addWords(builder1, 1, 101, "common-");
        addWords(builder1, 101, 121, "unique1-");
        addWords(builder2, 1, 101, "common-");
        addWords(builder2, 101, 511, "unique2-");

        ImmutableMap<ByteBuffer, Integer> dictMap1 = builder1.build();
        ImmutableMap<ByteBuffer, Integer> dictMap2 = builder2.build();
        ColumnDict dict1 = new ColumnDict(dictMap1, 1);
        ColumnDict dict2 = new ColumnDict(dictMap2, 2);

        Pair<ColumnDict, ColumnDict> res = ColumnDict.merge(dict1, dict2);

        Assertions.assertNull(res);
    }

    private void addWords(ImmutableMap.Builder<ByteBuffer, Integer> builder, int start, int end, String prefix) {
        for (int i = start; i < end; i++) {
            String key = prefix + i;
            ByteBuffer keyBuffer = toByteBuffer(key);
            builder.put(keyBuffer, i);
        }
    }

    private ByteBuffer toByteBuffer(String str) {
        byte[] keyBytes = str.getBytes(StandardCharsets.UTF_8);
        ByteBuffer keyBuffer = ByteBuffer.allocate(keyBytes.length);
        keyBuffer.put(keyBytes);
        keyBuffer.flip();
        return keyBuffer;
    }
}
