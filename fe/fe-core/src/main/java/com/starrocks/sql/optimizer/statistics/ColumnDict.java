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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.starrocks.common.Config;

import java.nio.ByteBuffer;
import java.util.Comparator;

public final class ColumnDict {
    /**
     * Unsigned-byte lexicographic comparator. BE sorts dictionary strings via memcmp, which the C
     * standard defines to compare bytes as unsigned char. ByteBuffer.compareTo on JDK 8 instead
     * compares bytes as signed (Java 9 fixed this to unsigned), so any UTF-8 string with a high-bit
     * byte (Cyrillic, CJK, etc.) sorts the opposite way on the two sides. Always use this comparator
     * when ordering dictionary keys on the FE so the result matches BE regardless of JDK version.
     */
    public static final Comparator<ByteBuffer> UNSIGNED_LEX = (a, b) -> {
        int aPos = a.position();
        int bPos = b.position();
        int aLen = a.limit() - aPos;
        int bLen = b.limit() - bPos;
        int n = Math.min(aLen, bLen);
        for (int i = 0; i < n; i++) {
            int diff = (a.get(aPos + i) & 0xff) - (b.get(bPos + i) & 0xff);
            if (diff != 0) {
                return diff;
            }
        }
        return aLen - bLen;
    };

    private final ImmutableMap<ByteBuffer, Integer> dict;
    // olap table use time info as version info.
    // table on lake use num as version, collectedVersion means historical version num,
    // while version means version in current period.
    private final long collectedVersion;
    private long version;
    // Serialized dict bytes, precomputed so the cache weigher is O(1).
    private final int byteSize;

    public ColumnDict(ImmutableMap<ByteBuffer, Integer> dict, long version) {
        // TODO: The default value of low_cardinality_threshold is 255. Should we set the check size to 255 or 256?
        Preconditions.checkState(!dict.isEmpty() && dict.size() <= Config.low_cardinality_threshold + 1,
                "dict size %s is illegal", dict.size());
        this.dict = dict;
        this.collectedVersion = version;
        this.version = version;
        this.byteSize = computeByteSize(dict);
    }

    public ColumnDict(ImmutableMap<ByteBuffer, Integer> dict, long collectedVersion, long version) {
        this.dict = dict;
        this.collectedVersion = collectedVersion;
        this.version = version;
        this.byteSize = computeByteSize(dict);
    }

    private static int computeByteSize(ImmutableMap<ByteBuffer, Integer> dict) {
        int size = 0;
        for (ByteBuffer buf : dict.keySet()) {
            size += buf.limit() - buf.position() + Integer.BYTES; // string bytes + offset id
        }
        return size;
    }

    public int getByteSize() {
        return byteSize;
    }

    public ImmutableMap<ByteBuffer, Integer> getDict() {
        return dict;
    }

    public long getVersion() {
        return version;
    }

    public long getCollectedVersion() {
        return collectedVersion;
    }

    public int getDictSize() {
        return dict.size();
    }

    void updateVersion(long version) {
        this.version = version;
    }
}