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
import com.google.gson.Gson;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.persist.gson.GsonUtils;

import java.nio.ByteBuffer;

public final class ColumnDict extends StatsVersion {
    private final ImmutableMap<ByteBuffer, Integer> dict;
    // olap table use time info as version info.
    // table on lake use num as version, collectedVersion means historical version num,
    // while version means version in current period.

    public ColumnDict(ImmutableMap<ByteBuffer, Integer> dict, long version) {
        super(version, version);
        // TODO: The default value of low_cardinality_threshold is 255. Should we set the check size to 255 or 256?
        Preconditions.checkState(!dict.isEmpty() && dict.size() <= Config.low_cardinality_threshold + 1,
                "dict size %s is illegal", dict.size());
        this.dict = dict;
    }

    public ColumnDict(ImmutableMap<ByteBuffer, Integer> dict, long collectedVersion, long version) {
        super(collectedVersion, version);
        this.dict = dict;
    }

    public ImmutableMap<ByteBuffer, Integer> getDict() {
        return dict;
    }

    public int getDictSize() {
        return dict.size();
    }

    public String toJson() {
        Gson gson = GsonUtils.GSON;
        // Manually build a JSON object with all fields
        // Convert ByteBuffer keys to base64 strings for JSON compatibility
        java.util.Map<String, Integer> dictMap = new java.util.HashMap<>();
        for (java.util.Map.Entry<ByteBuffer, Integer> entry : dict.entrySet()) {
            ByteBuffer key = entry.getKey();
            // Duplicate to avoid changing position
            ByteBuffer dup = key.duplicate();
            byte[] bytes = new byte[dup.remaining()];
            dup.get(bytes);
            // Convert bytes to string using UTF-8 encoding
            String strKey = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
            dictMap.put(strKey, entry.getValue());
        }
        java.util.Map<String, Object> jsonMap = new java.util.HashMap<>();
        jsonMap.put("dict", dictMap);
        jsonMap.put("collectedVersion", collectedVersion);
        jsonMap.put("version", version);
        return gson.toJson(jsonMap);
    }

    /**
     * Merge two dictionaries and return two new dictionaries.
     * The id of each dictionary is a continuous integer from 1 to n, where n is the number of words in the dictionary.
     * Ensure that the lexicographical order of the dictionary words is consistent with the order of the ids.
     * If the merged dictionary size exceeds Config.low_cardinality_threshold, return null.
     */
    public static Pair<ColumnDict, ColumnDict> merge(ColumnDict d1, ColumnDict d2) {
        final int n1 = d1.getDict().size() + 1;
        final int n2 = d2.getDict().size() + 1;
        ByteBuffer[] sortedKeys1 = new ByteBuffer[n1];
        ByteBuffer[] sortedKeys2 = new ByteBuffer[n2];
        d1.getDict().forEach((key, idx) -> sortedKeys1[idx] = key);
        d2.getDict().forEach((key, idx) -> sortedKeys2[idx] = key);

        ImmutableMap.Builder<ByteBuffer, Integer> builder =
                ImmutableMap.builderWithExpectedSize(Math.min(n1 + n2 - 2, Config.low_cardinality_threshold));
        int idx1 = 1;
        int idx2 = 1;
        int newIdx = 1;
        while (idx1 < n1 && idx2 < n2) {
            ByteBuffer key1 = sortedKeys1[idx1];
            ByteBuffer key2 = sortedKeys2[idx2];
            int cmp = key1.compareTo(key2);
            if (cmp == 0) {
                builder.put(key1, newIdx);
                idx1++;
                idx2++;
            } else if (cmp < 0) {
                builder.put(key1, newIdx);
                idx1++;
            } else {
                builder.put(key2, newIdx);
                idx2++;
            }
            newIdx++;
        }

        while (idx1 < n1) {
            builder.put(sortedKeys1[idx1++], newIdx++);
        }
        while (idx2 < n2) {
            builder.put(sortedKeys2[idx2++], newIdx++);
        }

        if (newIdx > Config.low_cardinality_threshold) {
            return null;
        }

        final ImmutableMap<ByteBuffer, Integer> newDict = builder.build();
        ColumnDict newD1 = new ColumnDict(newDict, d1.getCollectedVersion(), d1.getVersion());
        ColumnDict newD2 = new ColumnDict(newDict, d2.getCollectedVersion(), d2.getVersion());
        return new Pair<>(newD1, newD2);
    }
}
