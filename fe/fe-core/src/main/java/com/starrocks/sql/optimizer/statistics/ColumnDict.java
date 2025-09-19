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
}