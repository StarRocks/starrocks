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
// limitations under the License

package com.starrocks.sql.common;

import com.google.common.collect.Maps;
import com.starrocks.common.Config;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * A mapping of partition set to a PCell set, the key is the partition set name, the value is the partition cell set.
 */
public record PCellSetMapping(Map<String, PCellSortedSet> mapping) {
    public static PCellSetMapping of() {
        return new PCellSetMapping(Maps.newHashMap());
    }

    public static PCellSetMapping of(Map<String, PCellSortedSet> mapping) {
        return new PCellSetMapping(mapping);
    }

    public void put(String key, PCellWithName value) {
        mapping.computeIfAbsent(key, k -> PCellSortedSet.of()).add(value);
    }

    public void put(String key) {
        mapping.putIfAbsent(key, PCellSortedSet.of());
    }

    public void put(String key, Collection<PCellWithName> value) {
        mapping.computeIfAbsent(key, k -> PCellSortedSet.of()).addAll(value);
    }

    public PCellSortedSet get(String key) {
        return mapping.get(key);
    }

    public int size() {
        return mapping.size();
    }

    public boolean isEmpty() {
        return mapping.isEmpty();
    }

    public PCellSortedSet remove(String key) {
        return mapping.remove(key);
    }

    public Collection<PCellSortedSet> values() {
        return mapping.values();
    }

    public boolean containsKey(String key) {
        return mapping.containsKey(key);
    }

    public Map<String, PCellSortedSet> getBasePartitionsToRefreshMap() {
        return mapping;
    }

    public Map<String, Set<String>> getRefTablePartitionNames() {
        Map<String, Set<String>> result = Maps.newHashMap();
        for (Map.Entry<String, PCellSortedSet> entry : mapping.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getPartitionNames());
        }
        return result;
    }

    @Override
    public String toString() {
        if (mapping == null) {
            return "null";
        }

        int maxLen = Config.max_mv_task_run_meta_message_values_length;
        if (mapping.size() <= maxLen) {
            return mapping.toString();
        }

        // For large maps, show first maxLen entries with ellipsis
        StringBuilder sb = new StringBuilder("{");
        int count = 0;
        for (Map.Entry<String, PCellSortedSet> entry : mapping.entrySet()) {
            if (count >= maxLen) {
                sb.append("...");
                break;
            }
            if (count > 0) {
                sb.append(", ");
            }
            sb.append(entry.getKey()).append("=").append(entry.getValue());
            count++;
        }
        sb.append("}");
        return sb.toString();
    }
}
