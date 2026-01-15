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
import com.google.common.collect.Sets;
import com.starrocks.common.Config;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * A mapping of partition set to a partition name set, the key is the partition set name, the value is the partition name set.
 */
public record PartitionNameSetMap(Map<String, Set<String>> mapping) {

    public static PartitionNameSetMap of() {
        return new PartitionNameSetMap(Maps.newHashMap());
    }

    public static PartitionNameSetMap of(Map<String, Set<String>> mapping) {
        return new PartitionNameSetMap(mapping);
    }

    public void put(String key, String value) {
        mapping.computeIfAbsent(key, k -> Sets.newHashSet()).add(value);
    }

    public void put(String key) {
        mapping.putIfAbsent(key, Sets.newHashSet());
    }

    public void put(String key, Collection<String> value) {
        mapping.computeIfAbsent(key, k -> Sets.newHashSet()).addAll(value);
    }

    public Set<String> get(String key) {
        return mapping.get(key);
    }

    public int size() {
        return mapping.size();
    }

    public boolean isEmpty() {
        return mapping.isEmpty();
    }

    public Set<String> remove(String key) {
        return mapping.remove(key);
    }

    public Collection<Set<String>> values() {
        return mapping.values();
    }

    public boolean containsKey(String key) {
        return mapping.containsKey(key);
    }

    public Map<String, Set<String>> getBasePartitionsToRefreshMap() {
        return mapping;
    }

    @Override
    public String toString() {
        if (mapping == null) {
            return "null";
        }

        // For large maps, show first maxLen entries with ellipsis
        final int maxLen = Config.max_mv_task_run_meta_message_values_length;
        StringBuilder sb = new StringBuilder("{");
        int count = 0;
        for (Map.Entry<String, Set<String>> entry : mapping.entrySet()) {
            if (count >= maxLen) {
                sb.append("...");
                break;
            }
            if (count > 0) {
                sb.append(", ");
            }
            sb.append(entry.getKey()).append("=[");
            int i = 0;
            for (String partName : entry.getValue()) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(partName);
                i++;
                if (i > maxLen) {
                    sb.append("...");
                    break;
                }
            }
            sb.append("]");
            count++;
        }
        sb.append("}");
        return sb.toString();
    }
}
