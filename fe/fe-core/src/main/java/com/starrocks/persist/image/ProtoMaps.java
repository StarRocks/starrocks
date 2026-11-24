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

package com.starrocks.persist.image;

import com.google.common.collect.Maps;
import com.starrocks.persist.proto.StringMapPB;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Helpers for copying Java maps into proto map fields.
 */
public final class ProtoMaps {
    private ProtoMaps() {
    }

    /**
     * The entries whose value is not null, in iteration order. GSON drops a map entry with a null value from the
     * image, so a table loaded from a v2 image does not have it either; a proto map cannot hold a null value at all.
     */
    public static <K, V> Map<K, V> withoutNullValues(Map<K, V> map) {
        Map<K, V> result = new LinkedHashMap<>(map.size());
        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (entry.getValue() != null) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    /** A Java string map as its wrapper message; a null value is dropped, a proto map cannot hold one. */
    public static StringMapPB toStringMapProto(Map<String, String> map) {
        return StringMapPB.newBuilder().putAllValues(withoutNullValues(map)).build();
    }

    public static Map<String, String> fromStringMapProto(StringMapPB pb) {
        return Maps.newHashMap(pb.getValuesMap());
    }
}
