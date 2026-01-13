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

package com.starrocks.persist.gson.internal;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;

/*
 * The json adapter for Guava Multimap.
 * Current support:
 * 1. ArrayListMultimap
 * 2. HashMultimap
 * 3. LinkedListMultimap
 * 4. LinkedHashMultimap
 *
 * The key and value classes of multi map should also be json serializable.
 */
public class GuavaMultimapAdapter<K, V>
        implements JsonSerializer<Multimap<K, V>>, JsonDeserializer<Multimap<K, V>> {

    private static final Type AS_MAP_RETURN_TYPE = getAsMapMethod().getGenericReturnType();

    private static Type asMapType(Type multimapType) {
        return com.google.common.reflect.TypeToken.of(multimapType).resolveType(AS_MAP_RETURN_TYPE).getType();
    }

    private static Method getAsMapMethod() {
        try {
            return Multimap.class.getDeclaredMethod("asMap");
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public JsonElement serialize(Multimap<K, V> map, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("clazz", map.getClass().getSimpleName());
        Map<K, Collection<V>> asMap = map.asMap();
        Type type = asMapType(typeOfSrc);
        JsonElement jsonElement = context.serialize(asMap, type);
        jsonObject.add("map", jsonElement);
        return jsonObject;
    }

    @Override
    public Multimap<K, V> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
        JsonObject jsonObject = json.getAsJsonObject();
        String clazz = jsonObject.get("clazz").getAsString();

        JsonElement mapElement = jsonObject.get("map");
        Map<K, Collection<V>> asMap = context.deserialize(mapElement, asMapType(typeOfT));

        Multimap<K, V> map = null;
        switch (clazz) {
            case "ArrayListMultimap":
                map = ArrayListMultimap.create();
                break;
            case "HashMultimap":
                map = HashMultimap.create();
                break;
            case "LinkedListMultimap":
                map = LinkedListMultimap.create();
                break;
            case "LinkedHashMultimap":
                map = LinkedHashMultimap.create();
                break;
            default:
                Preconditions.checkState(false, "unknown guava multi map class: " + clazz);
                break;
        }

        for (Map.Entry<K, Collection<V>> entry : asMap.entrySet()) {
            map.putAll(entry.getKey(), entry.getValue());
        }
        return map;
    }
}

