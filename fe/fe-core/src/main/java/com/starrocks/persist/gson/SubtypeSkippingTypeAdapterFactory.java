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

package com.starrocks.persist.gson;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.internal.Streams;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * TypeAdapterFactory that shields deserialization from legacy data containing values encoded
 * with unregistered subtypes. It wraps Gson's default Map/List adapters and eagerly probes each
 * element via the existing delegate; any element that triggers {@link SubtypeNotFoundException}
 * is dropped while legitimate {@code null} values are preserved.
 */
public class SubtypeSkippingTypeAdapterFactory implements TypeAdapterFactory {
    private static final Logger LOG = LogManager.getLogger(SubtypeSkippingTypeAdapterFactory.class);

    @Override
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
        if (Map.class.isAssignableFrom(type.getRawType())) {
            TypeAdapter<T> delegate = gson.getDelegateAdapter(this, type);
            TypeAdapter<?> keyAdapter = resolveMapKeyAdapter(gson, type);
            TypeAdapter<?> valueAdapter = resolveMapValueAdapter(gson, type);
            if (delegate == null || valueAdapter == null) {
                return delegate;
            }

            // Wrap the default Map adapter so we can inspect and drop incompatible values first.
            return new FilteringMapTypeAdapter<>(delegate, keyAdapter, valueAdapter).nullSafe();
        }

        if (Collection.class.isAssignableFrom(type.getRawType())) {
            TypeAdapter<T> delegate = gson.getDelegateAdapter(this, type);
            TypeAdapter<?> elementAdapter = resolveCollectionElementAdapter(gson, type);
            if (delegate == null || elementAdapter == null) {
                return delegate;
            }

            // Same idea for Collection/List: probe each element before passing to Gson's adapter.
            return new FilteringCollectionTypeAdapter<>(delegate, elementAdapter).nullSafe();
        }

        return null;
    }

    private static TypeAdapter<?> resolveMapKeyAdapter(Gson gson, TypeToken<?> typeToken) {
        Type mapType = typeToken.getType();
        if (!(mapType instanceof ParameterizedType)) {
            return null;
        }
        Type keyType = ((ParameterizedType) mapType).getActualTypeArguments()[0];
        return gson.getAdapter(TypeToken.get(keyType));
    }

    private static TypeAdapter<?> resolveMapValueAdapter(Gson gson, TypeToken<?> typeToken) {
        Type mapType = typeToken.getType();
        if (!(mapType instanceof ParameterizedType)) {
            return null;
        }
        Type valueType = ((ParameterizedType) mapType).getActualTypeArguments()[1];
        return gson.getAdapter(TypeToken.get(valueType));
    }

    private static TypeAdapter<?> resolveCollectionElementAdapter(Gson gson, TypeToken<?> typeToken) {
        Type collectionType = typeToken.getType();
        if (!(collectionType instanceof ParameterizedType)) {
            return null;
        }
        Type elementType = ((ParameterizedType) collectionType).getActualTypeArguments()[0];
        return gson.getAdapter(TypeToken.get(elementType));
    }

    private static class FilteringMapTypeAdapter<T> extends TypeAdapter<T> {
        private final TypeAdapter<T> delegate;
        private final TypeAdapter<?> keyAdapter;
        private final TypeAdapter<?> valueAdapter;

        FilteringMapTypeAdapter(TypeAdapter<T> delegate, TypeAdapter<?> keyAdapter,
                                TypeAdapter<?> valueAdapter) {
            this.delegate = delegate;
            this.keyAdapter = keyAdapter;
            this.valueAdapter = valueAdapter;
        }

        @Override
        public T read(JsonReader in) throws IOException {
            JsonElement element = Streams.parse(in);
            if (element == null || element.isJsonNull()) {
                return delegate.fromJsonTree(element);
            }

            JsonElement filtered = filter(element);
            return delegate.fromJsonTree(filtered);
        }

        private JsonElement filter(JsonElement element) {
            if (element.isJsonObject()) {
                // MapTypeAdapterFactory handles string-keyed maps as JSON objects, so we drop
                // entries directly by field name when either key or value cannot be materialized.
                JsonObject object = element.getAsJsonObject();
                List<String> keysToRemove = new ArrayList<>();
                for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
                    if (shouldDrop(new JsonPrimitive(entry.getKey()), keyAdapter)
                            || shouldDrop(entry.getValue(), valueAdapter)) {
                        keysToRemove.add(entry.getKey());
                    }
                }
                for (String key : keysToRemove) {
                    object.remove(key);
                }
                return object;
            }

            if (element.isJsonArray()) {
                // Complex map keys (enableComplexMapKeySerialization) are encoded as arrays of
                // [key, value] pairs; we mirror that shape so the delegate still receives valid
                // tuples while skipping the incompatible values.
                JsonArray array = element.getAsJsonArray();
                JsonArray filtered = new JsonArray();
                for (JsonElement entry : array) {
                    if (!entry.isJsonArray()) {
                        filtered.add(entry);
                        continue;
                    }
                    JsonArray pair = entry.getAsJsonArray();
                    //enableComplexMapKeySerialization() will serialize a Map as a two-element array [keyJson, valueJson];
                    // according to Gson's normal output, this array will always contain exactly two elements.
                    if (pair.size() < 2) {
                        // Legacy dumps may contain truncated [key, value] tuples (for example, only the key
                        // survives after manual edits). These entries are not necessarily tied to subtype
                        // registration issues, so we keep them intact and rely on Gson's default adapter to
                        // decide whether to surface an error.
                        filtered.add(entry);
                        continue;
                    }
                    boolean dropKey = shouldDrop(pair.get(0), keyAdapter);
                    boolean dropValue = shouldDrop(pair.get(1), valueAdapter);
                    if (!dropKey && !dropValue) {
                        filtered.add(entry);
                    }
                }
                return filtered;
            }

            return element;
        }

        private boolean shouldDrop(JsonElement element, TypeAdapter<?> adapter) {
            if (adapter == null || element == null || element.isJsonNull()) {
                return false;
            }
            try {
                JsonElement candidate = element.deepCopy();
                adapter.fromJsonTree(candidate);
                return false;
            } catch (SubtypeNotFoundException e) {
                LOG.warn("Dropping map entry because its subtype is not registered. " +
                        "Element: {}", element, e);
                return true;
            }
        }

        @Override
        public void write(JsonWriter out, T value) throws IOException {
            delegate.write(out, value);
        }
    }

    private static class FilteringCollectionTypeAdapter<T> extends TypeAdapter<T> {
        private final TypeAdapter<T> delegate;
        private final TypeAdapter<?> elementAdapter;

        FilteringCollectionTypeAdapter(TypeAdapter<T> delegate, TypeAdapter<?> elementAdapter) {
            this.delegate = delegate;
            this.elementAdapter = elementAdapter;
        }

        @Override
        public T read(JsonReader in) throws IOException {
            JsonElement element = Streams.parse(in);
            if (element == null || element.isJsonNull()) {
                return delegate.fromJsonTree(element);
            }

            JsonElement filtered = filter(element);
            return delegate.fromJsonTree(filtered);
        }

        private JsonElement filter(JsonElement element) {
            if (!element.isJsonArray()) {
                return element;
            }
            JsonArray array = element.getAsJsonArray();
            JsonArray filtered = new JsonArray();
            for (JsonElement item : array) {
                if (!shouldDrop(item)) {
                    filtered.add(item);
                }
            }
            return filtered;
        }

        private boolean shouldDrop(JsonElement valueElement) {
            if (valueElement == null || valueElement.isJsonNull()) {
                return false;
            }
            try {
                // Keep explicit nulls while isolating adapters from mutating the original node.
                JsonElement candidate = valueElement.deepCopy();
                elementAdapter.fromJsonTree(candidate);
                return false;
            } catch (SubtypeNotFoundException e) {
                LOG.warn("Dropping collection element because its subtype is not registered. " +
                        "Element: {}", valueElement, e);
                return true;
            }
        }

        @Override
        public void write(JsonWriter out, T value) throws IOException {
            delegate.write(out, value);
        }
    }
}
