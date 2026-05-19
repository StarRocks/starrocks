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
package com.starrocks.connector.iceberg;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A Java 17-compatible kryo serializer for unmodifiable collections that does NOT
 * use reflection to access private fields (unlike de.javakaffee's version).
 * Based on fe-core's org.apache.iceberg.UnmodifiableCollectionsSerializer, with
 * toMutable() added to correctly handle both Collection and Map types on write.
 */
public class UnmodifiableCollectionsSerializer extends Serializer<Object> {

    public static void registerSerializers(Kryo kryo) {
        UnmodifiableCollectionsSerializer serializer = new UnmodifiableCollectionsSerializer();
        for (UnmodifiableCollection item : UnmodifiableCollection.CACHED_VALUES) {
            kryo.register(item.type, serializer);
        }
    }

    @Override
    public void write(Kryo kryo, Output output, Object object) {
        UnmodifiableCollection unmodifiableCollection = UnmodifiableCollection.valueOfType(
                object.getClass()
        );
        output.writeInt(unmodifiableCollection.ordinal(), true);
        kryo.writeClassAndObject(output, unmodifiableCollection.toMutable(object));
    }

    @Override
    public Object read(Kryo kryo, Input input, Class<Object> clazz) {
        int ordinal = input.readInt(true);
        UnmodifiableCollection unmodifiableCollection = UnmodifiableCollection.CACHED_VALUES[ordinal];
        Object sourceCollection = kryo.readClassAndObject(input);
        return unmodifiableCollection.create(sourceCollection);
    }

    @Override
    public Object copy(Kryo kryo, Object original) {
        UnmodifiableCollection unmodifiableCollection = UnmodifiableCollection.valueOfType(
                original.getClass()
        );
        Object sourceCollectionCopy = kryo.copy(unmodifiableCollection.toMutable(original));
        return unmodifiableCollection.create(sourceCollectionCopy);
    }

    private enum UnmodifiableCollection {
        COLLECTION(Collections.unmodifiableCollection(Arrays.asList("")).getClass()) {
            @Override
            public Object create(Object sourceCollection) {
                return Collections.unmodifiableCollection((Collection<?>) sourceCollection);
            }

            @Override
            public Object toMutable(Object obj) {
                return new ArrayList<>((Collection<?>) obj);
            }
        },
        RANDOM_ACCESS_LIST(Collections.unmodifiableList(new ArrayList<Void>()).getClass()) {
            @Override
            public Object create(Object sourceCollection) {
                return Collections.unmodifiableList((List<?>) sourceCollection);
            }

            @Override
            public Object toMutable(Object obj) {
                return new ArrayList<>((Collection<?>) obj);
            }
        },
        LIST(Collections.unmodifiableList(new LinkedList<Void>()).getClass()) {
            @Override
            public Object create(Object sourceCollection) {
                return Collections.unmodifiableList((List<?>) sourceCollection);
            }

            @Override
            public Object toMutable(Object obj) {
                return new ArrayList<>((Collection<?>) obj);
            }
        },
        SET(Collections.unmodifiableSet(new HashSet<Void>()).getClass()) {
            @Override
            public Object create(Object sourceCollection) {
                return Collections.unmodifiableSet((Set<?>) sourceCollection);
            }

            @Override
            public Object toMutable(Object obj) {
                return new HashSet<>((Collection<?>) obj);
            }
        },
        SORTED_SET(Collections.unmodifiableSortedSet(new TreeSet<Void>()).getClass()) {
            @Override
            public Object create(Object sourceCollection) {
                return Collections.unmodifiableSortedSet((SortedSet<?>) sourceCollection);
            }

            @Override
            public Object toMutable(Object obj) {
                return new TreeSet<>((Collection<?>) obj);
            }
        },
        MAP(Collections.unmodifiableMap(new HashMap<Void, Void>()).getClass()) {
            @Override
            public Object create(Object sourceCollection) {
                return Collections.unmodifiableMap((Map<?, ?>) sourceCollection);
            }

            @Override
            public Object toMutable(Object obj) {
                return new HashMap<>((Map<?, ?>) obj);
            }
        },
        SORTED_MAP(Collections.unmodifiableSortedMap(new TreeMap<Void, Void>()).getClass()) {
            @Override
            public Object create(Object sourceCollection) {
                return Collections.unmodifiableSortedMap((SortedMap<?, ?>) sourceCollection);
            }

            @Override
            public Object toMutable(Object obj) {
                return new TreeMap<>((Map<?, ?>) obj);
            }
        };

        static final UnmodifiableCollection[] CACHED_VALUES = values();

        private final Class<?> type;

        UnmodifiableCollection(Class<?> type) {
            this.type = type;
        }

        public abstract Object create(Object sourceCollection);

        public abstract Object toMutable(Object obj);

        static UnmodifiableCollection valueOfType(Class<?> type) {
            for (UnmodifiableCollection item : CACHED_VALUES) {
                if (item.type == type) {
                    return item;
                }
            }
            throw new IllegalArgumentException("The type " + type + " is not supported.");
        }
    }
}
