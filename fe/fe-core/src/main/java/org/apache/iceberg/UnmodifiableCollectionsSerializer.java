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
package org.apache.iceberg;

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
 * copy from <a href="https://github.com/opentripplanner/OpenTripPlanner/tree/dev-2.x/application/src/main/java/org/opentripplanner/kryo/UnmodifiableCollectionsSerializer.java">...</a>
 * A kryo {@link Serializer} for unmodifiable {@link Collection}s and {@link Map}s created via
 * {@link Collections}.
 *
 * @author <a href="mailto:martin.grotzke@javakaffee.de">Martin Grotzke</a>
 */
public class UnmodifiableCollectionsSerializer extends Serializer<Object> {

    /**
     * Creates a new {@link UnmodifiableCollectionsSerializer} and registers its serializer for the
     * several unmodifiable Collections that can be created via {@link Collections}, including {@link
     * Map}s.
     *
     * @param kryo the {@link Kryo} instance to set the serializer on.
     * @see Collections#unmodifiableCollection(Collection)
     * @see Collections#unmodifiableList(List)
     * @see Collections#unmodifiableSet(Set)
     * @see Collections#unmodifiableSortedSet(SortedSet)
     * @see Collections#unmodifiableMap(Map)
     * @see Collections#unmodifiableSortedMap(SortedMap)
     */
    public static void registerSerializers(Kryo kryo) {
        UnmodifiableCollectionsSerializer serializer = new UnmodifiableCollectionsSerializer();
        UnmodifiableCollection.values();
        for (UnmodifiableCollection item : UnmodifiableCollection.values()) {
            kryo.register(item.type, serializer);
        }
    }

    @Override
    public void write(Kryo kryo, Output output, Object object) {
        try {
            UnmodifiableCollection unmodifiableCollection = UnmodifiableCollection.valueOfType(
                    object.getClass()
            );
            // the ordinal could be replaced by s.th. else (e.g. a explicitely managed "id")
            output.writeInt(unmodifiableCollection.ordinal(), true);
            kryo.writeClassAndObject(output, getValues(object));
        } catch (RuntimeException e) {
            // Don't eat and wrap RuntimeExceptions because the ObjectBuffer.write...
            // handles SerializationException specifically (resizing the buffer)...
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object read(Kryo kryo, Input input, Class<Object> clazz) {
        int ordinal = input.readInt(true);
        UnmodifiableCollection unmodifiableCollection = UnmodifiableCollection.values()[ordinal];
        Object sourceCollection = kryo.readClassAndObject(input);
        return unmodifiableCollection.create(sourceCollection);
    }

    @Override
    public Object copy(Kryo kryo, Object original) {
        try {
            UnmodifiableCollection unmodifiableCollection = UnmodifiableCollection.valueOfType(
                    original.getClass()
            );
            Object sourceCollectionCopy = kryo.copy(getValues(original));
            return unmodifiableCollection.create(sourceCollectionCopy);
        } catch (RuntimeException e) {
            // Don't eat and wrap RuntimeExceptions
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Collection<?> getValues(Object coll) {
        return new ArrayList<>((Collection) coll);
    }

    private enum UnmodifiableCollection {
        COLLECTION(Collections.unmodifiableCollection(Arrays.asList("")).getClass()) {
            @Override
            public Object create(Object sourceCollection) {
                return Collections.unmodifiableCollection((Collection<?>) sourceCollection);
            }
        },
        RANDOM_ACCESS_LIST(Collections.unmodifiableList(new ArrayList<Void>()).getClass()) {
            @Override
            public Object create(Object sourceCollection) {
                return Collections.unmodifiableList((List<?>) sourceCollection);
            }
        },
        LIST(Collections.unmodifiableList(new LinkedList<Void>()).getClass()) {
            @Override
            public Object create(Object sourceCollection) {
                return Collections.unmodifiableList((List<?>) sourceCollection);
            }
        },
        SET(Collections.unmodifiableSet(new HashSet<Void>()).getClass()) {
            @Override
            public Object create(Object sourceCollection) {
                return Collections.unmodifiableSet((Set<?>) sourceCollection);
            }
        },
        SORTED_SET(Collections.unmodifiableSortedSet(new TreeSet<Void>()).getClass()) {
            @Override
            public Object create(Object sourceCollection) {
                return Collections.unmodifiableSortedSet((SortedSet<?>) sourceCollection);
            }
        },
        MAP(Collections.unmodifiableMap(new HashMap<Void, Void>()).getClass()) {
            @Override
            public Object create(Object sourceCollection) {
                return Collections.unmodifiableMap((Map<?, ?>) sourceCollection);
            }
        },
        SORTED_MAP(Collections.unmodifiableSortedMap(new TreeMap<Void, Void>()).getClass()) {
            @Override
            public Object create(Object sourceCollection) {
                return Collections.unmodifiableSortedMap((SortedMap<?, ?>) sourceCollection);
            }
        };

        private final Class<?> type;

        UnmodifiableCollection(Class<?> type) {
            this.type = type;
        }

        public abstract Object create(Object sourceCollection);

        static UnmodifiableCollection valueOfType(Class<?> type) {
            for (UnmodifiableCollection item : values()) {
                if (item.type.equals(type)) {
                    return item;
                }
            }
            throw new IllegalArgumentException("The type " + type + " is not supported.");
        }
    }
}
