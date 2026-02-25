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

package com.starrocks.udf;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ImmutableMap<K, V> extends AbstractMap<K, V> {
    private final List<K> keys;
    private final List<V> values;

    public ImmutableMap(List<K> keys, List<V> values) {
        this.keys = keys;
        this.values = values;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new AbstractSet<Entry<K, V>>() {
            @Override
            public ElementIterator iterator() {
                return new ElementIterator(0, keys.size());
            }

            @Override
            public int size() {
                return keys.size();
            }
        };
    }

    private class ElementIterator implements Iterator<Entry<K, V>> {
        private int cursor = 0;
        private int length = 0;

        ElementIterator(int cursor, int length) {
            this.cursor = cursor;
            this.length = length;
        }

        @Override
        public boolean hasNext() {
            return cursor < length;
        }

        @Override
        public Entry<K, V> next() {
            int i = cursor++;
            return new SimpleImmutableEntry<>(keys.get(i), values.get(i));
        }


        @Override
        public void remove() {
            throw new UnsupportedOperationException();

        }
    }

}
