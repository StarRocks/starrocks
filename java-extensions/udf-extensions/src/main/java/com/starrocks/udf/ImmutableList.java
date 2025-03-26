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

import java.util.AbstractList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;

public class ImmutableList<E> extends AbstractList<E>
        implements RandomAccess, java.io.Serializable{

    private E[] elements;
    private int offset;
    private int size;

    public ImmutableList(E[] elements, int offset, int size) {
        this.elements = elements;
        this.offset = offset;
        this.size = size;
    }

    @Override
    public E get(int index) {
        return elements[offset + index];
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public Object[] toArray() {
        return Arrays.copyOfRange(elements, offset, offset + size);
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        for (int i = offset; i < offset + size; i++) {
            Object e = elements[i];
            hashCode = 31 * hashCode + (e == null ? 0 : e.hashCode());
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof List)) {
            return false;
        }

        return equalsRange((List<?>)o, offset, offset + size);
    }

    private boolean equalsRange(List<?> other, int from, int to) {
        final Object[] es = elements;
        if (to > es.length) {
            throw new ConcurrentModificationException();
        }
        final Iterator<?> oit = other.iterator();
        for (; from < to; from++) {
            if (!oit.hasNext() || !Objects.equals(es[from], oit.next())) {
                return false;
            }
        }
        return !oit.hasNext();
    }

    @Override
    public int indexOf(Object o) {
        int index = indexOfRange(o, offset, offset + size);
        return index >= 0 ? index - offset : -1;
    }

    private int indexOfRange(Object o, int start, int end) {
        Object[] es = elements;
        if (o == null) {
            for (int i = start; i < end; i++) {
                if (es[i] == null) {
                    return i;
                }
            }
        } else {
            for (int i = start; i < end; i++) {
                if (o.equals(es[i])) {
                    return i;
                }
            }
        }
        return -1;
    }
}
