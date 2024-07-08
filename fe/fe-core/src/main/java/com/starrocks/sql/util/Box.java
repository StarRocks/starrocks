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

package com.starrocks.sql.util;

import java.util.Objects;

public final class Box<T> {
    private final T obj;

    private Box(T obj) {
        this.obj = Objects.requireNonNull(obj);
    }

    public static <T> Box<T> of(T obj) {
        return new Box<>(obj);
    }

    public T unboxed() {
        return obj;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that == null || getClass() != that.getClass()) {
            return false;
        }
        Box<?> thatBox = (Box<?>) that;
        return obj == thatBox.obj;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this.obj);
    }
}
