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

import com.google.common.base.Preconditions;

import java.util.Optional;

public class EitherOr<T> {
    private final T first;
    private final T second;

    private EitherOr(T first, T second) {
        Preconditions.checkArgument((first == null && second != null) || (first != null && second == null));
        this.first = first;
        this.second = second;
    }

    public static <T> EitherOr<T> either(T first) {
        return new EitherOr<>(first, null);
    }

    public static <T> EitherOr<T> or(T second) {
        return new EitherOr<>(null, second);
    }

    public Optional<T> getFirst() {
        return Optional.ofNullable(first);
    }

    public Optional<T> getSecond() {
        return Optional.ofNullable(second);
    }

    public T get() {
        return (first != null) ? first : second;
    }
}
