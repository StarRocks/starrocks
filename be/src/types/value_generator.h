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

#pragma once

#include "base/random/random.h"
#include "types/date_value.h"
#include "types/timestamp_value.h"

namespace starrocks {
template <class T>
struct DefaultValueGenerator {
    static T next_value() { return T(); }
};

template <>
struct DefaultValueGenerator<DateValue> {
    static DateValue next_value() { return DateValue::MIN_DATE_VALUE; }
};

template <>
struct DefaultValueGenerator<TimestampValue> {
    static TimestampValue next_value() { return TimestampValue::MIN_TIMESTAMP_VALUE; }
};

template <class T>
struct AlwaysZeroGenerator {
    static T next_value() { return 0; }
};

template <class T>
struct AlwaysOneGenerator {
    static T next_value() { return 1; }
};

template <class T, int range>
struct RandomGenerator {
    static T next_value() { return ThreadLocalRandomUniform(range); }
};

template <int range>
struct RandomGenerator<DateValue, range> {
    static DateValue next_value() {
        return DateValue{static_cast<int32_t>(DateValue::MIN_DATE_VALUE._julian + ThreadLocalRandomUniform(range))};
    }
};

template <int range>
struct RandomGenerator<TimestampValue, range> {
    static TimestampValue next_value() {
        return TimestampValue{
                static_cast<int64_t>(TimestampValue::MIN_TIMESTAMP_VALUE._timestamp + ThreadLocalRandomUniform(range))};
    }
};

template <class T, int range>
struct RandomConstGenerator {
    static T next_value() {
        if (!_is_initialize) {
            const_value = ThreadLocalRandomUniform(range);
            _is_initialize = true;
        }
        return const_value;
    }
    static T const_value;
    static bool _is_initialize;
};

template <class T, int range>
bool RandomConstGenerator<T, range>::_is_initialize = false;

template <class T, int range>
T RandomConstGenerator<T, range>::const_value = 0;

template <int range>
struct RandomConstGenerator<DateValue, range> {
    static DateValue next_value() {
        if (!_is_initialize) {
            const_value = RandomGenerator<DateValue, range>::next_value();
            _is_initialize = true;
        }
        return const_value;
    }
    static DateValue const_value;
    static bool _is_initialize;
};

template <int range>
bool RandomConstGenerator<DateValue, range>::_is_initialize = false;

template <int range>
DateValue RandomConstGenerator<DateValue, range>::const_value = DateValue::MIN_DATE_VALUE;

template <int range>
struct RandomConstGenerator<TimestampValue, range> {
    static TimestampValue next_value() {
        if (!_is_initialize) {
            const_value = RandomGenerator<TimestampValue, range>::next_value();
            _is_initialize = true;
        }
        return const_value;
    }
    static TimestampValue const_value;
    static bool _is_initialize;
};

template <int range>
bool RandomConstGenerator<TimestampValue, range>::_is_initialize = false;

template <int range>
TimestampValue RandomConstGenerator<TimestampValue, range>::const_value = TimestampValue::MIN_TIMESTAMP_VALUE;

template <class DataGenerator, class Container, int init_size>
struct ContainerIniter {
    static void init(Container& container) {
        container.resize(init_size);
        for (int i = 0; i < container.size(); ++i) {
            container[i] = DataGenerator::next_value();
        }
    }
};

} // namespace starrocks
