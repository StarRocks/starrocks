// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "types/date_value.h"
#include "types/timestamp_value.h"

namespace starrocks::vectorized {
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
    static T next_value() { return rand() % range; }
};

template <class DataGenerator, class Container, int init_size>
struct ContainerIniter {
    static void init(Container& container) {
        container.resize(init_size);
        for (int i = 0; i < container.size(); ++i) {
            container[i] = DataGenerator::next_value();
        }
    }
};

} // namespace starrocks::vectorized
