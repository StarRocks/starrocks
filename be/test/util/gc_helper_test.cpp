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

#include "util/gc_helper.h"

#include <gtest/gtest.h>

namespace starrocks {

static const size_t second_in_ns = size_t(1000) * 1000 * 1000;
static const size_t gc_period = 60;
static const size_t gc_interval = 1;                                     // 1 second
static const MonoDelta delta = MonoDelta::FromNanoseconds(second_in_ns); // 1 second

TEST(TestGCHelper, TestNormalDecrease) {
    MonoTime t(second_in_ns); // second 1
    GCHelper gch(gc_period, gc_interval, t);

    size_t total = size_t(1024) * 1024 * 1024; // 1G
    size_t bytes;
    for (size_t i = 0; i < gc_period + 1; ++i) {
        t += delta; // add 1 second
        bytes = gch.bytes_should_gc(t, total);
        total -= bytes;
    }
    ASSERT_EQ(total, 0);
}

TEST(TestGCHelper, TestAccumulation) {
    MonoTime t(second_in_ns); // second 1
    GCHelper gch(gc_period, gc_interval, t);

    size_t total = size_t(1024) * 1024 * 1024; // 1G
    size_t bytes;
    for (size_t i = 0; i < gc_period + 1; ++i) {
        t += delta; // add 1 second
        bytes = gch.bytes_should_gc(t, total);
    }
    ASSERT_EQ(total, bytes);
}

TEST(TestGCHelper, TestInterrupt) {
    MonoTime t(second_in_ns); // second 1
    GCHelper gch(gc_period, gc_interval, t);

    size_t total = size_t(1024) * 1024 * 1024; // 1G
    size_t bytes;
    for (size_t i = 0; i < gc_period / 2; ++i) {
        t += delta; // add 1 second
        bytes = gch.bytes_should_gc(t, total);
        if (i > 0) {
            ASSERT_TRUE(bytes > 0);
        }
    }
    bytes = gch.bytes_should_gc(t, 0);
    ASSERT_TRUE(bytes == 0);
}

TEST(TestGCHelper, TestDifferentConfig) {
    MonoTime t(10 * second_in_ns);                                   // second 10
    MonoDelta delta = MonoDelta::FromNanoseconds(10 * second_in_ns); // 10 second
    GCHelper gch(1, 10, t);

    size_t total = size_t(1024) * 1024 * 1024; // 1G
    size_t bytes;
    t += delta; // add 1 second
    bytes = gch.bytes_should_gc(t, total);
    t += delta; // add 1 second
    bytes = gch.bytes_should_gc(t, total);
    ASSERT_TRUE(bytes == total);
}

} // namespace starrocks
