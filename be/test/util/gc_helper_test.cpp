// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "util/gc_helper.h"

#include <gtest/gtest.h>

namespace starrocks {

static const size_t second_in_ns = size_t(1000) * 1000 * 1000;
static const size_t gc_period = 180;                                     // 180 second
static const MonoDelta delta = MonoDelta::FromNanoseconds(second_in_ns); // 1 second

TEST(TestGCHelper, TestNormalDecrease) {
    MonoTime t(second_in_ns); // second 1
    GCHelper gch(gc_period /* second */, t);

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
    GCHelper gch(gc_period /* second */, t);

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
    GCHelper gch(gc_period /* second */, t);

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

} // namespace starrocks
