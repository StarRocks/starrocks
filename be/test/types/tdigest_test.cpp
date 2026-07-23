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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/util/tdigest_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "types/tdigest.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <limits>
#include <random>
#include <vector>

#include "common/logging.h"

namespace starrocks {

class TDigestTest : public ::testing::Test {
protected:
    // You can remove any or all of the following functions if its body
    // is empty.
    TDigestTest() {
        // You can do set-up work for each test here.
    }

    ~TDigestTest() override {
        // You can do clean-up work that doesn't throw exceptions here.
    }

    // If the constructor and destructor are not enough for setting up
    // and cleaning up each test, you can define the following methods:

    void SetUp() override {
        // Code here will be called immediately after the constructor (right
        // before each test).
    }

    void TearDown() override {
        // Code here will be called immediately after each test (right
        // before the destructor).
    }

    static void SetUpTestCase() {}

    // Objects declared here can be used by all tests in the test case for Foo.
};

static double quantile(const double q, const std::vector<double>& values) {
    double q1;
    if (values.size() == 0) {
        q1 = NAN;
    } else if (q == 1 || values.size() == 1) {
        q1 = values[values.size() - 1];
    } else {
        auto index = q * values.size();
        if (index < 0.5) {
            q1 = values[0];
        } else if (values.size() - index < 0.5) {
            q1 = values[values.size() - 1];
        } else {
            index -= 0.5;
            const int intIndex = static_cast<int>(index);
            q1 = values[intIndex + 1] * (index - intIndex) + values[intIndex] * (intIndex + 1 - index);
        }
    }
    return q1;
}

TEST_F(TDigestTest, CrashAfterMerge) {
    TDigest digest(1000);
    std::uniform_real_distribution<> reals(0.0, 1.0);
    std::random_device gen;
    for (int i = 0; i < 100000; i++) {
        digest.add(reals(gen));
    }
    digest.compress();

    TDigest digest2(1000);
    digest2.merge(&digest);
    digest2.quantile(0.5);
}

TEST_F(TDigestTest, EmptyDigest) {
    TDigest digest(100);
    EXPECT_EQ(0, digest.processed().size());
}

TEST_F(TDigestTest, SingleValue) {
    TDigest digest(100);
    std::random_device gen;
    std::uniform_real_distribution<> dist(0, 1000);
    const auto value = dist(gen);
    digest.add(value);
    std::uniform_real_distribution<> dist2(0, 1.0);
    const double q = dist2(gen);
    EXPECT_NEAR(value, digest.quantile(0.0), 0.001f);
    EXPECT_NEAR(value, digest.quantile(q), 0.001f);
    EXPECT_NEAR(value, digest.quantile(1.0), 0.001f);
}

TEST_F(TDigestTest, FewValues) {
    // When there are few values in the tree, quantiles should be exact
    TDigest digest(1000);

    std::random_device gen;
    std::uniform_real_distribution<> reals(0.0, 100.0);
    std::uniform_int_distribution<> dist(0, 10);
    std::uniform_int_distribution<> bools(0, 1);
    std::uniform_real_distribution<> qvalue(0.0, 1.0);

    const auto length = 10; //dist(gen);

    std::vector<double> values;
    values.reserve(length);
    for (int i = 0; i < length; ++i) {
        auto const value = (i == 0 || bools(gen)) ? reals(gen) : values[i - 1];
        digest.add(value);
        values.push_back(value);
    }
    std::sort(values.begin(), values.end());
    digest.compress();

    EXPECT_EQ(digest.processed().size(), values.size());

    std::vector<double> testValues{0.0, 1.0e-10, qvalue(gen), 0.5, 1.0 - 1e-10, 1.0};
    for (auto q : testValues) {
        double q1 = quantile(q, values);
        auto q2 = digest.quantile(q);
        if (std::isnan(q1)) {
            EXPECT_TRUE(std::isnan(q2));
        } else {
            EXPECT_NEAR(q1, q2, 0.03) << "q = " << q;
        }
    }
}

TEST_F(TDigestTest, MoreThan2BValues) {
    TDigest digest(1000);

    std::random_device gen;
    std::uniform_real_distribution<> reals(0.0, 1.0);
    for (int i = 0; i < 1000; ++i) {
        const double next = reals(gen);
        digest.add(next);
    }
    for (int i = 0; i < 10; ++i) {
        const double next = reals(gen);
        const auto count = 1L << 28;
        digest.add(next, count);
    }
    EXPECT_EQ(static_cast<long>(1000 + float(10L * (1 << 28))), digest.totalWeight());
    EXPECT_GT(digest.totalWeight(), std::numeric_limits<int32_t>::max());
    std::vector<double> quantiles{0, 0.1, 0.5, 0.9, 1, reals(gen)};
    std::sort(quantiles.begin(), quantiles.end());
    auto prev = std::numeric_limits<double>::lowest();
    for (double q : quantiles) {
        const double v = digest.quantile(q);
        EXPECT_GE(v, prev) << "q = " << q;
        prev = v;
    }
}

TEST_F(TDigestTest, MergeTest) {
    TDigest digest1(1000);
    TDigest digest2(1000);

    digest2.add(std::vector<const TDigest*>{&digest1});
}

TEST_F(TDigestTest, TestSorted) {
    TDigest digest(1000);
    std::uniform_real_distribution<> reals(0.0, 1.0);
    std::uniform_int_distribution<> ints(0, 10);

    std::random_device gen;
    for (int i = 0; i < 10000; ++i) {
        digest.add(reals(gen), 1 + ints(gen));
    }
    digest.compress();
    Centroid previous(0, 0);
    for (auto centroid : digest.processed()) {
        if (previous.weight() != 0) {
            CHECK_LE(previous.mean(), centroid.mean());
        }
        previous = centroid;
    }
}

TEST_F(TDigestTest, ExtremeQuantiles) {
    TDigest digest(1000);
    // t-digest shouldn't merge extreme nodes, but let's still test how it would
    // answer to extreme quantiles in that case ('extreme' in the sense that the
    // quantile is either before the first node or after the last one)

    digest.add(10, 3);
    digest.add(20, 1);
    digest.add(40, 5);
    // this group tree is roughly equivalent to the following sorted array:
    // [ ?, 10, ?, 20, ?, ?, 50, ?, ? ]
    // and we expect it to compute approximate missing values:
    // [ 5, 10, 15, 20, 30, 40, 50, 60, 70]
    std::vector<double> values{5.0, 10.0, 15.0, 20.0, 30.0, 35.0, 40.0, 45.0, 50.0};
    std::vector<double> quantiles{1.5 / 9.0, 3.5 / 9.0, 6.5 / 9.0};
    for (auto q : quantiles) {
        EXPECT_NEAR(quantile(q, values), digest.quantile(q), 0.01) << "q = " << q;
    }
}

// Regression for D1: serialize_size() must match the number of bytes
// written by serialize(). Before the fix it over-reported by 12 B
// (3 * (sizeof(size_t) - sizeof(uint32_t))) and the trailing space leaked
// uninitialized memory to disk on every cell.
TEST_F(TDigestTest, SerializeSizeMatchesSerialized) {
    TDigest digest(1000);
    for (int i = 0; i < 50; ++i) {
        digest.add(static_cast<float>(i));
    }
    digest.compress();

    const uint64_t declared = digest.serialize_size();
    std::vector<uint8_t> buffer(declared, 0xCC);
    const size_t written = digest.serialize(buffer.data());
    EXPECT_EQ(declared, written);
}

TEST_F(TDigestTest, SerializeRoundTrip) {
    TDigest source(1000);
    for (int i = 0; i < 200; ++i) {
        source.add(static_cast<float>(i));
    }
    source.compress();

    std::vector<uint8_t> buffer(source.serialize_size());
    source.serialize(buffer.data());

    TDigest decoded;
    ASSERT_TRUE(decoded.deserialize(reinterpret_cast<const char*>(buffer.data()), buffer.size()));
    EXPECT_NEAR(source.quantile(0.5), decoded.quantile(0.5), 0.5);
    EXPECT_NEAR(source.quantile(0.99), decoded.quantile(0.99), 1.0);
}

// Regression: serialize() must not mutate the digest. Callers size their
// output buffer from serialize_size() before calling serialize(); if
// serialize() ran compress() first, the post-compress footprint
// (rebuilt _cumulative with M+1 weights) would exceed the caller buffer
// for unprocessed-only states and overflow the heap.
TEST_F(TDigestTest, SerializeDoesNotGrowUnprocessedOnly) {
    TDigest source(1000);
    for (int i = 0; i < 30; ++i) {
        source.add(static_cast<float>(i));
    }
    ASSERT_FALSE(source.unprocessed().empty());
    ASSERT_TRUE(source.processed().empty());

    const uint64_t declared = source.serialize_size();
    std::vector<uint8_t> buffer(declared, 0xCC);
    const size_t written = source.serialize(buffer.data());
    EXPECT_EQ(declared, written);
    EXPECT_FALSE(source.unprocessed().empty());
    EXPECT_TRUE(source.processed().empty());
}

// P15: deserialize must reject a blob whose declared header does not fit.
TEST_F(TDigestTest, DeserializeRejectsTruncatedHeader) {
    TDigest digest(1000);
    digest.add(1.0f);
    digest.compress();
    std::vector<uint8_t> buffer(digest.serialize_size());
    digest.serialize(buffer.data());

    TDigest decoded;
    ASSERT_FALSE(decoded.deserialize(reinterpret_cast<const char*>(buffer.data()), 10));
    EXPECT_TRUE(decoded.processed().empty());
    EXPECT_TRUE(decoded.unprocessed().empty());
}

// P15: deserialize must reject a centroid count that exceeds the cap, which
// previously would have done a multi-GiB resize() and OOM'd.
TEST_F(TDigestTest, DeserializeRejectsOversizedCount) {
    TDigest digest(1000);
    digest.add(1.0f);
    digest.compress();
    std::vector<uint8_t> buffer(digest.serialize_size());
    digest.serialize(buffer.data());

    // First centroid count lives right after the fixed header.
    constexpr size_t header_size = sizeof(float) * 5 + sizeof(size_t) * 2;
    const uint32_t evil = TDigest::kMaxCentroidsDeserialize + 1;
    std::memcpy(buffer.data() + header_size, &evil, sizeof(uint32_t));

    TDigest decoded;
    ASSERT_FALSE(decoded.deserialize(reinterpret_cast<const char*>(buffer.data()), buffer.size()));
    EXPECT_TRUE(decoded.processed().empty());
}

// P15: deserialize must reject when the declared count does not fit in the
// remaining bytes.
TEST_F(TDigestTest, DeserializeRejectsTruncatedBody) {
    TDigest digest(1000);
    for (int i = 0; i < 5; ++i) {
        digest.add(static_cast<float>(i));
    }
    digest.compress();
    std::vector<uint8_t> buffer(digest.serialize_size());
    digest.serialize(buffer.data());

    TDigest decoded;
    // Chop off the last few centroid bytes.
    const size_t truncated = buffer.size() - 4;
    ASSERT_FALSE(decoded.deserialize(reinterpret_cast<const char*>(buffer.data()), truncated));
    EXPECT_TRUE(decoded.processed().empty());
}

// Backward compat: legacy blobs were written into a 60+body buffer where the
// last 12 B were uninitialized. The reader only consumed 48 B of header and
// ignored the trailing bytes. Simulate that by appending 12 B of garbage and
// confirm deserialize still recovers the digest.
TEST_F(TDigestTest, LegacyTrailingBytesIgnored) {
    TDigest source(1000);
    for (int i = 0; i < 20; ++i) {
        source.add(static_cast<float>(i));
    }
    source.compress();
    std::vector<uint8_t> buffer(source.serialize_size());
    source.serialize(buffer.data());

    buffer.insert(buffer.end(), {0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x01, 0x23, 0x45, 0x67});

    TDigest decoded;
    ASSERT_TRUE(decoded.deserialize(reinterpret_cast<const char*>(buffer.data()), buffer.size()));
    EXPECT_NEAR(source.quantile(0.5), decoded.quantile(0.5), 0.5);
}

// P16: byte_size_in_memory tracks capacity, so it reports more than the
// logical serialize_size once any growth has happened.
TEST_F(TDigestTest, ByteSizeInMemoryIncludesCapacity) {
    TDigest digest(1000);
    for (int i = 0; i < 100; ++i) {
        digest.add(static_cast<float>(i));
    }
    EXPECT_GE(digest.byte_size_in_memory(), digest.serialize_size());
}

// P16: compress() empties _unprocessed but capacity is retained, so
// byte_size_in_memory must not drop below its pre-compress value. This is
// the regression that the old mem_usage = serialize_size() formula caused
// — process() would shrink serialize_size and produce a negative delta in
// FunctionContext::add_mem_usage(curr - prev).
TEST_F(TDigestTest, ByteSizeInMemoryStableAfterCompress) {
    TDigest digest(1000);
    for (int i = 0; i < 200; ++i) {
        digest.add(static_cast<float>(i));
    }
    const uint64_t before = digest.byte_size_in_memory();
    digest.compress();
    EXPECT_GE(digest.byte_size_in_memory(), before);
}

TEST_F(TDigestTest, Montonicity) {
    TDigest digest(1000);
    std::uniform_real_distribution<> reals(0.0, 1.0);
    std::random_device gen;
    for (int i = 0; i < 100000; i++) {
        digest.add(reals(gen));
    }

    double lastQuantile = -1;
    double lastX = -1;
    for (double z = 0; z <= 1; z += 1e-5) {
        double x = digest.quantile(z);
        EXPECT_GE(x, lastX);
        lastX = x;

        double q = digest.cdf(z);
        EXPECT_GE(q, lastQuantile);
        lastQuantile = q;
    }
}

} // namespace starrocks
