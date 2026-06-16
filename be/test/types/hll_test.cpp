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

#include "types/hll.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <string>
#include <vector>

#include "base/coding.h"
#include "base/hash/hash_util.hpp"
#include "base/string/slice.h"

namespace starrocks {

namespace {

constexpr size_t kHllRegisterAlignment = 4096;
bool g_hll_allocate_fail = false;

bool test_register_allocate(size_t size, void* /*ctx*/, MemChunk* chunk) {
    if (g_hll_allocate_fail) {
        chunk->data = nullptr;
        chunk->size = 0;
        chunk->core_id = -1;
        return false;
    }
    void* ptr = nullptr;
    if (posix_memalign(&ptr, kHllRegisterAlignment, size) != 0) {
        return false;
    }
    chunk->data = static_cast<uint8_t*>(ptr);
    chunk->size = size;
    chunk->core_id = -1;
    return true;
}

void free_with_stdlib(const MemChunk& chunk, void* /*ctx*/) {
    std::free(chunk.data);
}

class ScopedHllAllocateFail {
public:
    ScopedHllAllocateFail() { g_hll_allocate_fail = true; }
    ~ScopedHllAllocateFail() { g_hll_allocate_fail = false; }
};

} // namespace

class TestHll : public testing::Test {
public:
    static void SetUpTestSuite() {
        HyperLogLog::RegistersAllocator allocator;
        allocator.allocate = test_register_allocate;
        allocator.free = free_with_stdlib;
        Status st = HyperLogLog::set_registers_allocator(allocator);
        ASSERT_TRUE(st.ok()) << st.to_string();
    }

    ~TestHll() override = default;
};

static uint64_t hash(uint64_t value) {
    return HashUtil::murmur_hash64A(&value, 8, 0);
}
//  keep logic same with java version in fe when you change hll_test.cpp,see HllTest.java
TEST_F(TestHll, Normal) {
    uint8_t buf[HLL_REGISTERS_COUNT + 1] = {0};

    // empty
    {
        Slice str((char*)buf, 0);
        ASSERT_FALSE(HyperLogLog::is_valid(str));
    }
    // check unknown type
    {
        buf[0] = 60;
        Slice str((char*)buf, 1);
        ASSERT_FALSE(HyperLogLog::is_valid(str));
    }

    // empty
    {
        HyperLogLog empty_hll;
        int len = empty_hll.serialize(buf);
        ASSERT_EQ(1, len);
        HyperLogLog test_hll(Slice((char*)buf, len));
        ASSERT_EQ(0, test_hll.estimate_cardinality());

        // check serialize
        {
            Slice str((char*)buf, len);
            ASSERT_TRUE(HyperLogLog::is_valid(str));
        }
        {
            Slice str((char*)buf, len + 1);
            ASSERT_FALSE(HyperLogLog::is_valid(str));
        }
    }
    // explicit [0. 100)
    HyperLogLog explicit_hll;
    {
        for (int i = 0; i < 100; ++i) {
            explicit_hll.update(hash(i));
        }
        int len = explicit_hll.serialize(buf);
        ASSERT_EQ(1 + 1 + 100 * 8, len);

        // check serialize
        {
            Slice str((char*)buf, len);
            ASSERT_TRUE(HyperLogLog::is_valid(str));
        }
        {
            Slice str((char*)buf, 1);
            ASSERT_FALSE(HyperLogLog::is_valid(str));
        }

        HyperLogLog test_hll(Slice((char*)buf, len));
        test_hll.update(hash(0));
        {
            HyperLogLog other_hll;
            for (int i = 0; i < 100; ++i) {
                other_hll.update(hash(i));
            }
            test_hll.merge(other_hll);
        }
        ASSERT_EQ(100, test_hll.estimate_cardinality());
    }
    // sparse [1024, 2048)
    HyperLogLog sparse_hll;
    {
        for (int i = 0; i < 1024; ++i) {
            sparse_hll.update(hash(i + 1024));
        }
        int len = sparse_hll.serialize(buf);
        ASSERT_TRUE(len < HLL_REGISTERS_COUNT + 1);

        // check serialize
        {
            Slice str((char*)buf, len);
            ASSERT_TRUE(HyperLogLog::is_valid(str));
        }
        {
            Slice str((char*)buf, 1 + 3);
            ASSERT_FALSE(HyperLogLog::is_valid(str));
        }

        HyperLogLog test_hll(Slice((char*)buf, len));
        test_hll.update(hash(1024));
        {
            HyperLogLog other_hll;
            for (int i = 0; i < 1024; ++i) {
                other_hll.update(hash(i + 1024));
            }
            test_hll.merge(other_hll);
        }
        auto cardinality = test_hll.estimate_cardinality();
        ASSERT_EQ(sparse_hll.estimate_cardinality(), cardinality);
        // 2% error rate
        ASSERT_TRUE(cardinality > 1000 && cardinality < 1045);
    }
    // full [64 * 1024, 128 * 1024)
    HyperLogLog full_hll;
    {
        for (int i = 0; i < 64 * 1024; ++i) {
            full_hll.update(hash(64 * 1024 + i));
        }
        int len = full_hll.serialize(buf);
        ASSERT_EQ(HLL_REGISTERS_COUNT + 1, len);

        // check serialize
        {
            Slice str((char*)buf, len);
            ASSERT_TRUE(HyperLogLog::is_valid(str));
        }
        {
            Slice str((char*)buf, len + 1);
            ASSERT_FALSE(HyperLogLog::is_valid(str));
        }

        HyperLogLog test_hll(Slice((char*)buf, len));
        auto cardinality = test_hll.estimate_cardinality();
        ASSERT_EQ(full_hll.estimate_cardinality(), cardinality);
        // 2% error rate
        ASSERT_TRUE(cardinality > 62 * 1024 && cardinality < 66 * 1024);
    }
    // merge explicit to empty_hll
    {
        HyperLogLog new_explicit_hll;
        new_explicit_hll.merge(explicit_hll);
        ASSERT_EQ(100, new_explicit_hll.estimate_cardinality());

        // merge another explicit
        {
            HyperLogLog other_hll;
            for (int i = 100; i < 200; ++i) {
                other_hll.update(hash(i));
            }
            // this is converted to full
            other_hll.merge(new_explicit_hll);
            ASSERT_TRUE(other_hll.estimate_cardinality() > 190);
        }
        // merge full
        {
            new_explicit_hll.merge(full_hll);
            ASSERT_TRUE(new_explicit_hll.estimate_cardinality() > full_hll.estimate_cardinality());
        }
    }
    // merge sparse to empty_hll
    {
        HyperLogLog new_sparse_hll;
        new_sparse_hll.merge(sparse_hll);
        ASSERT_EQ(sparse_hll.estimate_cardinality(), new_sparse_hll.estimate_cardinality());

        // merge explicit
        new_sparse_hll.merge(explicit_hll);
        ASSERT_TRUE(new_sparse_hll.estimate_cardinality() > sparse_hll.estimate_cardinality());

        // merge full
        new_sparse_hll.merge(full_hll);
        ASSERT_TRUE(new_sparse_hll.estimate_cardinality() > full_hll.estimate_cardinality());
    }
}

static std::string serialize_to_string(const HyperLogLog& h) {
    std::string buf;
    buf.resize(h.max_serialized_size());
    size_t n = h.serialize(reinterpret_cast<uint8_t*>(buf.data()));
    buf.resize(n);
    return buf;
}

// merge(const Slice&) must yield byte-identical serialized state and the same
// cardinality as the temp-object path it replaces: merge(HyperLogLog(slice)).
static void expect_merge_slice_matches(const HyperLogLog& dst_proto, const std::string& src_bytes) {
    Slice slice(src_bytes);
    HyperLogLog via_slice(dst_proto);
    HyperLogLog via_temp(dst_proto);
    via_slice.merge(slice);
    via_temp.merge(HyperLogLog(slice));
    EXPECT_EQ(serialize_to_string(via_slice), serialize_to_string(via_temp));
    EXPECT_EQ(via_slice.estimate_cardinality(), via_temp.estimate_cardinality());
}

TEST_F(TestHll, MergeSlice) {
    // Build serialized sources of each on-wire format.
    HyperLogLog empty_src;
    HyperLogLog explicit_src; // EXPLICIT: <= 160 hashes
    for (int i = 0; i < 50; ++i) {
        explicit_src.update(hash(i));
    }
    HyperLogLog low_card; // serializes to SPARSE (few non-zero registers)
    for (int i = 0; i < 1024; ++i) {
        low_card.update(hash(i + 1024));
    }
    HyperLogLog full_src; // FULL
    for (int i = 0; i < 64 * 1024; ++i) {
        full_src.update(hash(64 * 1024 + i));
    }

    const std::string empty_bytes = serialize_to_string(empty_src);
    const std::string explicit_bytes = serialize_to_string(explicit_src);
    const std::string sparse_bytes = serialize_to_string(low_card);
    const std::string full_bytes = serialize_to_string(full_src);
    ASSERT_EQ(HLL_DATA_SPARSE, static_cast<uint8_t>(sparse_bytes[0]));
    ASSERT_EQ(HLL_DATA_FULL, static_cast<uint8_t>(full_bytes[0]));

    // Destination prototypes covering every in-memory _type, including a genuine
    // SPARSE state (only reachable by deserializing a sparse slice).
    HyperLogLog dst_empty;
    HyperLogLog dst_explicit;
    for (int i = 0; i < 50; ++i) {
        dst_explicit.update(hash(i + 500));
    }
    HyperLogLog dst_sparse{Slice(sparse_bytes)};
    HyperLogLog dst_full;
    for (int i = 0; i < 64 * 1024; ++i) {
        dst_full.update(hash(i + 200000));
    }

    const std::vector<const HyperLogLog*> dsts = {&dst_empty, &dst_explicit, &dst_sparse, &dst_full};
    const std::vector<const std::string*> srcs = {&empty_bytes, &explicit_bytes, &sparse_bytes, &full_bytes};
    for (const auto* dst : dsts) {
        for (const auto* src : srcs) {
            expect_merge_slice_matches(*dst, *src);
        }
    }

    // EXPLICIT + EXPLICIT crossing 160 unique => promotion to FULL.
    {
        HyperLogLog big_explicit; // 120 hashes, still EXPLICIT on the wire
        for (int i = 0; i < 120; ++i) {
            big_explicit.update(hash(i + 900000));
        }
        expect_merge_slice_matches(dst_explicit, serialize_to_string(big_explicit));
    }

    // EXPLICIT slice containing a hash == 0 must be preserved, not dropped.
    {
        HyperLogLog with_zero;
        with_zero.update(0);
        with_zero.update(hash(7));
        const std::string with_zero_bytes = serialize_to_string(with_zero);
        for (const auto* dst : dsts) {
            expect_merge_slice_matches(*dst, with_zero_bytes);
        }
    }

    // Non-canonical SPARSE slice with a duplicate index: deserialize is
    // last-write-wins, so merge(slice) must match it (it delegates, not max).
    {
        uint8_t buf[1 + 4 + 3 * 2];
        uint8_t* p = buf;
        *p++ = HLL_DATA_SPARSE;
        encode_fixed32_le(p, 2);
        p += 4;
        encode_fixed16_le(p, 100);
        p += 2;
        *p++ = 10;
        encode_fixed16_le(p, 100);
        p += 2;
        *p++ = 5; // last write (5) wins over 10 in deserialize
        std::string dup_bytes(reinterpret_cast<char*>(buf), sizeof(buf));
        ASSERT_TRUE(HyperLogLog::is_valid(Slice(dup_bytes)));
        for (const auto* dst : dsts) {
            expect_merge_slice_matches(*dst, dup_bytes);
        }
    }

    // Invalid / truncated / null slices must be a no-op (no crash, no change).
    {
        HyperLogLog ref(full_src);
        const std::string before = serialize_to_string(ref);

        HyperLogLog t1(full_src);
        t1.merge(Slice((char*)nullptr, 0));
        EXPECT_EQ(before, serialize_to_string(t1));

        // Null data pointer with a non-zero size must not be dereferenced by is_valid().
        HyperLogLog t1b(full_src);
        t1b.merge(Slice((char*)nullptr, 5));
        EXPECT_EQ(before, serialize_to_string(t1b));

        HyperLogLog t2(full_src);
        t2.merge(Slice(full_bytes.data(), full_bytes.size() - 1)); // truncated FULL
        EXPECT_EQ(before, serialize_to_string(t2));

        uint8_t bad_type = 60;
        HyperLogLog t3(full_src);
        t3.merge(Slice(reinterpret_cast<char*>(&bad_type), 1)); // unknown type
        EXPECT_EQ(before, serialize_to_string(t3));
    }
}

// Locks the byte layout that HllNdvAggregateFunction::convert_to_serialize_format
// relies on when it emits a single hashed value directly (Change 2): a one-element
// EXPLICIT record [type][count=1][hash:8], or an EMPTY record when the hash is 0.
TEST_F(TestHll, SingleValueExplicitFormat) {
    const uint64_t value = hash(424242);
    ASSERT_NE(0, value);

    HyperLogLog one;
    one.update(value);
    const std::string from_hll = serialize_to_string(one);

    uint8_t expected[2 + sizeof(uint64_t)];
    expected[0] = HLL_DATA_EXPLICIT;
    expected[1] = 1;
    encode_fixed64_le(expected + 2, value);
    EXPECT_EQ(std::string(reinterpret_cast<char*>(expected), sizeof(expected)), from_hll);

    HyperLogLog zero; // hash == 0 contributes nothing -> EMPTY record
    const std::string from_empty = serialize_to_string(zero);
    ASSERT_EQ(1u, from_empty.size());
    EXPECT_EQ(HLL_DATA_EMPTY, static_cast<uint8_t>(from_empty[0]));
}

TEST_F(TestHll, InvalidPtr) {
    {
        HyperLogLog hll(Slice((char*)nullptr, 0));
        ASSERT_EQ(0, hll.estimate_cardinality());
    }
    {
        uint8_t buf[64] = {60};
        HyperLogLog hll(Slice(buf, 1));
        ASSERT_EQ(0, hll.estimate_cardinality());
    }
}

TEST_F(TestHll, AllocateFail) {
    // prepare a FULL HLL with test allocator first.
    HyperLogLog full_hll;
    for (int i = 0; i < 64 * 1024; ++i) {
        full_hll.update(hash(64 * 1024 + i));
    }
    uint8_t buf[HLL_REGISTERS_COUNT + 1] = {0};
    int len = full_hll.serialize(buf);
    ASSERT_EQ(HLL_REGISTERS_COUNT + 1, len);

    ScopedHllAllocateFail scoped_fail;

    // copy construction should throw when allocation fails
    ASSERT_THROW(static_cast<void>(HyperLogLog(full_hll)), std::bad_alloc);

    // update() will allocate when converting explicit -> registers, expect throw
    {
        HyperLogLog hll;
        for (int i = 0; i <= HLL_EXPLICLIT_INT64_NUM; ++i) {
            if (i == HLL_EXPLICLIT_INT64_NUM) {
                ASSERT_THROW(hll.update(hash(i)), std::bad_alloc);
            } else {
                hll.update(hash(i));
            }
        }
    }

    // merge() will allocate registers when self is empty and other is full, expect throw
    {
        HyperLogLog empty_hll;
        ASSERT_THROW(empty_hll.merge(full_hll), std::bad_alloc);
    }

    // deserialize should fail gracefully and leave object empty
    HyperLogLog hll_after_fail(Slice((char*)buf, len));
    EXPECT_EQ(0, hll_after_fail.estimate_cardinality());
}

} // namespace starrocks
