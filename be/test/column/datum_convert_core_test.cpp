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

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "column/datum_convert.h"

namespace starrocks {

class TestTypeInfoAllocator {
public:
    TypeInfoAllocator make_allocator() { return TypeInfoAllocator{this, &TestTypeInfoAllocator::allocate}; }

private:
    static uint8_t* allocate(void* ctx, size_t size) {
        auto* self = static_cast<TestTypeInfoAllocator*>(ctx);
        return self->allocate_impl(size);
    }

    uint8_t* allocate_impl(size_t size) {
        auto block = std::make_unique<uint8_t[]>(size == 0 ? 1 : size);
        uint8_t* ptr = block.get();
        _blocks.emplace_back(std::move(block));
        return ptr;
    }

    std::vector<std::unique_ptr<uint8_t[]>> _blocks;
};

TEST(DatumConvertCoreTest, ParseNumericAndBooleanFromString) {
    Datum int_datum;
    auto st = datum_from_string(get_type_info(TYPE_INT).get(), &int_datum, "123", nullptr);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(123, int_datum.get_int32());

    Datum bool_datum;
    st = datum_from_string(get_type_info(TYPE_BOOLEAN).get(), &bool_datum, "1", nullptr);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(1, bool_datum.get_int8());
}

TEST(DatumConvertCoreTest, ToStringHandlesNullAndRoundTrip) {
    auto int_type = get_type_info(TYPE_INT);

    Datum null_datum;
    null_datum.set_null();
    EXPECT_EQ("null", datum_to_string(int_type.get(), null_datum));

    Datum roundtrip_datum;
    auto st = datum_from_string(int_type.get(), &roundtrip_datum, "456", nullptr);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ("456", datum_to_string(int_type.get(), roundtrip_datum));
}

TEST(DatumConvertCoreTest, StringTypesUseAllocatorAndTrimCharTailZero) {
    TestTypeInfoAllocator alloc_holder;
    TypeInfoAllocator allocator = alloc_holder.make_allocator();

    std::string varchar_input = "abc";
    Datum varchar_datum;
    auto st = datum_from_string(get_type_info(TYPE_VARCHAR).get(), &varchar_datum, varchar_input, &allocator);
    ASSERT_TRUE(st.ok()) << st.to_string();
    Slice varchar_slice = varchar_datum.get_slice();
    ASSERT_EQ(3, varchar_slice.size);
    EXPECT_NE(varchar_input.data(), varchar_slice.data);
    EXPECT_EQ("abc", std::string(varchar_slice.data, varchar_slice.size));

    std::string char_input("ab\0cd", 5);
    Datum char_datum;
    st = datum_from_string(get_type_info(TYPE_CHAR).get(), &char_datum, char_input, &allocator);
    ASSERT_TRUE(st.ok()) << st.to_string();
    Slice char_slice = char_datum.get_slice();
    EXPECT_EQ(2, char_slice.size);
    EXPECT_EQ("ab", std::string(char_slice.data, char_slice.size));
}

TEST(DatumConvertCoreTest, UnsupportedTypeReturnsNotSupported) {
    Datum datum;
    auto st = datum_from_string(get_type_info(TYPE_JSON).get(), &datum, "{}", nullptr);
    ASSERT_TRUE(st.is_not_supported()) << st.to_string();
}

} // namespace starrocks
