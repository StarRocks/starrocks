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
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/rowset/segment_v2/rle_page_test.cpp

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

#include "storage/rowset/rle_page.h"

#include <algorithm>
#include <cstdlib>
#include <memory>

#include "column/fixed_length_column.h"
#include "gtest/gtest.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/options.h"
#include "storage/rowset/page_builder.h"
#include "storage/rowset/page_decoder.h"
#include "util/logging.h"

using starrocks::PageBuilderOptions;

namespace starrocks {

class RlePageTest : public testing::Test {
public:
    ~RlePageTest() override = default;

    template <LogicalType type, class PageDecoderType>
    void copy_one(PageDecoderType* decoder, typename TypeTraits<type>::CppType* ret) {
        auto column = ChunkHelper::column_from_field_type(type, false);

        size_t n = 1;
        decoder->next_batch(&n, column.get());
        ASSERT_EQ(1, n);
        *ret = *reinterpret_cast<const typename TypeTraits<type>::CppType*>(column->raw_data());
    }

    template <LogicalType Type, class PageBuilderType = RlePageBuilder<Type>>
    OwnedSlice rle_encode(typename TypeTraits<Type>::CppType* src, size_t size) {
        typedef typename TypeTraits<Type>::CppType CppType;
        PageBuilderOptions builder_options;
        builder_options.data_page_size = 256 * 1024;
        PageBuilderType rle_page_builder(builder_options);
        size = rle_page_builder.add(reinterpret_cast<const uint8_t*>(src), size);
        OwnedSlice s = rle_page_builder.finish()->build();
        CHECK_EQ(size, rle_page_builder.count());

        //check first value and last value
        CppType first_value = {};
        rle_page_builder.get_first_value(&first_value);
        CHECK_EQ(src[0], first_value);
        CppType last_value = {};
        rle_page_builder.get_last_value(&last_value);
        CHECK_EQ(src[size - 1], last_value);
        return s;
    }

    template <LogicalType Type>
    void test_encode_decode_page_template(typename TypeTraits<Type>::CppType* src, size_t size) {
        typedef typename TypeTraits<Type>::CppType CppType;
        OwnedSlice s = rle_encode<Type>(src, size);

        RlePageDecoder<Type> rle_page_decoder(s.slice());
        Status status = rle_page_decoder.init();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(0, rle_page_decoder.current_index());
        ASSERT_EQ(size, rle_page_decoder.count());

        auto column = ChunkHelper::column_from_field_type(Type, true);
        size_t size_to_fetch = size;
        status = rle_page_decoder.next_batch(&size_to_fetch, column.get());
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(size, size_to_fetch);

        auto* values = reinterpret_cast<const CppType*>(column->raw_data());
        for (uint i = 0; i < size; i++) {
            if (src[i] != values[i]) {
                FAIL() << "Fail at index " << i << " inserted=" << src[i] << " got=" << values[i];
            }
        }

        // Test Seek within block by ordinal
        for (int i = 0; i < 100; i++) {
            uint32_t seek_off = random() % size;
            rle_page_decoder.seek_to_position_in_page(seek_off);
            EXPECT_EQ((int32_t)(seek_off), rle_page_decoder.current_index());
            CppType ret;
            copy_one<Type, RlePageDecoder<Type>>(&rle_page_decoder, &ret);
            EXPECT_EQ(values[seek_off], ret);
        }
    }

    template <LogicalType Type>
    void test_encode_decode_page_vectorized(typename TypeTraits<Type>::CppType* src, size_t size) {
        typedef typename TypeTraits<Type>::CppType CppType;
        OwnedSlice s = rle_encode<Type>(src, size);

        RlePageDecoder<Type> rle_page_decoder(s.slice());
        Status status = rle_page_decoder.init();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(0, rle_page_decoder.current_index());
        ASSERT_EQ(size, rle_page_decoder.count());

        auto column = FixedLengthColumn<CppType>::create();
        size_t size_to_fetch = size;
        status = rle_page_decoder.next_batch(&size_to_fetch, column.get());
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(size, size_to_fetch);

        for (uint i = 0; i < size; i++) {
            ASSERT_EQ(src[i], column->get_data()[i]);
        }

        rle_page_decoder.seek_to_position_in_page(0);
        ASSERT_EQ(0, rle_page_decoder.current_index());
        auto column1 = FixedLengthColumn<CppType>::create();
        SparseRange<> read_range;
        read_range.add(Range<>(0, size / 3));
        read_range.add(Range<>(size / 2, (size * 2 / 3)));
        read_range.add(Range<>((size * 3 / 4), size));
        size_t read_num = read_range.span_size();

        status = rle_page_decoder.next_batch(read_range, column1.get());
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(read_num, column1->size());

        size_t offset = 0;
        SparseRangeIterator<> read_iter = read_range.new_iterator();
        while (read_iter.has_more()) {
            Range<> r = read_iter.next(read_num);
            for (uint i = 0; i < r.span_size(); ++i) {
                ASSERT_EQ(src[r.begin() + i], column1->get_data()[i + offset]);
            }
            offset += r.span_size();
        }
    }
};

// Test for rle block, for INT32, BOOL
TEST_F(RlePageTest, TestRleInt32BlockEncoderRandom) {
    const uint32_t size = 10000;

    // TYPE_INT
    {
        using CppType = TypeTraits<TYPE_INT>::CppType;
        std::vector<CppType> ints(size, 0);
        std::generate(std::begin(ints), std::end(ints), []() -> CppType { return rand(); });
        test_encode_decode_page_template<TYPE_INT>(ints.data(), size);
        test_encode_decode_page_vectorized<TYPE_INT>(ints.data(), size);
    }
    // TYPE_BIGINT
    {
        using CppType = TypeTraits<TYPE_BIGINT>::CppType;
        std::vector<CppType> ints(size, 0);
        std::generate(std::begin(ints), std::end(ints), []() -> CppType { return rand(); });
        test_encode_decode_page_template<TYPE_BIGINT>(ints.data(), size);
        test_encode_decode_page_vectorized<TYPE_BIGINT>(ints.data(), size);
    }
}

TEST_F(RlePageTest, TestRleInt32BlockEncoderEqual) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 12345;
    }

    test_encode_decode_page_template<TYPE_INT>(ints.get(), size);
}

TEST_F(RlePageTest, TestRleInt32BlockEncoderSequence) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 12345 + i;
    }

    test_encode_decode_page_template<TYPE_INT>(ints.get(), size);
}

TEST_F(RlePageTest, TestRleInt32BlockEncoderSize) {
    size_t size = 100;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 0;
    }
    PageBuilderOptions builder_options;
    builder_options.data_page_size = 256 * 1024;
    RlePageBuilder<TYPE_INT> rle_page_builder(builder_options);
    size = rle_page_builder.add(reinterpret_cast<const uint8_t*>(ints.get()), size);
    OwnedSlice s = rle_page_builder.finish()->build();
    // 4 bytes header
    // 2 bytes indicate_value(): 0x64 << 1 | 1 = 201
    // 4 bytes values
    ASSERT_EQ(10, s.slice().size);
}

TEST_F(RlePageTest, TestRleBoolBlockEncoderRandom) {
    const uint32_t size = 10000;

    std::unique_ptr<bool[]> bools(new bool[size]);
    for (int i = 0; i < size; i++) {
        bools.get()[i] = (random() % 2 == 0);
    }

    test_encode_decode_page_template<TYPE_BOOLEAN>(bools.get(), size);
}

TEST_F(RlePageTest, TestRleBoolBlockEncoderSize) {
    size_t size = 100;

    std::unique_ptr<bool[]> bools(new bool[size]);
    for (int i = 0; i < size; i++) {
        bools.get()[i] = true;
    }
    PageBuilderOptions builder_options;
    builder_options.data_page_size = 256 * 1024;
    RlePageBuilder<TYPE_BOOLEAN> rle_page_builder(builder_options);
    size = rle_page_builder.add(reinterpret_cast<const uint8_t*>(bools.get()), size);
    OwnedSlice s = rle_page_builder.finish()->build();
    // 4 bytes header
    // 2 bytes indicate_value(): 0x64 << 1 | 1 = 201
    // 1 bytes values
    ASSERT_EQ(7, s.slice().size);
}

} // namespace starrocks
