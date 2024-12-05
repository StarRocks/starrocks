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
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/rowset/segment_v2/frame_of_reference_page_test.cpp

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

#include "storage/rowset/frame_of_reference_page.h"

#include <gtest/gtest.h>

#include <memory>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "gutil/int128.h"
#include "runtime/mem_pool.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/options.h"
#include "storage/rowset/page_builder.h"
#include "storage/rowset/page_decoder.h"
#include "types/large_int_value.h"
#include "util/logging.h"

using starrocks::PageBuilderOptions;
using starrocks::operator<<;

namespace starrocks {
class FrameOfReferencePageTest : public testing::Test {
public:
    template <LogicalType type, class PageDecoderType>
    void copy_one(PageDecoderType* decoder, typename TypeTraits<type>::CppType* ret) {
        LogicalType ltype = scalar_field_type_to_logical_type(type);
        TypeDescriptor index_type(ltype);
        // TODO(alvinz): To reuse this colum
        auto column = ColumnHelper::create_column(index_type, false);
        size_t n = 1;
        ASSERT_TRUE(decoder->next_batch(&n, column.get()).ok());
        ASSERT_EQ(1, n);
        *ret = *reinterpret_cast<const typename TypeTraits<type>::CppType*>(column->raw_data());
    }

    template <LogicalType Type, class PageBuilderType = FrameOfReferencePageBuilder<Type>,
              class PageDecoderType = FrameOfReferencePageDecoder<Type>>
    void test_encode_decode_page_template(typename TypeTraits<Type>::CppType* src, size_t size) {
        typedef typename TypeTraits<Type>::CppType CppType;
        PageBuilderOptions builder_options;
        builder_options.data_page_size = 256 * 1024;
        PageBuilderType for_page_builder(builder_options);
        size = for_page_builder.add(reinterpret_cast<const uint8_t*>(src), size);
        OwnedSlice s = for_page_builder.finish()->build();
        ASSERT_EQ(size, for_page_builder.count());
        LOG(INFO) << "FrameOfReference Encoded size for " << size << " values: " << s.slice().size
                  << ", original size:" << size * sizeof(CppType);

        PageDecoderType for_page_decoder(s.slice());
        Status status = for_page_decoder.init();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(0, for_page_decoder.current_index());
        ASSERT_EQ(size, for_page_decoder.count());

        auto column = ChunkHelper::column_from_field_type(Type, false);
        size_t size_to_fetch = size;
        status = for_page_decoder.next_batch(&size_to_fetch, column.get());
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(size, size_to_fetch);

        auto* values = reinterpret_cast<const CppType*>(column->raw_data());

        for (uint i = 0; i < size; i++) {
            ASSERT_EQ(src[i], values[i]);
        }

        // Test Seek within block by ordinal
        for (int i = 0; i < 100; i++) {
            uint32_t seek_off = random() % size;
            ASSERT_TRUE(for_page_decoder.seek_to_position_in_page(seek_off).ok());
            EXPECT_EQ((int32_t)(seek_off), for_page_decoder.current_index());
            CppType ret;
            copy_one<Type, PageDecoderType>(&for_page_decoder, &ret);
            EXPECT_EQ(values[seek_off], ret);
        }
    }

    template <LogicalType Type, class PageBuilderType = FrameOfReferencePageBuilder<Type>,
              class PageDecoderType = FrameOfReferencePageDecoder<Type>>
    void test_encode_decode_page_vectorize(typename TypeTraits<Type>::CppType* src, size_t size) {
        typedef typename TypeTraits<Type>::CppType CppType;
        PageBuilderOptions builder_options;
        builder_options.data_page_size = 256 * 1024;
        PageBuilderType for_page_builder(builder_options);
        size = for_page_builder.add(reinterpret_cast<const uint8_t*>(src), size);
        OwnedSlice s = for_page_builder.finish()->build();
        ASSERT_EQ(size, for_page_builder.count());
        LOG(INFO) << "FrameOfReference Encoded size for " << size << " values: " << s.slice().size
                  << ", original size:" << size * sizeof(CppType);

        PageDecoderType for_page_decoder(s.slice());
        Status status = for_page_decoder.init();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(0, for_page_decoder.current_index());
        ASSERT_EQ(size, for_page_decoder.count());

        auto column = ChunkHelper::column_from_field_type(Type, false);
        size_t size_to_fetch = size;
        status = for_page_decoder.next_batch(&size_to_fetch, column.get());
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(size, size_to_fetch);

        for (uint i = 0; i < size; i++) {
            ASSERT_EQ(src[i], column->get(i).get<CppType>());
        }

        status = for_page_decoder.seek_to_position_in_page(0);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(0, for_page_decoder.current_index());
        ASSERT_EQ(size, for_page_decoder.count());

        auto column1 = ChunkHelper::column_from_field_type(Type, false);
        SparseRange<> read_range;
        read_range.add(Range<>(0, size / 3));
        read_range.add(Range<>(size / 2, (size * 2 / 3)));
        read_range.add(Range<>((size * 3 / 4), size));
        size_t read_num = read_range.span_size();

        status = for_page_decoder.next_batch(read_range, column1.get());
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(read_num, column1->size());

        SparseRangeIterator<> read_iter = read_range.new_iterator();
        size_t offset = 0;
        while (read_iter.has_more()) {
            Range<> r = read_iter.next(read_num);
            for (uint i = 0; i < r.span_size(); ++i) {
                ASSERT_EQ(src[r.begin() + i], column1->get(offset + i).get<CppType>());
            }
            offset += r.span_size();
        }
    }
};

TEST_F(FrameOfReferencePageTest, TestInt32BlockEncoderRandom) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = random();
    }

    test_encode_decode_page_template<TYPE_INT>(ints.get(), size);
    test_encode_decode_page_vectorize<TYPE_INT>(ints.get(), size);
}

TEST_F(FrameOfReferencePageTest, TestInt32BlockEncoderEqual) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 12345;
    }

    test_encode_decode_page_template<TYPE_INT>(ints.get(), size);
    test_encode_decode_page_vectorize<TYPE_INT>(ints.get(), size);
}

TEST_F(FrameOfReferencePageTest, TestInt32BlockEncoderSequence) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 12345 + i;
    }

    test_encode_decode_page_template<TYPE_INT>(ints.get(), size);
    test_encode_decode_page_vectorize<TYPE_INT>(ints.get(), size);
}

TEST_F(FrameOfReferencePageTest, TestInt64BlockEncoderSequence) {
    const uint32_t size = 10000;

    std::unique_ptr<int64_t[]> ints(new int64_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 21474836478 + i;
    }

    test_encode_decode_page_template<TYPE_BIGINT>(ints.get(), size);
    test_encode_decode_page_vectorize<TYPE_BIGINT>(ints.get(), size);

    test_encode_decode_page_template<TYPE_DATETIME_V1>(ints.get(), size);
    // TODO(zhuming): uncomment this line after Column for DATETIME is implemented.
    // test_encode_decode_page_vectorize<TYPE_DATETIME_V1>(ints.get(), size);
}

TEST_F(FrameOfReferencePageTest, TestInt24BlockEncoderSequence) {
    const uint32_t size = 1311;

    std::unique_ptr<uint24_t[]> ints(new uint24_t[size]);
    // to guarantee the last value is 0xFFFFFF
    uint24_t first_value = 0xFFFFFF - size + 1;
    for (int i = 0; i < size; i++) {
        ints.get()[i] = first_value + i;
    }

    test_encode_decode_page_template<TYPE_DATE_V1>(ints.get(), size);
    // TODO(zhuming): uncomment this line after Column for DATE is implemented.
    // test_encode_decode_page_vectorize<TYPE_DATE_V1>(ints.get(), size);
}

TEST_F(FrameOfReferencePageTest, TestInt128BlockEncoderSequence) {
    const uint32_t size = 1000;

    std::unique_ptr<int128_t[]> ints(new int128_t[size]);
    // to guarantee the last value is numeric_limits<uint128_t>::max()
    int128_t first_value = numeric_limits<int128_t>::max() - size + 1;
    for (int i = 0; i < size; i++) {
        ints.get()[i] = first_value + i;
    }

    test_encode_decode_page_template<TYPE_LARGEINT>(ints.get(), size);
    test_encode_decode_page_vectorize<TYPE_LARGEINT>(ints.get(), size);
}

TEST_F(FrameOfReferencePageTest, TestInt24BlockEncoderMinMax) {
    std::unique_ptr<uint24_t[]> ints(new uint24_t[2]);
    ints.get()[0] = 0;
    ints.get()[1] = 0xFFFFFF;

    test_encode_decode_page_template<TYPE_DATE_V1>(ints.get(), 2);
    // TODO(zhuming): uncomment this line after Column for DATE is implemented.
    // test_encode_decode_page_vectorize<TYPE_DATE_V1>(ints.get(), 2);
}

TEST_F(FrameOfReferencePageTest, TestInt128BlockEncoderMinMax) {
    std::unique_ptr<int128_t[]> ints(new int128_t[2]);
    ints.get()[0] = numeric_limits<int128_t>::lowest();
    ints.get()[1] = numeric_limits<int128_t>::max();

    test_encode_decode_page_template<TYPE_LARGEINT>(ints.get(), 2);
    test_encode_decode_page_vectorize<TYPE_LARGEINT>(ints.get(), 2);
}

TEST_F(FrameOfReferencePageTest, TestInt32SequenceBlockEncoderSize) {
    size_t size = 128;
    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = i;
    }
    PageBuilderOptions builder_options;
    builder_options.data_page_size = 256 * 1024;
    FrameOfReferencePageBuilder<TYPE_INT> page_builder(builder_options);
    size = page_builder.add(reinterpret_cast<const uint8_t*>(ints.get()), size);
    OwnedSlice s = page_builder.finish()->build();
    // body: 4 bytes min value + 128 * 1 /8 packing value = 20
    // footer: (1 + 1) * 1 + 1 + 4 = 7
    ASSERT_EQ(27, s.slice().size);
}

TEST_F(FrameOfReferencePageTest, TestFirstLastValue) {
    size_t size = 128;
    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = i;
    }
    PageBuilderOptions builder_options;
    builder_options.data_page_size = 256 * 1024;
    FrameOfReferencePageBuilder<TYPE_INT> page_builder(builder_options);
    size = page_builder.add(reinterpret_cast<const uint8_t*>(ints.get()), size);
    OwnedSlice s = page_builder.finish()->build();
    int32_t first_value = -1;
    page_builder.get_first_value(&first_value);
    ASSERT_EQ(0, first_value);
    int32_t last_value = 0;
    page_builder.get_last_value(&last_value);
    ASSERT_EQ(127, last_value);
}

TEST_F(FrameOfReferencePageTest, TestInt32NormalBlockEncoderSize) {
    size_t size = 128;
    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 128 - i;
    }
    PageBuilderOptions builder_options;
    builder_options.data_page_size = 256 * 1024;
    FrameOfReferencePageBuilder<TYPE_INT> page_builder(builder_options);
    size = page_builder.add(reinterpret_cast<const uint8_t*>(ints.get()), size);
    OwnedSlice s = page_builder.finish()->build();
    // body: 4 bytes min value + 128 * 7 /8 packing value = 116
    // footer: (1 + 1) * 1 + 1 + 4 = 7
    ASSERT_EQ(123, s.slice().size);
}

TEST_F(FrameOfReferencePageTest, TestFindBitsOfInt) {
    int8_t bits_3 = 0x06;
    ASSERT_EQ(3, bits(bits_3));

    uint8_t bits_4 = 0x0F;
    ASSERT_EQ(4, bits(bits_4));

    int32_t bits_17 = 0x000100FF;
    ASSERT_EQ(17, bits(bits_17));

    int64_t bits_33 = 0x00000001FFFFFFFF;
    ASSERT_EQ(33, bits(bits_33));

    int128_t bits_0 = 0;
    ASSERT_EQ(0, bits(bits_0));

    int128_t bits_127 = numeric_limits<int128_t>::max();
    ASSERT_EQ(127, bits(bits_127));

    uint128_t bits_128 = numeric_limits<uint128_t>::max();
    ASSERT_EQ(128, bits(bits_128));

    int128_t bits_65 = ((int128_t)1) << 64;
    ASSERT_EQ(65, bits(bits_65));
}

} // namespace starrocks
