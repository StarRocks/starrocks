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
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/rowset/segment_v2/bitshuffle_page_test.cpp

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

#include "storage/rowset/bitshuffle_page.h"

#include <gtest/gtest.h>

#include <memory>

#include "column/datum_convert.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/options.h"
#include "storage/rowset/page_decoder.h"
#include "storage/rowset/storage_page_decoder.h"
#include "util/logging.h"

using starrocks::PageBuilderOptions;
using starrocks::DataDecoder;
using starrocks::StoragePageDecoder;

namespace starrocks {

class BitShufflePageTest : public testing::Test {
public:
    ~BitShufflePageTest() override = default;

    template <LogicalType type, class PageDecoderType>
    void copy_one(PageDecoderType* decoder, typename TypeTraits<type>::CppType* ret) {
        auto column = ChunkHelper::column_from_field_type(type, true);
        size_t n = 1;
        ASSERT_TRUE(decoder->next_batch(&n, column.get()).ok());
        ASSERT_EQ(1, n);
        *ret = *reinterpret_cast<const typename TypeTraits<type>::CppType*>(column->raw_data());
    }

    template <LogicalType Type, class PageBuilderType, class PageDecoderType, int ReserveHead = 0>
    void test_encode_decode_page_template(typename TypeTraits<Type>::CppType* src, size_t size) {
        typedef typename TypeTraits<Type>::CppType CppType;
        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        PageBuilderType page_builder(options);
        page_builder.reserve_head(ReserveHead);

        size = page_builder.add(reinterpret_cast<const uint8_t*>(src), size);
        OwnedSlice s = page_builder.finish()->build();

        //check first value and last value
        CppType first_value;
        page_builder.get_first_value(&first_value);
        ASSERT_EQ(src[0], first_value);
        CppType last_value;
        page_builder.get_last_value(&last_value);
        ASSERT_EQ(src[size - 1], last_value);

        Slice encoded_data = s.slice();
        encoded_data.remove_prefix(ReserveHead);

        starrocks::PageFooterPB footer;
        footer.set_type(starrocks::DATA_PAGE);
        starrocks::DataPageFooterPB* data_page_footer = footer.mutable_data_page_footer();
        data_page_footer->set_nullmap_size(0);
        std::unique_ptr<char[]> page = nullptr;

        Status st = StoragePageDecoder::decode_page(&footer, 0, starrocks::BIT_SHUFFLE, &page, &encoded_data);
        ASSERT_TRUE(st.ok());

        PageDecoderType page_decoder(encoded_data);
        Status status = page_decoder.init();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(0, page_decoder.current_index());

        auto column = ChunkHelper::column_from_field_type(Type, false);

        status = page_decoder.next_batch(&size, column.get());
        ASSERT_TRUE(status.ok());

        auto* values = reinterpret_cast<const CppType*>(column->raw_data());
        auto* decoded = (CppType*)values;
        for (uint i = 0; i < size; i++) {
            if (src[i] != decoded[i]) {
                FAIL() << "Fail at index " << i << " inserted=" << src[i] << " got=" << decoded[i];
            }
        }

        // Test Seek within block by ordinal
        for (int i = 0; i < 100; i++) {
            uint32_t seek_off = random() % size;
            ASSERT_TRUE(page_decoder.seek_to_position_in_page(seek_off).ok());
            EXPECT_EQ((int32_t)(seek_off), page_decoder.current_index());
            CppType ret;
            copy_one<Type, PageDecoderType>(&page_decoder, &ret);
            EXPECT_EQ(decoded[seek_off], ret);
        }
    }

    template <LogicalType Type, class PageBuilderType, class PageDecoderType>
    void test_encode_decode_page_vectorized() {
        using CppType = typename CppTypeTraits<Type>::CppType;
        auto src = ChunkHelper::column_from_field_type(Type, false);
        CppType value = 0;
        size_t count = 64 * 1024 / sizeof(CppType);
        src->reserve(count);
        for (size_t i = 0; i < count; ++i) {
            (void)src->append_numbers(&value, sizeof(CppType));
            value = value + 1;
        }

        PageBuilderOptions options;
        options.data_page_size = 64 * 1024;
        PageBuilderType page_builder(options);
        size_t added = page_builder.add(reinterpret_cast<const uint8_t*>(src->raw_data()), count);
        ASSERT_EQ(count, added);
        OwnedSlice s = page_builder.finish()->build();

        Slice encoded_data = s.slice();
        std::unique_ptr<char[]> page = nullptr;
        {
            starrocks::PageFooterPB footer;
            footer.set_type(starrocks::DATA_PAGE);
            starrocks::DataPageFooterPB* data_page_footer = footer.mutable_data_page_footer();
            data_page_footer->set_nullmap_size(0);

            Status st = StoragePageDecoder::decode_page(&footer, 0, starrocks::BIT_SHUFFLE, &page, &encoded_data);
            ASSERT_TRUE(st.ok());

            // read whole the page
            PageDecoderType page_decoder(encoded_data);
            Status status = page_decoder.init();
            ASSERT_TRUE(status.ok());
            ASSERT_EQ(0, page_decoder.current_index());

            auto dst = ChunkHelper::column_from_field_type(Type, false);
            dst->reserve(count);
            size_t size = count;
            status = page_decoder.next_batch(&size, dst.get());
            ASSERT_TRUE(status.ok());
            ASSERT_EQ(size, count);
            TypeInfoPtr type_info = get_type_info(Type);
            for (int i = 0; i < count; ++i) {
                ASSERT_EQ(0, type_info->cmp(src->get(i), dst->get(i)))
                        << " row " << i << ": " << datum_to_string(type_info.get(), src->get(i)) << " vs "
                        << datum_to_string(type_info.get(), dst->get(i));
            }
        }

        {
            // read half of the page
            PageDecoderType page_decoder(encoded_data);
            Status status = page_decoder.init();
            ASSERT_TRUE(status.ok());
            ASSERT_EQ(0, page_decoder.current_index());

            auto dst = ChunkHelper::column_from_field_type(Type, false);
            size_t size = count / 2;
            dst->reserve(size);
            status = page_decoder.next_batch(&size, dst.get());
            ASSERT_TRUE(status.ok());
            ASSERT_EQ(size, count / 2);
            TypeInfoPtr type_info = get_type_info(Type);
            for (int i = 0; i < size; ++i) {
                ASSERT_EQ(0, type_info->cmp(src->get(i), dst->get(i)))
                        << " row " << i << ": " << datum_to_string(type_info.get(), src->get(i)) << " vs "
                        << datum_to_string(type_info.get(), dst->get(i));
            }
        }

        {
            // read range data of page
            PageDecoderType page_decoder(encoded_data);
            Status status = page_decoder.init();
            ASSERT_TRUE(status.ok());
            ASSERT_EQ(0, page_decoder.current_index());

            auto dst = ChunkHelper::column_from_field_type(Type, false);
            SparseRange<> read_range;
            read_range.add(Range<>(0, count / 3));
            read_range.add(Range<>(count / 2, (count * 2 / 3)));
            read_range.add(Range<>((count * 3 / 4), count));
            size_t read_num = read_range.span_size();

            dst->reserve(read_range.span_size());
            status = page_decoder.next_batch(read_range, dst.get());
            ASSERT_TRUE(status.ok());
            ASSERT_EQ(read_num, dst->size());

            TypeInfoPtr type_info = get_type_info(Type);
            size_t offset = 0;
            SparseRangeIterator<> read_iter = read_range.new_iterator();
            while (read_iter.has_more()) {
                Range<> r = read_iter.next(read_num);
                for (int i = 0; i < r.span_size(); ++i) {
                    ASSERT_EQ(0, type_info->cmp(src->get(r.begin() + i), dst->get(i + offset)))
                            << " row " << i << ": " << datum_to_string(type_info.get(), src->get(r.begin() + i))
                            << " vs " << datum_to_string(type_info.get(), dst->get(i + offset));
                }
                offset += r.span_size();
            }
        }
    }

    // The values inserted should be sorted.
    template <LogicalType Type, class PageBuilderType, class PageDecoderType>
    void test_seek_at_or_after_value_template(typename TypeTraits<Type>::CppType* src, size_t size,
                                              typename TypeTraits<Type>::CppType* small_than_smallest,
                                              typename TypeTraits<Type>::CppType* bigger_than_biggest) {
        typedef typename TypeTraits<Type>::CppType CppType;
        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        PageBuilderType page_builder(options);

        size = page_builder.add(reinterpret_cast<const uint8_t*>(src), size);
        OwnedSlice s = page_builder.finish()->build();

        Slice encoded_data = s.slice();
        starrocks::PageFooterPB footer;
        footer.set_type(starrocks::DATA_PAGE);
        starrocks::DataPageFooterPB* data_page_footer = footer.mutable_data_page_footer();
        data_page_footer->set_nullmap_size(0);
        std::unique_ptr<char[]> page = nullptr;
        Status st = StoragePageDecoder::decode_page(&footer, 0, starrocks::BIT_SHUFFLE, &page, &encoded_data);
        ASSERT_TRUE(st.ok());

        PageDecoderType page_decoder(encoded_data);
        Status status = page_decoder.init();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(0, page_decoder.current_index());

        size_t index = random() % size;
        CppType seek_value = src[index];
        bool exact_match;
        status = page_decoder.seek_at_or_after_value(&seek_value, &exact_match);
        EXPECT_EQ(index, page_decoder.current_index());
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(exact_match);

        CppType last_value = src[size - 1];
        status = page_decoder.seek_at_or_after_value(&last_value, &exact_match);
        EXPECT_EQ(size - 1, page_decoder.current_index());
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(exact_match);

        CppType first_value = src[0];
        status = page_decoder.seek_at_or_after_value(&first_value, &exact_match);
        EXPECT_EQ(0, page_decoder.current_index());
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(exact_match);

        status = page_decoder.seek_at_or_after_value(small_than_smallest, &exact_match);
        EXPECT_EQ(0, page_decoder.current_index());
        ASSERT_TRUE(status.ok());
        ASSERT_FALSE(exact_match);

        status = page_decoder.seek_at_or_after_value(bigger_than_biggest, &exact_match);
        EXPECT_EQ(status.code(), TStatusCode::NOT_FOUND);
    }
};

// Test for bitshuffle block, for INT32, INT64, FLOAT, DOUBLE
// NOLINTNEXTLINE
TEST_F(BitShufflePageTest, TestBitShuffleInt32BlockEncoderRandom) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = random();
    }

    test_encode_decode_page_template<TYPE_INT, BitshufflePageBuilder<TYPE_INT>, BitShufflePageDecoder<TYPE_INT>>(
            ints.get(), size);
}

// NOLINTNEXTLINE
TEST_F(BitShufflePageTest, TestBitShuffleInt64BlockEncoderRandom) {
    const uint32_t size = 10000;

    std::unique_ptr<int64_t[]> ints(new int64_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = random();
    }

    test_encode_decode_page_template<TYPE_BIGINT, BitshufflePageBuilder<TYPE_BIGINT>,
                                     BitShufflePageDecoder<TYPE_BIGINT>>(ints.get(), size);
}

// NOLINTNEXTLINE
TEST_F(BitShufflePageTest, TestBitShuffleFloatBlockEncoderRandom) {
    const uint32_t size = 10000;

    std::unique_ptr<float[]> floats(new float[size]);
    for (int i = 0; i < size; i++) {
        floats.get()[i] = random() + static_cast<float>(random()) / std::numeric_limits<int>::max();
    }

    test_encode_decode_page_template<TYPE_FLOAT, BitshufflePageBuilder<TYPE_FLOAT>, BitShufflePageDecoder<TYPE_FLOAT>>(
            floats.get(), size);
}

// NOLINTNEXTLINE
TEST_F(BitShufflePageTest, TestBitShuffleDoubleBlockEncoderRandom) {
    const uint32_t size = 10000;

    std::unique_ptr<double[]> doubles(new double[size]);
    for (int i = 0; i < size; i++) {
        doubles.get()[i] = random() + static_cast<double>(random()) / std::numeric_limits<int>::max();
    }

    test_encode_decode_page_template<TYPE_DOUBLE, BitshufflePageBuilder<TYPE_DOUBLE>,
                                     BitShufflePageDecoder<TYPE_DOUBLE>>(doubles.get(), size);
}

// NOLINTNEXTLINE
TEST_F(BitShufflePageTest, TestBitShuffleDoubleBlockEncoderEqual) {
    const uint32_t size = 10000;

    std::unique_ptr<double[]> doubles(new double[size]);
    for (int i = 0; i < size; i++) {
        doubles.get()[i] = 19880217.19890323;
    }

    test_encode_decode_page_template<TYPE_DOUBLE, BitshufflePageBuilder<TYPE_DOUBLE>,
                                     BitShufflePageDecoder<TYPE_DOUBLE>>(doubles.get(), size);
}

// NOLINTNEXTLINE
TEST_F(BitShufflePageTest, TestBitShuffleDoubleBlockEncoderSequence) {
    const uint32_t size = 10000;

    double base = 19880217.19890323;
    double delta = 13.14;
    std::unique_ptr<double[]> doubles(new double[size]);
    for (int i = 0; i < size; i++) {
        base = base + delta;
        doubles.get()[i] = base;
    }

    test_encode_decode_page_template<TYPE_DOUBLE, BitshufflePageBuilder<TYPE_DOUBLE>,
                                     BitShufflePageDecoder<TYPE_DOUBLE>>(doubles.get(), size);
}

// NOLINTNEXTLINE
TEST_F(BitShufflePageTest, TestBitShuffleInt32BlockEncoderEqual) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 12345;
    }

    test_encode_decode_page_template<TYPE_INT, BitshufflePageBuilder<TYPE_INT>, BitShufflePageDecoder<TYPE_INT>>(
            ints.get(), size);
}

// NOLINTNEXTLINE
TEST_F(BitShufflePageTest, TestBitShuffleInt32BlockEncoderMaxNumberEqual) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 1234567890;
    }

    test_encode_decode_page_template<TYPE_INT, BitshufflePageBuilder<TYPE_INT>, BitShufflePageDecoder<TYPE_INT>>(
            ints.get(), size);
}

// NOLINTNEXTLINE
TEST_F(BitShufflePageTest, TestBitShuffleInt32BlockEncoderSequence) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    int32_t number = 0;
    for (int i = 0; i < size; i++) {
        ints.get()[i] = ++number;
    }

    test_encode_decode_page_template<TYPE_INT, BitshufflePageBuilder<TYPE_INT>, BitShufflePageDecoder<TYPE_INT>>(
            ints.get(), size);
}

// NOLINTNEXTLINE
TEST_F(BitShufflePageTest, TestBitShuffleInt32BlockEncoderMaxNumberSequence) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    int32_t number = 0;
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 1234567890 + number;
        ++number;
    }

    test_encode_decode_page_template<TYPE_INT, BitshufflePageBuilder<TYPE_INT>, BitShufflePageDecoder<TYPE_INT>>(
            ints.get(), size);
}

// NOLINTNEXTLINE
TEST_F(BitShufflePageTest, TestBitShuffleFloatBlockEncoderSeekValue) {
    const uint32_t size = 1000;
    std::unique_ptr<float[]> floats(new float[size]);
    for (int i = 0; i < size; i++) {
        floats.get()[i] = i + 100 + static_cast<float>(random()) / std::numeric_limits<int>::max();
    }

    float small_than_smallest = 99.9;
    float bigger_than_biggest = 1111.1;
    test_seek_at_or_after_value_template<TYPE_FLOAT, BitshufflePageBuilder<TYPE_FLOAT>,
                                         BitShufflePageDecoder<TYPE_FLOAT>>(floats.get(), size, &small_than_smallest,
                                                                            &bigger_than_biggest);
}

// NOLINTNEXTLINE
TEST_F(BitShufflePageTest, TestBitShuffleDoubleBlockEncoderSeekValue) {
    const uint32_t size = 1000;
    std::unique_ptr<double[]> doubles(new double[size]);
    for (int i = 0; i < size; i++) {
        doubles.get()[i] = i + 100 + static_cast<double>(random()) / std::numeric_limits<int>::max();
    }

    double small_than_smallest = 99.9;
    double bigger_than_biggest = 1111.1;
    test_seek_at_or_after_value_template<TYPE_DOUBLE, BitshufflePageBuilder<TYPE_DOUBLE>,
                                         BitShufflePageDecoder<TYPE_DOUBLE>>(doubles.get(), size, &small_than_smallest,
                                                                             &bigger_than_biggest);
}

// NOLINTNEXTLINE
TEST_F(BitShufflePageTest, TestBitShuffleDecimal12BlockEncoderSeekValue) {
    const uint32_t size = 1000;
    std::unique_ptr<decimal12_t[]> decimals(new decimal12_t[size]);
    for (int i = 0; i < size; i++) {
        decimals.get()[i] = decimal12_t(i + 100, random());
    }

    decimal12_t small_than_smallest = decimal12_t(99, 9);
    decimal12_t bigger_than_biggest = decimal12_t(1111, 1);
    test_seek_at_or_after_value_template<TYPE_DECIMAL, BitshufflePageBuilder<TYPE_DECIMAL>,
                                         BitShufflePageDecoder<TYPE_DECIMAL>>(
            decimals.get(), size, &small_than_smallest, &bigger_than_biggest);
}

// NOLINTNEXTLINE
TEST_F(BitShufflePageTest, TestReserveHead) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = random();
    }

    test_encode_decode_page_template<TYPE_INT, BitshufflePageBuilder<TYPE_INT>, BitShufflePageDecoder<TYPE_INT>, 4>(
            ints.get(), size);
}

TEST_F(BitShufflePageTest, TestDecodeVectorized) {
    test_encode_decode_page_vectorized<TYPE_TINYINT, BitshufflePageBuilder<TYPE_TINYINT>,
                                       BitShufflePageDecoder<TYPE_TINYINT>>();
    test_encode_decode_page_vectorized<TYPE_SMALLINT, BitshufflePageBuilder<TYPE_SMALLINT>,
                                       BitShufflePageDecoder<TYPE_SMALLINT>>();
    test_encode_decode_page_vectorized<TYPE_INT, BitshufflePageBuilder<TYPE_INT>, BitShufflePageDecoder<TYPE_INT>>();
    test_encode_decode_page_vectorized<TYPE_BIGINT, BitshufflePageBuilder<TYPE_BIGINT>,
                                       BitShufflePageDecoder<TYPE_BIGINT>>();
}

} // namespace starrocks
