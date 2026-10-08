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

#include "base/logging.h"
#include "base/types/decimal12.h"
#include "base/types/uint24.h"
#include "column/chunk_factory.h"
#include "column/column_helper.h"
#include "column/datum_convert.h"
#include "column/raw_data_visitor.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/encoding_info.h"
#include "storage/rowset/options.h"
#include "storage/rowset/page_decoder.h"
#include "storage/rowset/parsed_page.h"
#include "storage/rowset/storage_page_decoder.h"
#include "types/decimalv2_value.h"

using starrocks::PageBuilderOptions;
using starrocks::DataDecoder;
using starrocks::StoragePageDecoder;

namespace starrocks {

class BitShufflePageTest : public testing::Test {
public:
    ~BitShufflePageTest() override = default;

    template <LogicalType type, class PageDecoderType>
    void copy_one(PageDecoderType* decoder, StorageCppType<type>* ret) {
        auto column = ChunkFactory::column_from_field_type(type, true);
        size_t n = 1;
        ASSERT_TRUE(decoder->next_batch(&n, column.get()).ok());
        ASSERT_EQ(1, n);
        *ret = GetStorageContainer<type>::get_data(column, 0);
    }

    template <LogicalType Type, class PageBuilderType, class PageDecoderType, int ReserveHead = 0>
    void test_encode_decode_page_template(StorageCppType<Type>* src, size_t size) {
        using CppType = StorageCppType<Type>;
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
        std::unique_ptr<std::vector<uint8_t>> page = nullptr;

        Status st = StoragePageDecoder::decode_page(&footer, 0, starrocks::BIT_SHUFFLE, &page, &encoded_data);
        ASSERT_TRUE(st.ok());

        PageDecoderType page_decoder(encoded_data);
        Status status = page_decoder.init();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(0, page_decoder.current_index());
        for (uint i = 0; i < size; i++) {
            CppType out;
            page_decoder.at_index(i, &out);
            if (src[i] != out) {
                FAIL() << "Fail at index " << i << " inserted=" << src[i] << " got=" << out;
            }
        }
        auto column = ChunkFactory::column_from_field_type(Type, false);

        status = page_decoder.next_batch(&size, column.get());
        ASSERT_TRUE(status.ok());

        const auto values = GetStorageContainer<Type>::get_data(column);
        for (uint i = 0; i < size; i++) {
            if (src[i] != values[i]) {
                FAIL() << "Fail at index " << i << " inserted=" << src[i] << " got=" << values[i];
            }
        }

        // Test Seek within block by ordinal
        for (int i = 0; i < 100; i++) {
            uint32_t seek_off = random() % size;
            ASSERT_TRUE(page_decoder.seek_to_position_in_page(seek_off).ok());
            EXPECT_EQ((int32_t)(seek_off), page_decoder.current_index());
            CppType ret;
            copy_one<Type, PageDecoderType>(&page_decoder, &ret);
            EXPECT_EQ(values[seek_off], ret);
        }
    }

    template <LogicalType Type, class PageBuilderType, class PageDecoderType>
    void test_encode_decode_page_vectorized() {
        using CppType = StorageCppType<Type>;
        auto src = ChunkFactory::column_from_field_type(Type, false);
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
        RawDataVisitor visitor;
        ASSERT_TRUE(src->accept(&visitor).ok());
        size_t added = page_builder.add(visitor.result(), count);
        ASSERT_EQ(count, added);
        OwnedSlice s = page_builder.finish()->build();

        Slice encoded_data = s.slice();
        std::unique_ptr<std::vector<uint8_t>> page = nullptr;
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
            CppType src_value = 0;
            for (uint i = 0; i < count; i++) {
                CppType out;
                page_decoder.at_index(i, &out);
                if (src_value != out) {
                    FAIL() << "Fail at index " << i << " inserted=" << src_value << " got=" << out;
                }
                src_value++;
            }

            auto dst = ChunkFactory::column_from_field_type(Type, false);
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

            auto dst = ChunkFactory::column_from_field_type(Type, false);
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

            auto dst = ChunkFactory::column_from_field_type(Type, false);
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
    void test_seek_at_or_after_value_template(StorageCppType<Type>* src, size_t size,
                                              StorageCppType<Type>* small_than_smallest,
                                              StorageCppType<Type>* bigger_than_biggest) {
        using CppType = StorageCppType<Type>;
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
        std::unique_ptr<std::vector<uint8_t>> page = nullptr;
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
        floats.get()[i] = random() + random() / static_cast<float>(std::numeric_limits<int>::max());
    }

    test_encode_decode_page_template<TYPE_FLOAT, BitshufflePageBuilder<TYPE_FLOAT>, BitShufflePageDecoder<TYPE_FLOAT>>(
            floats.get(), size);
}

// NOLINTNEXTLINE
TEST_F(BitShufflePageTest, TestBitShuffleDoubleBlockEncoderRandom) {
    const uint32_t size = 10000;

    std::unique_ptr<double[]> doubles(new double[size]);
    for (int i = 0; i < size; i++) {
        doubles.get()[i] = random() + random() / static_cast<double>(std::numeric_limits<int>::max());
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
        floats.get()[i] = i + 100 + random() / static_cast<float>(std::numeric_limits<int>::max());
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
        doubles.get()[i] = i + 100 + random() / static_cast<double>(std::numeric_limits<int>::max());
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

// A corrupted or truncated bitshuffle page must be rejected by the page-load
// pre-decoder with a clear error instead of driving out-of-bounds reads or
// writes from sizes taken straight from the page bytes.
// NOLINTNEXTLINE
TEST_F(BitShufflePageTest, TestCorruptedPagePreDecodeRejected) {
    const uint32_t size = 1000;
    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = i;
    }

    PageBuilderOptions options;
    options.data_page_size = 256 * 1024;
    BitshufflePageBuilder<TYPE_INT> page_builder(options);
    ASSERT_EQ(size, page_builder.add(reinterpret_cast<const uint8_t*>(ints.get()), size));
    OwnedSlice s = page_builder.finish()->build();
    std::string good(s.slice().data, s.slice().size);

    auto decode = [](std::string page_bytes, uint32_t footer_size = 0) {
        Slice slice(page_bytes.data(), page_bytes.size());
        std::unique_ptr<std::vector<uint8_t>> page;
        starrocks::PageFooterPB footer;
        footer.set_type(starrocks::DATA_PAGE);
        footer.mutable_data_page_footer()->set_nullmap_size(0);
        return StoragePageDecoder::decode_page(&footer, footer_size, starrocks::BIT_SHUFFLE, &page, &slice);
    };

    // Sanity: the untouched page decodes fine.
    ASSERT_TRUE(decode(good).ok());

    // Page smaller than the 16-byte bitshuffle header.
    ASSERT_FALSE(decode(good.substr(0, BITSHUFFLE_PAGE_HEADER_SIZE - 1)).ok());

    // Compressed size larger than the page (header bytes [4,8)).
    {
        std::string bad = good;
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 4, good.size() + 100);
        ASSERT_FALSE(decode(bad).ok());
    }

    // Compressed size smaller than the bitshuffle header itself.
    {
        std::string bad = good;
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 4, BITSHUFFLE_PAGE_HEADER_SIZE - 8);
        ASSERT_FALSE(decode(bad).ok());
    }

    // Padded element count that does not match num_elements (header bytes [8,12)).
    {
        std::string bad = good;
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 8, 12345678);
        ASSERT_FALSE(decode(bad).ok());
    }

    // num_elements = 0xffffffff with a padded count of 0: ALIGN_UP(0xffffffff,
    // 8U) wraps to 0 with the 32-bit mask, so a full-width check is required to
    // reject this instead of reconstructing a page that reports ~4.29e9 rows.
    {
        std::string bad = good;
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 0, 0xffffffff);
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 8, 0);
        ASSERT_FALSE(decode(bad).ok());
    }

    // A page that declares elements but carries an empty compressed body
    // (compressed_size == BITSHUFFLE_PAGE_HEADER_SIZE) must be rejected before
    // the decompressor runs: decompress_lz4() takes no input length, so it
    // would read the trailer as block framing. Only a genuinely empty page
    // (padded count 0) is header-only.
    {
        std::string bad = good;
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 0, 1);
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 4, BITSHUFFLE_PAGE_HEADER_SIZE);
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 8, 8);
        ASSERT_FALSE(decode(bad).ok());
    }

    // An element size outside the supported set would drive an absurd
    // allocation (e.g. 8 * 0xffffffff bytes) before any decode.
    {
        std::string bad = good;
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 12, 0xffffffff);
        ASSERT_FALSE(decode(bad).ok());
    }

    // Consistent but implausibly large element counts (decoded size far past
    // what LZ4 could ever expand to) must be rejected before allocating.
    {
        std::string bad = good;
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 0, 100000000);
        encode_fixed32_le(reinterpret_cast<uint8_t*>(bad.data()) + 8, 100000000);
        ASSERT_FALSE(decode(bad).ok());
    }

    // A trailer (nullmap/footer) claimed to be larger than the bytes actually
    // present after the compressed body must be rejected, not copied.
    ASSERT_FALSE(decode(good, /*footer_size=*/8).ok());

    // Surplus bytes between the compressed body and the trailer must be
    // rejected too: the input has to be consumed exactly, otherwise the
    // cached page (whose footer is parsed from its end) is polluted.
    ASSERT_FALSE(decode(good + std::string(3, 'x')).ok());
}

TEST_F(BitShufflePageTest, TestReadByRowids) {
    const uint32_t size = 100;
    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = i;
    }

    PageBuilderOptions options;
    options.data_page_size = 256 * 1024;
    BitshufflePageBuilder<TYPE_INT> page_builder(options);

    size_t added = page_builder.add(reinterpret_cast<const uint8_t*>(ints.get()), size);
    ASSERT_EQ(size, added);
    OwnedSlice s = page_builder.finish()->build();

    Slice encoded_data = s.slice();
    starrocks::PageFooterPB footer;
    footer.set_type(starrocks::DATA_PAGE);
    starrocks::DataPageFooterPB* data_page_footer = footer.mutable_data_page_footer();
    data_page_footer->set_nullmap_size(0);
    std::unique_ptr<std::vector<uint8_t>> page = nullptr;
    Status st = StoragePageDecoder::decode_page(&footer, 0, starrocks::BIT_SHUFFLE, &page, &encoded_data);
    ASSERT_TRUE(st.ok());

    BitShufflePageDecoder<TYPE_INT> page_decoder(encoded_data);
    st = page_decoder.init();
    ASSERT_TRUE(st.ok());

    auto column = ChunkFactory::column_from_field_type(TYPE_INT, false);
    rowid_t rowids[] = {0, 50, 99};
    size_t num_read = 3;
    st = page_decoder.read_by_rowids(0, rowids, &num_read, column.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(3, num_read);
    ASSERT_EQ(3, column->size());

    ASSERT_EQ(0, column->get(0).get_int32());
    ASSERT_EQ(50, column->get(1).get_int32());
    ASSERT_EQ(99, column->get(2).get_int32());
}

namespace {

// An encoded bitshuffle page, its decoder and the source values it was built from.
template <LogicalType Type>
struct ReadByRowidsPage {
    using CppType = StorageCppType<Type>;

    std::vector<CppType> src;
    OwnedSlice owned;
    Slice encoded;
    std::unique_ptr<std::vector<uint8_t>> decoded_page;
    std::unique_ptr<BitShufflePageDecoder<Type>> decoder;

    void build(size_t rows) {
        src.resize(rows);
        for (size_t i = 0; i < rows; i++) {
            if constexpr (Type == TYPE_INT256 || Type == TYPE_DECIMAL256) {
                int128_t hi = static_cast<int128_t>(i) * 0x123456789ABCDEF0LL ^ 0x5555555555555555LL;
                uint128_t lo = static_cast<uint128_t>(i) * 0x0FEDCBA987654321ULL ^ 0xAAAAAAAAAAAAAAAAULL;
                src[i] = int256_t(hi, lo);
            } else if constexpr (Type == TYPE_DECIMAL) {
                src[i] = decimal12_t(i, 0);
            } else if constexpr (Type == TYPE_DECIMALV2) {
                src[i] = DecimalV2Value(i, 0);
            } else if constexpr (Type == TYPE_DATE_V1) {
                src[i] = static_cast<CppType>(static_cast<uint32_t>(i));
            } else if constexpr (Type == TYPE_DATETIME_V1) {
                src[i] = static_cast<CppType>(static_cast<int64_t>(i));
            } else {
                src[i] = static_cast<CppType>(static_cast<int64_t>(i) * 2654435761LL % 100003 - 50000);
            }
        }
        PageBuilderOptions options;
        options.data_page_size = 1024 * 1024;
        BitshufflePageBuilder<Type> builder(options);
        ASSERT_EQ(rows, builder.add(reinterpret_cast<const uint8_t*>(src.data()), rows));
        owned = builder.finish()->build();
        encoded = owned.slice();

        PageFooterPB footer;
        footer.set_type(DATA_PAGE);
        footer.mutable_data_page_footer()->set_nullmap_size(0);
        ASSERT_TRUE(StoragePageDecoder::decode_page(&footer, 0, BIT_SHUFFLE, &decoded_page, &encoded).ok());
        decoder = std::make_unique<BitShufflePageDecoder<Type>>(encoded);
        ASSERT_TRUE(decoder->init().ok());
    }
};

template <LogicalType Type>
void check_non_nullable_stride(size_t rows, size_t stride) {
    using CppType = StorageCppType<Type>;
    ReadByRowidsPage<Type> page;
    page.build(rows);
    ASSERT_NE(nullptr, page.decoder);

    std::vector<rowid_t> rowids;
    for (size_t i = 0; i < rows; i += stride) {
        rowids.push_back(static_cast<rowid_t>(i));
    }
    auto column = ChunkFactory::column_from_field_type(Type, false);
    size_t count = rowids.size();
    ASSERT_TRUE(page.decoder->read_by_rowids(0, rowids.data(), &count, column.get()).ok());
    ASSERT_EQ(rowids.size(), count);
    ASSERT_EQ(count, column->size());

    const auto values = GetStorageContainer<Type>::get_data(column);
    for (size_t i = 0; i < count; i++) {
        ASSERT_EQ(0, memcmp(&page.src[rowids[i]], &values[i], sizeof(CppType))) << "type=" << Type << " at " << i;
    }
}

template <LogicalType StorageType, LogicalType DecimalType>
void check_delegated_empty_column() {
    using CppType = StorageCppType<StorageType>;
    static_assert(std::is_same_v<CppType, StorageCppType<DecimalType>>);

    constexpr size_t kRows = 1003;
    ReadByRowidsPage<StorageType> page;
    page.build(kRows);
    ASSERT_NE(nullptr, page.decoder);

    std::vector<rowid_t> rowids;
    for (size_t i = 0; i < kRows; i += 7) {
        rowids.push_back(static_cast<rowid_t>(i));
    }
    if (rowids.back() != kRows - 1) {
        rowids.push_back(static_cast<rowid_t>(kRows - 1));
    }

    auto column = ChunkFactory::column_from_field_type(DecimalType, false);
    ASSERT_EQ(0, column->size());

    size_t count = rowids.size();
    ASSERT_TRUE(page.decoder->read_by_rowids(0, rowids.data(), &count, column.get()).ok());
    ASSERT_EQ(rowids.size(), count);
    ASSERT_EQ(count, column->size());

    const auto values = GetStorageContainer<DecimalType>::get_data(column);
    for (size_t i = 0; i < count; i++) {
        ASSERT_EQ(0, memcmp(&page.src[rowids[i]], &values[i], sizeof(CppType)))
                << "storage_type=" << StorageType << " decimal_type=" << DecimalType << " at index=" << i;
        EXPECT_EQ(page.src[rowids[i]], values[i])
                << "storage_type=" << StorageType << " decimal_type=" << DecimalType << " at index=" << i;
    }

    // Verify non-zero first ordinal in page
    {
        constexpr ordinal_t kFirst = 500;
        const rowid_t first_rowids[] = {500, 510, 520, 599};
        auto col = ChunkFactory::column_from_field_type(DecimalType, false);
        size_t n = 4;
        ASSERT_TRUE(page.decoder->read_by_rowids(kFirst, first_rowids, &n, col.get()).ok());
        ASSERT_EQ(4, n);
        ASSERT_EQ(4, col->size());
        const auto v = GetStorageContainer<DecimalType>::get_data(col);
        EXPECT_EQ(page.src[0], v[0]);
        EXPECT_EQ(page.src[10], v[1]);
        EXPECT_EQ(page.src[20], v[2]);
        EXPECT_EQ(page.src[99], v[3]);
    }
}

template <LogicalType StorageType, LogicalType DecimalType>
void check_delegated_truncation_and_reread() {
    using CppType = StorageCppType<StorageType>;
    static_assert(std::is_same_v<CppType, StorageCppType<DecimalType>>);

    constexpr size_t kRows = 200;
    ReadByRowidsPage<StorageType> page;
    page.build(kRows);
    ASSERT_NE(nullptr, page.decoder);

    auto column = ChunkFactory::column_from_field_type(DecimalType, false);

    // 1. Initial read into empty column
    const rowid_t first_rowids[] = {1, 5, 20, 50, 99};
    size_t count = 5;
    ASSERT_TRUE(page.decoder->read_by_rowids(0, first_rowids, &count, column.get()).ok());
    ASSERT_EQ(5, count);
    ASSERT_EQ(5, column->size());
    auto values = GetStorageContainer<DecimalType>::get_data(column);
    for (size_t i = 0; i < 5; i++) {
        EXPECT_EQ(page.src[first_rowids[i]], values[i]);
    }

    // 2. Truncate / resize column down to 2 elements and re-read
    column->resize(2);
    ASSERT_EQ(2, column->size());

    const rowid_t second_rowids[] = {10, 30, 70};
    count = 3;
    ASSERT_TRUE(page.decoder->read_by_rowids(0, second_rowids, &count, column.get()).ok());
    ASSERT_EQ(3, count);
    ASSERT_EQ(5, column->size());
    values = GetStorageContainer<DecimalType>::get_data(column);
    EXPECT_EQ(page.src[first_rowids[0]], values[0]);
    EXPECT_EQ(page.src[first_rowids[1]], values[1]);
    for (size_t i = 0; i < 3; i++) {
        EXPECT_EQ(page.src[second_rowids[i]], values[2 + i]);
    }

    // 3. Clear / truncate to 0 elements and re-read
    column->resize(0);
    ASSERT_EQ(0, column->size());

    const rowid_t third_rowids[] = {0, 42, 100, 199};
    count = 4;
    ASSERT_TRUE(page.decoder->read_by_rowids(0, third_rowids, &count, column.get()).ok());
    ASSERT_EQ(4, count);
    ASSERT_EQ(4, column->size());
    values = GetStorageContainer<DecimalType>::get_data(column);
    for (size_t i = 0; i < 4; i++) {
        EXPECT_EQ(page.src[third_rowids[i]], values[i]);
    }

    // 4. Out-of-bounds rowids triggering decoder-level truncation
    const rowid_t out_of_bounds_rowids[] = {15, 25, static_cast<rowid_t>(kRows), static_cast<rowid_t>(kRows + 10)};
    count = 4;
    ASSERT_TRUE(page.decoder->read_by_rowids(0, out_of_bounds_rowids, &count, column.get()).ok());
    ASSERT_EQ(2, count);
    ASSERT_EQ(6, column->size());
    values = GetStorageContainer<DecimalType>::get_data(column);
    for (size_t i = 0; i < 4; i++) {
        EXPECT_EQ(page.src[third_rowids[i]], values[i]);
    }
    EXPECT_EQ(page.src[15], values[4]);
    EXPECT_EQ(page.src[25], values[5]);
}

template <LogicalType StorageType, LogicalType DecimalType>
void check_delegated_appends_to_populated_column() {
    using CppType = StorageCppType<StorageType>;
    static_assert(std::is_same_v<CppType, StorageCppType<DecimalType>>);

    constexpr size_t kRows = 1000;
    ReadByRowidsPage<StorageType> page;
    page.build(kRows);
    ASSERT_NE(nullptr, page.decoder);

    auto column = ChunkFactory::column_from_field_type(DecimalType, false);
    std::vector<CppType> existing(5);
    for (size_t i = 0; i < 5; i++) {
        if constexpr (StorageType == TYPE_INT256) {
            existing[i] = int256_t(~static_cast<int128_t>(i), static_cast<uint128_t>(i) + 100);
        } else {
            existing[i] = static_cast<CppType>(-(static_cast<int64_t>(i) + 1));
        }
    }
    ASSERT_EQ(5, column->append_numbers(existing.data(), existing.size() * sizeof(CppType)));
    ASSERT_EQ(5, column->size());

    const rowid_t rowids[] = {3, 10, 500, 999};
    size_t count = 4;
    ASSERT_TRUE(page.decoder->read_by_rowids(0, rowids, &count, column.get()).ok());
    ASSERT_EQ(4, count);
    ASSERT_EQ(9, column->size());

    const auto values = GetStorageContainer<DecimalType>::get_data(column);
    for (size_t i = 0; i < 5; i++) {
        EXPECT_EQ(existing[i], values[i])
                << "existing mismatch storage_type=" << StorageType << " decimal_type=" << DecimalType << " at " << i;
    }
    for (size_t i = 0; i < 4; i++) {
        EXPECT_EQ(page.src[rowids[i]], values[5 + i])
                << "appended mismatch storage_type=" << StorageType << " decimal_type=" << DecimalType << " at " << i;
    }

    // Verify destination backed by shared resource
    {
        std::vector<CppType> shared_vec(4);
        for (size_t i = 0; i < 4; i++) {
            if constexpr (StorageType == TYPE_INT256) {
                shared_vec[i] = int256_t(static_cast<int128_t>(i) + 1, static_cast<uint128_t>(i) + 100);
            } else {
                shared_vec[i] = static_cast<CppType>((i + 1) * 11);
            }
        }
        auto shared = std::make_shared<std::vector<CppType>>(shared_vec);
        const std::vector<CppType> shared_before = *shared;
        auto shared_col = ChunkFactory::column_from_field_type(DecimalType, false);
        ContainerResource resource(shared, shared->data(), shared->size() * sizeof(CppType));
        ASSERT_EQ(4, shared_col->append_numbers(resource));

        const rowid_t shared_rowids[] = {0, 1, 2, 998, 999};
        size_t shared_count = 5;
        ASSERT_TRUE(page.decoder->read_by_rowids(0, shared_rowids, &shared_count, shared_col.get()).ok());
        ASSERT_EQ(5, shared_count);
        ASSERT_EQ(9, shared_col->size());

        const auto shared_vals = GetStorageContainer<DecimalType>::get_data(shared_col);
        for (size_t i = 0; i < 4; i++) {
            EXPECT_EQ(shared_before[i], shared_vals[i]);
        }
        for (size_t i = 0; i < 5; i++) {
            EXPECT_EQ(page.src[shared_rowids[i]], shared_vals[4 + i]);
        }
        EXPECT_EQ(shared_before, *shared) << "shared backing storage was modified";
    }
}

} // namespace

TEST_F(BitShufflePageTest, non_nullable_matches_source_all_fast_path_types) {
    constexpr size_t kRows = 16384;
    constexpr size_t kStride = 7;
    check_non_nullable_stride<TYPE_TINYINT>(kRows, kStride);
    check_non_nullable_stride<TYPE_SMALLINT>(kRows, kStride);
    check_non_nullable_stride<TYPE_INT>(kRows, kStride);
    check_non_nullable_stride<TYPE_BIGINT>(kRows, kStride);
    check_non_nullable_stride<TYPE_LARGEINT>(kRows, kStride);
    check_non_nullable_stride<TYPE_FLOAT>(kRows, kStride);
    check_non_nullable_stride<TYPE_DOUBLE>(kRows, kStride);
    check_non_nullable_stride<TYPE_DATE>(kRows, kStride);
    check_non_nullable_stride<TYPE_DATETIME>(kRows, kStride);
    check_non_nullable_stride<TYPE_DECIMAL32>(kRows, kStride);
    check_non_nullable_stride<TYPE_DECIMAL64>(kRows, kStride);
    check_non_nullable_stride<TYPE_DECIMAL128>(kRows, kStride);
    check_non_nullable_stride<TYPE_INT256>(kRows, kStride);
    check_non_nullable_stride<TYPE_DECIMAL256>(kRows, kStride);
}

TEST_F(BitShufflePageTest, non_nullable_matches_source_legacy_types) {
    constexpr size_t kRows = 16384;
    constexpr size_t kStride = 7;
    check_non_nullable_stride<TYPE_DATE_V1>(kRows, kStride);
    check_non_nullable_stride<TYPE_DATETIME_V1>(kRows, kStride);
    check_non_nullable_stride<TYPE_DECIMAL>(kRows, kStride);
    check_non_nullable_stride<TYPE_DECIMALV2>(kRows, kStride);
}

TEST_F(BitShufflePageTest, delegated_decimal_empty_column) {
    check_delegated_empty_column<TYPE_INT, TYPE_DECIMAL32>();
    check_delegated_empty_column<TYPE_BIGINT, TYPE_DECIMAL64>();
    check_delegated_empty_column<TYPE_LARGEINT, TYPE_DECIMAL128>();
    check_delegated_empty_column<TYPE_INT256, TYPE_DECIMAL256>();
}

TEST_F(BitShufflePageTest, delegated_decimal_truncation_and_reread) {
    check_delegated_truncation_and_reread<TYPE_INT, TYPE_DECIMAL32>();
    check_delegated_truncation_and_reread<TYPE_BIGINT, TYPE_DECIMAL64>();
    check_delegated_truncation_and_reread<TYPE_LARGEINT, TYPE_DECIMAL128>();
    check_delegated_truncation_and_reread<TYPE_INT256, TYPE_DECIMAL256>();
}

TEST_F(BitShufflePageTest, delegated_decimal_appends_to_populated_column) {
    check_delegated_appends_to_populated_column<TYPE_INT, TYPE_DECIMAL32>();
    check_delegated_appends_to_populated_column<TYPE_BIGINT, TYPE_DECIMAL64>();
    check_delegated_appends_to_populated_column<TYPE_LARGEINT, TYPE_DECIMAL128>();
    check_delegated_appends_to_populated_column<TYPE_INT256, TYPE_DECIMAL256>();
}

// Review focus 2: output appends and never overwrites rows already in the column.
TEST_F(BitShufflePageTest, appends_to_populated_column) {
    ReadByRowidsPage<TYPE_INT> page;
    page.build(1000);
    auto column = ChunkFactory::column_from_field_type(TYPE_INT, false);
    const int32_t existing[] = {-1, -2, -3, -4, -5};
    ASSERT_EQ(5, column->append_numbers(existing, sizeof(existing)));

    const rowid_t rowids[] = {3, 10, 500, 999};
    size_t count = 4;
    ASSERT_TRUE(page.decoder->read_by_rowids(0, rowids, &count, column.get()).ok());
    ASSERT_EQ(4, count);
    ASSERT_EQ(9, column->size());

    const auto values = GetStorageContainer<TYPE_INT>::get_data(column);
    for (size_t i = 0; i < 5; i++) {
        EXPECT_EQ(existing[i], values[i]);
    }
    for (size_t i = 0; i < 4; i++) {
        EXPECT_EQ(page.src[rowids[i]], values[5 + i]);
    }
}

// Review focus 2: a column whose rows live in shared (zero-copy) storage must be appended to
// without writing through to the shared buffer.
TEST_F(BitShufflePageTest, destination_backed_by_shared_resource) {
    ReadByRowidsPage<TYPE_INT> page;
    page.build(1000);

    auto shared = std::make_shared<std::vector<int32_t>>(std::vector<int32_t>{11, 22, 33, 44});
    const std::vector<int32_t> shared_before = *shared;
    auto column = ChunkFactory::column_from_field_type(TYPE_INT, false);
    ContainerResource resource(shared, shared->data(), shared->size() * sizeof(int32_t));
    ASSERT_EQ(4, column->append_numbers(resource));

    const rowid_t rowids[] = {0, 1, 2, 998, 999};
    size_t count = 5;
    ASSERT_TRUE(page.decoder->read_by_rowids(0, rowids, &count, column.get()).ok());
    ASSERT_EQ(5, count);
    ASSERT_EQ(9, column->size());

    const auto values = GetStorageContainer<TYPE_INT>::get_data(column);
    for (size_t i = 0; i < 4; i++) {
        EXPECT_EQ(shared_before[i], values[i]);
    }
    for (size_t i = 0; i < 5; i++) {
        EXPECT_EQ(page.src[rowids[i]], values[4 + i]);
    }
    EXPECT_EQ(shared_before, *shared) << "shared backing storage was modified";
}

// Review focus 3: stop at the first rowid outside the page; `count` reports rows actually read.
TEST_F(BitShufflePageTest, truncates_at_first_out_of_page_rowid) {
    constexpr uint32_t kRows = 100;
    ReadByRowidsPage<TYPE_INT> page;
    page.build(kRows);
    auto column = ChunkFactory::column_from_field_type(TYPE_INT, false);
    const rowid_t rowids[] = {5, 10, kRows, kRows + 10};
    size_t count = 4;
    ASSERT_TRUE(page.decoder->read_by_rowids(0, rowids, &count, column.get()).ok());
    ASSERT_EQ(2, count);
    ASSERT_EQ(2, column->size());
    const auto values = GetStorageContainer<TYPE_INT>::get_data(column);
    EXPECT_EQ(page.src[5], values[0]);
    EXPECT_EQ(page.src[10], values[1]);
}

// Truncation must also leave rows that were already in the column untouched.
TEST_F(BitShufflePageTest, truncation_preserves_existing_rows) {
    constexpr uint32_t kRows = 100;
    ReadByRowidsPage<TYPE_INT> page;
    page.build(kRows);
    auto column = ChunkFactory::column_from_field_type(TYPE_INT, false);
    const int32_t existing[] = {-7, -8, -9};
    ASSERT_EQ(3, column->append_numbers(existing, sizeof(existing)));
    const rowid_t rowids[] = {1, 2, 3, kRows + 5, 4};
    size_t count = 5;
    ASSERT_TRUE(page.decoder->read_by_rowids(0, rowids, &count, column.get()).ok());
    ASSERT_EQ(3, count);
    ASSERT_EQ(6, column->size());
    const auto values = GetStorageContainer<TYPE_INT>::get_data(column);
    for (size_t i = 0; i < 3; i++) {
        EXPECT_EQ(existing[i], values[i]);
        EXPECT_EQ(page.src[rowids[i]], values[3 + i]);
    }
}

// The page's first ordinal is subtracted from the rowids before bounds checking.
TEST_F(BitShufflePageTest, nonzero_first_ordinal_in_page) {
    constexpr uint32_t kRows = 100;
    constexpr ordinal_t kFirst = 1000;
    ReadByRowidsPage<TYPE_INT> page;
    page.build(kRows);
    auto column = ChunkFactory::column_from_field_type(TYPE_INT, false);
    const rowid_t rowids[] = {1000, 1050, 1099, 1100};
    size_t count = 4;
    ASSERT_TRUE(page.decoder->read_by_rowids(kFirst, rowids, &count, column.get()).ok());
    ASSERT_EQ(3, count);
    ASSERT_EQ(3, column->size());
    const auto values = GetStorageContainer<TYPE_INT>::get_data(column);
    EXPECT_EQ(page.src[0], values[0]);
    EXPECT_EQ(page.src[50], values[1]);
    EXPECT_EQ(page.src[99], values[2]);
}

// Review focus 1: row counts that are not a multiple of 8 (the bitshuffle block width).
TEST_F(BitShufflePageTest, page_not_multiple_of_eight) {
    constexpr uint32_t kRows = 1003;
    ReadByRowidsPage<TYPE_INT> page;
    page.build(kRows);
    std::vector<rowid_t> rowids;
    for (uint32_t i = 0; i < kRows; i += 3) {
        rowids.push_back(i);
    }
    rowids.push_back(kRows - 1); // 1002, the last row, which sits in the padded tail group
    auto column = ChunkFactory::column_from_field_type(TYPE_INT, false);
    size_t count = rowids.size();
    ASSERT_TRUE(page.decoder->read_by_rowids(0, rowids.data(), &count, column.get()).ok());
    ASSERT_EQ(rowids.size(), count);
    ASSERT_EQ(rowids.size(), column->size());
    const auto values = GetStorageContainer<TYPE_INT>::get_data(column);
    for (size_t i = 0; i < count; i++) {
        ASSERT_EQ(page.src[rowids[i]], values[i]) << "at " << i;
    }
    EXPECT_EQ(page.src[kRows - 1], values[count - 1]);
}

TEST_F(BitShufflePageTest, count_zero_is_noop) {
    ReadByRowidsPage<TYPE_INT> page;
    page.build(100);
    auto column = ChunkFactory::column_from_field_type(TYPE_INT, false);
    const int32_t existing[] = {42};
    ASSERT_EQ(1, column->append_numbers(existing, sizeof(existing)));
    const rowid_t rowids[] = {1};
    size_t count = 0;
    ASSERT_TRUE(page.decoder->read_by_rowids(0, rowids, &count, column.get()).ok());
    EXPECT_EQ(0, count);
    ASSERT_EQ(1, column->size());
    EXPECT_EQ(42, GetStorageContainer<TYPE_INT>::get_data(column)[0]);
}

TEST_F(BitShufflePageTest, ParsedPageV2ReadByRowidsNullable) {
    constexpr size_t kRows = 1000;
    std::vector<int32_t> src(kRows);
    std::vector<uint8_t> null_flags(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        src[i] = static_cast<int32_t>(i * 17 + 5);
        null_flags[i] = (i % 3 == 0) ? 1 : 0;
    }

    PageBuilderOptions options;
    options.data_page_size = 256 * 1024;
    BitshufflePageBuilder<TYPE_INT> page_builder(options);
    size_t added = page_builder.add(reinterpret_cast<const uint8_t*>(src.data()), kRows);
    ASSERT_EQ(kRows, added);
    OwnedSlice data_owned = page_builder.finish()->build();

    size_t padded_null_size = ALIGN_UP(kRows, 8u);
    std::vector<uint8_t> padded_nulls(padded_null_size, 0);
    memcpy(padded_nulls.data(), null_flags.data(), kRows);
    std::vector<uint8_t> compressed_nulls(bitshuffle::compress_lz4_bound(padded_null_size, sizeof(uint8_t), 0));
    int64_t r = bitshuffle::compress_lz4(padded_nulls.data(), compressed_nulls.data(), padded_null_size,
                                         sizeof(uint8_t), 0);
    ASSERT_GT(r, 0);
    compressed_nulls.resize(r);

    std::string encoded(data_owned.slice().data, data_owned.slice().size);
    encoded.append(reinterpret_cast<const char*>(compressed_nulls.data()), compressed_nulls.size());

    PageFooterPB page_footer;
    page_footer.set_type(DATA_PAGE);
    DataPageFooterPB* data_page_footer = page_footer.mutable_data_page_footer();
    data_page_footer->set_format_version(2);
    data_page_footer->set_nullmap_size(compressed_nulls.size());
    data_page_footer->set_first_ordinal(0);
    data_page_footer->set_num_values(kRows);

    Slice body(encoded);
    std::unique_ptr<std::vector<uint8_t>> decoded_page;
    ASSERT_TRUE(StoragePageDecoder::decode_page(&page_footer, 0, BIT_SHUFFLE, &decoded_page, &body).ok());

    const EncodingInfo* encoding = nullptr;
    ASSERT_TRUE(EncodingInfo::get(TYPE_INT, BIT_SHUFFLE, &encoding).ok());
    std::unique_ptr<ParsedPage> parsed_page;
    PagePointer page_pointer;
    ASSERT_TRUE(parse_page(&parsed_page, PageHandle(), body, *data_page_footer, encoding, page_pointer, 0).ok());
    ASSERT_TRUE(parsed_page->supports_read_by_rowids());

    auto nullable_col = ChunkFactory::column_from_field_type(TYPE_INT, true);
    std::vector<rowid_t> rowids;
    for (size_t i = 0; i < kRows; i += 7) {
        rowids.push_back(static_cast<rowid_t>(i));
    }
    if (rowids.back() != kRows - 1) {
        rowids.push_back(static_cast<rowid_t>(kRows - 1));
    }
    size_t count = rowids.size();
    ASSERT_TRUE(parsed_page->read_by_rowids(nullable_col.get(), rowids.data(), &count).ok());
    ASSERT_EQ(rowids.size(), count);
    ASSERT_EQ(count, nullable_col->size());

    auto* nc = down_cast<NullableColumn*>(nullable_col.get());
    const auto values = GetStorageContainer<TYPE_INT>::get_data(nc->data_column());
    for (size_t i = 0; i < count; ++i) {
        rowid_t rid = rowids[i];
        bool expected_null = (null_flags[rid] != 0);
        EXPECT_EQ(expected_null, nc->is_null(i)) << "null mismatch at rowid=" << rid << " index=" << i;
        EXPECT_EQ(src[rid], values[i]) << "data mismatch at rowid=" << rid << " index=" << i;
    }

    // Verify appending to populated NullableColumn preserves existing values
    {
        auto col = ChunkFactory::column_from_field_type(TYPE_INT, true);
        auto* nc_col = down_cast<NullableColumn*>(col.get());
        nc_col->append_datum(Datum());
        nc_col->append_datum(Datum(static_cast<int32_t>(42)));
        ASSERT_EQ(2, col->size());
        ASSERT_TRUE(col->is_null(0));
        ASSERT_FALSE(col->is_null(1));

        const rowid_t append_rowids[] = {1, 3, 5, 999};
        size_t append_count = 4;
        ASSERT_TRUE(parsed_page->read_by_rowids(col.get(), append_rowids, &append_count).ok());
        ASSERT_EQ(4, append_count);
        ASSERT_EQ(6, col->size());

        EXPECT_TRUE(col->is_null(0));
        EXPECT_FALSE(col->is_null(1));
        EXPECT_EQ(42, GetStorageContainer<TYPE_INT>::get_data(nc_col->data_column())[1]);

        const auto append_vals = GetStorageContainer<TYPE_INT>::get_data(nc_col->data_column());
        for (size_t i = 0; i < 4; ++i) {
            rowid_t rid = append_rowids[i];
            bool expected_null = (null_flags[rid] != 0);
            EXPECT_EQ(expected_null, col->is_null(2 + i));
            EXPECT_EQ(src[rid], append_vals[2 + i]);
        }
    }
}

} // namespace starrocks
