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

#include "storage/rowset/dict_page.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>
#include <limits>

#include "column/column.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/binary_plain_page.h"
#include "storage/rowset/page_decoder.h"
#include "storage/rowset/storage_page_decoder.h"
#include "storage/types.h"
#include "util/debug_util.h"

namespace starrocks {

class DictPageTest : public testing::Test {
public:
    template <LogicalType Type>
    void test_encode_decode_page_template(typename TypeTraits<Type>::CppType* data, size_t size) {
        using CppType = typename TypeTraits<Type>::CppType;
        // encode
        PageBuilderOptions options;
        // 64K
        options.data_page_size = 1024 * 1024;
        options.dict_page_size = 1024 * 1024;
        int data_num = size;
        uint8_t** data_handler = new uint8_t*[data_num];
        for (int i = 0; i < data_num; i++) {
            data_handler[i] = (uint8*)&data[i];
        }
        DictPageBuilder<Type> page_builder(options);
        Status status;
        int added_count = page_builder.add(data_handler[0], data_num);
        ASSERT_EQ(data_num, added_count);
        auto s = page_builder.finish()->build();
        ASSERT_EQ(data_num, page_builder.count());
        ASSERT_FALSE(page_builder.is_page_full());

        // check first value and last value
        CppType first_value;
        status = page_builder.get_first_value(&first_value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(data[0], first_value);
        CppType last_value;
        status = page_builder.get_last_value(&last_value);
        ASSERT_EQ(data[data_num - 1], last_value);

        // construct dict page
        OwnedSlice dict_slice = page_builder.get_dictionary_page()->build();
        Slice encoded_dict = dict_slice.slice();
        starrocks::PageFooterPB dict_footer;
        dict_footer.set_type(starrocks::DATA_PAGE);
        starrocks::DataPageFooterPB* dict_page_footer = dict_footer.mutable_data_page_footer();
        dict_page_footer->set_nullmap_size(0);
        std::unique_ptr<char[]> dict_page = nullptr;
        Status st = StoragePageDecoder::decode_page(&dict_footer, 0, starrocks::BIT_SHUFFLE, &dict_page, &encoded_dict);
        ASSERT_TRUE(st.ok());

        auto dict_page_decoder = std::make_unique<BitShufflePageDecoder<Type>>(encoded_dict);
        status = dict_page_decoder->init();
        ASSERT_TRUE(status.ok());
        // because every record is unique
        ASSERT_EQ(data_num, dict_page_decoder->count());

        // decode
        Slice encoded_data = s.slice();
        PageFooterPB footer_data;
        footer_data.set_type(DATA_PAGE);
        starrocks::DataPageFooterPB* data_page_footer = footer_data.mutable_data_page_footer();
        data_page_footer->set_nullmap_size(0);
        std::unique_ptr<char[]> data_page = nullptr;
        st = StoragePageDecoder::decode_page(&footer_data, 0, starrocks::DICT_ENCODING, &data_page, &encoded_data);
        ASSERT_TRUE(st.ok());

        DictPageDecoder<Type> page_decoder(encoded_data);
        page_decoder.set_dict_decoder(dict_page_decoder.get());

        status = page_decoder.init();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(data_num, page_decoder.count());

        // check values
        auto column = ChunkHelper::column_from_field_type(Type, false);
        size_t decode_size = data_num;
        status = page_decoder.next_batch(&decode_size, column.get());
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(data_num, decode_size);
        auto* values = reinterpret_cast<const CppType*>(column->raw_data());
        auto* decoded = (CppType*)values;
        for (uint i = 0; i < decode_size; i++) {
            if (data[i] != decoded[i]) {
                if constexpr (std::is_same_v<int128_t, CppType>) {
                    FAIL() << "Fail at index " << i;
                } else {
                    FAIL() << "Fail at index " << i << " inserted=" << data[i] << " got=" << decoded[i];
                }
            }
        }

        column->resize(0);
        ASSERT_TRUE(size > 100);
        status = page_decoder.seek_to_position_in_page(100);
        ASSERT_TRUE(status.ok()) << status.to_string();
        status = page_decoder.next_batch(&decode_size, column.get());
        ASSERT_TRUE(status.ok()) << status.to_string();
        // 2000 - 100
        ASSERT_EQ(size - 100, decode_size);
        values = reinterpret_cast<const CppType*>(column->raw_data());
        decoded = (CppType*)values;
        for (uint i = 0; i < decode_size; i++) {
            if (data[i + 100] != decoded[i]) {
                if constexpr (std::is_same_v<int128_t, CppType>) {
                    FAIL() << "Fail at index " << i;
                } else {
                    FAIL() << "Fail at index " << i << " inserted=" << data[i + 100] << " got=" << decoded[i];
                }
            }
        }

        ASSERT_TRUE(page_decoder.seek_to_position_in_page(0).ok());
        ASSERT_EQ(0, page_decoder.current_index());
        column = ChunkHelper::column_from_field_type(Type, false);
        SparseRange<> read_range;
        read_range.add(Range<>(0, 2));
        read_range.add(Range<>(4, 7));
        status = page_decoder.next_batch(read_range, column.get());
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(5, column->size());
        ASSERT_EQ(data[0], column->get(0).get<CppType>());
        ASSERT_EQ(data[1], column->get(1).get<CppType>());
        ASSERT_EQ(data[4], column->get(2).get<CppType>());
        ASSERT_EQ(data[5], column->get(3).get<CppType>());
        ASSERT_EQ(data[6], column->get(4).get<CppType>());
    }
};

TEST_F(DictPageTest, TestSmallDataSizeWithInt32) {
    const uint32_t size = 10000;
    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < 10000; i++) {
        ints.get()[i] = i;
    }
    test_encode_decode_page_template<TYPE_INT>(ints.get(), size);
}

TEST_F(DictPageTest, TestSmallDataSizeWithInt64_t) {
    const uint32_t size = 10000;
    std::unique_ptr<int64_t[]> ints(new int64_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = i;
    }
    test_encode_decode_page_template<TYPE_BIGINT>(ints.get(), size);
}

TEST_F(DictPageTest, TestSmallDataSizeWithInt128_t) {
    const uint32_t size = 10000;
    std::unique_ptr<int128_t[]> ints(new int128_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = i;
    }
    test_encode_decode_page_template<TYPE_LARGEINT>(ints.get(), size);
}

TEST_F(DictPageTest, TestSmallDataSizeWithFloat) {
    const uint32_t size = 10000;
    std::unique_ptr<float[]> floats(new float[size]);
    int upper = 100000;
    for (int i = 0; i < size; i++) {
        float random_value = static_cast<float>(20000 + i) / (static_cast<float>(RAND_MAX / upper));
        floats.get()[i] = random_value;
    }
    test_encode_decode_page_template<TYPE_FLOAT>(floats.get(), size);
}

TEST_F(DictPageTest, TestSmallDataSizeWithDouble) {
    const uint32_t size = 10000;
    std::unique_ptr<double[]> floats(new double[size]);
    int upper = 100000;
    for (int i = 0; i < size; i++) {
        double random_value = static_cast<double>(20000 + i) / (static_cast<double>(RAND_MAX / upper));
        floats.get()[i] = random_value;
    }
    test_encode_decode_page_template<TYPE_DOUBLE>(floats.get(), size);
}

TEST_F(DictPageTest, TestSmallDataSizeWithInt8_t) {
    int lower = std::numeric_limits<int8_t>::min();
    int upper = std::numeric_limits<int8_t>::max();
    const uint32_t size = upper - lower + 1;
    std::unique_ptr<int8_t[]> ints(new int8_t[size]);
    int cur = 0;
    // Generate all int8_t numbers, we can test if dict index overflow.
    for (int i = lower; i <= upper; i++) {
        ints.get()[cur++] = i;
    }
    test_encode_decode_page_template<TYPE_TINYINT>(ints.get(), size);
}

TEST_F(DictPageTest, TestSmallDataSizeWithInt16_t) {
    int lower = std::numeric_limits<int16_t>::min();
    int upper = std::numeric_limits<int16_t>::max();
    const uint32_t size = upper - lower + 1;
    std::unique_ptr<int16_t[]> ints(new int16_t[size]);
    int cur = 0;
    // Generate all int16_t numbers, we can test if dict index overflow.
    for (int i = lower; i <= upper; i++) {
        ints.get()[cur++] = i;
    }
    test_encode_decode_page_template<TYPE_SMALLINT>(ints.get(), size);
}

TEST_F(DictPageTest, TestSmallDataSizeWithDecimal) {
    const uint32_t size = 100 * 100;
    std::unique_ptr<decimal12_t[]> decimals(new decimal12_t[size]);
    for (int i = 0; i < 100; i++) {
        for (int j = 0; j < 100; j++) {
            decimals.get()[i * 100 + j] = decimal12_t(i, j);
        }
    }
    test_encode_decode_page_template<TYPE_DECIMAL>(decimals.get(), size);
}

TEST_F(DictPageTest, TestSmallDataSizeWithDecimalV2) {
    const uint32_t size = 100 * 100;
    std::unique_ptr<DecimalV2Value[]> decimals(new DecimalV2Value[size]);
    for (int i = 0; i < 100; i++) {
        for (int j = 0; j < 100; j++) {
            decimals.get()[i * 100 + j] = DecimalV2Value(i, j);
        }
    }
    test_encode_decode_page_template<TYPE_DECIMALV2>(decimals.get(), size);
}

TEST_F(DictPageTest, TestLargeDataSize) {
    // 0.1kw 8byte data, about 8MB
    const uint32_t size = 100 * 10000;
    std::unique_ptr<int64_t[]> ints(new int64_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = i;
    }
    using CppType = typename TypeTraits<TYPE_BIGINT>::CppType;
    // encode
    PageBuilderOptions options;
    options.data_page_size = 1024 * 1024;
    options.dict_page_size = 1024 * 1024;
    int data_num = size;
    uint8_t** data_handler = new uint8_t*[data_num];
    for (int i = 0; i < data_num; i++) {
        data_handler[i] = (uint8*)&ints[i];
    }
    DictPageBuilder<TYPE_BIGINT> page_builder(options);
    std::vector<OwnedSlice> results;
    std::vector<size_t> page_start_ids;
    page_start_ids.push_back(0);
    size_t data_page_count = 0;
    for (int i = 0; i < data_num;) {
        int add_num = std::min(100, data_num - i);
        add_num = page_builder.add(data_handler[i], add_num);
        i += add_num;
        if (page_builder.is_page_full()) {
            data_page_count++;
            OwnedSlice s = page_builder.finish()->build();
            results.emplace_back(std::move(s));
            page_builder.reset();
            page_start_ids.push_back(i);
        }
    }
    data_page_count++;
    ASSERT_TRUE(page_builder.count() != 0);
    OwnedSlice s = page_builder.finish()->build();
    results.emplace_back(std::move(s));
    ASSERT_EQ(data_page_count, page_start_ids.size());
    OwnedSlice dict_slice = page_builder.get_dictionary_page()->build();
    Slice encoded_dict = dict_slice.slice();
    starrocks::PageFooterPB dict_footer;
    dict_footer.set_type(starrocks::DATA_PAGE);
    starrocks::DataPageFooterPB* dict_page_footer = dict_footer.mutable_data_page_footer();
    dict_page_footer->set_nullmap_size(0);
    std::unique_ptr<char[]> dict_page = nullptr;
    Status st = StoragePageDecoder::decode_page(&dict_footer, 0, starrocks::BIT_SHUFFLE, &dict_page, &encoded_dict);
    ASSERT_TRUE(st.ok());
    auto dict_page_decoder = std::make_unique<BitShufflePageDecoder<TYPE_BIGINT>>(encoded_dict);
    st = dict_page_decoder->init();
    ASSERT_TRUE(st.ok());
    page_start_ids.push_back(data_num);
    for (int i = 0; i < page_start_ids.size() - 1; i++) {
        // decode
        Slice encoded_data = results[i].slice();
        PageFooterPB footer;
        footer.set_type(DATA_PAGE);
        DataPageFooterPB* data_page_footer = footer.mutable_data_page_footer();
        data_page_footer->set_nullmap_size(0);
        std::unique_ptr<char[]> page = nullptr;

        st = StoragePageDecoder::decode_page(&footer, 0, starrocks::DICT_ENCODING, &page, &encoded_data);
        ASSERT_TRUE(st.ok());
        DictPageDecoder<TYPE_BIGINT> page_decoder(encoded_data);
        st = page_decoder.init();
        ASSERT_TRUE(st.ok());
        page_decoder.set_dict_decoder(dict_page_decoder.get());

        // check values
        auto column = ChunkHelper::column_from_field_type(TYPE_BIGINT, false);
        size_t page_start_id = page_start_ids[i];
        size_t page_size = page_start_ids[i + 1] - page_start_id;
        size_t decode_size = page_size;
        st = page_decoder.next_batch(&decode_size, column.get());
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(page_size, decode_size);
        auto* values = reinterpret_cast<const CppType*>(column->raw_data());
        auto* decoded = (CppType*)values;
        for (int j = page_start_id; j < page_start_ids[i + 1]; j++) {
            if (decoded[j - page_start_id] != j) {
                FAIL() << "Fail at index " << i << " inserted=" << j << " got=" << decoded[j - page_start_id];
            }
        }
    }
}

} // namespace starrocks
