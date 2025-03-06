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

#include "formats/parquet/encoding.h"

#include <gtest/gtest.h>

#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "formats/parquet/encoding_dict.h"
#include "formats/parquet/encoding_plain.h"

namespace starrocks::parquet {
class ParquetEncodingTest : public testing::Test {
public:
    ParquetEncodingTest() = default;
    ~ParquetEncodingTest() override = default;
};

template <typename T, bool is_dictionary>
struct DecoderChecker {
    static void check(const std::vector<T>& values, const Slice& encoded_data, Decoder* decoder) {
        if (!is_dictionary) {
            // normal read
            {
                std::vector<T> checks(values.size());
                decoder->set_data(encoded_data);
                auto st = decoder->next_batch(values.size(), (uint8_t*)&checks[0]);
                ASSERT_TRUE(st.ok());
                for (int i = 0; i < values.size(); ++i) {
                    ASSERT_EQ(values[i], checks[i]);
                }

                // out-of-bounds access
                st = decoder->next_batch(values.size(), (uint8_t*)&checks[0]);
                ASSERT_FALSE(st.ok());
            }
            // skip + read
            {
                size_t values_to_skip = values.size() / 2;
                size_t remain_values = values.size() - values_to_skip;

                std::vector<T> checks(remain_values);
                decoder->set_data(encoded_data);
                decoder->skip(values_to_skip);
                auto st = decoder->next_batch(remain_values, (uint8_t*)&checks[0]);
                ASSERT_TRUE(st.ok());
                for (int i = 0; i < remain_values; ++i) {
                    ASSERT_EQ(values[values_to_skip + i], checks[i]);
                }

                // out-of-bounds skip
                st = decoder->skip(2);
                ASSERT_FALSE(st.ok());
            }
        }
        {// normal read
         {auto column = starrocks::FixedLengthColumn<T>::create();

        decoder->set_data(encoded_data);
        auto st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
        ASSERT_TRUE(st.ok());

        const T* check = (const T*)column->raw_data();
        for (int i = 0; i < values.size(); ++i) {
            ASSERT_EQ(values[i], *check);
            check++;
        }

        if (!is_dictionary) {
            // out-of-bounds access
            st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
            ASSERT_FALSE(st.ok());
        }
    }
    // skip + read
    {
        size_t values_to_skip = values.size() / 2;
        size_t remain_values = values.size() - values_to_skip;

        auto column = starrocks::FixedLengthColumn<T>::create();
        decoder->set_data(encoded_data);
        auto st = decoder->skip(values_to_skip);
        st = decoder->next_batch(remain_values, ColumnContentType::VALUE, column.get());
        ASSERT_TRUE(st.ok());

        const T* check = (const T*)column->raw_data();
        for (int i = 0; i < remain_values; ++i) {
            ASSERT_EQ(values[values_to_skip + i], check[i]);
        }

        if (!is_dictionary) {
            // out-of-bounds access
            st = decoder->skip(2);
            ASSERT_FALSE(st.ok());
        }
    }
} {
    // read
    {
        auto data_column = starrocks::FixedLengthColumn<T>::create();
        auto column = NullableColumn::create(std::move(data_column), NullColumn::create());

        decoder->set_data(encoded_data);
        auto st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
        ASSERT_TRUE(st.ok());

        const T* check = (const T*)column->data_column()->raw_data();
        for (int i = 0; i < values.size(); ++i) {
            ASSERT_EQ(values[i], *check);
            check++;
        }

        if (!is_dictionary) {
            // out-of-bounds access
            st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
            ASSERT_FALSE(st.ok());
        }
    }
    // read+skip
    {
        size_t values_to_skip = values.size() / 2;
        size_t remain_values = values.size() - values_to_skip;

        auto data_column = starrocks::FixedLengthColumn<T>::create();
        auto column = NullableColumn::create(std::move(data_column), NullColumn::create());

        decoder->set_data(encoded_data);
        auto st = decoder->skip(values_to_skip);
        st = decoder->next_batch(remain_values, ColumnContentType::VALUE, column.get());
        ASSERT_TRUE(st.ok());

        const T* check = (const T*)column->data_column()->raw_data();
        for (int i = 0; i < remain_values; ++i) {
            ASSERT_EQ(values[values_to_skip + i], check[i]);
        }

        if (!is_dictionary) {
            // out-of-bounds access
            st = decoder->skip(2);
            ASSERT_FALSE(st.ok());
        }
    }
}
} // namespace starrocks::parquet
}
;

template <bool is_dictionary>
struct DecoderChecker<Slice, is_dictionary> {
    static void check(const std::vector<Slice>& values, const Slice& encoded_data, Decoder* decoder) {
        if (!is_dictionary) {
            // read
            {
                std::vector<Slice> checks(values.size());
                decoder->set_data(encoded_data);
                auto st = decoder->next_batch(values.size(), (uint8_t*)&checks[0]);
                ASSERT_TRUE(st.ok());
                for (int i = 0; i < values.size(); ++i) {
                    ASSERT_EQ(values[i], checks[i]);
                }

                // out-of-bounds access
                st = decoder->next_batch(values.size(), (uint8_t*)&checks[0]);
                ASSERT_FALSE(st.ok());
            }
            // skip + read
            {
                size_t values_to_skip = values.size() / 2;
                size_t remain_values = values.size() - values_to_skip;

                std::vector<Slice> checks(remain_values);
                decoder->set_data(encoded_data);
                auto st = decoder->skip(values_to_skip);
                st = decoder->next_batch(remain_values, (uint8_t*)&checks[0]);
                ASSERT_TRUE(st.ok());
                for (int i = 0; i < remain_values; ++i) {
                    ASSERT_EQ(values[values_to_skip + i], checks[i]);
                }

                // out-of-bounds access
                st = decoder->skip(2);
                ASSERT_FALSE(st.ok());
            }
        }
        {// read
         {auto column = starrocks::BinaryColumn::create();

        decoder->set_data(encoded_data);
        auto st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
        ASSERT_TRUE(st.ok());

        const auto* check = (const Slice*)column->raw_data();
        for (auto value : values) {
            ASSERT_EQ(value, *check);
            check++;
        }

        if (!is_dictionary) {
            // out-of-bounds access
            st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
            ASSERT_FALSE(st.ok());
        }
    }
    // skip+read
    {
        size_t values_to_skip = values.size() / 2;
        size_t remain_values = values.size() - values_to_skip;

        auto column = starrocks::BinaryColumn::create();

        decoder->set_data(encoded_data);
        auto st = decoder->skip(values_to_skip);
        st = decoder->next_batch(remain_values, ColumnContentType::VALUE, column.get());
        ASSERT_TRUE(st.ok());

        const auto* check = (const Slice*)column->raw_data();
        for (size_t i = 0; i < remain_values; i++) {
            EXPECT_EQ(values[values_to_skip + i], check[i]);
        }

        if (!is_dictionary) {
            // out-of-bounds access
            st = decoder->skip(2);
            ASSERT_FALSE(st.ok());
        }
    }
} {
    // read
    {
        auto data_column = starrocks::BinaryColumn::create();
        auto column = NullableColumn::create(std::move(data_column), NullColumn::create());

        decoder->set_data(encoded_data);
        auto st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
        ASSERT_TRUE(st.ok());

        const auto* check = (const Slice*)column->data_column()->raw_data();
        for (auto value : values) {
            ASSERT_EQ(value, *check);
            check++;
        }

        if (!is_dictionary) {
            // out-of-bounds access
            st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
            ASSERT_FALSE(st.ok());
        }
    }
    // skip + read
    {
        size_t values_to_skip = values.size() / 2;
        size_t remain_values = values.size() - values_to_skip;

        auto data_column = starrocks::BinaryColumn::create();
        auto column = NullableColumn::create(std::move(data_column), NullColumn::create());

        decoder->set_data(encoded_data);
        auto st = decoder->skip(values_to_skip);
        st = decoder->next_batch(remain_values, ColumnContentType::VALUE, column.get());
        ASSERT_TRUE(st.ok());

        const auto* check = (const Slice*)column->data_column()->raw_data();
        for (size_t i = 0; i < remain_values; i++) {
            EXPECT_EQ(values[values_to_skip + i], check[i]);
        }

        if (!is_dictionary) {
            // out-of-bounds access
            st = decoder->skip(2);
            ASSERT_FALSE(st.ok());
        }
    }
}
}
}
;

TEST_F(ParquetEncodingTest, FindNoneExistedEncoding) {
    const EncodingInfo* enc_info = nullptr;
    auto st = EncodingInfo::get(tparquet::Type::BOOLEAN, tparquet::Encoding::RLE_DICTIONARY, &enc_info);
    EXPECT_FALSE(st.ok());
}

TEST_F(ParquetEncodingTest, Int32) {
    std::vector<int32_t> values;
    for (int i = 0; i < 20; i++) {
        values.push_back(i);
    }

    const EncodingInfo* plain_encoding = nullptr;
    EncodingInfo::get(tparquet::Type::INT32, tparquet::Encoding::PLAIN, &plain_encoding);
    ASSERT_TRUE(plain_encoding != nullptr);
    // plain
    {
        std::unique_ptr<Decoder> decoder;
        auto st = plain_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok());

        std::unique_ptr<Encoder> encoder;
        st = plain_encoding->create_encoder(&encoder);

        st = encoder->append(reinterpret_cast<uint8_t*>(&values[0]), 20);
        ASSERT_TRUE(st.ok());

        DecoderChecker<int32_t, false>::check(values, encoder->build(), decoder.get());
    }

    const EncodingInfo* dict_encoding = nullptr;
    EncodingInfo::get(tparquet::Type::INT32, tparquet::Encoding::RLE_DICTIONARY, &dict_encoding);
    ASSERT_TRUE(dict_encoding != nullptr);
    // dict
    {
        std::unique_ptr<Decoder> decoder;
        auto st = dict_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok());

        std::unique_ptr<Encoder> encoder;
        st = dict_encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok());

        st = encoder->append(reinterpret_cast<uint8_t*>(&values[0]), 20);
        ASSERT_TRUE(st.ok());

        // construct dictionary encoder
        std::unique_ptr<Encoder> dict_encoder;
        st = plain_encoding->create_encoder(&dict_encoder);
        ASSERT_TRUE(st.ok());

        size_t num_dicts = 0;
        st = encoder->encode_dict(dict_encoder.get(), &num_dicts);
        ASSERT_TRUE(st.ok());

        // construct dictionary decoder
        std::unique_ptr<Decoder> dict_decoder;
        st = plain_encoding->create_decoder(&dict_decoder);
        ASSERT_TRUE(st.ok());

        dict_decoder->set_data(dict_encoder->build());

        st = decoder->set_dict(config::vector_chunk_size, num_dicts, dict_decoder.get());
        ASSERT_TRUE(st.ok());

        DecoderChecker<int32_t, true>::check(values, encoder->build(), decoder.get());
    }
}

TEST_F(ParquetEncodingTest, String) {
    std::vector<std::string> values;
    for (int i = 0; i < 20; i++) {
        values.push_back(std::to_string(i));
    }

    std::vector<Slice> slices;
    for (int i = 0; i < 20; ++i) {
        slices.emplace_back(values[i]);
    }

    const EncodingInfo* plain_encoding = nullptr;
    EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::PLAIN, &plain_encoding);
    ASSERT_TRUE(plain_encoding != nullptr);
    // plain
    {
        std::unique_ptr<Decoder> decoder;
        auto st = plain_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok());

        std::unique_ptr<Encoder> encoder;
        st = plain_encoding->create_encoder(&encoder);

        st = encoder->append(reinterpret_cast<uint8_t*>(&slices[0]), 20);
        ASSERT_TRUE(st.ok());

        DecoderChecker<Slice, false>::check(slices, encoder->build(), decoder.get());
    }
    const EncodingInfo* dict_encoding = nullptr;
    EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::RLE_DICTIONARY, &dict_encoding);
    ASSERT_TRUE(dict_encoding != nullptr);
    // dict
    {
        std::unique_ptr<Decoder> decoder;
        auto st = dict_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok());

        std::unique_ptr<Encoder> encoder;
        st = dict_encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok());

        st = encoder->append((uint8_t*)&slices[0], 20);
        ASSERT_TRUE(st.ok());

        // construct dictionary encoder
        std::unique_ptr<Encoder> dict_encoder;
        st = plain_encoding->create_encoder(&dict_encoder);
        ASSERT_TRUE(st.ok());

        size_t num_dicts = 0;
        st = encoder->encode_dict(dict_encoder.get(), &num_dicts);
        ASSERT_TRUE(st.ok());

        // construct dictionary decoder
        std::unique_ptr<Decoder> dict_decoder;
        st = plain_encoding->create_decoder(&dict_decoder);
        ASSERT_TRUE(st.ok());

        dict_decoder->set_data(dict_encoder->build());

        st = decoder->set_dict(config::vector_chunk_size, num_dicts, dict_decoder.get());
        ASSERT_TRUE(st.ok());

        DecoderChecker<Slice, true>::check(slices, encoder->build(), decoder.get());
    }
}

TEST_F(ParquetEncodingTest, FixedString) {
    std::vector<std::string> values;
    for (int i = 100; i < 200; i++) {
        values.push_back(std::to_string(i));
    }

    std::vector<Slice> slices;
    for (int i = 100; i < 200; ++i) {
        slices.emplace_back(values[i - 100]);
    }

    const EncodingInfo* plain_encoding = nullptr;
    EncodingInfo::get(tparquet::Type::FIXED_LEN_BYTE_ARRAY, tparquet::Encoding::PLAIN, &plain_encoding);
    ASSERT_TRUE(plain_encoding != nullptr);
    // plain
    {
        std::unique_ptr<Decoder> decoder;
        auto st = plain_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok());

        std::unique_ptr<Encoder> encoder;
        st = plain_encoding->create_encoder(&encoder);

        st = encoder->append(reinterpret_cast<uint8_t*>(&slices[0]), 100);
        ASSERT_TRUE(st.ok());

        decoder->set_type_length(3);
        DecoderChecker<Slice, false>::check(slices, encoder->build(), decoder.get());
    }
    const EncodingInfo* dict_encoding = nullptr;
    EncodingInfo::get(tparquet::Type::FIXED_LEN_BYTE_ARRAY, tparquet::Encoding::RLE_DICTIONARY, &dict_encoding);
    ASSERT_TRUE(dict_encoding != nullptr);
    // dict
    {
        std::unique_ptr<Decoder> decoder;
        auto st = dict_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok());

        std::unique_ptr<Encoder> encoder;
        st = dict_encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok());

        st = encoder->append((uint8_t*)&slices[0], 100);
        ASSERT_TRUE(st.ok());

        // construct dictionary encoder
        std::unique_ptr<Encoder> dict_encoder;
        st = plain_encoding->create_encoder(&dict_encoder);
        ASSERT_TRUE(st.ok());

        size_t num_dicts = 0;
        st = encoder->encode_dict(dict_encoder.get(), &num_dicts);
        ASSERT_TRUE(st.ok());

        // construct dictionary decoder
        std::unique_ptr<Decoder> dict_decoder;
        st = plain_encoding->create_decoder(&dict_decoder);
        ASSERT_TRUE(st.ok());

        dict_decoder->set_data(dict_encoder->build());
        dict_decoder->set_type_length(3);

        st = decoder->set_dict(config::vector_chunk_size, num_dicts, dict_decoder.get());
        ASSERT_TRUE(st.ok());

        DecoderChecker<Slice, true>::check(slices, encoder->build(), decoder.get());
    }
}

TEST_F(ParquetEncodingTest, Boolean) {
    std::vector<uint8_t> values;
    for (int i = 0; i < 32; i++) {
        values.push_back(i % 3 == 0);
    }

    const EncodingInfo* plain_encoding = nullptr;
    EncodingInfo::get(tparquet::Type::BOOLEAN, tparquet::Encoding::PLAIN, &plain_encoding);
    ASSERT_TRUE(plain_encoding != nullptr);
    // plain
    {
        std::unique_ptr<Decoder> decoder;
        auto st = plain_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok());

        std::unique_ptr<Encoder> encoder;
        st = plain_encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok());

        // decode without buffer
        st = encoder->append(reinterpret_cast<uint8_t*>(&values[0]), 32);
        ASSERT_TRUE(st.ok());
        DecoderChecker<uint8_t, false>::check(values, encoder->build(), decoder.get());

        // decode with buffer
        values.resize(31);
        DecoderChecker<uint8_t, false>::check(values, encoder->build(), decoder.get());
    }
}

} // namespace starrocks::parquet
