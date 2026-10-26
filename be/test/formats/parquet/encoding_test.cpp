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

#include <limits>
#include <random>

#include "base/bit/rle_encoding.h"
#include "base/coding.h"
#include "base/simd/byte_stream_split.h"
#include "base/string/faststring.h"
#include "base/testutil/assert.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/config_exec_fwd.h"
#include "formats/parquet/level_codec.h"
#include "formats/parquet/types.h"
#include "formats/parquet/utils.h"

namespace starrocks::parquet {
class ParquetEncodingTest : public testing::Test {
public:
    ParquetEncodingTest() = default;
    ~ParquetEncodingTest() override = default;
};

template <typename T, bool is_dictionary>
struct DecoderChecker {
    static void check(const std::vector<T>& values, const Slice& encoded_data, Decoder* decoder) {
        Status st = Status::OK();
        if (!is_dictionary) {
            // normal read
            {
                std::vector<T> checks(values.size());
                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->next_batch(values.size(), (uint8_t*)&checks[0]);
                ASSERT_TRUE(st.ok()) << st.to_string();
                for (int i = 0; i < values.size(); ++i) {
                    ASSERT_EQ(values[i], checks[i]);
                }

                // out-of-bounds access
                st = decoder->next_batch(values.size(), (uint8_t*)&checks[0]);
                ASSERT_FALSE(st.ok()) << st.to_string();
            }
            // skip + read
            {
                size_t values_to_skip = values.size() / 2;
                size_t remain_values = values.size() - values_to_skip;

                std::vector<T> checks(remain_values);
                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->skip(values_to_skip);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->next_batch(remain_values, (uint8_t*)&checks[0]);
                ASSERT_TRUE(st.ok()) << st.to_string();
                for (int i = 0; i < remain_values; ++i) {
                    ASSERT_EQ(values[values_to_skip + i], checks[i]);
                }

                // out-of-bounds skip
                st = decoder->skip(2);
                ASSERT_FALSE(st.ok()) << st.to_string();
            }
        }
        if (true) {
            // normal read
            {
                auto column = starrocks::FixedLengthColumn<T>::create();

                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
                ASSERT_TRUE(st.ok()) << st.to_string();

                const auto& check = column->immutable_data();
                for (int i = 0; i < values.size(); ++i) {
                    ASSERT_EQ(values[i], check[i]);
                }

                if (!is_dictionary) {
                    // out-of-bounds access
                    st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
                    ASSERT_FALSE(st.ok()) << st.to_string();
                }
            }
            // skip + read
            {
                size_t values_to_skip = values.size() / 2;
                size_t remain_values = values.size() - values_to_skip;

                auto column = FixedLengthColumn<T>::create();
                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->skip(values_to_skip);
                st = decoder->next_batch(remain_values, ColumnContentType::VALUE, column.get());
                ASSERT_TRUE(st.ok()) << st.to_string();

                const auto check = column->immutable_data();
                for (int i = 0; i < remain_values; ++i) {
                    ASSERT_EQ(values[values_to_skip + i], check[i]);
                }

                if (!is_dictionary) {
                    // out-of-bounds access
                    st = decoder->skip(2);
                    ASSERT_FALSE(st.ok()) << st.to_string();
                }
            }
        }
        if (true) {
            // read
            {
                auto data_column = starrocks::FixedLengthColumn<T>::create();
                auto column = NullableColumn::create(std::move(data_column), NullColumn::create());

                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
                ASSERT_TRUE(st.ok()) << st.to_string();

                const auto check =
                        down_cast<const FixedLengthColumn<T>*>(column->data_column().get())->immutable_data();
                for (int i = 0; i < values.size(); ++i) {
                    ASSERT_EQ(values[i], check[i]);
                }

                if (!is_dictionary) {
                    // out-of-bounds access
                    st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
                    ASSERT_FALSE(st.ok()) << st.to_string();
                }
            }
            // read+skip
            {
                size_t values_to_skip = values.size() / 2;
                size_t remain_values = values.size() - values_to_skip;

                auto data_column = FixedLengthColumn<T>::create();
                auto column = NullableColumn::create(std::move(data_column), NullColumn::create());

                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->skip(values_to_skip);
                st = decoder->next_batch(remain_values, ColumnContentType::VALUE, column.get());
                ASSERT_TRUE(st.ok()) << st.to_string();

                const auto check =
                        down_cast<const FixedLengthColumn<T>*>(column->data_column().get())->immutable_data();
                for (int i = 0; i < remain_values; ++i) {
                    ASSERT_EQ(values[values_to_skip + i], check[i]);
                }

                if (!is_dictionary) {
                    // out-of-bounds access
                    st = decoder->skip(2);
                    ASSERT_FALSE(st.ok()) << st.to_string();
                }
            }
        }
    }
};

template <bool is_dictionary>
struct DecoderChecker<Slice, is_dictionary> {
    static void check(const std::vector<Slice>& values, const Slice& encoded_data, Decoder* decoder) {
        Status st = Status::OK();
        if (!is_dictionary) {
            // read
            {
                std::vector<Slice> checks(values.size());
                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->next_batch(values.size(), (uint8_t*)&checks[0]);
                ASSERT_TRUE(st.ok()) << st.to_string();
                for (int i = 0; i < values.size(); ++i) {
                    ASSERT_EQ(values[i], checks[i]);
                }

                // out-of-bounds access
                st = decoder->next_batch(values.size(), (uint8_t*)&checks[0]);
                ASSERT_FALSE(st.ok()) << st.to_string();
            }
            // skip + read
            {
                size_t values_to_skip = values.size() / 2;
                size_t remain_values = values.size() - values_to_skip;

                std::vector<Slice> checks(remain_values);
                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->skip(values_to_skip);
                st = decoder->next_batch(remain_values, (uint8_t*)&checks[0]);
                ASSERT_TRUE(st.ok()) << st.to_string();
                for (int i = 0; i < remain_values; ++i) {
                    ASSERT_EQ(values[values_to_skip + i], checks[i]);
                }

                // out-of-bounds access
                st = decoder->skip(2);
                ASSERT_FALSE(st.ok()) << st.to_string();
            }
        }
        if (true) {
            // read
            {
                auto column = BinaryColumn::create();

                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
                ASSERT_TRUE(st.ok()) << st.to_string();

                const auto check = column->immutable_data();
                for (int i = 0; i < values.size(); ++i) {
                    ASSERT_EQ(values[i], check[i]);
                }

                if (!is_dictionary) {
                    // out-of-bounds access
                    st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
                    ASSERT_FALSE(st.ok()) << st.to_string();
                }
            }
            // skip+read
            {
                size_t values_to_skip = values.size() / 2;
                size_t remain_values = values.size() - values_to_skip;

                auto column = starrocks::BinaryColumn::create();

                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->skip(values_to_skip);
                st = decoder->next_batch(remain_values, ColumnContentType::VALUE, column.get());
                ASSERT_TRUE(st.ok()) << st.to_string();

                const auto check = column->immutable_data();
                for (size_t i = 0; i < remain_values; i++) {
                    EXPECT_EQ(values[values_to_skip + i], check[i]);
                }

                if (!is_dictionary) {
                    // out-of-bounds access
                    st = decoder->skip(2);
                    ASSERT_FALSE(st.ok()) << st.to_string();
                }
            }
        }
        if (true) {
            // read
            {
                auto data_column = starrocks::BinaryColumn::create();
                auto column = NullableColumn::create(std::move(data_column), NullColumn::create());

                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
                ASSERT_TRUE(st.ok()) << st.to_string();

                const auto& checks = GetContainer<TYPE_VARCHAR>::get_data(column.get());
                for (size_t i = 0; i < values.size(); i++) {
                    ASSERT_EQ(values[i], checks[i]);
                }

                if (!is_dictionary) {
                    // out-of-bounds access
                    st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
                    ASSERT_FALSE(st.ok()) << st.to_string();
                }
            }
            // skip + read
            {
                size_t values_to_skip = values.size() / 2;
                size_t remain_values = values.size() - values_to_skip;

                auto data_column = starrocks::BinaryColumn::create();
                auto column = NullableColumn::create(std::move(data_column), NullColumn::create());

                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->skip(values_to_skip);
                st = decoder->next_batch(remain_values, ColumnContentType::VALUE, column.get());
                ASSERT_TRUE(st.ok()) << st.to_string();

                const auto& checks = GetContainer<TYPE_VARCHAR>::get_data(column.get());
                for (size_t i = 0; i < remain_values; i++) {
                    EXPECT_EQ(values[values_to_skip + i], checks[i]);
                }

                if (!is_dictionary) {
                    // out-of-bounds access
                    st = decoder->skip(2);
                    ASSERT_FALSE(st.ok()) << st.to_string();
                }
            }
        }
    }
};

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
    (void)EncodingInfo::get(tparquet::Type::INT32, tparquet::Encoding::PLAIN, &plain_encoding);
    ASSERT_TRUE(plain_encoding != nullptr);
    // plain
    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = plain_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = plain_encoding->create_encoder(&encoder);

        st = encoder->append(reinterpret_cast<uint8_t*>(&values[0]), 20);
        ASSERT_TRUE(st.ok()) << st.to_string();

        DecoderChecker<int32_t, false>::check(values, encoder->build(), decoder.get());
    }

    const EncodingInfo* dict_encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::INT32, tparquet::Encoding::RLE_DICTIONARY, &dict_encoding);
    ASSERT_TRUE(dict_encoding != nullptr);
    // dict
    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = dict_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = dict_encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = encoder->append(reinterpret_cast<uint8_t*>(&values[0]), 20);
        ASSERT_TRUE(st.ok()) << st.to_string();

        // construct dictionary encoder
        std::unique_ptr<Encoder> dict_encoder;
        st = plain_encoding->create_encoder(&dict_encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        size_t num_dicts = 0;
        st = encoder->encode_dict(dict_encoder.get(), &num_dicts);
        ASSERT_TRUE(st.ok()) << st.to_string();

        // construct dictionary decoder
        std::unique_ptr<Decoder> dict_decoder;
        st = plain_encoding->create_decoder(&dict_decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        dict_decoder->set_data(dict_encoder->build());

        st = decoder->set_dict(config::vector_chunk_size, num_dicts, dict_decoder.get());
        ASSERT_TRUE(st.ok()) << st.to_string();

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
    (void)EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::PLAIN, &plain_encoding);
    ASSERT_TRUE(plain_encoding != nullptr);
    // plain
    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = plain_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = plain_encoding->create_encoder(&encoder);

        st = encoder->append(reinterpret_cast<uint8_t*>(&slices[0]), 20);
        ASSERT_TRUE(st.ok()) << st.to_string();

        DecoderChecker<Slice, false>::check(slices, encoder->build(), decoder.get());
    }
    const EncodingInfo* dict_encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::RLE_DICTIONARY, &dict_encoding);
    ASSERT_TRUE(dict_encoding != nullptr);
    // dict
    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = dict_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = dict_encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = encoder->append((uint8_t*)&slices[0], 20);
        ASSERT_TRUE(st.ok()) << st.to_string();

        // construct dictionary encoder
        std::unique_ptr<Encoder> dict_encoder;
        st = plain_encoding->create_encoder(&dict_encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        size_t num_dicts = 0;
        st = encoder->encode_dict(dict_encoder.get(), &num_dicts);
        ASSERT_TRUE(st.ok()) << st.to_string();

        // construct dictionary decoder
        std::unique_ptr<Decoder> dict_decoder;
        st = plain_encoding->create_decoder(&dict_decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = dict_decoder->set_data(dict_encoder->build());
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = decoder->set_dict(config::vector_chunk_size, num_dicts, dict_decoder.get());
        ASSERT_TRUE(st.ok()) << st.to_string();

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
    (void)EncodingInfo::get(tparquet::Type::FIXED_LEN_BYTE_ARRAY, tparquet::Encoding::PLAIN, &plain_encoding);
    ASSERT_TRUE(plain_encoding != nullptr);
    // plain
    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = plain_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = plain_encoding->create_encoder(&encoder);

        st = encoder->append(reinterpret_cast<uint8_t*>(&slices[0]), 100);
        ASSERT_TRUE(st.ok()) << st.to_string();

        decoder->set_type_length(3);
        DecoderChecker<Slice, false>::check(slices, encoder->build(), decoder.get());
    }
    const EncodingInfo* dict_encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::FIXED_LEN_BYTE_ARRAY, tparquet::Encoding::RLE_DICTIONARY, &dict_encoding);
    ASSERT_TRUE(dict_encoding != nullptr);
    // dict
    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = dict_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = dict_encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = encoder->append((uint8_t*)&slices[0], 100);
        ASSERT_TRUE(st.ok()) << st.to_string();

        // construct dictionary encoder
        std::unique_ptr<Encoder> dict_encoder;
        st = plain_encoding->create_encoder(&dict_encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        size_t num_dicts = 0;
        st = encoder->encode_dict(dict_encoder.get(), &num_dicts);
        ASSERT_TRUE(st.ok()) << st.to_string();

        // construct dictionary decoder
        std::unique_ptr<Decoder> dict_decoder;
        st = plain_encoding->create_decoder(&dict_decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = dict_decoder->set_data(dict_encoder->build());
        ASSERT_TRUE(st.ok()) << st.to_string();
        dict_decoder->set_type_length(3);

        st = decoder->set_dict(config::vector_chunk_size, num_dicts, dict_decoder.get());
        ASSERT_TRUE(st.ok()) << st.to_string();

        DecoderChecker<Slice, true>::check(slices, encoder->build(), decoder.get());
    }
}

TEST_F(ParquetEncodingTest, Boolean) {
    std::vector<uint8_t> values;
    for (int i = 0; i < 32; i++) {
        values.push_back(i % 3 == 0);
    }

    const EncodingInfo* plain_encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::BOOLEAN, tparquet::Encoding::PLAIN, &plain_encoding);
    ASSERT_TRUE(plain_encoding != nullptr);
    // plain
    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = plain_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = plain_encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        // decode without buffer
        st = encoder->append(reinterpret_cast<uint8_t*>(&values[0]), 32);
        ASSERT_TRUE(st.ok()) << st.to_string();
        DecoderChecker<uint8_t, false>::check(values, encoder->build(), decoder.get());

        // decode with buffer
        values.resize(31);
        DecoderChecker<uint8_t, false>::check(values, encoder->build(), decoder.get());
    }
}

TEST_F(ParquetEncodingTest, DeltaBinaryPacked) {
    auto fn = []<tparquet::Type::type PT>(int rep, int n, int seed) {
        std::cout << "running DeltaBinaryPacked test for type: " << PT << ", rep: " << rep << ", n: " << n
                  << ", seed: " << seed << std::endl;
        using T = typename PhysicalTypeTraits<PT>::CppType;
        std::vector<T> values;
        auto gen = std::mt19937(seed);
        std::uniform_int_distribution<T> dist(std::numeric_limits<T>::min(), std::numeric_limits<T>::max());

        if (seed == -1) {
            values.resize(rep * n);
            std::fill(values.begin(), values.end(), dist(gen));
        } else {
            for (int rep = 0; rep < 10; rep++) {
                values.push_back(std::numeric_limits<T>::max());
                values.push_back(std::numeric_limits<T>::min());
                for (int i = 0; i < n; i++) {
                    values.push_back(dist(gen));
                }
                values.push_back(std::numeric_limits<T>::max());
                values.push_back(std::numeric_limits<T>::min());
            }
        }

        // unaligned access.
        T* buffer = values.data();
        size_t size = values.size();
        {
            buffer += 3;
            size -= 3;
        }

        const EncodingInfo* encoding = nullptr;
        (void)EncodingInfo::get(PT, tparquet::Encoding::DELTA_BINARY_PACKED, &encoding);
        ASSERT_TRUE(encoding != nullptr);

        {
            Status st = Status::OK();
            std::unique_ptr<Decoder> decoder;
            st = encoding->create_decoder(&decoder);
            ASSERT_TRUE(st.ok()) << st.to_string();

            std::unique_ptr<Encoder> encoder;
            st = encoding->create_encoder(&encoder);
            ASSERT_TRUE(st.ok()) << st.to_string();

            st = encoder->append((const uint8_t*)buffer, size);
            ASSERT_TRUE(st.ok()) << st.to_string();

            // simple verification.
            Slice encoded_data = encoder->build();
            std::vector<T> check(size);
            st = decoder->set_data(encoded_data);
            ASSERT_TRUE(st.ok()) << st.to_string();
            st = decoder->next_batch(size, (uint8_t*)(check.data()));
            ASSERT_TRUE(st.ok()) << st.to_string();

            for (int i = 0; i < check.size(); i++) {
                ASSERT_EQ(check[i], buffer[i]);
            }

            // enhanced verification.
            DecoderChecker<T, false>::check(check, encoded_data, decoder.get());
        }
    };

    fn.operator()<tparquet::Type::INT32>(10, 8, -1);
    fn.operator()<tparquet::Type::INT32>(10, 8, 0);
    fn.operator()<tparquet::Type::INT32>(10, 31, 0);
    fn.operator()<tparquet::Type::INT32>(10, 127, 0);
    fn.operator()<tparquet::Type::INT32>(10, 255, 0);

    fn.operator()<tparquet::Type::INT64>(10, 8, -1);
    fn.operator()<tparquet::Type::INT64>(10, 8, 0);
    fn.operator()<tparquet::Type::INT64>(10, 31, 0);
    fn.operator()<tparquet::Type::INT64>(10, 127, 0);
    fn.operator()<tparquet::Type::INT64>(10, 255, 0);
}

TEST_F(ParquetEncodingTest, DeltaLengthByteArrayNonFixedSizeString) {
    std::vector<std::string> strings;
    for (int i = 0; i < 1000; i++) {
        strings.push_back(std::to_string(i));
    }

    std::vector<Slice> values;
    for (const auto& s : strings) {
        values.emplace_back(s);
    }

    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, &encoding);
    ASSERT_TRUE(encoding != nullptr);

    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = encoder->append((uint8_t*)(&values[0]), values.size());
        ASSERT_TRUE(st.ok()) << st.to_string();

        // simple verification.
        Slice encoded_data = encoder->build();
        std::vector<Slice> check(values.size());
        st = decoder->set_data(encoded_data);
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = decoder->next_batch(values.size(), (uint8_t*)(&check[0]));
        ASSERT_TRUE(st.ok()) << st.to_string();

        for (int i = 0; i < check.size(); i++) {
            ASSERT_EQ(check[i], values[i]);
        }

        // enhanced verification.
        DecoderChecker<Slice, false>::check(values, encoded_data, decoder.get());
    }
}

TEST_F(ParquetEncodingTest, DeltaLengthByteArrayFixedSizeString) {
    std::vector<std::string> strings;
    for (int i = 0; i < 1000; i++) {
        strings.push_back(fmt::format("{:0>4}", i));
    }

    std::vector<Slice> values;
    for (const auto& s : strings) {
        values.emplace_back(s);
    }

    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, &encoding);
    ASSERT_TRUE(encoding != nullptr);

    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = encoding->create_decoder(&decoder);
        decoder->set_type_length(4);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = encoder->append((uint8_t*)(&values[0]), values.size());
        ASSERT_TRUE(st.ok()) << st.to_string();

        // simple verification.
        Slice encoded_data = encoder->build();
        std::vector<Slice> check(values.size());
        st = decoder->set_data(encoded_data);
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = decoder->next_batch(values.size(), (uint8_t*)(&check[0]));
        ASSERT_TRUE(st.ok()) << st.to_string();

        for (int i = 0; i < check.size(); i++) {
            ASSERT_EQ(check[i], values[i]);
        }

        // enhanced verification.
        DecoderChecker<Slice, false>::check(values, encoded_data, decoder.get());
    }
}

TEST_F(ParquetEncodingTest, DeltaByteArrayNonFixedSizeStringDebug) {
    // examples from https://parquet.apache.org/docs/file-format/data-pages/encodings/#delta-strings-delta_byte_array--7
    std::vector<std::string> strings = {"axis", "axle", "babble", "babyhood"};
    std::vector<Slice> values;
    for (const auto& s : strings) {
        values.emplace_back(s);
    }

    const EncodingInfo* encoding = nullptr;
    EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::DELTA_BYTE_ARRAY, &encoding);
    ASSERT_TRUE(encoding != nullptr);
    {
        std::unique_ptr<Decoder> decoder;
        auto st = encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = encoder->append((uint8_t*)(&values[0]), values.size());
        ASSERT_TRUE(st.ok()) << st.to_string();

        // simple verification.
        Slice encoded_data = encoder->build();
        std::vector<Slice> check(values.size());
        st = decoder->set_data(encoded_data);
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = decoder->next_batch(values.size(), (uint8_t*)(&check[0]));
        ASSERT_TRUE(st.ok()) << st.to_string();

        for (int i = 0; i < check.size(); i++) {
            std::cout << "check[" << i << "]=" << check[i].to_string() << std::endl;
        }

        for (int i = 0; i < check.size(); i++) {
            ASSERT_EQ(check[i], values[i]);
        }

        // initialize with wrong data.
        for (int i = 0; i < check.size(); i++) {
            check[i].data = (char*)(&decoder);
            check[i].size = 32;
        }
        st = decoder->set_data(encoded_data);
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = decoder->next_batch(values.size(), (uint8_t*)(&check[0]));
        ASSERT_TRUE(st.ok()) << st.to_string();

        for (int i = 0; i < check.size(); i++) {
            std::cout << "check[" << i << "]=" << check[i].to_string() << std::endl;
        }

        for (int i = 0; i < check.size(); i++) {
            ASSERT_EQ(check[i], values[i]);
        }

        // enhanced verification.
        DecoderChecker<Slice, false>::check(values, encoded_data, decoder.get());
    }
}

TEST_F(ParquetEncodingTest, DeltaByteArrayNonFixedSizeString) {
    std::vector<std::string> strings;
    for (int i = 0; i < 1000; i++) {
        strings.push_back(std::to_string(i));
    }

    std::vector<Slice> values;
    for (const auto& s : strings) {
        values.emplace_back(s);
    }

    const EncodingInfo* encoding = nullptr;
    EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::DELTA_BYTE_ARRAY, &encoding);
    ASSERT_TRUE(encoding != nullptr);

    {
        std::unique_ptr<Decoder> decoder;
        auto st = encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = encoder->append((uint8_t*)(&values[0]), values.size());
        ASSERT_TRUE(st.ok()) << st.to_string();

        // simple verification.
        Slice encoded_data = encoder->build();
        std::vector<Slice> check(values.size());
        st = decoder->set_data(encoded_data);
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = decoder->next_batch(values.size(), (uint8_t*)(&check[0]));
        ASSERT_TRUE(st.ok()) << st.to_string();

        // enhanced verification.
        DecoderChecker<Slice, false>::check(values, encoded_data, decoder.get());
    }
}

TEST_F(ParquetEncodingTest, DeltaByteArrayFixedSizeString) {
    std::vector<std::string> strings;
    for (int i = 0; i < 1000; i++) {
        strings.push_back(fmt::format("{:0>4}", i));
    }

    std::vector<Slice> values;
    for (const auto& s : strings) {
        values.emplace_back(s);
    }

    const EncodingInfo* encoding = nullptr;
    EncodingInfo::get(tparquet::Type::FIXED_LEN_BYTE_ARRAY, tparquet::Encoding::DELTA_BYTE_ARRAY, &encoding);
    ASSERT_TRUE(encoding != nullptr);

    {
        std::unique_ptr<Decoder> decoder;
        auto st = encoding->create_decoder(&decoder);
        decoder->set_type_length(4);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = encoder->append((uint8_t*)(&values[0]), values.size());
        ASSERT_TRUE(st.ok()) << st.to_string();

        // simple verification.
        Slice encoded_data = encoder->build();
        std::vector<Slice> check(values.size());
        st = decoder->set_data(encoded_data);
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = decoder->next_batch(values.size(), (uint8_t*)(&check[0]));
        ASSERT_TRUE(st.ok()) << st.to_string();

        // enhanced verification.
        DecoderChecker<Slice, false>::check(values, encoded_data, decoder.get());
    }
}

TEST_F(ParquetEncodingTest, ByteStreamSplitInt32Debug) {
    using T = int32_t;
    // examples from https://parquet.apache.org/docs/file-format/data-pages/encodings/#byte-stream-split-byte_stream_split--9
    std::vector<T> values = {
            (int)0xDDCCBBAA,
            (int)0x33221100,
            (int)0xD6C5B4A3,
    };
    uint8_t expected[12] = {0xAA, 0x00, 0xA3, 0xBB, 0x11, 0xB4, 0xCC, 0x22, 0xC5, 0xDD, 0x33, 0xD6};

    {
        uint8_t encoded[12];
        ByteStreamSplitUtil::ByteStreamSplitEncode((uint8_t*)(&values[0]), 4, 3, encoded);
        for (int i = 0; i < 12; i++) {
            ASSERT_EQ(encoded[i], expected[i]);
        }
        uint8_t decoded[12];
        ByteStreamSplitUtil::ByteStreamSplitDecode(encoded, 4, 3, 3, decoded);
        for (int i = 0; i < 12; i++) {
            ASSERT_EQ(decoded[i], *((uint8_t*)(values.data()) + i));
        }
    }

    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::INT32, tparquet::Encoding::BYTE_STREAM_SPLIT, &encoding);
    ASSERT_TRUE(encoding != nullptr);

    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = encoder->append((uint8_t*)(&values[0]), values.size());
        ASSERT_TRUE(st.ok()) << st.to_string();

        // simple verification.
        Slice encoded_data = encoder->build();
        ASSERT_EQ(encoded_data.size, 12);

        for (int i = 0; i < 12; i++) {
            ASSERT_EQ((uint8_t)encoded_data.data[i], expected[i]);
        }

        std::vector<T> check(values.size());
        st = decoder->set_data(encoded_data);
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = decoder->next_batch(values.size(), (uint8_t*)(&check[0]));
        ASSERT_TRUE(st.ok()) << st.to_string();

        for (int i = 0; i < check.size(); i++) {
            ASSERT_EQ(check[i], values[i]);
        }
    }
}

TEST_F(ParquetEncodingTest, ByteStreamSplitNonFLBA) {
    auto fn = []<tparquet::Type::type PT>(int rep, int n, int seed) {
        std::cout << "running ByteStreamSplitInteger test for type: " << PT << ", rep: " << rep << ", n: " << n
                  << ", seed: " << seed << std::endl;
        using T = typename PhysicalTypeTraits<PT>::CppType;
        std::vector<T> values;
        std::mt19937 gen(seed);
        for (int rep = 0; rep < 10; rep++) {
            values.push_back(std::numeric_limits<T>::max());
            values.push_back(std::numeric_limits<T>::min());
            values.push_back(std::numeric_limits<T>::lowest());
            if constexpr (PT == tparquet::Type::INT32 || PT == tparquet::Type::INT64) {
                for (int i = 0; i < n; i++) {
                    std::uniform_int_distribution<T> dist(std::numeric_limits<T>::min(), std::numeric_limits<T>::max());
                    values.push_back(dist(gen));
                }
            } else {
                std::uniform_real_distribution<T> dist(std::numeric_limits<T>::lowest(), std::numeric_limits<T>::max());
                for (int i = 0; i < n; i++) {
                    values.push_back(dist(gen));
                }
            }
            values.push_back(std::numeric_limits<T>::lowest());
            values.push_back(std::numeric_limits<T>::max());
            values.push_back(std::numeric_limits<T>::min());
        }

        const EncodingInfo* encoding = nullptr;
        (void)EncodingInfo::get(PT, tparquet::Encoding::BYTE_STREAM_SPLIT, &encoding);
        ASSERT_TRUE(encoding != nullptr);

        {
            Status st = Status::OK();
            std::unique_ptr<Decoder> decoder;
            st = encoding->create_decoder(&decoder);
            ASSERT_TRUE(st.ok()) << st.to_string();

            std::unique_ptr<Encoder> encoder;
            st = encoding->create_encoder(&encoder);
            ASSERT_TRUE(st.ok()) << st.to_string();

            st = encoder->append((uint8_t*)(&values[0]), values.size());
            ASSERT_TRUE(st.ok()) << st.to_string();

            // simple verification.
            Slice encoded_data = encoder->build();
            std::vector<T> check(values.size());
            st = decoder->set_data(encoded_data);
            ASSERT_TRUE(st.ok()) << st.to_string();
            st = decoder->next_batch(values.size(), (uint8_t*)(&check[0]));
            ASSERT_TRUE(st.ok()) << st.to_string();

            for (int i = 0; i < check.size(); i++) {
                ASSERT_EQ(check[i], values[i]);
            }

            // enhanced verification.
            DecoderChecker<T, false>::check(values, encoded_data, decoder.get());
        }
    };

    fn.operator()<tparquet::Type::INT32>(10, 8, 0);
    fn.operator()<tparquet::Type::INT32>(10, 31, 0);
    fn.operator()<tparquet::Type::INT32>(10, 127, 0);
    fn.operator()<tparquet::Type::INT32>(10, 255, 0);

    fn.operator()<tparquet::Type::INT64>(10, 8, 0);
    fn.operator()<tparquet::Type::INT64>(10, 31, 0);
    fn.operator()<tparquet::Type::INT64>(10, 127, 0);
    fn.operator()<tparquet::Type::INT64>(10, 255, 0);

    fn.operator()<tparquet::Type::FLOAT>(10, 8, 0);
    fn.operator()<tparquet::Type::FLOAT>(10, 31, 0);
    fn.operator()<tparquet::Type::FLOAT>(10, 127, 0);
    fn.operator()<tparquet::Type::FLOAT>(10, 255, 0);

    fn.operator()<tparquet::Type::DOUBLE>(10, 8, 0);
    fn.operator()<tparquet::Type::DOUBLE>(10, 31, 0);
    fn.operator()<tparquet::Type::DOUBLE>(10, 127, 0);
    fn.operator()<tparquet::Type::DOUBLE>(10, 255, 0);
}

TEST_F(ParquetEncodingTest, ByteStreamSplitFLBA) {
    auto f = [](int byte_width, int num_values) {
        std::cout << "running ByteStreamSplitFLBA test for byte_width: " << byte_width << ", N: " << num_values
                  << std::endl;
        const int SIZE = byte_width * num_values;
        std::vector<uint8_t> bytes_data(SIZE);

        for (int i = 0; i < SIZE; i++) {
            bytes_data[i] = (uint8_t)(i % 256);
        }

        std::vector<uint8_t> expected(SIZE);
        int idx = 0;
        for (int i = 0; i < byte_width; i++) {
            for (int j = 0; j < num_values; j++) {
                expected[idx++] = bytes_data[j * byte_width + i];
            }
        }

        std::vector<uint8_t> encoded(SIZE);
        ByteStreamSplitUtil::ByteStreamSplitEncode(bytes_data.data(), byte_width, num_values, encoded.data());
        for (int i = 0; i < SIZE; i++) {
            ASSERT_EQ(encoded[i], expected[i]);
        }
        std::vector<uint8_t> decoded(SIZE);
        ByteStreamSplitUtil::ByteStreamSplitDecode(encoded.data(), byte_width, num_values, num_values, decoded.data());
        for (int i = 0; i < SIZE; i++) {
            ASSERT_EQ(decoded[i], bytes_data[i]);
        }

        std::vector<Slice> values;
        for (int i = 0; i < num_values; i++) {
            values.emplace_back(bytes_data.data() + i * byte_width, byte_width);
        }

        const EncodingInfo* encoding = nullptr;
        (void)EncodingInfo::get(tparquet::Type::FIXED_LEN_BYTE_ARRAY, tparquet::Encoding::BYTE_STREAM_SPLIT, &encoding);
        ASSERT_TRUE(encoding != nullptr);

        {
            Status st = Status::OK();
            std::unique_ptr<Encoder> encoder;
            st = encoding->create_encoder(&encoder);
            ASSERT_TRUE(st.ok()) << st.to_string();

            encoder->set_type_length(byte_width);
            st = encoder->append((uint8_t*)(&values[0]), values.size());
            ASSERT_TRUE(st.ok()) << st.to_string();

            // simple verification.
            Slice encoded_data = encoder->build();
            ASSERT_EQ(encoded_data.size, expected.size());
            ASSERT_EQ(encoded_data.size, encoded.size());
            for (int i = 0; i < SIZE; i++) {
                ASSERT_EQ((uint8_t)encoded_data.data[i], expected[i]);
                ASSERT_EQ((uint8_t)encoded_data.data[i], encoded[i]);
            }

            std::unique_ptr<Decoder> decoder;
            st = encoding->create_decoder(&decoder);
            ASSERT_TRUE(st.ok()) << st.to_string();
            decoder->set_type_length(byte_width);

            // real all.
            st = decoder->set_data(encoded_data);
            ASSERT_TRUE(st.ok()) << st.to_string();
            std::vector<Slice> check(values.size());
            st = decoder->next_batch(num_values, (uint8_t*)(&check[0]));
            ASSERT_TRUE(st.ok()) << st.to_string();
            for (int i = 0; i < check.size(); i++) {
                ASSERT_EQ(check[i], values[i]);
            }
        }

        {
            Status st = Status::OK();
            std::unique_ptr<Decoder> decoder;
            st = encoding->create_decoder(&decoder);
            ASSERT_TRUE(st.ok()) << st.to_string();

            decoder->set_type_length(byte_width);

            std::unique_ptr<Encoder> encoder;
            st = encoding->create_encoder(&encoder);
            ASSERT_TRUE(st.ok()) << st.to_string();

            encoder->set_type_length(byte_width);

            st = encoder->append((uint8_t*)(&values[0]), values.size());
            ASSERT_TRUE(st.ok()) << st.to_string();

            // simple verification.
            Slice encoded_data = encoder->build();
            std::vector<Slice> check(values.size());
            st = decoder->set_data(encoded_data);
            ASSERT_TRUE(st.ok()) << st.to_string();
            st = decoder->next_batch(values.size(), (uint8_t*)(&check[0]));
            ASSERT_TRUE(st.ok()) << st.to_string();

            for (int i = 0; i < check.size(); i++) {
                ASSERT_EQ(check[i], values[i]);
            }

            // enhanced verification.
            DecoderChecker<Slice, false>::check(values, encoded_data, decoder.get());
        }
    };

    f(5, 3);
    f(5, 10);
    f(5, 31);
    f(5, 127);
    f(5, 255);

    f(31, 3);
    f(31, 10);
    f(31, 31);
    f(31, 127);
    f(31, 255);
}

// A DELTA_BINARY_PACKED page header is entirely file-controlled, so a corrupt one must come back
// as a Status. Throwing here escapes the scan worker thread, which has no handler in any build
// type, and aborts the BE process.
TEST_F(ParquetEncodingTest, DeltaBinaryPackedCorruptHeader) {
    // The header fields are ULEB128: values_per_block, mini_blocks_per_block, total_value_count
    // and zigzag(first_value). Here values_per_block is 128 in both cases.
    // 128 / 1000 == 0, i.e. more miniblocks than values.
    const std::vector<uint8_t> zero_values_per_mini_block = {0x80, 0x01, 0xe8, 0x07, 0x00, 0x00};
    // 128 / 3 == 42, which is not a multiple of 32.
    const std::vector<uint8_t> unaligned_mini_block = {0x80, 0x01, 0x03, 0x00, 0x00};

    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::INT32, tparquet::Encoding::DELTA_BINARY_PACKED, &encoding);
    ASSERT_TRUE(encoding != nullptr);

    for (const auto& header : {zero_values_per_mini_block, unaligned_mini_block}) {
        std::unique_ptr<Decoder> decoder;
        ASSERT_TRUE(encoding->create_decoder(&decoder).ok());
        Status st = decoder->set_data(Slice(reinterpret_cast<const char*>(header.data()), header.size()));
        ASSERT_TRUE(st.is_corruption()) << st;
    }
}

namespace {

enum class NullPattern { ALL_NULL, NO_NULL, ALTERNATE, RANDOM_5_PCT, RANDOM_50_PCT, LEADING_AND_TRAILING };

std::vector<uint8_t> make_nulls(NullPattern pattern, size_t count, std::mt19937& rng) {
    std::vector<uint8_t> nulls(count, 0);
    std::uniform_int_distribution<int> pct(0, 99);
    for (size_t i = 0; i < count; i++) {
        switch (pattern) {
        case NullPattern::ALL_NULL:
            nulls[i] = 1;
            break;
        case NullPattern::NO_NULL:
            break;
        case NullPattern::ALTERNATE:
            nulls[i] = i % 2;
            break;
        case NullPattern::RANDOM_5_PCT:
            nulls[i] = pct(rng) < 5;
            break;
        case NullPattern::RANDOM_50_PCT:
            nulls[i] = pct(rng) < 50;
            break;
        case NullPattern::LEADING_AND_TRAILING:
            // nulls at both ends, so the last value sits exactly at the end of the page buffer
            nulls[i] = i < 3 || i + 5 >= count || i % 7 == 0;
            break;
        }
    }
    return nulls;
}

void fill_null_infos(const std::vector<uint8_t>& nulls, size_t begin, size_t count, NullInfos* infos) {
    infos->reset_with_capacity(count);
    memcpy(infos->nulls_data(), nulls.data() + begin, count);
    for (size_t i = 0; i < count; i++) {
        infos->num_nulls += nulls[begin + i];
        infos->num_ranges += (i == 0 || nulls[begin + i] != nulls[begin + i - 1]);
    }
}

// Decodes a nullable PLAIN page batch by batch and checks every row against the source values.
template <typename T>
void check_plain_with_nulls(tparquet::Type::type type, NullPattern pattern, size_t num_rows, size_t batch_size,
                            const MutableColumnPtr& data_column, bool with_filter, size_t misalign) {
    std::mt19937 rng(static_cast<uint32_t>(num_rows * 131 + batch_size));
    std::vector<uint8_t> nulls = make_nulls(pattern, num_rows, rng);
    std::vector<T> values;
    for (size_t i = 0; i < num_rows; i++) {
        if (!nulls[i]) {
            T v;
            memset(&v, 0, sizeof(T));
            int64_t seed = static_cast<int64_t>(i) * 2654435761LL + 17;
            memcpy(&v, &seed, std::min(sizeof(T), sizeof(seed)));
            values.push_back(v);
        }
    }
    // The page buffer ends exactly after the non-null values, with no padding, so ASAN catches an over-read.
    // `misalign` bytes in front move the values off T's alignment, as definition levels of any length do.
    std::vector<uint8_t> page(misalign + values.size() * sizeof(T));
    if (!values.empty()) {
        memcpy(page.data() + misalign, values.data(), values.size() * sizeof(T));
    }

    const EncodingInfo* encoding = nullptr;
    ASSERT_OK(EncodingInfo::get(type, tparquet::Encoding::PLAIN, &encoding));
    std::unique_ptr<Decoder> decoder;
    ASSERT_OK(encoding->create_decoder(&decoder));
    ASSERT_OK(decoder->set_data(Slice(page.data() + misalign, page.size() - misalign)));

    auto dst = NullableColumn::create(data_column->clone_empty(), NullColumn::create());
    std::vector<uint8_t> filter(batch_size, 1);
    for (size_t i = 0; i < filter.size(); i++) {
        filter[i] = i % 3 == 0;
    }
    NullInfos infos;
    for (size_t begin = 0; begin < num_rows; begin += batch_size) {
        size_t count = std::min(batch_size, num_rows - begin);
        fill_null_infos(nulls, begin, count, &infos);
        ASSERT_OK(decoder->next_batch_with_nulls(count, infos, ColumnContentType::VALUE, dst.get(),
                                                 with_filter ? filter.data() : nullptr));
    }
    // the whole page is consumed
    ASSERT_FALSE(decoder->skip(1).ok());

    ASSERT_EQ(num_rows, dst->size());
    ASSERT_EQ(num_rows, dst->null_column()->size());
    const auto* data = reinterpret_cast<const T*>(
            down_cast<const FixedLengthColumnBase<T>*>(dst->data_column_raw_ptr())->raw_data());
    size_t k = 0;
    for (size_t i = 0; i < num_rows; i++) {
        ASSERT_EQ(nulls[i], dst->is_null(i)) << "row " << i;
        if (!nulls[i]) {
            ASSERT_EQ(0, memcmp(&values[k], &data[i], sizeof(T))) << "row " << i;
            k++;
        }
    }
    ASSERT_EQ(values.size(), k);
    size_t num_nulls = std::count(nulls.begin(), nulls.end(), 1);
    ASSERT_EQ(num_nulls > 0, dst->has_null());
}

} // namespace

TEST_F(ParquetEncodingTest, PlainFixedLengthWithNulls) {
    const std::vector<NullPattern> patterns = {NullPattern::ALL_NULL,      NullPattern::NO_NULL,
                                               NullPattern::ALTERNATE,     NullPattern::RANDOM_5_PCT,
                                               NullPattern::RANDOM_50_PCT, NullPattern::LEADING_AND_TRAILING};
    for (auto pattern : patterns) {
        for (size_t batch_size : {1, 7, 64, 4096}) {
            for (bool with_filter : {false, true}) {
                for (size_t misalign : {0, 1}) {
                    SCOPED_TRACE(fmt::format("pattern={} batch_size={} filter={} misalign={}",
                                             static_cast<int>(pattern), batch_size, with_filter, misalign));
                    check_plain_with_nulls<int32_t>(tparquet::Type::INT32, pattern, 4099, batch_size,
                                                    Int32Column::create(), with_filter, misalign);
                    check_plain_with_nulls<int64_t>(tparquet::Type::INT64, pattern, 4099, batch_size,
                                                    Int64Column::create(), with_filter, misalign);
                    check_plain_with_nulls<float>(tparquet::Type::FLOAT, pattern, 4099, batch_size,
                                                  FloatColumn::create(), with_filter, misalign);
                    check_plain_with_nulls<double>(tparquet::Type::DOUBLE, pattern, 4099, batch_size,
                                                   DoubleColumn::create(), with_filter, misalign);
                    check_plain_with_nulls<int96_t>(tparquet::Type::INT96, pattern, 4099, batch_size,
                                                    FixedLengthColumn<int96_t>::create(), with_filter, misalign);
                    // DECIMAL(7,2) stored as INT32 and read straight into a decimal column
                    check_plain_with_nulls<int32_t>(tparquet::Type::INT32, pattern, 4099, batch_size,
                                                    Decimal32Column::create(7, 2), with_filter, misalign);
                }
            }
        }
    }
}

TEST_F(ParquetEncodingTest, AssignNullsFromDefLevels) {
    std::mt19937 rng(3);
    std::uniform_int_distribution<int> pct(0, 99);
    for (int16_t max_def_level : {1, 2}) {
        for (int null_pct : {0, 5, 50, 100}) {
            // sizes around the 240-row accumulation block
            for (size_t n : {0, 1, 2, 17, 239, 240, 241, 255, 256, 481, 4096}) {
                SCOPED_TRACE(fmt::format("max_def_level={} null_pct={} n={}", max_def_level, null_pct, n));
                std::vector<int16_t> levels(n);
                for (auto& l : levels) {
                    // levels below max_def_level (0 .. max-1) are nulls of this column or of an ancestor
                    l = pct(rng) < null_pct ? static_cast<int16_t>(rng() % max_def_level) : max_def_level;
                }
                NullInfos infos;
                infos.reset_with_capacity(n);
                assign_nulls_from_def_levels(levels.data(), max_def_level, n, &infos);
                size_t num_nulls = 0;
                size_t num_ranges = 1;
                for (size_t i = 0; i < n; i++) {
                    bool is_null = levels[i] < max_def_level;
                    ASSERT_EQ(is_null, infos.nulls_data()[i]) << "row " << i;
                    num_nulls += is_null;
                    num_ranges += i > 0 && is_null != (levels[i - 1] < max_def_level);
                }
                ASSERT_EQ(num_nulls, infos.num_nulls);
                ASSERT_EQ(num_ranges, infos.num_ranges);
            }
        }
    }
}

// PLAIN BYTE_ARRAY with a pushed-down filter: selected rows keep their value and excluded rows are empty,
// for short and long values (the batched append switches strategy at 128 bytes) and several batches.
TEST_F(ParquetEncodingTest, PlainStringWithFilter) {
    std::mt19937 rng(41);
    for (size_t avg_len : {5, 40, 300}) {
        SCOPED_TRACE(fmt::format("avg_len={}", avg_len));
        const size_t num_rows = 1000;
        std::vector<std::string> values;
        std::string page;
        for (size_t i = 0; i < num_rows; i++) {
            std::string v(rng() % (2 * avg_len), static_cast<char>('a' + i % 26));
            char len[4];
            encode_fixed32_le(reinterpret_cast<uint8_t*>(len), static_cast<uint32_t>(v.size()));
            page.append(len, 4);
            page.append(v);
            values.push_back(std::move(v));
        }
        std::vector<uint8_t> filter(num_rows);
        for (auto& f : filter) f = rng() % 5 == 0;

        const EncodingInfo* encoding = nullptr;
        ASSERT_OK(EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::PLAIN, &encoding));
        std::unique_ptr<Decoder> decoder;
        ASSERT_OK(encoding->create_decoder(&decoder));
        ASSERT_OK(decoder->set_data(Slice(page.data(), page.size())));
        auto dst = BinaryColumn::create();
        for (size_t begin = 0; begin < num_rows; begin += 333) {
            size_t count = std::min<size_t>(333, num_rows - begin);
            ASSERT_OK(decoder->next_batch(count, ColumnContentType::VALUE, dst.get(), filter.data() + begin));
        }
        ASSERT_EQ(num_rows, dst->size());
        for (size_t i = 0; i < num_rows; i++) {
            ASSERT_EQ(filter[i] ? values[i] : std::string(), dst->get_slice(i).to_string()) << "row " << i;
        }
    }
}

// Nullable BOOLEAN PLAIN (bit packed): the batched null path matches the source for every null pattern and for
// batch sizes that leave the bit reader mid-byte.
TEST_F(ParquetEncodingTest, PlainBooleanWithNulls) {
    std::mt19937 rng(43);
    const std::vector<NullPattern> patterns = {NullPattern::ALL_NULL,      NullPattern::NO_NULL,
                                               NullPattern::ALTERNATE,     NullPattern::RANDOM_5_PCT,
                                               NullPattern::RANDOM_50_PCT, NullPattern::LEADING_AND_TRAILING};
    for (auto pattern : patterns) {
        for (size_t batch_size : {1, 7, 64, 4096}) {
            SCOPED_TRACE(fmt::format("pattern={} batch_size={}", static_cast<int>(pattern), batch_size));
            const size_t num_rows = 4099;
            std::vector<uint8_t> nulls = make_nulls(pattern, num_rows, rng);
            std::vector<uint8_t> values;
            for (size_t i = 0; i < num_rows; i++) {
                if (!nulls[i]) values.push_back(rng() % 3 == 0);
            }
            const EncodingInfo* encoding = nullptr;
            ASSERT_OK(EncodingInfo::get(tparquet::Type::BOOLEAN, tparquet::Encoding::PLAIN, &encoding));
            std::unique_ptr<Encoder> encoder;
            ASSERT_OK(encoding->create_encoder(&encoder));
            ASSERT_OK(encoder->append(values.data(), values.size()));
            Slice page = encoder->build();
            std::unique_ptr<Decoder> decoder;
            ASSERT_OK(encoding->create_decoder(&decoder));
            ASSERT_OK(decoder->set_data(page));

            auto dst = NullableColumn::create(BooleanColumn::create(), NullColumn::create());
            NullInfos infos;
            for (size_t begin = 0; begin < num_rows; begin += batch_size) {
                size_t count = std::min(batch_size, num_rows - begin);
                fill_null_infos(nulls, begin, count, &infos);
                ASSERT_OK(decoder->next_batch_with_nulls(count, infos, ColumnContentType::VALUE, dst.get(), nullptr));
            }
            ASSERT_EQ(num_rows, dst->size());
            const auto& data = down_cast<const BooleanColumn*>(dst->data_column_raw_ptr())->get_data();
            size_t k = 0;
            for (size_t i = 0; i < num_rows; i++) {
                ASSERT_EQ(nulls[i], dst->is_null(i)) << "row " << i;
                if (!nulls[i]) {
                    ASSERT_EQ(values[k], data[i]) << "row " << i;
                    k++;
                }
            }
            ASSERT_EQ(values.size(), k);
        }
    }
}

namespace {

// 1-bit RLE definition levels (V2 layout: no length prefix) for `levels`.
std::string encode_def_levels(const std::vector<int16_t>& levels) {
    faststring buf;
    RleEncoder<int16_t> encoder(&buf, 1);
    for (auto l : levels) encoder.Put(l);
    encoder.Flush();
    return std::string(reinterpret_cast<const char*>(buf.data()), buf.size());
}

} // namespace

// decode_nulls() and skip_count_non_null() match the levels for random and clustered nulls, including when it is mixed with the
// level_t API on the same page: levels that get_avail_levels() decoded ahead are used first, and the
// level_t API continues from where decode_nulls() stopped.
TEST_F(ParquetEncodingTest, LevelDecoderDecodeNulls) {
    std::mt19937 rng(47);
    for (int pattern = 0; pattern < 3; pattern++) {
        SCOPED_TRACE(fmt::format("pattern={}", pattern));
        const size_t num_levels = 20000;
        std::vector<int16_t> levels(num_levels);
        for (size_t i = 0; i < num_levels; i++) {
            if (pattern == 0) {
                levels[i] = rng() % 20 != 0; // 5% nulls, mostly literal runs
            } else if (pattern == 1) {
                levels[i] = (i / 97) % 3 != 0; // long repeated runs
            } else {
                levels[i] = rng() % 2; // 50% nulls
            }
        }
        std::string encoded = encode_def_levels(levels);
        int64_t timer = 0;
        LevelDecoder decoder(&timer);
        Slice slice(encoded.data(), encoded.size());
        ASSERT_OK(decoder.parse_v2(encoded.size(), 1, num_levels, &slice));
        ASSERT_TRUE(decoder.can_decode_nulls());

        size_t pos = 0;
        auto check_nulls = [&](size_t n) {
            NullInfos infos;
            ASSERT_TRUE(decoder.decode_nulls(n, &infos));
            size_t nulls = 0, ranges = 1;
            for (size_t i = 0; i < n; i++) {
                ASSERT_EQ(levels[pos + i] == 0, infos.nulls_data()[i]) << "level " << pos + i;
                nulls += levels[pos + i] == 0;
                ranges += i > 0 && levels[pos + i] != levels[pos + i - 1];
            }
            ASSERT_EQ(nulls, infos.num_nulls);
            ASSERT_EQ(ranges, infos.num_ranges);
            pos += n;
        };
        auto check_levels = [&](size_t n) {
            level_t* got = nullptr;
            // decodes at least 4096 levels ahead into the buffer
            ASSERT_GE(decoder.get_avail_levels(n, &got), n);
            for (size_t i = 0; i < n; i++) {
                ASSERT_EQ(levels[pos + i], got[i]) << "level " << pos + i;
            }
            decoder.consume_levels(n);
            pos += n;
        };
        auto check_skip = [&](size_t n) {
            size_t non_null = 0;
            ASSERT_TRUE(decoder.skip_count_non_null(n, &non_null));
            size_t expected = 0;
            for (size_t i = 0; i < n; i++) expected += levels[pos + i] != 0;
            ASSERT_EQ(expected, non_null);
            pos += n;
        };
        check_levels(37);  // leaves ~4059 levels buffered
        check_nulls(1000); // served from the buffer
        check_nulls(5000); // drains the buffer, then reads the stream
        check_levels(500); // level_t API continues from the stream position
        check_skip(100);   // from the buffer
        check_skip(9000);  // drains the buffer, then several stream chunks
        decoder.reset();
        check_nulls(4096);
        check_nulls(num_levels - pos);
        NullInfos infos;
        ASSERT_FALSE(decoder.decode_nulls(1, &infos));
    }
}

} // namespace starrocks::parquet
