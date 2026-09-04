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

#include <cstring>
#include <limits>
#include <random>

#include "base/coding.h"
#include "base/simd/byte_stream_split.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/config_exec_fwd.h"
#include "formats/parquet/types.h"

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

// ---- BOOLEAN + RLE -------------------------------------------------------------------------------
// Wire format: 4-byte little-endian payload length, then the RLE / bit-packed hybrid stream with a
// bit width of 1. parquet-mr writes every boolean column this way in PARQUET_2_0 mode.

namespace {

const EncodingInfo* rle_boolean_encoding() {
    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::BOOLEAN, tparquet::Encoding::RLE, &encoding);
    return encoding;
}

Slice rle_boolean_encode(const EncodingInfo* encoding, const std::vector<uint8_t>& values,
                         std::unique_ptr<Encoder>* encoder) {
    EXPECT_TRUE(encoding->create_encoder(encoder).ok());
    EXPECT_TRUE((*encoder)->append(values.data(), values.size()).ok());
    Slice encoded = (*encoder)->build();
    // the prefix must describe exactly the payload that follows
    EXPECT_GE(encoded.size, static_cast<size_t>(4));
    EXPECT_EQ(decode_fixed32_le(reinterpret_cast<const uint8_t*>(encoded.data)),
              static_cast<uint32_t>(encoded.size - 4));
    return encoded;
}

} // namespace

TEST_F(ParquetEncodingTest, RleBooleanRoundTrip) {
    const EncodingInfo* encoding = rle_boolean_encoding();
    ASSERT_TRUE(encoding != nullptr);

    // Every stream here ends on a repeated run or on a full literal group, so it carries no literal
    // padding and DecoderChecker's out-of-bounds assertions hold.
    std::vector<std::vector<uint8_t>> cases;
    cases.emplace_back(100, 1);
    cases.emplace_back(100, 0);
    {
        std::vector<uint8_t> alternating(64);
        for (size_t i = 0; i < alternating.size(); ++i) {
            alternating[i] = i % 2;
        }
        cases.push_back(alternating);
    }
    {
        // repeated run -> repeated run -> literal groups -> repeated run
        std::vector<uint8_t> mixed;
        mixed.insert(mixed.end(), 50, 1);
        mixed.insert(mixed.end(), 50, 0);
        for (size_t i = 0; i < 64; ++i) {
            mixed.push_back(i % 3 == 0);
        }
        mixed.insert(mixed.end(), 100, 1);
        cases.push_back(mixed);
    }
    {
        // Random values interleave literal groups and repeated runs. A repeated run whose length is
        // not a multiple of 8 shifts the literal-group alignment, so the stream could end on a partial
        // (padded) group; a trailing repeated run keeps the final flush exact.
        std::vector<uint8_t> random(4096);
        auto gen = std::mt19937(7);
        std::bernoulli_distribution dist(0.5);
        for (auto& v : random) {
            v = dist(gen);
        }
        random.insert(random.end(), 64, 1);
        cases.push_back(random);
    }

    for (const auto& values : cases) {
        std::unique_ptr<Encoder> encoder;
        Slice encoded = rle_boolean_encode(encoding, values, &encoder);

        std::unique_ptr<Decoder> decoder;
        ASSERT_TRUE(encoding->create_decoder(&decoder).ok());
        DecoderChecker<uint8_t, false>::check(values, encoded, decoder.get());

        // build() is idempotent: the same bytes come back
        Slice again = encoder->build();
        ASSERT_EQ(encoded.size, again.size);
        ASSERT_EQ(0, memcmp(encoded.data, again.data, encoded.size));
    }
}

TEST_F(ParquetEncodingTest, RleBooleanUnalignedSizes) {
    // The format pads the last literal group to 8 values, so sizes that are not a multiple of 8
    // must still decode exactly through every read path.
    const EncodingInfo* encoding = rle_boolean_encoding();
    ASSERT_TRUE(encoding != nullptr);

    for (size_t n : {1, 2, 7, 9, 31, 33, 127, 1001}) {
        std::vector<uint8_t> values(n);
        for (size_t i = 0; i < n; ++i) {
            values[i] = (i * 7) % 3 == 0;
        }
        std::unique_ptr<Encoder> encoder;
        Slice encoded = rle_boolean_encode(encoding, values, &encoder);

        std::unique_ptr<Decoder> decoder;
        ASSERT_TRUE(encoding->create_decoder(&decoder).ok());

        // raw read
        {
            std::vector<uint8_t> check(n);
            ASSERT_TRUE(decoder->set_data(encoded).ok());
            ASSERT_TRUE(decoder->next_batch(n, check.data()).ok());
            ASSERT_EQ(values, check) << "n=" << n;
        }
        // skip, then read the rest into a column
        {
            size_t to_skip = n / 3;
            auto column = FixedLengthColumn<uint8_t>::create();
            ASSERT_TRUE(decoder->set_data(encoded).ok());
            ASSERT_TRUE(decoder->skip(to_skip).ok());
            ASSERT_TRUE(decoder->next_batch(n - to_skip, ColumnContentType::VALUE, column.get()).ok());
            const auto& data = column->immutable_data();
            ASSERT_EQ(n - to_skip, data.size());
            for (size_t i = 0; i < data.size(); ++i) {
                ASSERT_EQ(values[to_skip + i], data[i]) << "n=" << n << " i=" << i;
            }
        }
        // nullable column: nulls interleaved through the default next_batch_with_nulls path
        {
            auto column = NullableColumn::create(FixedLengthColumn<uint8_t>::create(), NullColumn::create());
            std::vector<uint8_t> is_null(n);
            NullInfos null_infos;
            null_infos.reset_with_capacity(n);
            for (size_t i = 0; i < n; ++i) {
                is_null[i] = (i % 5 == 0);
                null_infos.nulls_data()[i] = is_null[i];
                null_infos.num_nulls += is_null[i];
                null_infos.num_ranges += (i > 0 && is_null[i] != is_null[i - 1]);
            }
            ASSERT_TRUE(decoder->set_data(encoded).ok());
            ASSERT_TRUE(decoder->next_batch_with_nulls(n, null_infos, ColumnContentType::VALUE, column.get(), nullptr)
                                .ok());
            ASSERT_EQ(n, column->size());
            const auto& data =
                    down_cast<const FixedLengthColumn<uint8_t>*>(column->data_column().get())->immutable_data();
            size_t value_idx = 0;
            for (size_t i = 0; i < n; ++i) {
                if (is_null[i]) {
                    ASSERT_TRUE(column->is_null(i)) << "n=" << n << " i=" << i;
                } else {
                    ASSERT_FALSE(column->is_null(i)) << "n=" << n << " i=" << i;
                    // non-null values are consumed from the stream in order
                    ASSERT_EQ(values[value_idx], data[i]) << "n=" << n << " i=" << i;
                    ++value_idx;
                }
            }
        }
    }
}

TEST_F(ParquetEncodingTest, RleBooleanHandCraftedStream) {
    // Independent of our encoder. Payload:
    //   0x90 0x03 = ULEB128(400): lsb 0 -> repeated run of 400 >> 1 = 200 values; value byte 0x01
    //   0x03      = ULEB128(3):   lsb 1 -> literal run of (3 >> 1) * 8 = 8 values
    //   0xAA      = 0b10101010, LSB first -> 0,1,0,1,0,1,0,1
    const std::vector<uint8_t> stream = {0x05, 0x00, 0x00, 0x00, 0x90, 0x03, 0x01, 0x03, 0xAA};
    std::vector<uint8_t> expected(200, 1);
    for (int i = 0; i < 8; ++i) {
        expected.push_back(i % 2);
    }

    const EncodingInfo* encoding = rle_boolean_encoding();
    ASSERT_TRUE(encoding != nullptr);
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(encoding->create_decoder(&decoder).ok());
    DecoderChecker<uint8_t, false>::check(expected, Slice(stream.data(), stream.size()), decoder.get());
}

TEST_F(ParquetEncodingTest, RleBooleanCorruptInput) {
    const EncodingInfo* encoding = rle_boolean_encoding();
    ASSERT_TRUE(encoding != nullptr);
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(encoding->create_decoder(&decoder).ok());
    std::vector<uint8_t> out(256);

    // shorter than the length prefix
    {
        const std::vector<uint8_t> stream = {0x01, 0x00};
        ASSERT_FALSE(decoder->set_data(Slice(stream.data(), stream.size())).ok());
    }
    // prefix claims more payload than the page holds
    {
        const std::vector<uint8_t> stream = {0x10, 0x00, 0x00, 0x00, 0x03, 0xAA};
        ASSERT_FALSE(decoder->set_data(Slice(stream.data(), stream.size())).ok());
    }
    // well-formed prefix, but the repeated-run header is missing its value byte
    {
        const std::vector<uint8_t> stream = {0x02, 0x00, 0x00, 0x00, 0x90, 0x03};
        ASSERT_TRUE(decoder->set_data(Slice(stream.data(), stream.size())).ok());
        ASSERT_FALSE(decoder->next_batch(200, out.data()).ok());
    }
    // empty payload: nothing to read, nothing to skip
    {
        const std::vector<uint8_t> stream = {0x00, 0x00, 0x00, 0x00};
        ASSERT_TRUE(decoder->set_data(Slice(stream.data(), stream.size())).ok());
        ASSERT_FALSE(decoder->next_batch(1, out.data()).ok());
        ASSERT_TRUE(decoder->set_data(Slice(stream.data(), stream.size())).ok());
        ASSERT_FALSE(decoder->skip(1).ok());
    }
    // a literal run that promises more groups than the payload contains
    {
        const std::vector<uint8_t> stream = {0x02, 0x00, 0x00, 0x00, 0x07, 0xAA};
        ASSERT_TRUE(decoder->set_data(Slice(stream.data(), stream.size())).ok());
        ASSERT_FALSE(decoder->next_batch(24, out.data()).ok());
    }
}

} // namespace starrocks::parquet
