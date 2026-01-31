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

#include "base/simd/byte_stream_split.h"
#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/config.h"
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

                const T* check = (const T*)column->raw_data();
                for (int i = 0; i < values.size(); ++i) {
                    ASSERT_EQ(values[i], *check);
                    check++;
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

                auto column = starrocks::FixedLengthColumn<T>::create();
                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->skip(values_to_skip);
                st = decoder->next_batch(remain_values, ColumnContentType::VALUE, column.get());
                ASSERT_TRUE(st.ok()) << st.to_string();

                const T* check = (const T*)column->raw_data();
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

                const T* check = (const T*)column->data_column()->raw_data();
                for (int i = 0; i < values.size(); ++i) {
                    ASSERT_EQ(values[i], *check);
                    check++;
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

                auto data_column = starrocks::FixedLengthColumn<T>::create();
                auto column = NullableColumn::create(std::move(data_column), NullColumn::create());

                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->skip(values_to_skip);
                st = decoder->next_batch(remain_values, ColumnContentType::VALUE, column.get());
                ASSERT_TRUE(st.ok()) << st.to_string();

                const T* check = (const T*)column->data_column()->raw_data();
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
                auto column = starrocks::BinaryColumn::create();

                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
                ASSERT_TRUE(st.ok()) << st.to_string();

                const auto* check = (const Slice*)column->raw_data();
                for (auto value : values) {
                    ASSERT_EQ(value, *check);
                    check++;
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

                const auto* check = (const Slice*)column->raw_data();
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

                const auto* check = (const Slice*)column->data_column()->raw_data();
                for (auto value : values) {
                    ASSERT_EQ(value, *check);
                    check++;
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

                const auto* check = (const Slice*)column->data_column()->raw_data();
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

} // namespace starrocks::parquet
