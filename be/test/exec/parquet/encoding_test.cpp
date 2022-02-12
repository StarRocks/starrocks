// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/parquet/encoding.h"

#include <gtest/gtest.h>

#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "exec/parquet/encoding_dict.h"
#include "exec/parquet/encoding_plain.h"

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
        {
            auto column = starrocks::vectorized::FixedLengthColumn<T>::create();

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
        {
            auto data_column = starrocks::vectorized::FixedLengthColumn<T>::create();
            auto column = vectorized::NullableColumn::create(data_column, vectorized::NullColumn::create());

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
    }
};

template <bool is_dictionary>
struct DecoderChecker<Slice, is_dictionary> {
    static void check(const std::vector<Slice>& values, const Slice& encoded_data, Decoder* decoder) {
        if (!is_dictionary) {
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
        {
            auto column = starrocks::vectorized::BinaryColumn::create();

            decoder->set_data(encoded_data);
            auto st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
            ASSERT_TRUE(st.ok());

            const auto* check = (const Slice*)column->raw_data();
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
        {
            auto data_column = starrocks::vectorized::BinaryColumn::create();
            auto column = vectorized::NullableColumn::create(data_column, vectorized::NullColumn::create());

            decoder->set_data(encoded_data);
            auto st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
            ASSERT_TRUE(st.ok());

            const auto* check = (const Slice*)column->data_column()->raw_data();
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
    }
};

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
        slices.push_back(values[i]);
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
        slices.push_back(values[i - 100]);
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

        decoder->set_type_legth(3);
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
        dict_decoder->set_type_legth(3);

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
