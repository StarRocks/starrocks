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

#include <fs/fs.h>
#include <gtest/gtest.h>
#include <util/variant.h>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "base/testutil/assert.h"
#include "runtime/decimalv3.h"
#include "types/timestamp_value.h"
#include "util/url_coding.h"

namespace starrocks::parquet {

class ParquetVariantTest : public testing::Test {
public:
    ParquetVariantTest() = default;
    ~ParquetVariantTest() override = default;

protected:
    uint8_t primitive_header(VariantType primitive) { return (static_cast<uint8_t>(primitive) << 2); }

    void SetUp() override {
        std::string starrocks_home = getenv("STARROCKS_HOME");
        test_exec_dir = starrocks_home + "/be/test/formats/parquet/test_data/variant";

        _primitive_metadata_file_names = {
                "primitive_null.metadata",      "primitive_boolean_true.metadata", "primitive_boolean_false.metadata",
                "primitive_date.metadata",      "primitive_decimal4.metadata",     "primitive_decimal8.metadata",
                "primitive_decimal16.metadata", "primitive_float.metadata",        "primitive_double.metadata",
                "primitive_int8.metadata",      "primitive_int16.metadata",        "primitive_int32.metadata",
                "primitive_int64.metadata",     "primitive_binary.metadata",       "primitive_string.metadata",
                "array_primitive.metadata",
        };

        _boolean_file_names = {
                {"primitive_boolean_true.metadata", "primitive_boolean_true.value"},
                {"primitive_boolean_false.metadata", "primitive_boolean_false.value"},
        };
    }

    std::string read_file_content(const std::string& file_path) {
        FileSystem* fs = FileSystem::Default();
        auto random_access_file = *fs->new_random_access_file(file_path);
        return *random_access_file->read_all();
    }

    std::pair<std::string, std::string> load_variant_data(const std::string& metadata_file,
                                                          const std::string& value_file) {
        std::string metadata_content = read_file_content(test_exec_dir + "/" + metadata_file);
        std::string value_content = read_file_content(test_exec_dir + "/" + value_file);

        return {std::move(metadata_content), std::move(value_content)};
    }

protected:
    std::string test_exec_dir;
    std::vector<std::string> _primitive_metadata_file_names;
    std::vector<std::pair<std::string, std::string>> _boolean_file_names;
};

TEST_F(ParquetVariantTest, NullValue) {
    const uint8_t null_chars[] = {primitive_header(VariantType::NULL_TYPE)};
    VariantValue variant{std::string_view{reinterpret_cast<const char*>(null_chars), 1}};
    EXPECT_EQ(VariantType::NULL_TYPE, variant.type());
}

TEST_F(ParquetVariantTest, PrimitiveMetadata) {
    for (auto& test_file : _primitive_metadata_file_names) {
        std::string file_path = test_exec_dir + "/" + test_file;
        std::string content = read_file_content(file_path);

        std::string_view metadata_buf{content};
        EXPECT_EQ(metadata_buf, VariantMetadata::kEmptyMetadata);

        VariantMetadata metadata(metadata_buf);
        // Only test get_key if metadata was successfully created
        EXPECT_EQ(metadata.get_key(0).status().message(), "Variant index out of range: 0 >= 0");
    }

    // object metadata
    {
        std::string file_path = test_exec_dir + "/object_primitive.metadata";
        std::string content = read_file_content(file_path);

        VariantMetadata metadata(std::string_view{content});
        EXPECT_EQ("int_field", *metadata.get_key(0));
        EXPECT_EQ("double_field", *metadata.get_key(1));
        EXPECT_EQ("boolean_true_field", *metadata.get_key(2));
        EXPECT_EQ("boolean_false_field", *metadata.get_key(3));
        EXPECT_EQ("string_field", *metadata.get_key(4));
        EXPECT_EQ("null_field", *metadata.get_key(5));
        EXPECT_EQ("timestamp_field", *metadata.get_key(6));
    }
}

TEST_F(ParquetVariantTest, BooleanValue) {
    auto [t_fst, t_snd] = _boolean_file_names[0];
    auto [t_metadata, t_value] = load_variant_data(t_fst, t_snd);
    VariantValue variant_true{std::string_view(t_value)};
    EXPECT_EQ(VariantType::BOOLEAN_TRUE, variant_true.type());
    EXPECT_EQ(true, *variant_true.get_bool());

    auto [f_fst, f_snd] = _boolean_file_names[1];
    auto [f_metadata, f_value] = load_variant_data(f_fst, f_snd);
    VariantValue variant_false{std::string_view(f_value)};
    EXPECT_EQ(VariantType::BOOLEAN_FALSE, variant_false.type());
    EXPECT_EQ(false, *variant_false.get_bool());
}

TEST_F(ParquetVariantTest, IntValue) {
    // int8
    {
        auto [int8_metadata, int8_value] = load_variant_data("primitive_int8.metadata", "primitive_int8.value");
        VariantValue variant{std::string_view(int8_value)};
        EXPECT_EQ(VariantType::INT8, variant.type());
        EXPECT_EQ(42, *variant.get_int8());
        // get incorrect type
        EXPECT_ERROR(variant.get_int16());
    }
    // int16
    {
        auto [int16_metadata, int16_value] = load_variant_data("primitive_int16.metadata", "primitive_int16.value");
        VariantValue variant{std::string_view(int16_value)};
        EXPECT_EQ(VariantType::INT16, variant.type());
        EXPECT_EQ(1234, *variant.get_int16());
        // get incorrect type
        EXPECT_ERROR(variant.get_float());
    }
    // int32
    {
        auto [int32_metadata, int32_value] = load_variant_data("primitive_int32.metadata", "primitive_int32.value");
        VariantValue variant{std::string_view(int32_value)};
        EXPECT_EQ(VariantType::INT32, variant.type());
        EXPECT_EQ(123456, *variant.get_int32());
    }
    // int64
    {
        auto [int64_metadata, int64_value] = load_variant_data("primitive_int64.metadata", "primitive_int64.value");
        VariantValue variant{std::string_view(int64_value)};
        EXPECT_EQ(VariantType::INT64, variant.type());
        EXPECT_EQ(1234567890123456789, *variant.get_int64());
    }
}

TEST_F(ParquetVariantTest, FloatValue) {
    auto [float_metadata, float_value] = load_variant_data("primitive_float.metadata", "primitive_float.value");
    VariantValue variant{std::string_view(float_value)};
    EXPECT_EQ(VariantType::FLOAT, variant.type());
    EXPECT_FLOAT_EQ(1234567940.0, *variant.get_float());
    // get incorrect type
    EXPECT_ERROR(variant.get_double());
}

TEST_F(ParquetVariantTest, DoubleValue) {
    auto [double_metadata, double_value] = load_variant_data("primitive_double.metadata", "primitive_double.value");
    VariantValue variant{std::string_view(double_value)};
    EXPECT_EQ(VariantType::DOUBLE, variant.type());
    EXPECT_DOUBLE_EQ(1234567890.1234, *variant.get_double());
    // get incorrect type
    EXPECT_ERROR(variant.get_string());
}

TEST_F(ParquetVariantTest, StringValue) {
    auto [string_metadata, string_value] = load_variant_data("primitive_string.metadata", "primitive_string.value");
    VariantValue variant{std::string_view(string_value)};
    EXPECT_EQ(VariantType::STRING, variant.type());
    EXPECT_EQ(
            "This string is longer than 64 bytes and therefore does not fit in a short_string and it also includes "
            "several non ascii characters such as 🐢, 💖, ♥️, 🎣 and 🤦!!",
            *variant.get_string());

    // short string
    auto [short_string_metadata, short_string_value] = load_variant_data("short_string.metadata", "short_string.value");
    VariantValue short_string_variant{std::string_view(short_string_value)};
    EXPECT_EQ(VariantType::STRING, short_string_variant.type());
    EXPECT_EQ(VariantValue::BasicType::SHORT_STRING, short_string_variant.basic_type());
    EXPECT_EQ("Less than 64 bytes (❤️ with utf8)", *short_string_variant.get_string());
}

TEST_F(ParquetVariantTest, BinaryValue) {
    auto [binary_metadata, binary_value] = load_variant_data("primitive_binary.metadata", "primitive_binary.value");
    VariantValue variant{std::string_view(binary_value)};
    EXPECT_EQ(VariantType::BINARY, variant.type());
    const std::string_view result = *variant.get_binary();
    std::string expected;
    expected.resize(result.size() + 3);
    base64_decode("AxM33q2+78r+", &expected);
    EXPECT_EQ(expected, result);
}

TEST_F(ParquetVariantTest, DecimalValue) {
    {
        auto [decimal4_metadata, decimal4_value] =
                load_variant_data("primitive_decimal4.metadata", "primitive_decimal4.value");
        VariantValue variant{std::string_view(decimal4_value)};
        EXPECT_EQ(VariantType::DECIMAL4, variant.type());
        auto decimal4_result = *variant.get_decimal4();
        EXPECT_EQ(2, decimal4_result.scale);
        EXPECT_EQ(1234, decimal4_result.value);
        EXPECT_EQ("12.34", DecimalV3Cast::to_string<int32_t>(decimal4_result.value, 4, decimal4_result.scale));
    }
    {
        auto [decimal8_metadata, decimal8_value] =
                load_variant_data("primitive_decimal8.metadata", "primitive_decimal8.value");
        VariantValue variant{std::string_view(decimal8_value)};
        EXPECT_EQ(VariantType::DECIMAL8, variant.type());
        auto decimal8_result = *variant.get_decimal8();
        EXPECT_EQ(2, decimal8_result.scale);
        EXPECT_EQ(1234567890, decimal8_result.value);
        EXPECT_EQ("12345678.90", DecimalV3Cast::to_string<int64_t>(decimal8_result.value, 10, decimal8_result.scale));
    }
    {
        auto [decimal16_metadata, decimal16_value] =
                load_variant_data("primitive_decimal16.metadata", "primitive_decimal16.value");
        VariantValue variant{std::string_view(decimal16_value)};
        EXPECT_EQ(VariantType::DECIMAL16, variant.type());
        auto decimal16_result = *variant.get_decimal16();
        EXPECT_EQ(2, decimal16_result.scale);
        EXPECT_EQ(1234567891234567890, decimal16_result.value);
        EXPECT_EQ("12345678912345678.90",
                  DecimalV3Cast::to_string<int128_t>(decimal16_result.value, 20, decimal16_result.scale));
    }
}

TEST_F(ParquetVariantTest, UUIDValue) {
    const uint8_t uuid_chars[] = {primitive_header(VariantType::UUID),
                                  0xf2,
                                  0x4f,
                                  0x9b,
                                  0x64,
                                  0x81,
                                  0xfa,
                                  0x49,
                                  0xd1,
                                  0xb7,
                                  0x4e,
                                  0x8c,
                                  0x09,
                                  0xa6,
                                  0xe3,
                                  0x1c,
                                  0x56};

    std::string_view uuid_string(reinterpret_cast<const char*>(uuid_chars), sizeof(uuid_chars));
    VariantValue variant{uuid_string};
    EXPECT_EQ(VariantType::UUID, variant.type());
    auto uuid_result = *variant.get_uuid();
    boost::uuids::uuid uuid{};
    for (size_t i = 0; i < uuid.size(); ++i) {
        uuid.data[i] = uuid_result[i];
    }
    EXPECT_EQ("f24f9b64-81fa-49d1-b74e-8c09a6e31c56", boost::uuids::to_string(uuid));
}

TEST_F(ParquetVariantTest, TimestampValue) {
    {
        auto [ts_metadata, ts_value] = load_variant_data("primitive_timestamp.metadata", "primitive_timestamp.value");
        VariantValue variant{std::string_view(ts_value)};
        EXPECT_EQ(VariantType::TIMESTAMP_TZ, variant.type());

        // 2025-04-16 12:34:56.78-04:00
        TimestampValue tsv = TimestampValue::create(2025, 4, 16, 12, 34, 56, 780000);
        cctz::time_zone ctz;
        TimezoneUtils::find_cctz_time_zone("-04:00", ctz);
        int64_t expect_mills = tsv.to_unixtime(ctz);

        EXPECT_EQ(expect_mills * 1000, *variant.get_timestamp_micros());
    }
    {
        auto [ts_ntz_metadata, ts_ntz_value] =
                load_variant_data("primitive_timestampntz.metadata", "primitive_timestampntz.value");
        VariantValue variant{std::string_view(ts_ntz_value)};
        EXPECT_EQ(VariantType::TIMESTAMP_NTZ, variant.type());
        // 2025-04-16 12:34:56.78
        TimestampValue tsv = TimestampValue::create(2025, 4, 16, 12, 34, 56, 780000);
        EXPECT_EQ(tsv.to_unixtime() * 1000, *variant.get_timestamp_micros_ntz());
    }
}

std::string epoch_day_to_date(int32_t epoch_days) {
    std::time_t raw_time = epoch_days * 86400; // to seconds
    std::tm* ptm = std::gmtime(&raw_time);     // to UTC
    char buffer[11];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d", ptm);
    return buffer;
}

TEST_F(ParquetVariantTest, DateValue) {
    auto [date_metadata, date_value] = load_variant_data("primitive_date.metadata", "primitive_date.value");
    VariantValue variant{std::string_view(date_value)};
    EXPECT_EQ(VariantType::DATE, variant.type());
    // 2025-04-16
    EXPECT_EQ("2025-04-16", epoch_day_to_date(*variant.get_date()));

    // get incorrect type
    EXPECT_ERROR(variant.get_int32());
}

TEST_F(ParquetVariantTest, ObjectNested) {
    auto [object_metadata, object_value] = load_variant_data("object_nested.metadata", "object_nested.value");
    VariantMetadata metadata{std::string_view(object_metadata)};
    VariantValue variant{std::string_view(object_value)};
    EXPECT_EQ(VariantType::OBJECT, variant.type());
    EXPECT_EQ(3, *variant.num_elements());

    VariantValue id = *variant.get_object_by_key(metadata, "id");
    EXPECT_EQ(VariantType::INT8, id.type());
    EXPECT_EQ(1, *id.get_int8());

    VariantValue species = *variant.get_object_by_key(metadata, "species");
    EXPECT_EQ(VariantType::OBJECT, species.type());
    EXPECT_EQ(2, *species.num_elements());
    {
        auto name = species.get_object_by_key(metadata, "name");
        EXPECT_EQ(VariantType::STRING, name->type());
        EXPECT_EQ("lava monster", *name->get_string());
    }
    {
        auto population = species.get_object_by_key(metadata, "population");
        EXPECT_EQ(VariantType::INT16, population->type());
        EXPECT_EQ(6789, *population->get_int16());
    }

    VariantValue observation = *variant.get_object_by_key(metadata, "observation");
    EXPECT_EQ(VariantType::OBJECT, observation.type());
    EXPECT_EQ(3, *observation.num_elements());
    {
        auto location = observation.get_object_by_key(metadata, "location");
        EXPECT_EQ(VariantType::STRING, location->type());
        EXPECT_EQ("In the Volcano", *location->get_string());
    }
    {
        auto time = observation.get_object_by_key(metadata, "time");
        EXPECT_EQ(VariantType::STRING, time->type());
        EXPECT_EQ("12:34:56", *time->get_string());
    }
    {
        auto value = observation.get_object_by_key(metadata, "value");
        EXPECT_EQ(VariantType::OBJECT, value->type());
        EXPECT_EQ(2, *value->num_elements());
        {
            auto value_id = value->get_object_by_key(metadata, "humidity");
            EXPECT_EQ(VariantType::INT16, value_id->type());
            EXPECT_EQ(456, *value_id->get_int16());
        }
        {
            auto value_value = value->get_object_by_key(metadata, "temperature");
            EXPECT_EQ(VariantType::INT8, value_value->type());
            EXPECT_DOUBLE_EQ(123, *value_value->get_int8());
        }
    }
}

TEST_F(ParquetVariantTest, ObjectPrimitive) {
    auto [object_metadata, object_value] = load_variant_data("object_primitive.metadata", "object_primitive.value");
    VariantMetadata metadata{std::string_view(object_metadata)};
    VariantValue variant{std::string_view(object_value)};
    EXPECT_EQ(VariantType::OBJECT, variant.type());
    EXPECT_EQ(7, *variant.num_elements());

    VariantValue int_field = *variant.get_object_by_key(metadata, "int_field");
    EXPECT_EQ(VariantType::INT8, int_field.type());
    EXPECT_EQ(1, *int_field.get_int8());

    VariantValue double_field = *variant.get_object_by_key(metadata, "double_field");
    EXPECT_EQ(VariantType::DECIMAL4, double_field.type());
    auto decimal4_result = *double_field.get_decimal4();
    EXPECT_EQ("1.23456789", DecimalV3Cast::to_string<int32_t>(decimal4_result.value, 4, decimal4_result.scale));

    VariantValue boolean_true_field = *variant.get_object_by_key(metadata, "boolean_true_field");
    EXPECT_EQ(VariantType::BOOLEAN_TRUE, boolean_true_field.type());
    EXPECT_TRUE(*boolean_true_field.get_bool());

    VariantValue boolean_false_field = *variant.get_object_by_key(metadata, "boolean_false_field");
    EXPECT_EQ(VariantType::BOOLEAN_FALSE, boolean_false_field.type());
    EXPECT_FALSE(*boolean_false_field.get_bool());

    VariantValue string_field = *variant.get_object_by_key(metadata, "string_field");
    EXPECT_EQ(VariantType::STRING, string_field.type());
    EXPECT_EQ("Apache Parquet", *string_field.get_string());

    VariantValue null_field = *variant.get_object_by_key(metadata, "null_field");
    EXPECT_EQ(VariantType::NULL_TYPE, null_field.type());

    VariantValue timestamp_field = *variant.get_object_by_key(metadata, "timestamp_field");
    EXPECT_EQ(VariantType::STRING, timestamp_field.type());
    EXPECT_EQ("2025-04-16T12:34:56.78", *timestamp_field.get_string());

    EXPECT_EQ(variant.get_object_by_key(metadata, "unknow")->type(), VariantType::NULL_TYPE);
}

TEST_F(ParquetVariantTest, ObjectEmpty) {
    auto [object_empty_metadata, object_empty_value] = load_variant_data("object_empty.metadata", "object_empty.value");
    VariantMetadata metadata{std::string_view(object_empty_metadata)};
    VariantValue variant{std::string_view(object_empty_value)};
    EXPECT_EQ(VariantType::OBJECT, variant.type());
    EXPECT_EQ(0, *variant.num_elements());

    EXPECT_EQ(variant.get_object_by_key(metadata, "key")->type(), VariantType::NULL_TYPE);
    EXPECT_ERROR(variant.get_element_at_index(metadata, 0));
}

TEST_F(ParquetVariantTest, ArrayPrimitive) {
    auto [array_metadata, array_value] = load_variant_data("array_primitive.metadata", "array_primitive.value");
    VariantMetadata metadata{std::string_view(array_metadata)};
    VariantValue variant{std::string_view(array_value)};
    EXPECT_EQ(VariantType::ARRAY, variant.type());
    EXPECT_EQ(4, *variant.num_elements());

    VariantValue first_element = *variant.get_element_at_index(metadata, 0);
    EXPECT_EQ(VariantType::INT8, first_element.type());
    EXPECT_EQ(2, *first_element.get_int8());

    VariantValue second_element = *variant.get_element_at_index(metadata, 1);
    EXPECT_EQ(VariantType::INT8, second_element.type());
    EXPECT_EQ(1, *second_element.get_int8());

    VariantValue third_element = *variant.get_element_at_index(metadata, 2);
    EXPECT_EQ(VariantType::INT8, third_element.type());
    EXPECT_EQ(5, *third_element.get_int8());

    VariantValue fourth_element = *variant.get_element_at_index(metadata, 3);
    EXPECT_EQ(VariantType::INT8, fourth_element.type());
    EXPECT_EQ(9, *fourth_element.get_int8());
}

TEST_F(ParquetVariantTest, ArrayEmpty) {
    auto [array_empty_metadata, array_empty_value] = load_variant_data("array_empty.metadata", "array_empty.value");
    VariantMetadata metadata{std::string_view(array_empty_metadata)};
    VariantValue variant{std::string_view(array_empty_value)};
    EXPECT_EQ(VariantType::ARRAY, variant.type());
    EXPECT_EQ(0, *variant.num_elements());

    EXPECT_EQ(variant.get_element_at_index(metadata, 0)->type(), VariantType::NULL_TYPE);
    EXPECT_ERROR(variant.get_object_by_key(metadata, "key"));
}

TEST_F(ParquetVariantTest, ArrayNested) {
    auto [array_nested_metadata, array_nested_value] = load_variant_data("array_nested.metadata", "array_nested.value");
    VariantMetadata metadata{std::string_view(array_nested_metadata)};
    VariantValue variant{std::string_view(array_nested_value)};
    EXPECT_EQ(VariantType::ARRAY, variant.type());
    EXPECT_EQ(3, *variant.num_elements());

    {
        auto first_element = *variant.get_element_at_index(metadata, 0);
        EXPECT_EQ(VariantType::OBJECT, first_element.type());
        EXPECT_EQ(2, *first_element.num_elements());
        auto id = first_element.get_object_by_key(metadata, "id");
        EXPECT_EQ(VariantType::INT8, id->type());
        EXPECT_EQ(1, *id->get_int8());

        auto thing = first_element.get_object_by_key(metadata, "thing");
        EXPECT_EQ(VariantType::OBJECT, thing->type());
        auto names = thing->get_object_by_key(metadata, "names");
        EXPECT_EQ(VariantType::ARRAY, names->type());
        EXPECT_EQ(2, *names->num_elements());
        {
            auto first_name = *names->get_element_at_index(metadata, 0);
            EXPECT_EQ(VariantType::STRING, first_name.type());
            EXPECT_EQ("Contrarian", *first_name.get_string());
        }
        {
            auto second_name = *names->get_element_at_index(metadata, 1);
            EXPECT_EQ(VariantType::STRING, second_name.type());
            EXPECT_EQ("Spider", *second_name.get_string());
        }
    }
    {
        auto second_element = *variant.get_element_at_index(metadata, 1);
        EXPECT_EQ(VariantType::NULL_TYPE, second_element.type());
    }
    {
        auto third_element = *variant.get_element_at_index(metadata, 2);
        EXPECT_EQ(VariantType::OBJECT, third_element.type());
        EXPECT_EQ(3, *third_element.num_elements());
        auto id = third_element.get_object_by_key(metadata, "id");
        EXPECT_EQ(VariantType::INT8, id->type());
        EXPECT_EQ(2, *id->get_int8());

        auto names = third_element.get_object_by_key(metadata, "names");
        EXPECT_EQ(VariantType::ARRAY, names->type());
        EXPECT_EQ(3, *names->num_elements());
        {
            auto first_name = names->get_element_at_index(metadata, 0);
            EXPECT_EQ(VariantType::STRING, first_name->type());
            EXPECT_EQ("Apple", *first_name->get_string());
        }
        {
            auto second_name = names->get_element_at_index(metadata, 1);
            EXPECT_EQ(VariantType::STRING, second_name->type());
            EXPECT_EQ("Ray", *second_name->get_string());
        }
        {
            auto third_name = names->get_element_at_index(metadata, 2);
            EXPECT_EQ(VariantType::NULL_TYPE, third_name->type());
        }

        auto type = third_element.get_object_by_key(metadata, "type");
        EXPECT_EQ(VariantType::STRING, type->type());
        EXPECT_EQ("if", *type->get_string());
    }
}

} // namespace starrocks::parquet
