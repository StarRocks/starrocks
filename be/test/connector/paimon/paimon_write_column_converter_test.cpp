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

#include "connector/hive/paimon/paimon_write_column_converter.h"

#include <cstring>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <gtest/gtest.h>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "column/variant_column.h"
#include "types/timestamp_value.h"
#include "types/variant_value.h"

namespace starrocks {
namespace {

std::string bytes(std::initializer_list<uint8_t> values) {
    std::string result;
    for (auto value : values) result.push_back(static_cast<char>(value));
    return result;
}

ColumnPtr nullable(ColumnPtr data, std::initializer_list<uint8_t> nulls) {
    auto flags = NullColumn::create();
    for (const auto flag : nulls) flags->append(flag);
    return NullableColumn::create(data, std::move(flags));
}

std::shared_ptr<arrow::Field> variant_field(bool is_nullable = true) {
    return arrow::field("v", arrow::struct_({arrow::field("value", arrow::binary(), false),
                                           arrow::field("metadata", arrow::binary(), false)}),
                        is_nullable, arrow::key_value_metadata({"paimon.extension.type"}, {"paimon.type.variant"}));
}

StatusOr<std::shared_ptr<arrow::RecordBatch>> convert_one(
        ColumnPtr column, const TypeDescriptor& type, const std::shared_ptr<arrow::Field>& target,
        const std::string& time_zone = "UTC") {
    return paimon_write_columns_to_arrow({std::move(column)}, {type}, arrow::schema({target}), time_zone,
                                          arrow::default_memory_pool());
}

TEST(PaimonWriteColumnConverterTest, PreservesNumericWidthsTargetNamesAndMetadata) {
    auto boolean = BooleanColumn::create();
    boolean->append(1);
    auto tiny = Int8Column::create();
    tiny->append(-128);
    auto small = Int16Column::create();
    small->append(-32768);
    auto integer = Int32Column::create();
    integer->append(std::numeric_limits<int32_t>::min());
    auto bigint = Int64Column::create();
    bigint->append(std::numeric_limits<int64_t>::min());
    auto real = FloatColumn::create();
    real->append(1.25f);
    auto floating = DoubleColumn::create();
    floating->append(-3.5);
    const auto schema = arrow::schema(
            {arrow::field("BOOL", arrow::boolean(), false), arrow::field("Tiny", arrow::int8(), false),
             arrow::field("Small", arrow::int16(), false), arrow::field("Int", arrow::int32(), false),
             arrow::field("Big", arrow::int64(), false), arrow::field("Float", arrow::float32(), false),
             arrow::field("Double", arrow::float64(), false)},
            arrow::key_value_metadata({"paimon.test.schema"}, {"preserved"}));
    auto batch = paimon_write_columns_to_arrow(
            {boolean, tiny, small, integer, bigint, real, floating},
            {TypeDescriptor(TYPE_BOOLEAN), TypeDescriptor(TYPE_TINYINT), TypeDescriptor(TYPE_SMALLINT),
             TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_FLOAT),
             TypeDescriptor(TYPE_DOUBLE)},
            schema, "UTC", arrow::default_memory_pool());
    ASSERT_TRUE(batch.ok()) << batch.status().to_string();
    ASSERT_EQ((*batch)->schema().get(), schema.get());
    EXPECT_TRUE((*batch)->ValidateFull().ok());
    EXPECT_EQ(static_cast<const arrow::BooleanArray&>(*(*batch)->column(0)).Value(0), true);
    EXPECT_EQ(static_cast<const arrow::Int8Array&>(*(*batch)->column(1)).Value(0), -128);
    EXPECT_EQ(static_cast<const arrow::Int16Array&>(*(*batch)->column(2)).Value(0), -32768);
    EXPECT_EQ(static_cast<const arrow::Int32Array&>(*(*batch)->column(3)).Value(0),
              std::numeric_limits<int32_t>::min());
    EXPECT_EQ(static_cast<const arrow::Int64Array&>(*(*batch)->column(4)).Value(0),
              std::numeric_limits<int64_t>::min());
    EXPECT_FLOAT_EQ(static_cast<const arrow::FloatArray&>(*(*batch)->column(5)).Value(0), 1.25f);
    EXPECT_DOUBLE_EQ(static_cast<const arrow::DoubleArray&>(*(*batch)->column(6)).Value(0), -3.5);
    EXPECT_FALSE(convert_one(integer, TypeDescriptor(TYPE_INT), arrow::field("wrong", arrow::int64())).ok());
}

TEST(PaimonWriteColumnConverterTest, ConstantNullableAndRequiredFields) {
    auto integer = Int32Column::create();
    integer->append(7);
    auto constant = ConstColumn::create(integer, 3);
    auto batch = convert_one(constant, TypeDescriptor(TYPE_INT), arrow::field("constant", arrow::int32(), false));
    ASSERT_TRUE(batch.ok()) << batch.status().to_string();
    ASSERT_EQ((*batch)->num_rows(), 3);
    EXPECT_EQ(static_cast<const arrow::Int32Array&>(*(*batch)->column(0)).Value(2), 7);
    auto null_value = nullable(integer, {1});
    EXPECT_FALSE(
            convert_one(null_value, TypeDescriptor(TYPE_INT), arrow::field("required", arrow::int32(), false)).ok());
    auto null_batch = convert_one(ConstColumn::create(null_value, 2), TypeDescriptor(TYPE_INT),
                                  arrow::field("nullable", arrow::int32(), true));
    ASSERT_TRUE(null_batch.ok()) << null_batch.status().to_string();
    EXPECT_EQ((*null_batch)->column(0)->null_count(), 2);
    EXPECT_EQ(integer->immutable_data()[0], 7);
}

TEST(PaimonWriteColumnConverterTest, DecimalPrecisionScaleAndOverflow) {
    auto decimal = Decimal64Column::create(18, 4);
    decimal->append(-123456);
    const auto type = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 4);
    auto batch = convert_one(decimal, type, arrow::field("d", arrow::decimal128(18, 4), false));
    ASSERT_TRUE(batch.ok()) << batch.status().to_string();
    const auto& array = static_cast<const arrow::Decimal128Array&>(*(*batch)->column(0));
    EXPECT_EQ(array.FormatValue(0), "-12.3456");
    EXPECT_FALSE(convert_one(decimal, type, arrow::field("wrong_scale", arrow::decimal128(18, 2))).ok());
    EXPECT_FALSE(convert_one(decimal, type, arrow::field("wrong_precision", arrow::decimal128(17, 4))).ok());
    auto overflow = Decimal32Column::create(3, 0);
    overflow->append(1000);
    EXPECT_FALSE(convert_one(overflow, TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 3, 0),
                             arrow::field("overflow", arrow::decimal128(3, 0))).ok());
}

TEST(PaimonWriteColumnConverterTest, CharacterBinaryBoundsEmbeddedNulsAndUtf8) {
    auto text = BinaryColumn::create();
    text->append(Slice("ab"));
    const auto field = arrow::field("C", arrow::utf8(), false,
                                    arrow::key_value_metadata({"starrocks.paimon.length"}, {"2"}));
    auto batch = convert_one(text, TypeDescriptor::create_char_type(4), field);
    ASSERT_TRUE(batch.ok()) << batch.status().to_string();
    EXPECT_EQ(static_cast<const arrow::StringArray&>(*(*batch)->column(0)).GetString(0), "ab");
    text->append(Slice("abc"));
    EXPECT_FALSE(convert_one(text, TypeDescriptor::create_varchar_type(4), field).ok());
    const std::string payload("a\0b", 3);
    auto binary = BinaryColumn::create();
    binary->append(Slice(payload));
    auto binary_batch = convert_one(binary, TypeDescriptor::create_varbinary_type(3),
                                    arrow::field("b", arrow::binary(), false));
    ASSERT_TRUE(binary_batch.ok()) << binary_batch.status().to_string();
    EXPECT_EQ(static_cast<const arrow::BinaryArray&>(*(*binary_batch)->column(0)).GetString(0), payload);
    EXPECT_FALSE(
            convert_one(binary, TypeDescriptor::create_varbinary_type(2), arrow::field("b", arrow::binary())).ok());
    auto invalid = BinaryColumn::create();
    const auto invalid_bytes = bytes({0xc0, 0x80});
    invalid->append(Slice(invalid_bytes));
    EXPECT_FALSE(convert_one(invalid, TypeDescriptor::create_varchar_type(4), arrow::field("s", arrow::utf8())).ok());
    auto utf8 = BinaryColumn::create();
    const std::string multibyte = "\xe4\xb8\xad";
    utf8->append(Slice(multibyte));
    EXPECT_FALSE(convert_one(utf8, TypeDescriptor::create_varchar_type(2), arrow::field("s", arrow::utf8())).ok());
}

TEST(PaimonWriteColumnConverterTest, DateNtzLtzNegativeEpochAndCheckedNanos) {
    auto dates = DateColumn::create();
    dates->append(DateValue::create(1969, 12, 31));
    auto date_batch = convert_one(dates, TypeDescriptor(TYPE_DATE), arrow::field("day", arrow::date32(), false));
    ASSERT_TRUE(date_batch.ok()) << date_batch.status().to_string();
    EXPECT_EQ(static_cast<const arrow::Date32Array&>(*(*date_batch)->column(0)).Value(0), -1);
    auto timestamp = TimestampColumn::create();
    timestamp->append(TimestampValue::create(1969, 12, 31, 23, 59, 59, 999999));
    for (const auto unit : {arrow::TimeUnit::SECOND, arrow::TimeUnit::MILLI, arrow::TimeUnit::MICRO,
                            arrow::TimeUnit::NANO}) {
        auto batch = convert_one(timestamp, TypeDescriptor(TYPE_DATETIME), arrow::field("t", arrow::timestamp(unit)),
                                 "Asia/Shanghai");
        ASSERT_TRUE(batch.ok()) << batch.status().to_string();
        EXPECT_EQ(static_cast<const arrow::TimestampArray&>(*(*batch)->column(0)).Value(0),
                  unit == arrow::TimeUnit::NANO ? -1000 : -1);
    }
    auto wall = TimestampColumn::create();
    wall->append(TimestampValue::create(1970, 1, 1, 8, 0, 0));
    auto ltz = convert_one(wall, TypeDescriptor(TYPE_DATETIME),
                           arrow::field("instant", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")), "Asia/Shanghai");
    ASSERT_TRUE(ltz.ok()) << ltz.status().to_string();
    EXPECT_EQ(static_cast<const arrow::TimestampArray&>(*(*ltz)->column(0)).Value(0), 0);
    auto ntz = convert_one(wall, TypeDescriptor(TYPE_DATETIME),
                           arrow::field("wall", arrow::timestamp(arrow::TimeUnit::MICRO)), "Asia/Shanghai");
    ASSERT_TRUE(ntz.ok()) << ntz.status().to_string();
    EXPECT_EQ(static_cast<const arrow::TimestampArray&>(*(*ntz)->column(0)).Value(0), 8LL * 3600 * 1000000);
    auto far = TimestampColumn::create();
    far->append(TimestampValue::create(9999, 12, 31, 23, 59, 59));
    EXPECT_FALSE(convert_one(far, TypeDescriptor(TYPE_DATETIME),
                             arrow::field("nanos", arrow::timestamp(arrow::TimeUnit::NANO))).ok());
    auto dst_gap = TimestampColumn::create();
    dst_gap->append(TimestampValue::create(2024, 3, 10, 2, 30, 0));
    EXPECT_FALSE(convert_one(dst_gap, TypeDescriptor(TYPE_DATETIME),
                             arrow::field("ltz", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
                             "America/Los_Angeles").ok());
    EXPECT_FALSE(convert_one(wall, TypeDescriptor(TYPE_DATETIME),
                             arrow::field("ltz", arrow::timestamp(arrow::TimeUnit::MICRO, "bad/zone"))).ok());
}

TEST(PaimonWriteColumnConverterTest, NativeTimestampPrecisionIsNotInferredOnlyFromArrowUnit) {
    auto timestamp = TimestampColumn::create();
    timestamp->append(TimestampValue::create(1969, 12, 31, 23, 59, 59, 120000));
    for (int precision = 0; precision <= 9; ++precision) {
        auto unit = precision == 0 ? arrow::TimeUnit::SECOND
                : precision <= 3 ? arrow::TimeUnit::MILLI
                : precision <= 6 ? arrow::TimeUnit::MICRO : arrow::TimeUnit::NANO;
        const auto target = arrow::field("t", arrow::timestamp(unit), true,
                arrow::key_value_metadata({"starrocks.paimon.timestamp.precision"}, {std::to_string(precision)}));
        auto result = convert_one(timestamp, TypeDescriptor(TYPE_DATETIME), target);
        if (precision < 2) {
            EXPECT_FALSE(result.ok());
        } else {
            ASSERT_TRUE(result.ok()) << result.status().to_string();
        }
    }
    auto fraction = TimestampColumn::create();
    fraction->append(TimestampValue::create(1969, 12, 31, 23, 59, 59, 999999));
    const auto precision_five = arrow::field("t", arrow::timestamp(arrow::TimeUnit::MICRO), true,
            arrow::key_value_metadata({"starrocks.paimon.timestamp.precision"}, {"5"}));
    EXPECT_FALSE(convert_one(fraction, TypeDescriptor(TYPE_DATETIME), precision_five).ok());
    const auto wrong_unit = arrow::field("t", arrow::timestamp(arrow::TimeUnit::MICRO), true,
            arrow::key_value_metadata({"starrocks.paimon.timestamp.precision"}, {"2"}));
    EXPECT_FALSE(convert_one(timestamp, TypeDescriptor(TYPE_DATETIME), wrong_unit).ok());
}

TEST(PaimonWriteColumnConverterTest, RecursiveStructNamesTargetOrderAndNullability) {
    auto numbers = Int32Column::create();
    numbers->append(42);
    auto strings = BinaryColumn::create();
    strings->append(Slice("value"));
    auto source = StructColumn::create(Columns{numbers, strings}, {"Number", "TEXT"});
    const auto type = TypeDescriptor::create_struct_type(
            {"Number", "TEXT"}, {TypeDescriptor(TYPE_INT), TypeDescriptor::create_varchar_type(8)});
    const auto target = arrow::field("s", arrow::struct_({arrow::field("text", arrow::utf8(), false),
                                                         arrow::field("NUMBER", arrow::int32(), false)}), false);
    auto batch = convert_one(source, type, target);
    ASSERT_TRUE(batch.ok()) << batch.status().to_string();
    const auto& output = static_cast<const arrow::StructArray&>(*(*batch)->column(0));
    EXPECT_EQ(static_cast<const arrow::StringArray&>(*output.field(0)).GetString(0), "value");
    EXPECT_EQ(static_cast<const arrow::Int32Array&>(*output.field(1)).Value(0), 42);
    EXPECT_EQ(output.type()->field(0)->name(), "text");
    EXPECT_FALSE(convert_one(source, TypeDescriptor::create_struct_type({"x", "X"}, type.children), target).ok());
    EXPECT_FALSE(convert_one(source, type,
                             arrow::field("s", arrow::struct_({arrow::field("x", arrow::utf8()),
                                                              arrow::field("number", arrow::int32())}))).ok());
}

TEST(PaimonWriteColumnConverterTest, NullAncestorSkipsInvalidRequiredChildrenWithoutMutation) {
    auto malformed = BinaryColumn::create();
    malformed->append(Slice("not an int"));
    // The physical child's type disagrees with its descriptor. No child access is legal when
    // the nullable struct ancestor is NULL; output still needs valid, non-null placeholders.
    auto source = StructColumn::create(Columns{malformed}, {"required"});
    const auto type = TypeDescriptor::create_struct_type({"required"}, {TypeDescriptor(TYPE_INT)});
    auto wrapped = nullable(source, {1});
    auto batch = convert_one(
            wrapped, type,
            arrow::field("s", arrow::struct_({arrow::field("REQUIRED", arrow::int32(), false)}), true));
    ASSERT_TRUE(batch.ok()) << batch.status().to_string();
    const auto& result = static_cast<const arrow::StructArray&>(*(*batch)->column(0));
    EXPECT_TRUE(result.IsNull(0));
    EXPECT_EQ(result.field(0)->null_count(), 0);
    EXPECT_EQ(static_cast<const arrow::Int32Array&>(*result.field(0)).Value(0), 0);
    EXPECT_EQ(malformed->get_slice(0).to_string(), "not an int");
    EXPECT_FALSE(convert_one(source, type,
                             arrow::field("s", arrow::struct_({arrow::field("required", arrow::int32(), false)})))
                         .ok());
}

TEST(PaimonWriteColumnConverterTest, ArraysMapsEmptyContainersAndHiddenInvalidOffsets) {
    auto elements = Int32Column::create();
    elements->append(1);
    elements->append(2);
    auto offsets = UInt32Column::create();
    for (uint32_t value : {0, 0, 2, 99}) offsets->append(value);
    auto source = ArrayColumn::create(nullable(elements, {0, 0}), offsets);
    auto wrapped = nullable(source, {0, 0, 1});
    const auto type = TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT));
    auto batch = convert_one(wrapped, type,
                             arrow::field("a", arrow::list(arrow::field("element", arrow::int32(), false))));
    ASSERT_TRUE(batch.ok()) << batch.status().to_string();
    const auto& result = static_cast<const arrow::ListArray&>(*(*batch)->column(0));
    EXPECT_EQ(result.value_length(0), 0);
    EXPECT_EQ(result.value_length(1), 2);
    EXPECT_TRUE(result.IsNull(2));
    EXPECT_EQ(offsets->immutable_data()[3], 99);
    EXPECT_FALSE(convert_one(source, type, arrow::field("a", arrow::list(arrow::int32()))).ok());

    auto keys = Int32Column::create();
    keys->append(8);
    auto values = BinaryColumn::create();
    values->append(Slice("v"));
    auto map_offsets = UInt32Column::create();
    for (uint32_t value : {0, 0, 1}) map_offsets->append(value);
    auto map = MapColumn::create(nullable(keys, {0}), nullable(values, {0}), map_offsets);
    const auto map_type =
            TypeDescriptor::create_map_type(TypeDescriptor(TYPE_INT), TypeDescriptor::create_varchar_type(4));
    auto map_batch = convert_one(map, map_type, arrow::field("m", arrow::map(arrow::int32(), arrow::utf8())));
    ASSERT_TRUE(map_batch.ok()) << map_batch.status().to_string();
    const auto& map_result = static_cast<const arrow::MapArray&>(*(*map_batch)->column(0));
    EXPECT_EQ(map_result.value_length(0), 0);
    EXPECT_EQ(map_result.value_length(1), 1);
    auto null_keys = nullable(keys, {1});
    auto bad_map = MapColumn::create(null_keys, nullable(values, {0}), map_offsets);
    EXPECT_FALSE(convert_one(bad_map, map_type, arrow::field("m", arrow::map(arrow::int32(), arrow::utf8()))).ok());
}

TEST(PaimonWriteColumnConverterTest, MaterializedVariantPreservesBytesIncludingNonmonotonicObject) {
    const auto metadata = bytes({0x11, 2, 0, 1, 2, 'a', 'b'});
    // Field-id order a,b, physical payload order b,a. This is legal public Variant encoding.
    const auto value = bytes({2, 2, 0, 1, 2, 0, 4, 12, 22, 12, 11});
    auto variant = VariantColumn::create();
    variant->append(VariantRowValue(metadata, value));
    auto batch = convert_one(variant, TypeDescriptor::create_variant_type(), variant_field());
    ASSERT_TRUE(batch.ok()) << batch.status().to_string();
    const auto& result = static_cast<const arrow::StructArray&>(*(*batch)->column(0));
    EXPECT_EQ(static_cast<const arrow::BinaryArray&>(*result.field(0)).GetString(0), value);
    EXPECT_EQ(static_cast<const arrow::BinaryArray&>(*result.field(1)).GetString(0), metadata);
    EXPECT_EQ(variant->remain_value_column()->get_slice(0).to_string(), value);
    EXPECT_EQ(variant->metadata_column()->get_slice(0).to_string(), metadata);
}

TEST(PaimonWriteColumnConverterTest, VariantSqlNullIsDistinctFromVariantNullAndEmptyBase) {
    auto variant = VariantColumn::create();
    variant->append(VariantRowValue::from_null());
    variant->append(VariantRowValue::from_null());
    auto batch = convert_one(nullable(variant, {0, 1}), TypeDescriptor::create_variant_type(), variant_field());
    ASSERT_TRUE(batch.ok()) << batch.status().to_string();
    const auto& result = static_cast<const arrow::StructArray&>(*(*batch)->column(0));
    EXPECT_TRUE(result.IsValid(0));
    EXPECT_TRUE(result.IsNull(1));
    EXPECT_EQ(result.field(0)->null_count(), 0);
    EXPECT_EQ(result.field(1)->null_count(), 0);
    EXPECT_EQ(static_cast<const arrow::BinaryArray&>(*result.field(0)).GetString(0), bytes({0}));
    auto empty = VariantColumn::create();
    empty->append_shredded(Slice(""), Slice(""));
    EXPECT_FALSE(convert_one(empty, TypeDescriptor::create_variant_type(), variant_field()).ok());
    EXPECT_TRUE(convert_one(nullable(empty, {1}), TypeDescriptor::create_variant_type(), variant_field()).ok());
}

TEST(PaimonWriteColumnConverterTest, VariantRejectsMalformedMetadataValuesAndUnsupportedPublicTags) {
    const auto valid_metadata = std::string(VariantMetadata::kEmptyMetadata);
    const auto valid_object_metadata = bytes({0x11, 2, 0, 1, 2, 'a', 'b'});
    const std::vector<std::pair<std::string, std::string>> invalid = {
            {bytes({1, 1, 0, 8, 'a'}), bytes({0})}, // metadata end offset outside bytes
            {bytes({0x11, 2, 0, 1, 2, 'b', 'a'}), bytes({0})}, // sorted flag violated
            {valid_metadata, bytes({12})}, // truncated int8
            {valid_metadata, bytes({0, 0})}, // primitive trailing bytes
            {valid_metadata, bytes({16 << 2, 1, 0, 0, 0, 0xff})}, // invalid UTF-8
            {valid_metadata, bytes({17 << 2, 0, 0, 0, 0, 0, 0, 0, 0})}, // unsupported public TIME_NTZ
            {valid_metadata, bytes({3, 1, 0, 2, 12})}, // malformed array extent
            {valid_object_metadata, bytes({2, 2, 0, 1, 0, 0, 2, 12, 1})}, // duplicate object offset
            {valid_object_metadata, bytes({2, 1, 2, 0, 1, 0})}, // out-of-range dictionary id
    };
    for (const auto& [metadata, value] : invalid) {
        auto variant = VariantColumn::create();
        variant->append_shredded(Slice(metadata), Slice(value));
        EXPECT_FALSE(convert_one(variant, TypeDescriptor::create_variant_type(), variant_field()).ok());
    }
    auto valid = VariantColumn::create();
    valid->append(VariantRowValue::from_null());
    const auto reversed = arrow::field("v", arrow::struct_({arrow::field("metadata", arrow::binary(), false),
                                                            arrow::field("value", arrow::binary(), false)}), true,
                                       arrow::key_value_metadata({"paimon.extension.type"}, {"paimon.type.variant"}));
    EXPECT_FALSE(convert_one(valid, TypeDescriptor::create_variant_type(), reversed).ok());
}

TEST(PaimonWriteColumnConverterTest, VariantFullInt128PayloadIsNotNarrowedToDecimal38) {
    // These assertions establish exact byte preservation, not Java/Paimon decimal semantic
    // compatibility. The public Java protocol boundary must be validated by the integration tests.
    for (const uint8_t last : {uint8_t{0x7f}, uint8_t{0x80}}) {
        std::string value(18, static_cast<char>(last == 0x7f ? 0xff : 0));
        value[0] = 10 << 2;
        value[1] = 0;
        value[17] = static_cast<char>(last);
        auto variant = VariantColumn::create();
        variant->append(VariantRowValue(VariantMetadata::kEmptyMetadata, value));
        auto batch = convert_one(variant, TypeDescriptor::create_variant_type(), variant_field(false));
        ASSERT_TRUE(batch.ok()) << batch.status().to_string();
        const auto& result = static_cast<const arrow::StructArray&>(*(*batch)->column(0));
        EXPECT_EQ(static_cast<const arrow::BinaryArray&>(*result.field(0)).GetString(0), value);
    }
}

TEST(PaimonWriteColumnConverterTest, ShreddedVariantStrictlyMaterializesWithoutLosingOverlays) {
    auto typed = Int64Column::create();
    typed->append(42);
    MutableColumns typed_columns;
    typed_columns.emplace_back(std::move(typed));
    auto variant = VariantColumn::create();
    variant->set_shredded_columns({"a"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed_columns), nullptr, nullptr);
    auto batch = convert_one(variant, TypeDescriptor::create_variant_type(), variant_field());
    ASSERT_TRUE(batch.ok()) << batch.status().to_string();
    const auto& structure = static_cast<const arrow::StructArray&>(*(*batch)->column(0));
    const auto value = static_cast<const arrow::BinaryArray&>(*structure.field(0)).GetString(0);
    const auto metadata = static_cast<const arrow::BinaryArray&>(*structure.field(1)).GetString(0);
    VariantRowValue materialized(metadata, value);
    auto json = materialized.to_json();
    ASSERT_TRUE(json.ok()) << json.status().to_string();
    EXPECT_EQ("{\"a\":42}", *json);
    EXPECT_FALSE(variant->has_metadata_column());
    EXPECT_FALSE(variant->has_remain_value());
    EXPECT_EQ(42, variant->typed_columns()[0]->get(0).get_int64());
    // SQL NULL ancestors do not require materializing their inaccessible shredded payloads.
    EXPECT_TRUE(convert_one(nullable(variant, {1}), TypeDescriptor::create_variant_type(), variant_field()).ok());
}

TEST(PaimonWriteColumnConverterTest, InvalidArgumentsAndZeroRows) {
    auto empty = Int32Column::create();
    auto batch = convert_one(empty, TypeDescriptor(TYPE_INT), arrow::field("empty", arrow::int32(), false));
    ASSERT_TRUE(batch.ok()) << batch.status().to_string();
    EXPECT_EQ((*batch)->num_rows(), 0);
    EXPECT_FALSE(
            paimon_write_columns_to_arrow({empty}, {}, arrow::schema({}), "UTC", arrow::default_memory_pool()).ok());
    EXPECT_FALSE(paimon_write_columns_to_arrow({empty}, {TypeDescriptor(TYPE_INT)}, nullptr, "UTC",
                                               arrow::default_memory_pool()).ok());
    EXPECT_FALSE(paimon_write_columns_to_arrow({empty}, {TypeDescriptor(TYPE_INT)},
                                               arrow::schema({arrow::field("i", arrow::int32())}), "UTC", nullptr)
                         .ok());
    EXPECT_FALSE(paimon_write_chunk_to_arrow(nullptr, {}, arrow::schema({}), "UTC", arrow::default_memory_pool()).ok());
}

} // namespace
} // namespace starrocks
