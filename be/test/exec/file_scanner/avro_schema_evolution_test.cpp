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

#include <gtest/gtest.h>

#include <avrocpp/Compiler.hh>
#include <avrocpp/GenericDatum.hh>
#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "exec/file_scanner/avro_stream_scanner.h"    // avro_record_to_columns, avro_schema_needs_evolution
#include "runtime/routine_load/data_consumer_group.h" // parse_confluent_schema_id
#include "types/type_descriptor.h"

namespace starrocks {

// --- parse_confluent_schema_id: Confluent wire-format header parsing ---

static bool parse_id(const std::vector<uint8_t>& wire, int32_t* id) {
    return parse_confluent_schema_id(reinterpret_cast<const char*>(wire.data()), wire.size(), id);
}

TEST(ParseConfluentSchemaIdTest, valid_small_id) {
    int32_t id = -99;
    ASSERT_TRUE(parse_id({0x00, 0x00, 0x00, 0x00, 0x07}, &id));
    EXPECT_EQ(7, id);
}

TEST(ParseConfluentSchemaIdTest, big_endian_order) {
    int32_t id = 0;
    // 0x00 magic + 0x00000100 == 256: proves the 4 bytes are read most-significant first.
    ASSERT_TRUE(parse_id({0x00, 0x00, 0x00, 0x01, 0x00}, &id));
    EXPECT_EQ(256, id);
}

TEST(ParseConfluentSchemaIdTest, full_four_bytes) {
    int32_t id = 0;
    ASSERT_TRUE(parse_id({0x00, 0x12, 0x34, 0x56, 0x78}, &id));
    EXPECT_EQ(0x12345678, id);
}

TEST(ParseConfluentSchemaIdTest, max_int_does_not_overflow) {
    int32_t id = 0;
    ASSERT_TRUE(parse_id({0x00, 0x7F, 0xFF, 0xFF, 0xFF}, &id));
    EXPECT_EQ(INT32_MAX, id);
}

TEST(ParseConfluentSchemaIdTest, ignores_trailing_payload) {
    int32_t id = 0;
    // 5-byte header (id == 7) followed by the avro payload; only the header is parsed.
    ASSERT_TRUE(parse_id({0x00, 0x00, 0x00, 0x00, 0x07, 0x36, 0x04, 0x68, 0x69}, &id));
    EXPECT_EQ(7, id);
}

TEST(ParseConfluentSchemaIdTest, wrong_magic_byte) {
    int32_t id = 0;
    EXPECT_FALSE(parse_id({0x01, 0x00, 0x00, 0x00, 0x07}, &id));
}

TEST(ParseConfluentSchemaIdTest, too_short) {
    int32_t id = 0;
    EXPECT_FALSE(parse_id({0x00, 0x00, 0x00, 0x07}, &id)); // 4 bytes, need at least 5
}

TEST(ParseConfluentSchemaIdTest, null_data) {
    int32_t id = 0;
    EXPECT_FALSE(parse_confluent_schema_id(nullptr, 0, &id));
}

static avro::NodePtr record_root(const char* json) {
    return avro::compileJsonSchemaFromString(json).root();
}

// --- avro_record_to_columns: serialize the whole writer schema (a full schema, never a diff) ---

TEST(AvroRecordToColumnsTest, serializes_all_top_level_fields_nullable) {
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"id","type":"long"},{"name":"name","type":"string"}]})");
    auto res = avro_record_to_columns(node);
    ASSERT_TRUE(res.ok());
    ASSERT_EQ(2u, res.value().size());
    EXPECT_EQ("id", res.value()[0].column_name);
    EXPECT_EQ("name", res.value()[1].column_name);
    // Added columns must be nullable: existing rows have no value for them.
    EXPECT_TRUE(res.value()[0].is_allow_null);
    EXPECT_TRUE(res.value()[1].is_allow_null);
}

TEST(AvroRecordToColumnsTest, preserves_nested_struct_type) {
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"addr","type":{"type":"record","name":"a","fields":[
            {"name":"city","type":"string"}]}}]})");
    auto res = avro_record_to_columns(node);
    ASSERT_TRUE(res.ok());
    ASSERT_EQ(1u, res.value().size());
    const TColumn& col = res.value()[0];
    EXPECT_EQ("addr", col.column_name);
    // The nested struct shape survives into type_desc so the FE can diff subfields and emit ADD FIELD.
    ASSERT_FALSE(col.type_desc.types.empty());
    EXPECT_EQ(TTypeNodeType::STRUCT, col.type_desc.types[0].type);
    ASSERT_EQ(1u, col.type_desc.types[0].struct_fields.size());
    EXPECT_EQ("city", col.type_desc.types[0].struct_fields[0].name);
}

// --- avro_schema_needs_evolution: recursive coverage gate (decides whether to escalate to the FE) ---

TEST(AvroSchemaNeedsEvolutionTest, all_top_level_covered_is_false) {
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"id","type":"long"},{"name":"name","type":"string"}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {{"id", TypeDescriptor(TYPE_BIGINT)},
                                                               {"name", TypeDescriptor(TYPE_VARCHAR)}};
    EXPECT_FALSE(avro_schema_needs_evolution(node, payload, {}));
}

TEST(AvroSchemaNeedsEvolutionTest, new_top_level_field_is_true) {
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"id","type":"long"},{"name":"age","type":"int"}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {{"id", TypeDescriptor(TYPE_BIGINT)}};
    EXPECT_TRUE(avro_schema_needs_evolution(node, payload, {}));
}

TEST(AvroSchemaNeedsEvolutionTest, new_nested_struct_subfield_is_true) {
    // Table has addr STRUCT<city>; the message's addr now also carries zip -> evolution needed. The old
    // top-level-only check missed this (addr is a known column); the recursive gate catches it.
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"addr","type":{"type":"record","name":"a","fields":[
            {"name":"city","type":"string"},{"name":"zip","type":"string"}]}}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {
            {"addr", TypeDescriptor::create_struct_type({"city"}, {TypeDescriptor(TYPE_VARCHAR)})}};
    EXPECT_TRUE(avro_schema_needs_evolution(node, payload, {}));
}

TEST(AvroSchemaNeedsEvolutionTest, fully_covered_nested_struct_is_false) {
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"addr","type":{"type":"record","name":"a","fields":[
            {"name":"city","type":"string"},{"name":"zip","type":"string"}]}}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {
            {"addr", TypeDescriptor::create_struct_type({"city", "zip"},
                                                        {TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR)})}};
    EXPECT_FALSE(avro_schema_needs_evolution(node, payload, {}));
}

TEST(AvroSchemaNeedsEvolutionTest, nested_subfield_under_nullable_union_is_true) {
    // addr is an optional ["null", record] in the writer schema; the union is unwrapped before the
    // subfield comparison, otherwise the new zip would be missed.
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"addr","type":["null",{"type":"record","name":"a","fields":[
            {"name":"city","type":"string"},{"name":"zip","type":"string"}]}]}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {
            {"addr", TypeDescriptor::create_struct_type({"city"}, {TypeDescriptor(TYPE_VARCHAR)})}};
    EXPECT_TRUE(avro_schema_needs_evolution(node, payload, {}));
}

TEST(AvroSchemaNeedsEvolutionTest, metadata_collision_is_true) {
    // src_part is a hidden metadata column (e.g. kafka_partition()); a payload field of the same name
    // must escalate so the FE fails the job rather than silently shadowing it.
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"id","type":"long"},{"name":"src_part","type":"int"}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {{"id", TypeDescriptor(TYPE_BIGINT)}};
    EXPECT_TRUE(avro_schema_needs_evolution(node, payload, {"src_part"}));
}

// --- avro_schema_needs_evolution: type fit (same name, changed/incompatible writer type) ---

TEST(AvroSchemaNeedsEvolutionTest, integer_widening_into_narrow_column_is_true) {
    // Field was int (column INT); writer promoted it to long -> column too narrow -> escalate.
    auto node = record_root(R"({"type":"record","name":"r","fields":[{"name":"x","type":"long"}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {{"x", TypeDescriptor(TYPE_INT)}};
    EXPECT_TRUE(avro_schema_needs_evolution(node, payload, {}));
}

TEST(AvroSchemaNeedsEvolutionTest, integer_into_wider_column_is_false) {
    // Writer int, column BIGINT: fits, no evolution (the common "declared BIGINT, send int" case).
    auto node = record_root(R"({"type":"record","name":"r","fields":[{"name":"x","type":"int"}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {{"x", TypeDescriptor(TYPE_BIGINT)}};
    EXPECT_FALSE(avro_schema_needs_evolution(node, payload, {}));
}

TEST(AvroSchemaNeedsEvolutionTest, long_into_bigint_is_false) {
    auto node = record_root(R"({"type":"record","name":"r","fields":[{"name":"x","type":"long"}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {{"x", TypeDescriptor(TYPE_BIGINT)}};
    EXPECT_FALSE(avro_schema_needs_evolution(node, payload, {}));
}

TEST(AvroSchemaNeedsEvolutionTest, double_into_float_column_is_true) {
    auto node = record_root(R"({"type":"record","name":"r","fields":[{"name":"x","type":"double"}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {{"x", TypeDescriptor(TYPE_FLOAT)}};
    EXPECT_TRUE(avro_schema_needs_evolution(node, payload, {}));
}

TEST(AvroSchemaNeedsEvolutionTest, decimal_precision_growth_is_true) {
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":12,"scale":4}}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {
            {"amt", TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 10, 2)}};
    EXPECT_TRUE(avro_schema_needs_evolution(node, payload, {}));
}

TEST(AvroSchemaNeedsEvolutionTest, decimal_that_fits_is_false) {
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":8,"scale":1}}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {
            {"amt", TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 10, 2)}};
    EXPECT_FALSE(avro_schema_needs_evolution(node, payload, {}));
}

TEST(AvroSchemaNeedsEvolutionTest, scalar_turned_struct_is_true) {
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"x","type":{"type":"record","name":"x_rec","fields":[{"name":"a","type":"long"}]}}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {{"x", TypeDescriptor(TYPE_INT)}};
    EXPECT_TRUE(avro_schema_needs_evolution(node, payload, {}));
}

TEST(AvroSchemaNeedsEvolutionTest, record_into_varchar_column_is_false) {
    // A varchar column accommodates any value (the reader JSON-encodes the record), so no escalation.
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"x","type":{"type":"record","name":"x_rec","fields":[{"name":"a","type":"long"}]}}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {{"x", TypeDescriptor(TYPE_VARCHAR)}};
    EXPECT_FALSE(avro_schema_needs_evolution(node, payload, {}));
}

TEST(AvroSchemaNeedsEvolutionTest, nested_struct_subfield_widened_is_true) {
    // addr.a was int (STRUCT<a INT>); writer promoted the subfield to long -> escalate via recursion.
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"addr","type":{"type":"record","name":"a_rec","fields":[{"name":"a","type":"long"}]}}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {
            {"addr", TypeDescriptor::create_struct_type({"a"}, {TypeDescriptor(TYPE_INT)})}};
    EXPECT_TRUE(avro_schema_needs_evolution(node, payload, {}));
}

// --- avro_value_overflows_varchar: per-value varchar/varbinary length overrun (widen signal) ---

TEST(AvroValueOverflowsVarcharTest, overlong_string_in_narrow_varchar_is_true) {
    avro::GenericDatum d(std::string("abcdef")); // length 6
    EXPECT_TRUE(avro_value_overflows_varchar(TypeDescriptor::create_varchar_type(5), d));
}

TEST(AvroValueOverflowsVarcharTest, fitting_string_is_false) {
    avro::GenericDatum d(std::string("abc")); // length 3
    EXPECT_FALSE(avro_value_overflows_varchar(TypeDescriptor::create_varchar_type(5), d));
}

TEST(AvroValueOverflowsVarcharTest, max_width_column_never_overflows) {
    avro::GenericDatum d(std::string("abcdef"));
    EXPECT_FALSE(
            avro_value_overflows_varchar(TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH), d));
}

TEST(AvroValueOverflowsVarcharTest, non_varchar_column_is_false) {
    avro::GenericDatum d(std::string("abcdef"));
    EXPECT_FALSE(avro_value_overflows_varchar(TypeDescriptor(TYPE_INT), d));
}

// --- avro_schema_needs_evolution: ARRAY recurses the element type ---

TEST(AvroSchemaNeedsEvolutionTest, array_element_widened_is_true) {
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"arr","type":{"type":"array","items":"long"}}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {
            {"arr", TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT))}};
    EXPECT_TRUE(avro_schema_needs_evolution(node, payload, {}));
}

TEST(AvroSchemaNeedsEvolutionTest, array_element_that_fits_is_false) {
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"arr","type":{"type":"array","items":"int"}}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {
            {"arr", TypeDescriptor::create_array_type(TypeDescriptor(TYPE_BIGINT))}};
    EXPECT_FALSE(avro_schema_needs_evolution(node, payload, {}));
}

TEST(AvroSchemaNeedsEvolutionTest, array_into_varchar_column_is_false) {
    // A varchar column accommodates an array (the reader JSON-encodes it), so no escalation.
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"arr","type":{"type":"array","items":"int"}}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {{"arr", TypeDescriptor(TYPE_VARCHAR)}};
    EXPECT_FALSE(avro_schema_needs_evolution(node, payload, {}));
}

// --- avro_schema_needs_evolution: MAP recurses the value type (the key is a scalar by Avro) ---

TEST(AvroSchemaNeedsEvolutionTest, map_value_widened_is_true) {
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"m","type":{"type":"map","values":"long"}}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {
            {"m", TypeDescriptor::create_map_type(TypeDescriptor::create_varchar_type(16), TypeDescriptor(TYPE_INT))}};
    EXPECT_TRUE(avro_schema_needs_evolution(node, payload, {}));
}

TEST(AvroSchemaNeedsEvolutionTest, map_value_that_fits_is_false) {
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"m","type":{"type":"map","values":"int"}}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {
            {"m",
             TypeDescriptor::create_map_type(TypeDescriptor::create_varchar_type(16), TypeDescriptor(TYPE_BIGINT))}};
    EXPECT_FALSE(avro_schema_needs_evolution(node, payload, {}));
}

TEST(AvroSchemaNeedsEvolutionTest, map_with_non_string_key_column_is_true) {
    // Avro map keys are always strings; a MAP<INT,...> column cannot take them even though the value
    // side fits, so the schema must escalate (the FE pauses the job).
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"m","type":{"type":"map","values":"int"}}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {
            {"m", TypeDescriptor::create_map_type(TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_BIGINT))}};
    EXPECT_TRUE(avro_schema_needs_evolution(node, payload, {}));
}

TEST(AvroSchemaNeedsEvolutionTest, case_only_struct_subfield_is_true) {
    // Subfield names are matched exactly here, while the FE's StructType lookup is case-insensitive;
    // the FE must treat a case-only subfield match as uncovered too, or this escalation would loop.
    auto node = record_root(R"({"type":"record","name":"r","fields":[
        {"name":"addr","type":{"type":"record","name":"a","fields":[
            {"name":"foo","type":"string"}]}}]})");
    std::unordered_map<std::string, TypeDescriptor> payload = {
            {"addr", TypeDescriptor::create_struct_type({"Foo"}, {TypeDescriptor(TYPE_VARCHAR)})}};
    EXPECT_TRUE(avro_schema_needs_evolution(node, payload, {}));
}

// --- avro_value_overflows_varchar: varbinary (bytes) length overrun ---

TEST(AvroValueOverflowsVarcharTest, overlong_bytes_in_narrow_varbinary_is_true) {
    avro::GenericDatum d(std::vector<uint8_t>{1, 2, 3, 4, 5, 6}); // length 6
    EXPECT_TRUE(avro_value_overflows_varchar(TypeDescriptor::create_varbinary_type(5), d));
}

TEST(AvroValueOverflowsVarcharTest, fitting_bytes_is_false) {
    avro::GenericDatum d(std::vector<uint8_t>{1, 2, 3}); // length 3
    EXPECT_FALSE(avro_value_overflows_varchar(TypeDescriptor::create_varbinary_type(5), d));
}

} // namespace starrocks
