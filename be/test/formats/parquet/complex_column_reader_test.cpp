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

#include "formats/parquet/complex_column_reader.h"

#include <gtest/gtest.h>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/variant_encoder.h"

namespace starrocks::parquet {

namespace {

VariantRowValue parse_variant_json(std::string_view json_text) {
    auto row = VariantEncoder::encode_json_text_to_variant(json_text);
    EXPECT_TRUE(row.ok()) << row.status().to_string();
    return std::move(row).value();
}

std::string nullable_variant_json_at(const Column* column, size_t row) {
    auto* nullable = down_cast<const NullableColumn*>(column);
    EXPECT_NE(nullable, nullptr);
    EXPECT_FALSE(nullable->is_null(row));

    auto* variant = down_cast<const VariantColumn*>(nullable->data_column().get());
    VariantRowValue out;
    EXPECT_TRUE(variant->try_materialize_row(row, &out));

    auto json = out.to_json();
    EXPECT_TRUE(json.ok()) << json.status().to_string();
    return std::move(json).value();
}

// Build a ShreddedFieldNode for the given path.
ShreddedFieldNode make_node(const std::string& full_path, ShreddedFieldNode::Kind kind = ShreddedFieldNode::Kind::NONE) {
    ShreddedFieldNode node;
    node.name = full_path;
    node.full_path = full_path;
    auto parsed = VariantPathParser::parse_shredded_path(std::string_view(full_path));
    EXPECT_TRUE(parsed.ok()) << parsed.status().to_string();
    node.parsed_full_path = std::move(parsed).value();
    node.kind = kind;
    return node;
}

// Create a NullableColumn<BinaryColumn> with one row whose bytes are the variant value
// portion of the encoded JSON row.
ColumnPtr make_variant_value_column(std::string_view json_text) {
    auto row = parse_variant_json(json_text);
    auto value_raw = row.get_value().raw();
    auto col = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    Slice slice(value_raw.data(), value_raw.size());
    col->as_mutable_ptr()->append_datum(Datum(slice));
    return col;
}

// Create a one-row NullableColumn<BinaryColumn> that is null.
ColumnPtr make_null_binary_column() {
    auto col = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    col->as_mutable_ptr()->append_nulls(1);
    return col;
}

// Create a NullableColumn for TYPE_BIGINT with one null row.
ColumnPtr make_null_bigint_column() {
    auto col = ColumnHelper::create_column(TYPE_BIGINT_DESC, true);
    col->as_mutable_ptr()->append_nulls(1);
    return col;
}

// Metadata bytes from any VariantRowValue (all share the same root metadata).
std::string get_metadata_raw(const VariantRowValue& rv) {
    auto raw = rv.get_metadata().raw();
    return std::string(raw.data(), raw.size());
}

} // namespace

// ─── existing test ──────────────────────────────────────────────────────────

TEST(ParquetComplexColumnReaderTest, AppendVariantBindingFromStructNode) {
    ShreddedFieldNode salary_node;
    salary_node.name = "salary";
    salary_node.full_path = "profile.salary";
    auto salary_path = VariantPathParser::parse_shredded_path(std::string_view("profile.salary"));
    ASSERT_TRUE(salary_path.ok()) << salary_path.status().to_string();
    salary_node.parsed_full_path = std::move(salary_path).value();
    salary_node.kind = ShreddedFieldNode::Kind::SCALAR;
    salary_node.typed_value_read_type = std::make_unique<TypeDescriptor>(TYPE_BIGINT_DESC);
    salary_node.typed_value_column = ColumnHelper::create_column(TYPE_BIGINT_DESC, true);
    salary_node.typed_value_column->as_mutable_ptr()->append_datum(Datum(int64_t{100}));

    ShreddedFieldNode profile_node;
    profile_node.name = "profile";
    profile_node.full_path = "profile";
    auto profile_path = VariantPathParser::parse_shredded_path(std::string_view("profile"));
    ASSERT_TRUE(profile_path.ok()) << profile_path.status().to_string();
    profile_node.parsed_full_path = std::move(profile_path).value();
    profile_node.children.emplace_back(std::move(salary_node));

    auto full_row = parse_variant_json(R"({"ignored":1})");
    auto dst = NullableColumn::create(VariantColumn::create(), NullColumn::create());
    TopBinding binding{.kind = TopBinding::Kind::VARIANT,
                       .path = "profile",
                       .type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT),
                       .node = &profile_node};

    auto st = VariantColumnReader::append_variant_binding_row(
            0, binding, full_row.get_metadata().raw(),
            VariantRowRef(full_row.get_metadata().raw(), full_row.get_value().raw()), dst.get());
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(1, dst->size());
    ASSERT_EQ(R"({"salary":100})", nullable_variant_json_at(dst.get(), 0));
}

// ─── build_variant_binding_from_node ────────────────────────────────────────

// SCALAR node: typed column is null for this row, no value_column → nullopt  (line 1078)
TEST(ParquetComplexColumnReaderTest, BuildVariantBindingScalarNullTypedNoBase) {
    ShreddedFieldNode node = make_node("age", ShreddedFieldNode::Kind::SCALAR);
    node.typed_value_read_type = std::make_unique<TypeDescriptor>(TYPE_BIGINT_DESC);
    node.typed_value_column = make_null_bigint_column();
    // no value_column

    auto result = VariantColumnReader::build_variant_binding_from_node(0, node, "");
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    EXPECT_FALSE(result->has_value());
}

// NONE node, no children, has value_column → returns base  (lines 1095-1098)
TEST(ParquetComplexColumnReaderTest, BuildVariantBindingNoneNoChildHasBase) {
    auto base_row = parse_variant_json(R"({"x":42})");
    std::string metadata_raw(base_row.get_metadata().raw());

    ShreddedFieldNode node = make_node("obj");
    node.value_column = make_variant_value_column(R"({"x":42})");

    auto result = VariantColumnReader::build_variant_binding_from_node(0, node, metadata_raw);
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    ASSERT_TRUE(result->has_value());
    auto json = (*result)->to_json();
    ASSERT_TRUE(json.ok()) << json.status().to_string();
    EXPECT_EQ(R"({"x":42})", json.value());
}

// NONE node, no children, no value_column → nullopt  (line 1099)
TEST(ParquetComplexColumnReaderTest, BuildVariantBindingNoneNoChildNoBase) {
    ShreddedFieldNode node = make_node("obj");
    // no value_column, no children

    auto result = VariantColumnReader::build_variant_binding_from_node(0, node, "");
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    EXPECT_FALSE(result->has_value());
}

// NONE node with children that all return nullopt, parent has value_column → base  (lines 1117-1121)
TEST(ParquetComplexColumnReaderTest, BuildVariantBindingNoneWithChildrenAllNullHasBase) {
    auto base_row = parse_variant_json(R"({"a":1})");
    std::string metadata_raw(base_row.get_metadata().raw());

    ShreddedFieldNode child = make_node("obj.k", ShreddedFieldNode::Kind::SCALAR);
    child.typed_value_read_type = std::make_unique<TypeDescriptor>(TYPE_BIGINT_DESC);
    child.typed_value_column = make_null_bigint_column(); // null → no overlay

    ShreddedFieldNode node = make_node("obj");
    node.value_column = make_variant_value_column(R"({"a":1})");
    node.children.emplace_back(std::move(child));

    auto result = VariantColumnReader::build_variant_binding_from_node(0, node, metadata_raw);
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    ASSERT_TRUE(result->has_value());
    auto json = (*result)->to_json();
    ASSERT_TRUE(json.ok()) << json.status().to_string();
    EXPECT_EQ(R"({"a":1})", json.value());
}

// NONE node with children that all return nullopt, no value_column → nullopt  (line 1122)
TEST(ParquetComplexColumnReaderTest, BuildVariantBindingNoneWithChildrenAllNullNoBase) {
    ShreddedFieldNode child = make_node("obj.k", ShreddedFieldNode::Kind::SCALAR);
    child.typed_value_read_type = std::make_unique<TypeDescriptor>(TYPE_BIGINT_DESC);
    child.typed_value_column = make_null_bigint_column();

    ShreddedFieldNode node = make_node("obj");
    // no value_column
    node.children.emplace_back(std::move(child));

    auto result = VariantColumnReader::build_variant_binding_from_node(0, node, "");
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    EXPECT_FALSE(result->has_value());
}

// ARRAY node: typed column row is null, value_column has data → _rebuild_array_overlay
// returns the base binary  (lines 1083-1084, 823-824)
TEST(ParquetComplexColumnReaderTest, BuildVariantBindingArrayNullTypedHasBase) {
    // Encode an array variant as the base fallback.
    auto base_row = parse_variant_json(R"([1,2,3])");
    std::string metadata_raw(base_row.get_metadata().raw());

    ShreddedFieldNode node = make_node("arr", ShreddedFieldNode::Kind::ARRAY);
    // typed_value_column exists but this row is null → _rebuild_array_overlay takes the fallback path
    node.typed_value_read_type =
            std::make_unique<TypeDescriptor>(TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY));
    node.typed_value_column = make_null_binary_column();
    // value_column holds the base array binary
    node.value_column = make_variant_value_column(R"([1,2,3])");

    auto result = VariantColumnReader::build_variant_binding_from_node(0, node, metadata_raw);
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    ASSERT_TRUE(result->has_value());
}

// ARRAY node: typed column row is null, no value_column → nullopt  (line 826)
TEST(ParquetComplexColumnReaderTest, BuildVariantBindingArrayNullTypedNoBase) {
    ShreddedFieldNode node = make_node("arr", ShreddedFieldNode::Kind::ARRAY);
    node.typed_value_read_type =
            std::make_unique<TypeDescriptor>(TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY));
    node.typed_value_column = make_null_binary_column();
    // no value_column

    auto result = VariantColumnReader::build_variant_binding_from_node(0, node, "");
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    EXPECT_FALSE(result->has_value());
}

// Child path shorter than parent prefix → make_relative_variant_path error  (line 1267)
TEST(ParquetComplexColumnReaderTest, BuildVariantBindingChildPathShorterThanPrefix) {
    // Parent has path "a.b" (2 segments); child has path "a" (1 segment) which is shorter.
    ShreddedFieldNode child = make_node("a", ShreddedFieldNode::Kind::SCALAR);
    child.typed_value_read_type = std::make_unique<TypeDescriptor>(TYPE_BIGINT_DESC);
    child.typed_value_column = ColumnHelper::create_column(TYPE_BIGINT_DESC, true);
    child.typed_value_column->as_mutable_ptr()->append_datum(Datum(int64_t{1}));

    ShreddedFieldNode node = make_node("a.b");
    node.children.emplace_back(std::move(child));

    auto result = VariantColumnReader::build_variant_binding_from_node(0, node, "");
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_internal_error());
}

// ─── append_variant_binding_row ─────────────────────────────────────────────

// node != nullptr, build returns nullopt → null is appended  (lines 1308-1310)
TEST(ParquetComplexColumnReaderTest, AppendVariantBindingRowNulloptResult) {
    ShreddedFieldNode node = make_node("age", ShreddedFieldNode::Kind::SCALAR);
    node.typed_value_read_type = std::make_unique<TypeDescriptor>(TYPE_BIGINT_DESC);
    node.typed_value_column = make_null_bigint_column();

    auto full_row = parse_variant_json(R"({"age":null})");
    auto dst = NullableColumn::create(VariantColumn::create(), NullColumn::create());
    TopBinding binding{.kind = TopBinding::Kind::VARIANT,
                       .path = "age",
                       .type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT),
                       .node = &node};

    auto st = VariantColumnReader::append_variant_binding_row(
            0, binding, full_row.get_metadata().raw(),
            VariantRowRef(full_row.get_metadata().raw(), full_row.get_value().raw()), dst.get());
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(1, dst->size());
    EXPECT_TRUE(down_cast<NullableColumn*>(dst.get())->is_null(0));
}

// node == nullptr, valid path, seek succeeds → value is appended  (line 1328)
TEST(ParquetComplexColumnReaderTest, AppendVariantBindingRowNoNodeValidPath) {
    auto full_row = parse_variant_json(R"({"city":"Beijing","code":100})");
    auto dst = NullableColumn::create(VariantColumn::create(), NullColumn::create());
    TopBinding binding{.kind = TopBinding::Kind::VARIANT,
                       .path = "city",
                       .type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT),
                       .node = nullptr};

    auto st = VariantColumnReader::append_variant_binding_row(
            0, binding, full_row.get_metadata().raw(),
            VariantRowRef(full_row.get_metadata().raw(), full_row.get_value().raw()), dst.get());
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(1, dst->size());
    EXPECT_FALSE(down_cast<NullableColumn*>(dst.get())->is_null(0));
}

// node == nullptr, path string is syntactically invalid → parse error  (lines 1318-1319)
TEST(ParquetComplexColumnReaderTest, AppendVariantBindingRowNoNodeInvalidPath) {
    auto full_row = parse_variant_json(R"({"a":1})");
    auto dst = NullableColumn::create(VariantColumn::create(), NullColumn::create());
    // Bracket syntax that VariantPathParser rejects as invalid
    TopBinding binding{.kind = TopBinding::Kind::VARIANT,
                       .path = "[invalid",
                       .type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT),
                       .node = nullptr};

    auto st = VariantColumnReader::append_variant_binding_row(
            0, binding, full_row.get_metadata().raw(),
            VariantRowRef(full_row.get_metadata().raw(), full_row.get_value().raw()), dst.get());
    EXPECT_FALSE(st.ok());
}

} // namespace starrocks::parquet
