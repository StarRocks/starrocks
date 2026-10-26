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

#include "base/testutil/assert.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/global_dict/types_fwd_decl.h"
#include "column/nullable_column.h"
#include "column/variant_encoder.h"
#include "column/variant_path_parser.h"
#include "common/object_pool.h"
#include "formats/parquet/column_reader_factory.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/scalar_column_reader.h"
#include "formats/parquet/schema.h"
#include "storage_primitive/column_predicate_factory.h"
#include "storage_primitive/predicate_tree/predicate_tree_fwd.h"
#include "types/type_descriptor.h"
#include "types/type_info.h"
#include "types/variant.h"

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
ShreddedFieldNode make_node(const std::string& full_path,
                            ShreddedFieldNode::Kind kind = ShreddedFieldNode::Kind::NONE) {
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

class FailingConvertPredicate final : public ColumnPredicate {
public:
    explicit FailingConvertPredicate(const TypeInfoPtr& type_info) : ColumnPredicate(type_info, 0) {}

    Status evaluate(const Column*, uint8_t*, uint16_t, uint16_t) const override {
        return Status::NotSupported("test only");
    }
    Status evaluate_and(const Column*, uint8_t*, uint16_t, uint16_t) const override {
        return Status::NotSupported("test only");
    }
    Status evaluate_or(const Column*, uint8_t*, uint16_t, uint16_t) const override {
        return Status::NotSupported("test only");
    }
    bool can_vectorized() const override { return true; }
    PredicateType type() const override { return PredicateType::kGT; }
    Status convert_to(const ColumnPredicate**, const TypeInfoPtr&, ObjectPool*) const override {
        return Status::NotSupported("failing test predicate");
    }
};

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

// ARRAY node: typed column row is null and value_column holds a variant null (a JSON null at the array path).
// The null is a value of the field and must be kept: its bytes equal VariantValue::kEmptyValue, which used to be
// mistaken for "no value" and dropped the field from the rebuilt row.
TEST(ParquetComplexColumnReaderTest, BuildVariantBindingArrayNullTypedVariantNullBase) {
    auto base_row = parse_variant_json("null");
    std::string metadata_raw(base_row.get_metadata().raw());

    ShreddedFieldNode node = make_node("arr", ShreddedFieldNode::Kind::ARRAY);
    node.typed_value_read_type =
            std::make_unique<TypeDescriptor>(TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY));
    node.typed_value_column = make_null_binary_column();
    node.value_column = make_variant_value_column("null");

    auto result = VariantColumnReader::build_variant_binding_from_node(0, node, metadata_raw);
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    ASSERT_TRUE(result->has_value());
    EXPECT_EQ("null", (*result)->to_json().value());
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
                       .node = nullptr,
                       .parsed_path = VariantPath({VariantSegment::make_object("city")})};

    auto st = VariantColumnReader::append_variant_binding_row(
            0, binding, full_row.get_metadata().raw(),
            VariantRowRef(full_row.get_metadata().raw(), full_row.get_value().raw()), dst.get());
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(1, dst->size());
    EXPECT_FALSE(down_cast<NullableColumn*>(dst.get())->is_null(0));
}

// ─── encode_datum error paths ───────────────────────────────────────────────

// Helper: TYPE_DECIMAL32 with scale=-1, which encode_datum rejects.
static TypeDescriptor invalid_decimal32_type() {
    TypeDescriptor t;
    t.type = TYPE_DECIMAL32;
    t.scale = -1;
    return t;
}

// Helper: NullableColumn<ArrayColumn> wrapping one row of a given DatumArray.
static ColumnPtr make_typed_array_column(const DatumArray& elems) {
    auto elements = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto offsets = UInt32Column::create();
    auto array_col = ArrayColumn::create(std::move(elements), std::move(offsets));
    array_col->append_datum(elems);
    auto nullable = NullableColumn::create(std::move(array_col), NullColumn::create());
    nullable->null_column_data().push_back(0);
    return nullable;
}

// SCALAR node: encode_datum fails (invalid decimal type) → error returned  (lines 1069-1070)
TEST(ParquetComplexColumnReaderTest, BuildVariantBindingScalarEncodeFail) {
    ShreddedFieldNode node = make_node("x", ShreddedFieldNode::Kind::SCALAR);
    node.typed_value_read_type = std::make_unique<TypeDescriptor>(invalid_decimal32_type());
    node.typed_value_column = ColumnHelper::create_column(TYPE_BIGINT_DESC, true);
    node.typed_value_column->as_mutable_ptr()->append_datum(Datum(int64_t{1}));

    auto result = VariantColumnReader::build_variant_binding_from_node(0, node, "");
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_invalid_argument());
}

// append_variant_binding_row: node causes encode error → error propagated  (lines 1305-1306)
TEST(ParquetComplexColumnReaderTest, AppendVariantBindingRowNodeEncodeFail) {
    ShreddedFieldNode node = make_node("x", ShreddedFieldNode::Kind::SCALAR);
    node.typed_value_read_type = std::make_unique<TypeDescriptor>(invalid_decimal32_type());
    node.typed_value_column = ColumnHelper::create_column(TYPE_BIGINT_DESC, true);
    node.typed_value_column->as_mutable_ptr()->append_datum(Datum(int64_t{1}));

    auto full_row = parse_variant_json(R"({"x":1})");
    auto dst = NullableColumn::create(VariantColumn::create(), NullColumn::create());
    TopBinding binding{.kind = TopBinding::Kind::VARIANT,
                       .path = "x",
                       .type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT),
                       .node = &node};

    auto st = VariantColumnReader::append_variant_binding_row(
            0, binding, full_row.get_metadata().raw(),
            VariantRowRef(full_row.get_metadata().raw(), full_row.get_value().raw()), dst.get());
    EXPECT_FALSE(st.ok());
}

// ARRAY node Path 2 (scalar array): element type is invalid → encode fails  (lines 951-952, 1087-1089)
TEST(ParquetComplexColumnReaderTest, BuildVariantBindingArrayPathTwoEncodeFail) {
    ShreddedFieldNode node = make_node("arr", ShreddedFieldNode::Kind::ARRAY);
    TypeDescriptor arr_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
    arr_type.children.push_back(invalid_decimal32_type());
    node.typed_value_read_type = std::make_unique<TypeDescriptor>(arr_type);
    node.typed_value_column = make_typed_array_column(DatumArray{Datum(int32_t(1))});
    // no children → Path 2

    auto result = VariantColumnReader::build_variant_binding_from_node(0, node, VariantMetadata::kEmptyMetadata);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_invalid_argument());
}

// ARRAY node Path 1: SCALAR child has encode error  (lines 1005-1006, 1087-1089)
TEST(ParquetComplexColumnReaderTest, BuildVariantBindingArrayScalarChildEncodeFail) {
    ShreddedFieldNode scalar_child = make_node("arr.item.x", ShreddedFieldNode::Kind::SCALAR);
    scalar_child.typed_value_read_type = std::make_unique<TypeDescriptor>(invalid_decimal32_type());
    scalar_child.typed_value_column = ColumnHelper::create_column(TYPE_BIGINT_DESC, true);
    scalar_child.typed_value_column->as_mutable_ptr()->append_datum(Datum(int64_t{42}));

    ShreddedFieldNode node = make_node("arr", ShreddedFieldNode::Kind::ARRAY);
    TypeDescriptor arr_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
    arr_type.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    node.typed_value_read_type = std::make_unique<TypeDescriptor>(arr_type);
    node.typed_value_column = make_typed_array_column(DatumArray{Datum(int32_t(1))});
    node.children.emplace_back(std::move(scalar_child));

    auto result = VariantColumnReader::build_variant_binding_from_node(0, node, VariantMetadata::kEmptyMetadata);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_invalid_argument());
}

// ARRAY node Path 1: NONE child has its own children → grandchild recursion  (line 1043)
TEST(ParquetComplexColumnReaderTest, BuildVariantBindingArrayNoneChildWithGrandchildren) {
    // Grandchild: SCALAR, typed_value_column is null → produces no overlay
    ShreddedFieldNode grandchild = make_node("arr.item.sub.leaf", ShreddedFieldNode::Kind::SCALAR);
    grandchild.typed_value_read_type = std::make_unique<TypeDescriptor>(TYPE_BIGINT_DESC);
    // no typed_value_column → get_non_null_data returns false

    // Child: NONE with grandchild
    ShreddedFieldNode child = make_node("arr.item.sub");
    child.children.emplace_back(std::move(grandchild));

    ShreddedFieldNode node = make_node("arr", ShreddedFieldNode::Kind::ARRAY);
    TypeDescriptor arr_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
    arr_type.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    node.typed_value_read_type = std::make_unique<TypeDescriptor>(arr_type);
    node.typed_value_column = make_typed_array_column(DatumArray{Datum(int32_t(1))});
    node.children.emplace_back(std::move(child));

    auto result = VariantColumnReader::build_variant_binding_from_node(0, node, VariantMetadata::kEmptyMetadata);
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    // One null element in the array (no overlays produced)
    ASSERT_TRUE(result->has_value());
}

// node == nullptr, seek on primitive variant (non-object) with key path → seek fails  (lines 1324-1325)
TEST(ParquetComplexColumnReaderTest, AppendVariantBindingRowSeekFail) {
    // A primitive integer row, not an object; seeking key "name" on a non-object value
    // is treated as "path not found" (null result) rather than a hard error.
    // This matches the behaviour of variant_path_reader.cpp which silently returns kMissing.
    auto full_row = parse_variant_json("42");
    auto dst = NullableColumn::create(VariantColumn::create(), NullColumn::create());
    TopBinding binding{.kind = TopBinding::Kind::VARIANT,
                       .path = "name",
                       .type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT),
                       .node = nullptr,
                       .parsed_path = VariantPath({VariantSegment::make_object("name")})};

    auto st = VariantColumnReader::append_variant_binding_row(
            0, binding, full_row.get_metadata().raw(),
            VariantRowRef(full_row.get_metadata().raw(), full_row.get_value().raw()), dst.get());
    EXPECT_TRUE(st.ok());
    ASSERT_EQ(1, dst->size());
    EXPECT_TRUE(dst->is_null(0));
}

// ─── VariantVirtualZoneMapReader tests ───────────────────────────────────────
//
// Builds a VariantColumnReader with a single shredded INT32 "age" leaf:
//
//   data (STRUCT)
//     metadata (BYTE_ARRAY, col 0)
//     value    (BYTE_ARRAY, col 1)
//     typed_value (STRUCT)
//       age (STRUCT)
//         value       (BYTE_ARRAY, col 2)
//         typed_value (INT32,      col 3)  <-- zone-map leaf
//
// When stats_on_age_chunk=true, col 3 gets INT32 statistics [20, 24].
// When age_value_all_null=true, the fallback `value` payload for this shredded path is
// marked all-null in the row-group metadata so typed_value statistics are safe to use.
// Caller owns `root_field`: ColumnReaderFactory stores raw pointers into the field tree,
// so it must outlive the returned reader.
static StatusOr<std::unique_ptr<ColumnReader>> make_shredded_variant_reader(
        tparquet::RowGroup& rg, ColumnReaderOptions& opts, ParquetField& root_field, bool stats_on_age_chunk = false,
        bool age_value_all_null = false, bool include_age_value = true) {
    ParquetField meta_f;
    meta_f.name = "metadata";
    meta_f.type = ColumnType::SCALAR;
    meta_f.physical_type = tparquet::Type::BYTE_ARRAY;
    meta_f.physical_column_index = 0;

    ParquetField val_f;
    val_f.name = "value";
    val_f.type = ColumnType::SCALAR;
    val_f.physical_type = tparquet::Type::BYTE_ARRAY;
    val_f.physical_column_index = 1;

    ParquetField age_val_f;
    age_val_f.name = "value";
    age_val_f.type = ColumnType::SCALAR;
    age_val_f.physical_type = tparquet::Type::BYTE_ARRAY;
    age_val_f.physical_column_index = 2;

    ParquetField age_typed_f;
    age_typed_f.name = "typed_value";
    age_typed_f.type = ColumnType::SCALAR;
    age_typed_f.physical_type = tparquet::Type::INT32;
    age_typed_f.physical_column_index = 3;
    {
        tparquet::IntType int_type;
        int_type.__set_bitWidth(32);
        int_type.__set_isSigned(true);
        tparquet::LogicalType logical_type;
        logical_type.__set_INTEGER(int_type);
        age_typed_f.schema_element.__set_logicalType(logical_type);
    }

    ParquetField age_node;
    age_node.name = "age";
    age_node.type = ColumnType::STRUCT;
    age_node.children = include_age_value ? std::vector<ParquetField>{age_val_f, age_typed_f}
                                          : std::vector<ParquetField>{age_typed_f};

    ParquetField tv_struct;
    tv_struct.name = "typed_value";
    tv_struct.type = ColumnType::STRUCT;
    tv_struct.children = {age_node};

    root_field = ParquetField();
    root_field.name = "data";
    root_field.type = ColumnType::VARIANT;
    root_field.children = {meta_f, val_f, tv_struct};

    for (int i = 0; i <= 3; ++i) {
        tparquet::ColumnChunk chunk;
        chunk.__set_file_path("col" + std::to_string(i));
        chunk.file_offset = 0;
        chunk.meta_data.data_page_offset = 4;
        chunk.meta_data.__set_type(i == 3 ? tparquet::Type::INT32 : tparquet::Type::BYTE_ARRAY);
        if ((i == 1 || i == 2) && age_value_all_null) {
            tparquet::Statistics stats;
            stats.__set_null_count(5);
            chunk.meta_data.__set_statistics(stats);
        }
        if (i == 3 && stats_on_age_chunk) {
            int32_t min_val = 20, max_val = 24;
            tparquet::Statistics stats;
            stats.__set_null_count(0);
            stats.__set_min_value(std::string(reinterpret_cast<const char*>(&min_val), sizeof(min_val)));
            stats.__set_max_value(std::string(reinterpret_cast<const char*>(&max_val), sizeof(max_val)));
            chunk.meta_data.__set_statistics(stats);
        }
        rg.columns.emplace_back(std::move(chunk));
    }
    rg.__set_num_rows(5);
    opts.row_group_meta = &rg;

    return ColumnReaderFactory::create(opts, &root_field, TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT));
}

static StatusOr<std::unique_ptr<ColumnReader>> make_shredded_variant_reader_with_commit_operation_hint(
        tparquet::RowGroup& rg, ColumnReaderOptions& opts, ParquetField& root_field) {
    auto scalar_field = [](std::string name, tparquet::Type::type physical_type, int column_index) {
        ParquetField field;
        field.name = std::move(name);
        field.type = ColumnType::SCALAR;
        field.physical_type = physical_type;
        field.physical_column_index = column_index;
        return field;
    };

    ParquetField meta_f = scalar_field("metadata", tparquet::Type::BYTE_ARRAY, 0);
    ParquetField val_f = scalar_field("value", tparquet::Type::BYTE_ARRAY, 1);

    ParquetField time_value_f = scalar_field("value", tparquet::Type::BYTE_ARRAY, 2);
    ParquetField time_typed_f = scalar_field("typed_value", tparquet::Type::INT64, 3);
    ParquetField time_node;
    time_node.name = "time_us";
    time_node.type = ColumnType::STRUCT;
    time_node.children = {time_value_f, time_typed_f};

    ParquetField commit_value_f = scalar_field("value", tparquet::Type::BYTE_ARRAY, 4);
    ParquetField operation_value_f = scalar_field("value", tparquet::Type::BYTE_ARRAY, 5);
    ParquetField operation_typed_f = scalar_field("typed_value", tparquet::Type::INT32, 6);
    ParquetField operation_node;
    operation_node.name = "operation";
    operation_node.type = ColumnType::STRUCT;
    operation_node.children = {operation_value_f, operation_typed_f};

    ParquetField collection_value_f = scalar_field("value", tparquet::Type::BYTE_ARRAY, 7);
    ParquetField collection_typed_f = scalar_field("typed_value", tparquet::Type::INT32, 8);
    ParquetField collection_node;
    collection_node.name = "collection";
    collection_node.type = ColumnType::STRUCT;
    collection_node.children = {collection_value_f, collection_typed_f};

    ParquetField commit_typed_struct;
    commit_typed_struct.name = "typed_value";
    commit_typed_struct.type = ColumnType::STRUCT;
    commit_typed_struct.children = {operation_node, collection_node};

    ParquetField commit_node;
    commit_node.name = "commit";
    commit_node.type = ColumnType::STRUCT;
    commit_node.children = {commit_value_f, commit_typed_struct};

    ParquetField tv_struct;
    tv_struct.name = "typed_value";
    tv_struct.type = ColumnType::STRUCT;
    tv_struct.children = {time_node, commit_node};

    root_field = ParquetField();
    root_field.name = "data";
    root_field.type = ColumnType::VARIANT;
    root_field.children = {meta_f, val_f, tv_struct};

    for (int i = 0; i <= 8; ++i) {
        tparquet::ColumnChunk chunk;
        chunk.__set_file_path("col" + std::to_string(i));
        chunk.file_offset = 0;
        chunk.meta_data.__set_type((i == 3) ? tparquet::Type::INT64
                                            : (i == 6 || i == 8) ? tparquet::Type::INT32 : tparquet::Type::BYTE_ARRAY);
        chunk.meta_data.data_page_offset = i * 10;
        chunk.meta_data.total_compressed_size = 1;
        rg.columns.emplace_back(std::move(chunk));
    }
    rg.__set_num_rows(5);
    opts.row_group_meta = &rg;

    VariantShreddedReadHints hints;
    RETURN_IF_ERROR(hints.add_path("commit.operation"));
    return ColumnReaderFactory::create_variant_column_reader(opts, &root_field, hints);
}

TEST(VariantShreddedPruningTest, CollectIORangeSkipsUnrequestedSiblings) {
    tparquet::RowGroup rg;
    ColumnReaderOptions opts;
    ParquetField root_field;
    ASSIGN_OR_ABORT(auto reader, make_shredded_variant_reader_with_commit_operation_hint(rg, opts, root_field));

    std::vector<SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    reader->collect_column_io_range(&ranges, &end_offset, ColumnIOType::PAGES, true);

    std::vector<int64_t> offsets;
    offsets.reserve(ranges.size());
    for (const auto& range : ranges) {
        offsets.push_back(range.offset);
    }
    const std::vector<int64_t> expected_offsets = {0, 10, 50, 60};
    EXPECT_EQ(expected_offsets, offsets);
}

static StatusOr<std::unique_ptr<ColumnReader>> make_shredded_decimal_variant_reader(
        tparquet::RowGroup& rg, ColumnReaderOptions& opts, ParquetField& root_field, bool stats_on_leaf_chunk = false,
        bool leaf_value_all_null = false, bool include_leaf_value = true) {
    ParquetField meta_f;
    meta_f.name = "metadata";
    meta_f.type = ColumnType::SCALAR;
    meta_f.physical_type = tparquet::Type::BYTE_ARRAY;
    meta_f.physical_column_index = 0;

    ParquetField val_f;
    val_f.name = "value";
    val_f.type = ColumnType::SCALAR;
    val_f.physical_type = tparquet::Type::BYTE_ARRAY;
    val_f.physical_column_index = 1;

    ParquetField price_val_f;
    price_val_f.name = "value";
    price_val_f.type = ColumnType::SCALAR;
    price_val_f.physical_type = tparquet::Type::BYTE_ARRAY;
    price_val_f.physical_column_index = 2;

    ParquetField price_typed_f;
    price_typed_f.name = "typed_value";
    price_typed_f.type = ColumnType::SCALAR;
    price_typed_f.physical_type = tparquet::Type::INT32;
    price_typed_f.physical_column_index = 3;
    {
        tparquet::DecimalType decimal_type;
        decimal_type.__set_precision(5);
        decimal_type.__set_scale(2);
        tparquet::LogicalType logical_type;
        logical_type.__set_DECIMAL(decimal_type);
        price_typed_f.schema_element.__set_logicalType(logical_type);
        price_typed_f.precision = 5;
        price_typed_f.scale = 2;
    }

    ParquetField tv_struct;
    tv_struct.name = "typed_value";
    tv_struct.type = ColumnType::STRUCT;
    ParquetField price_node;
    price_node.name = "price";
    price_node.type = ColumnType::STRUCT;
    price_node.children = include_leaf_value ? std::vector<ParquetField>{price_val_f, price_typed_f}
                                             : std::vector<ParquetField>{price_typed_f};
    tv_struct.children = {price_node};

    root_field = ParquetField();
    root_field.name = "data";
    root_field.type = ColumnType::VARIANT;
    root_field.children = {meta_f, val_f, tv_struct};

    for (int i = 0; i <= 3; ++i) {
        tparquet::ColumnChunk chunk;
        chunk.__set_file_path("col" + std::to_string(i));
        chunk.file_offset = 0;
        chunk.meta_data.data_page_offset = 4;
        chunk.meta_data.__set_type(i == 3 ? tparquet::Type::INT32 : tparquet::Type::BYTE_ARRAY);
        if ((i == 1 || i == 2) && leaf_value_all_null) {
            tparquet::Statistics stats;
            stats.__set_null_count(5);
            chunk.meta_data.__set_statistics(stats);
        }
        if (i == 3 && stats_on_leaf_chunk) {
            int32_t min_val = 1050;
            int32_t max_val = 1250;
            tparquet::Statistics stats;
            stats.__set_null_count(0);
            stats.__set_min_value(std::string(reinterpret_cast<const char*>(&min_val), sizeof(min_val)));
            stats.__set_max_value(std::string(reinterpret_cast<const char*>(&max_val), sizeof(max_val)));
            chunk.meta_data.__set_statistics(stats);
        }
        rg.columns.emplace_back(std::move(chunk));
    }
    rg.__set_num_rows(5);
    opts.row_group_meta = &rg;

    return ColumnReaderFactory::create(opts, &root_field, TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT));
}

// Build a minimal FileMetaData (writer version "unknown") so that
// has_correct_min_max_stats does not dereference a null pointer.
// For INT32 with SIGNED sort order HasCorrectStatistics returns true regardless of version.
static std::unique_ptr<FileMetaData> make_minimal_file_meta() {
    tparquet::FileMetaData t_meta;
    t_meta.__set_version(2);
    t_meta.__set_num_rows(5);
    // Root must have num_children > 0 so that SchemaDescriptor::from_thrift
    // considers it a group (is_group() checks num_children > 0).
    tparquet::SchemaElement root_sch;
    root_sch.__set_name("hive_schema");
    root_sch.__set_num_children(1);
    // Minimal INT32 leaf child so the schema parses correctly.
    tparquet::SchemaElement leaf_sch;
    leaf_sch.__set_name("dummy");
    leaf_sch.__set_type(tparquet::Type::INT32);
    t_meta.__set_schema({root_sch, leaf_sch});
    auto meta = std::make_unique<FileMetaData>();
    if (!meta->init(t_meta, false).ok()) return nullptr;
    return meta;
}

// filterable_typed_value_reader_for_path: empty path → nullptr
TEST(VariantZoneMapTest, TypedValueReaderForPathEmptyPath) {
    tparquet::RowGroup rg;
    ColumnReaderOptions opts;
    ParquetField root_field;
    ASSIGN_OR_ABORT(auto reader, make_shredded_variant_reader(rg, opts, root_field));
    auto* vr = down_cast<VariantColumnReader*>(reader.get());

    EXPECT_EQ(nullptr, vr->filterable_typed_value_reader_for_path(VariantPath{}));
}

// filterable_typed_value_reader_for_path: key not in shredded fields → nullptr
TEST(VariantZoneMapTest, TypedValueReaderForPathNonExistentKey) {
    tparquet::RowGroup rg;
    ColumnReaderOptions opts;
    ParquetField root_field;
    ASSIGN_OR_ABORT(auto reader, make_shredded_variant_reader(rg, opts, root_field));
    auto* vr = down_cast<VariantColumnReader*>(reader.get());

    auto path = VariantPathParser::parse_shredded_path(std::string_view("nonexistent"));
    ASSERT_OK(path);
    EXPECT_EQ(nullptr, vr->filterable_typed_value_reader_for_path(*path));
}

// filterable_typed_value_reader_for_path: "age" is a SCALAR INT32 leaf → returns non-null reader
TEST(VariantZoneMapTest, TypedValueReaderForPathValidScalarLeaf) {
    tparquet::RowGroup rg;
    ColumnReaderOptions opts;
    ParquetField root_field;
    ASSIGN_OR_ABORT(auto reader, make_shredded_variant_reader(rg, opts, root_field));
    auto* vr = down_cast<VariantColumnReader*>(reader.get());

    auto path = VariantPathParser::parse_shredded_path(std::string_view("age"));
    ASSERT_OK(path);
    EXPECT_NE(nullptr, vr->filterable_typed_value_reader_for_path(*path));
}

TEST(VariantZoneMapTest, VariantVirtualZoneMapReaderNoOpApis) {
    tparquet::RowGroup rg;
    ColumnReaderOptions opts;
    ParquetField root_field;
    ASSIGN_OR_ABORT(auto reader, make_shredded_variant_reader(rg, opts, root_field));
    auto* vr = down_cast<VariantColumnReader*>(reader.get());

    auto path = VariantPathParser::parse_shredded_path(std::string_view("age"));
    ASSERT_OK(path);
    VariantVirtualZoneMapReader zm_reader(vr, *path);

    ASSERT_OK(zm_reader.prepare());

    ColumnPtr dst = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true);
    auto st = zm_reader.read_range(Range<uint64_t>(0, 1), nullptr, dst);
    ASSERT_TRUE(st.is_not_supported());

    level_t* def_levels = nullptr;
    level_t* rep_levels = nullptr;
    size_t num_levels = 0;
    zm_reader.get_levels(&def_levels, &rep_levels, &num_levels);
    zm_reader.set_need_parse_levels(true);

    std::vector<SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 77;
    zm_reader.collect_column_io_range(&ranges, &end_offset, ColumnIOType::PAGES, true);
    EXPECT_TRUE(ranges.empty());
    EXPECT_EQ(77, end_offset);

    SparseRange<uint64_t> sparse_range;
    sparse_range.add(Range<uint64_t>(0, 3));
    zm_reader.select_offset_index(sparse_range, 0);
}

// VariantVirtualZoneMapReader: non-existent path → row_group_zone_map_filter returns false
TEST(VariantZoneMapTest, ZoneMapReaderNonExistentPathReturnsFalse) {
    tparquet::RowGroup rg;
    ColumnReaderOptions opts;
    ParquetField root_field;
    ASSIGN_OR_ABORT(auto reader, make_shredded_variant_reader(rg, opts, root_field));
    auto* vr = down_cast<VariantColumnReader*>(reader.get());

    VariantVirtualZoneMapReader zm_reader(vr, *VariantPathParser::parse_shredded_path(std::string_view("nonexistent")));

    ObjectPool pool;
    TypeInfoPtr ti = get_type_info(LogicalType::TYPE_INT);
    auto* pred = pool.add(new_column_gt_predicate_from_datum(ti, 0, Datum(int32_t(10))));

    auto result = zm_reader.row_group_zone_map_filter({pred}, CompoundNodeType::AND, 0, 5);
    ASSERT_OK(result);
    EXPECT_FALSE(result.value()); // no shredded leaf found → no filtering
}

// VariantVirtualZoneMapReader: valid path but no statistics → returns false (can't filter)
TEST(VariantZoneMapTest, ZoneMapReaderValidPathNoStatsReturnsFalse) {
    tparquet::RowGroup rg;
    ColumnReaderOptions opts;
    ParquetField root_field;
    ASSIGN_OR_ABORT(auto reader, make_shredded_variant_reader(rg, opts, root_field));
    auto* vr = down_cast<VariantColumnReader*>(reader.get());

    VariantVirtualZoneMapReader zm_reader(vr, *VariantPathParser::parse_shredded_path(std::string_view("age")));

    ObjectPool pool;
    TypeInfoPtr ti = get_type_info(LogicalType::TYPE_INT);
    auto* pred = pool.add(new_column_gt_predicate_from_datum(ti, 0, Datum(int32_t(100))));

    auto result = zm_reader.row_group_zone_map_filter({pred}, CompoundNodeType::AND, 0, 5);
    ASSERT_OK(result);
    EXPECT_FALSE(result.value()); // stats absent → can't filter
}

// VariantVirtualZoneMapReader: statistics [20,24]; predicate "age > 100" → row group filtered
TEST(VariantZoneMapTest, ZoneMapReaderFiltersWhenPredicateOutOfStatRange) {
    auto file_meta = make_minimal_file_meta();
    ASSERT_NE(file_meta, nullptr);

    tparquet::RowGroup rg;
    ColumnReaderOptions opts;
    opts.file_meta_data = file_meta.get();
    ParquetField root_field;
    ASSIGN_OR_ABORT(auto reader,
                    make_shredded_variant_reader(rg, opts, root_field, /*stats_on_age_chunk=*/true,
                                                 /*age_value_all_null=*/false, /*include_age_value=*/false));
    auto* vr = down_cast<VariantColumnReader*>(reader.get());

    VariantVirtualZoneMapReader zm_reader(vr, *VariantPathParser::parse_shredded_path(std::string_view("age")));

    ObjectPool pool;
    TypeInfoPtr ti = get_type_info(LogicalType::TYPE_INT);
    // age > 100; but all ages are in [20,24] → entire row group can be skipped
    auto* pred = pool.add(new_column_gt_predicate_from_datum(ti, 0, Datum(int32_t(100))));

    auto result = zm_reader.row_group_zone_map_filter({pred}, CompoundNodeType::AND, 0, 5);
    ASSERT_OK(result);
    EXPECT_TRUE(result.value()); // row group should be filtered (skipped)
}

// VariantVirtualZoneMapReader: statistics [20,24]; predicate "age > 10" → row group kept
TEST(VariantZoneMapTest, ZoneMapReaderDoesNotFilterWhenPredicateInStatRange) {
    auto file_meta = make_minimal_file_meta();
    ASSERT_NE(file_meta, nullptr);

    tparquet::RowGroup rg;
    ColumnReaderOptions opts;
    opts.file_meta_data = file_meta.get();
    ParquetField root_field;
    ASSIGN_OR_ABORT(auto reader,
                    make_shredded_variant_reader(rg, opts, root_field, /*stats_on_age_chunk=*/true,
                                                 /*age_value_all_null=*/false, /*include_age_value=*/false));
    auto* vr = down_cast<VariantColumnReader*>(reader.get());

    VariantVirtualZoneMapReader zm_reader(vr, *VariantPathParser::parse_shredded_path(std::string_view("age")));

    ObjectPool pool;
    TypeInfoPtr ti = get_type_info(LogicalType::TYPE_INT);
    // age > 10; max is 24 > 10, so some rows may satisfy → do not skip
    auto* pred = pool.add(new_column_gt_predicate_from_datum(ti, 0, Datum(int32_t(10))));

    auto result = zm_reader.row_group_zone_map_filter({pred}, CompoundNodeType::AND, 0, 5);
    ASSERT_OK(result);
    EXPECT_FALSE(result.value()); // row group should NOT be filtered
}

TEST(VariantZoneMapTest, ZoneMapReaderDelegatesPredicatesWhenTypesMatch) {
    auto file_meta = make_minimal_file_meta();
    ASSERT_NE(file_meta, nullptr);

    tparquet::RowGroup rg;
    ColumnReaderOptions opts;
    opts.file_meta_data = file_meta.get();
    ParquetField root_field;
    ASSIGN_OR_ABORT(auto reader,
                    make_shredded_variant_reader(rg, opts, root_field, /*stats_on_age_chunk=*/true,
                                                 /*age_value_all_null=*/false, /*include_age_value=*/false));
    auto* vr = down_cast<VariantColumnReader*>(reader.get());

    auto path = VariantPathParser::parse_shredded_path(std::string_view("age"));
    ASSERT_OK(path);
    VariantVirtualZoneMapReader zm_reader(vr, *path, TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    ObjectPool pool;
    TypeInfoPtr ti = get_type_info(LogicalType::TYPE_INT);
    auto* pred = pool.add(new_column_gt_predicate_from_datum(ti, 0, Datum(int32_t(100))));

    auto row_group_result = zm_reader.row_group_zone_map_filter({pred}, CompoundNodeType::AND, 0, 5);
    ASSERT_OK(row_group_result);
    EXPECT_TRUE(row_group_result.value());

    SparseRange<uint64_t> row_ranges;
    auto page_result = zm_reader.page_index_zone_map_filter({pred}, &row_ranges, CompoundNodeType::AND, 0, 5);
    ASSERT_OK(page_result);
    EXPECT_FALSE(page_result.value());
    EXPECT_TRUE(row_ranges.empty());

    auto bloom_result = zm_reader.row_group_bloom_filter({pred}, CompoundNodeType::AND, 0, 5);
    ASSERT_OK(bloom_result);
    EXPECT_FALSE(bloom_result.value());
}

TEST(VariantZoneMapTest, ZoneMapReaderDelegatesDecimalPredicatesWhenLayoutsMatch) {
    auto file_meta = make_minimal_file_meta();
    ASSERT_NE(file_meta, nullptr);

    tparquet::RowGroup rg;
    ColumnReaderOptions opts;
    opts.file_meta_data = file_meta.get();
    ParquetField root_field;
    ASSIGN_OR_ABORT(auto reader,
                    make_shredded_decimal_variant_reader(rg, opts, root_field, /*stats_on_leaf_chunk=*/true,
                                                         /*leaf_value_all_null=*/true, /*include_leaf_value=*/true));
    auto* vr = down_cast<VariantColumnReader*>(reader.get());

    auto path = VariantPathParser::parse_shredded_path(std::string_view("price"));
    ASSERT_OK(path);
    auto virtual_slot_type = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 5, 2);
    VariantVirtualZoneMapReader zm_reader(vr, *path, virtual_slot_type);

    ObjectPool pool;
    TypeInfoPtr ti = get_type_info(virtual_slot_type);
    ASSERT_NE(ti, nullptr);
    auto* pred = pool.add(new_column_gt_predicate_from_datum(ti, 0, Datum(int32_t(2000))));

    auto row_group_result = zm_reader.row_group_zone_map_filter({pred}, CompoundNodeType::AND, 0, 5);
    ASSERT_OK(row_group_result);
    EXPECT_TRUE(row_group_result.value());

    SparseRange<uint64_t> row_ranges;
    auto page_result = zm_reader.page_index_zone_map_filter({pred}, &row_ranges, CompoundNodeType::AND, 0, 5);
    ASSERT_OK(page_result);
    EXPECT_FALSE(page_result.value());
    EXPECT_TRUE(row_ranges.empty());
}

TEST(VariantZoneMapTest, ZoneMapReaderSkipsDecimalToBigintPredicateDelegation) {
    auto file_meta = make_minimal_file_meta();
    ASSERT_NE(file_meta, nullptr);

    tparquet::RowGroup rg;
    ColumnReaderOptions opts;
    opts.file_meta_data = file_meta.get();
    ParquetField root_field;
    ASSIGN_OR_ABORT(auto reader,
                    make_shredded_decimal_variant_reader(rg, opts, root_field, /*stats_on_leaf_chunk=*/true,
                                                         /*leaf_value_all_null=*/true, /*include_leaf_value=*/true));
    auto* vr = down_cast<VariantColumnReader*>(reader.get());

    auto path = VariantPathParser::parse_shredded_path(std::string_view("price"));
    ASSERT_OK(path);
    VariantVirtualZoneMapReader zm_reader(vr, *path, TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT));

    ObjectPool pool;
    TypeInfoPtr ti = get_type_info(LogicalType::TYPE_BIGINT);
    auto* pred = pool.add(new_column_eq_predicate_from_datum(ti, 0, Datum(int64_t(10))));

    const ColumnReader* leaf_reader = nullptr;
    std::vector<const ColumnPredicate*> rewritten_predicates;
    EXPECT_FALSE(zm_reader._prepare_delegate_predicates({pred}, &pool, 5, &leaf_reader, &rewritten_predicates));
    EXPECT_NE(leaf_reader, nullptr);
    EXPECT_TRUE(rewritten_predicates.empty());

    auto row_group_result = zm_reader.row_group_zone_map_filter({pred}, CompoundNodeType::AND, 0, 5);
    ASSERT_OK(row_group_result);
    EXPECT_FALSE(row_group_result.value());
}

TEST(VariantZoneMapTest, ZoneMapReaderSkipsPushdownWhenPredicateRewriteFails) {
    auto file_meta = make_minimal_file_meta();
    ASSERT_NE(file_meta, nullptr);

    tparquet::RowGroup rg;
    ColumnReaderOptions opts;
    opts.file_meta_data = file_meta.get();
    ParquetField root_field;
    ASSIGN_OR_ABORT(auto reader,
                    make_shredded_variant_reader(rg, opts, root_field, /*stats_on_age_chunk=*/true,
                                                 /*age_value_all_null=*/false, /*include_age_value=*/false));
    auto* vr = down_cast<VariantColumnReader*>(reader.get());

    auto path = VariantPathParser::parse_shredded_path(std::string_view("age"));
    ASSERT_OK(path);
    VariantVirtualZoneMapReader zm_reader(vr, *path, TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    FailingConvertPredicate pred(get_type_info(LogicalType::TYPE_INT));

    auto row_group_result = zm_reader.row_group_zone_map_filter({&pred}, CompoundNodeType::AND, 0, 5);
    ASSERT_OK(row_group_result);
    EXPECT_FALSE(row_group_result.value());

    SparseRange<uint64_t> row_ranges;
    auto page_result = zm_reader.page_index_zone_map_filter({&pred}, &row_ranges, CompoundNodeType::AND, 0, 5);
    ASSERT_OK(page_result);
    EXPECT_FALSE(page_result.value());
    EXPECT_TRUE(row_ranges.empty());

    auto bloom_result = zm_reader.row_group_bloom_filter({&pred}, CompoundNodeType::AND, 0, 5);
    ASSERT_OK(bloom_result);
    EXPECT_FALSE(bloom_result.value());
}

TEST(VariantZoneMapTest, ZoneMapReaderSkipsWhenFallbackValueMayContainNonNullRows) {
    auto file_meta = make_minimal_file_meta();
    ASSERT_NE(file_meta, nullptr);

    tparquet::RowGroup rg;
    ColumnReaderOptions opts;
    opts.file_meta_data = file_meta.get();
    ParquetField root_field;
    ASSIGN_OR_ABORT(auto reader, make_shredded_variant_reader(rg, opts, root_field, /*stats_on_age_chunk=*/true,
                                                              /*age_value_all_null=*/false));
    auto* vr = down_cast<VariantColumnReader*>(reader.get());

    auto path = VariantPathParser::parse_shredded_path(std::string_view("age"));
    ASSERT_OK(path);
    VariantVirtualZoneMapReader zm_reader(vr, *path, TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    ObjectPool pool;
    TypeInfoPtr ti = get_type_info(LogicalType::TYPE_INT);
    auto* pred = pool.add(new_column_gt_predicate_from_datum(ti, 0, Datum(int32_t(100))));

    auto row_group_result = zm_reader.row_group_zone_map_filter({pred}, CompoundNodeType::AND, 0, 5);
    ASSERT_OK(row_group_result);
    EXPECT_FALSE(row_group_result.value());

    SparseRange<uint64_t> row_ranges;
    auto page_result = zm_reader.page_index_zone_map_filter({pred}, &row_ranges, CompoundNodeType::AND, 0, 5);
    ASSERT_OK(page_result);
    EXPECT_FALSE(page_result.value());
    EXPECT_TRUE(row_ranges.empty());

    auto bloom_result = zm_reader.row_group_bloom_filter({pred}, CompoundNodeType::AND, 0, 5);
    ASSERT_OK(bloom_result);
    EXPECT_FALSE(bloom_result.value());
}

TEST(ParquetComplexColumnReaderTest, VariantVirtualZoneMapReaderSkipsWhenSourceIsNull) {
    ASSIGN_OR_ABORT(auto leaf_path, VariantPathParser::parse_shredded_path(std::string_view("age")));
    VariantVirtualZoneMapReader zm_reader(nullptr, std::move(leaf_path),
                                          TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT));

    ObjectPool pool;
    const ColumnReader* leaf_reader = nullptr;
    std::vector<const ColumnPredicate*> rewritten_predicates;
    bool prepared = zm_reader._prepare_delegate_predicates({}, &pool, 10, &leaf_reader, &rewritten_predicates);
    EXPECT_FALSE(prepared);
    EXPECT_EQ(nullptr, leaf_reader);
    EXPECT_TRUE(rewritten_predicates.empty());
}

// White-box coverage for the fail-fast invariants introduced by the dict-code-leak fix.
//
// In normal flow fill_dst_column()/_restore_physical_column() are only reached right after read_range()
// has swapped in the reader's PHYSICAL column, so the "source is not a physical column"
// and "lost logical destination" guards are unreachable end to end. The test target is built
// with -fno-access-control, so construct the readers directly and reach into their internals.
TEST(ParquetScalarColumnReaderGuardTest, FillAndRestoreRejectNonTemporarySource) {
    ColumnReaderOptions opts;
    ParquetField field;
    tparquet::ColumnChunk chunk_meta;
    TypeDescriptor varchar_type = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    GlobalDictMap dict;
    auto make_int_col = [] { return ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true); };

    // LowCardColumnReader::fill_dst_column: when src is not _code_column
    // (PHYSICAL dict codes), it is treated as already LOGICAL → swap directly.
    {
        ScalarColumnReader base(&field, &chunk_meta, &varchar_type, opts);
        LowCardColumnReader reader(base, &dict, /*slot_id=*/1);
        ColumnPtr dst = make_int_col();
        ColumnPtr src = make_int_col();
        EXPECT_TRUE(reader.fill_dst_column(dst, src).ok());
    }

    // LowRowsColumnReader::fill_dst_column: same LOGICAL fallback.
    {
        ScalarColumnReader base(&field, &chunk_meta, &varchar_type, opts);
        LowRowsColumnReader reader(base, &dict, /*slot_id=*/1);
        ColumnPtr dst = make_int_col();
        ColumnPtr src = make_int_col();
        EXPECT_TRUE(reader.fill_dst_column(dst, src).ok());
    }

    // RawColumnReader::_restore_physical_column errors when a PHYSICAL column
    // is still the caller-visible column but _logical_dst was lost.
    {
        ScalarColumnReader base(&field, &chunk_meta, &varchar_type, opts);
        LowCardColumnReader reader(base, &dict, /*slot_id=*/1);
        ColumnPtr code = make_int_col();
        reader._code_column = code;
        reader._logical_dst = nullptr;
        ColumnPtr col = code;
        EXPECT_FALSE(reader._restore_physical_column(col).ok());
    }
}

// Reports a single def level `tag` so a test can see which child a StructColumnReader takes its levels from.
class LevelTagColumnReader final : public ColumnReader {
public:
    LevelTagColumnReader(const ParquetField* field, level_t tag) : ColumnReader(field), _tag(tag) {}

    Status prepare() override { return Status::OK(); }
    Status read_range(const Range<uint64_t>&, const Filter*, ColumnPtr&) override { return Status::OK(); }
    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        *def_levels = &_tag;
        *rep_levels = nullptr;
        *num_levels = 1;
    }
    void set_need_parse_levels(bool) override {}
    void collect_column_io_range(std::vector<SharedBufferedInputStream::IORange>*, int64_t*, ColumnIOTypeFlags,
                                 bool) override {}
    void select_offset_index(const SparseRange<uint64_t>&, const uint64_t) override {}

private:
    level_t _tag;
};

TEST(ParquetComplexColumnReaderTest, StructLevelSourcePrefersChildWithoutExtraRepetition) {
    // struct { a: list<int>, b: int, c: int, d: <no parquet field> }: `a` is first in map order but carries an extra
    // repetition level; `b` is the first child with one level per struct slot.
    ParquetField list_field;
    list_field.name = "a";
    list_field.type = ColumnType::ARRAY;
    list_field.level_info.max_rep_level = 1;
    ParquetField b_field;
    b_field.name = "b";
    b_field.type = ColumnType::SCALAR;
    ParquetField c_field = b_field;
    c_field.name = "c";
    ParquetField struct_field;
    struct_field.name = "s";
    struct_field.type = ColumnType::STRUCT;

    std::map<std::string, ColumnReaderPtr> children;
    children.emplace("a", std::make_unique<LevelTagColumnReader>(&list_field, 1));
    children.emplace("b", std::make_unique<LevelTagColumnReader>(&b_field, 2));
    children.emplace("c", std::make_unique<LevelTagColumnReader>(&c_field, 3));
    children.emplace("d", std::make_unique<LevelTagColumnReader>(nullptr, 4));
    children.emplace("e", nullptr);
    StructColumnReader reader(&struct_field, std::move(children));
    ASSERT_OK(reader.prepare());

    level_t* def_levels = nullptr;
    level_t* rep_levels = nullptr;
    size_t num_levels = 0;
    reader.get_levels(&def_levels, &rep_levels, &num_levels);
    ASSERT_EQ(1, num_levels);
    EXPECT_EQ(2, def_levels[0]);

    // Only repeated / field-less children: the repeated one with a parquet field wins over the field-less one.
    std::map<std::string, ColumnReaderPtr> children2;
    children2.emplace("a", std::make_unique<LevelTagColumnReader>(nullptr, 4));
    children2.emplace("b", std::make_unique<LevelTagColumnReader>(&list_field, 1));
    StructColumnReader reader2(&struct_field, std::move(children2));
    ASSERT_OK(reader2.prepare());
    reader2.get_levels(&def_levels, &rep_levels, &num_levels);
    EXPECT_EQ(1, def_levels[0]);
}

// ─── ARRAY nodes as ARRAY<VARIANT> batch columns ───────────────────────────

namespace {

using OptInt = std::optional<int64_t>;
using OptBytes = std::optional<std::string>;

// Variant value bytes of a JSON scalar; scalars do not reference the metadata dictionary.
std::string scalar_variant_bytes(std::string_view json_text) {
    auto row = parse_variant_json(json_text);
    return std::string(row.get_value().raw());
}

ColumnPtr make_bigint_column(const std::vector<OptInt>& values) {
    auto col = ColumnHelper::create_column(TYPE_BIGINT_DESC, true);
    for (const auto& v : values) {
        if (v.has_value()) {
            col->append_datum(Datum(*v));
        } else {
            col->append_nulls(1);
        }
    }
    return col;
}

ColumnPtr make_binary_column(const std::vector<OptBytes>& values) {
    auto col = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    for (const auto& v : values) {
        if (v.has_value()) {
            col->append_datum(Datum(Slice(v->data(), v->size())));
        } else {
            col->append_nulls(1);
        }
    }
    return col;
}

// Nullable ARRAY<BIGINT>: one array (or null) per row.
ColumnPtr make_bigint_array_column(const std::vector<std::optional<std::vector<int64_t>>>& rows) {
    auto elements = ColumnHelper::create_column(TYPE_BIGINT_DESC, true);
    auto offsets = UInt32Column::create();
    auto nulls = NullColumn::create();
    offsets->append(0);
    for (const auto& row : rows) {
        if (row.has_value()) {
            for (int64_t v : *row) {
                elements->append_datum(Datum(v));
            }
        }
        offsets->append(static_cast<uint32_t>(elements->size()));
        nulls->append(row.has_value() ? 0 : 1);
    }
    return NullableColumn::create(ArrayColumn::create(std::move(elements), std::move(offsets)), std::move(nulls));
}

// The rewritten typed column of an object array: ARRAY<VARBINARY> over element.value, here with null element values
// (elements made only of shredded fields). A row is null when its count is nullopt.
ColumnPtr make_element_value_array_column(const std::vector<std::optional<size_t>>& counts) {
    auto elements = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    auto offsets = UInt32Column::create();
    auto nulls = NullColumn::create();
    offsets->append(0);
    for (const auto& count : counts) {
        if (count.has_value()) {
            elements->append_nulls(*count);
        }
        offsets->append(static_cast<uint32_t>(elements->size()));
        nulls->append(count.has_value() ? 0 : 1);
    }
    return NullableColumn::create(ArrayColumn::create(std::move(elements), std::move(offsets)), std::move(nulls));
}

ShreddedFieldNode make_bigint_child(const std::string& path, const std::vector<OptInt>& typed,
                                    const std::vector<OptBytes>& value) {
    ShreddedFieldNode node = make_node(path, ShreddedFieldNode::Kind::SCALAR);
    node.typed_value_read_type = std::make_unique<TypeDescriptor>(TYPE_BIGINT_DESC);
    node.typed_value_column = make_bigint_column(typed);
    node.value_column = make_binary_column(value);
    return node;
}

ShreddedFieldNode make_bigint_array_child(const std::string& path, bool scalar_array_layout,
                                          const std::vector<std::optional<std::vector<int64_t>>>& rows) {
    ShreddedFieldNode node = make_node(path, ShreddedFieldNode::Kind::ARRAY);
    node.scalar_array_layout = scalar_array_layout;
    node.typed_value_read_type = std::make_unique<TypeDescriptor>(TypeDescriptor::create_array_type(TYPE_BIGINT_DESC));
    node.typed_value_column = make_bigint_array_column(rows);
    return node;
}

ShreddedFieldNode make_object_array_node(const std::string& path, const std::vector<std::optional<size_t>>& counts) {
    ShreddedFieldNode node = make_node(path, ShreddedFieldNode::Kind::ARRAY);
    node.typed_value_read_type =
            std::make_unique<TypeDescriptor>(TypeDescriptor::create_array_type(TYPE_VARBINARY_DESC));
    node.typed_value_column = make_element_value_array_column(counts);
    return node;
}

// The element VariantColumn (as a nullable column) of an ARRAY<VARIANT> batch column.
const Column* array_elements_of(const Column* batch_column) {
    const auto* nullable = down_cast<const NullableColumn*>(batch_column);
    return down_cast<const ArrayColumn*>(nullable->data_column().get())->elements_column().get();
}

} // namespace

// Object elements: scalar child with fallback values, field without typed_value, partially shredded object without
// residual (flattened into its leaves), nested scalar array in scalar-array layout (structured), and a nested array
// in a layout that is not structured (kept as a VARIANT path rebuilt per element).
TEST(ParquetComplexColumnReaderTest, BuildArrayBindingColumnObjectElements) {
    auto row = parse_variant_json(R"({"a":0,"k":0,"l":0,"n":0,"o":0,"s":0})");
    const std::string metadata_raw(row.get_metadata().raw());
    const std::vector<Slice> row_metadata(2, Slice(metadata_raw));

    // row 0: [elem0, elem1]; row 1: not an array (string in the array node's value column).
    ShreddedFieldNode node = make_object_array_node("arr", {2, std::nullopt});
    node.value_column = make_binary_column({std::nullopt, scalar_variant_bytes(R"("notarray")")});
    node.children.emplace_back(
            make_bigint_child("a", {1, std::nullopt}, {std::nullopt, scalar_variant_bytes(R"("s")")}));
    ShreddedFieldNode n = make_node("n");
    n.value_column = make_binary_column({scalar_variant_bytes(R"("x")"), std::nullopt});
    node.children.emplace_back(std::move(n));
    ShreddedFieldNode o = make_node("o");
    o.value_column = make_binary_column({std::nullopt, std::nullopt});
    o.children.emplace_back(make_bigint_child("o.k", {2, std::nullopt}, {std::nullopt, std::nullopt}));
    node.children.emplace_back(std::move(o));
    node.children.emplace_back(
            make_bigint_array_child("s", true, {std::vector<int64_t>{7}, std::vector<int64_t>{8, 9}}));
    node.children.emplace_back(make_bigint_array_child("l", false, {std::vector<int64_t>{1, 2}, std::nullopt}));

    auto batch = VariantColumnReader::build_array_binding_column(node, row_metadata);
    ASSERT_TRUE(batch.ok()) << batch.status().to_string();
    const Column* batch_column = batch.value().get();
    ASSERT_EQ(2, batch_column->size());
    EXPECT_FALSE(batch_column->is_null(0));
    EXPECT_TRUE(batch_column->is_null(1));

    const Column* elements = array_elements_of(batch_column);
    ASSERT_EQ(2, elements->size());
    EXPECT_EQ(R"({"a":1,"l":[1,2],"n":"x","o":{"k":2},"s":[7]})", nullable_variant_json_at(elements, 0));
    EXPECT_EQ(R"({"a":"s","s":[8,9]})", nullable_variant_json_at(elements, 1));

    // Row 0 is an array; row 1 keeps its non-array value in the fallback column.
    TopBinding binding;
    binding.kind = TopBinding::Kind::ARRAY;
    binding.node = &node;
    auto dst = batch_column->clone_empty();
    auto fallback = VariantColumn::create_fallback_column();
    VariantColumnReader::append_array_binding_row(0, binding, *batch_column, dst.get(), fallback.get());
    VariantColumnReader::append_array_binding_row(1, binding, *batch_column, dst.get(), fallback.get());
    ASSERT_EQ(2, dst->size());
    EXPECT_FALSE(dst->is_null(0));
    EXPECT_TRUE(dst->is_null(1));
    ASSERT_EQ(2, fallback->size());
    EXPECT_TRUE(fallback->is_null(0));
    ASSERT_FALSE(fallback->is_null(1));
    EXPECT_EQ(scalar_variant_bytes(R"("notarray")"), fallback->get(1).get_slice().to_string());

    // Without a fallback destination only the array column is appended.
    auto dst2 = batch_column->clone_empty();
    VariantColumnReader::append_array_binding_row(1, binding, *batch_column, dst2.get(), nullptr);
    ASSERT_EQ(1, dst2->size());
    EXPECT_TRUE(dst2->is_null(0));
}

// A partially shredded object inside the elements has residual fields: every element is rebuilt in full into the
// element remain, including a nested array child.
TEST(ParquetComplexColumnReaderTest, BuildArrayBindingColumnNestedObjectResidual) {
    auto row = parse_variant_json(R"({"k":0,"l":0,"o":{"r":3}})");
    const std::string metadata_raw(row.get_metadata().raw());
    auto residual = VariantPath::seek_view(row.as_ref(), VariantPathParser::parse(std::string("$.o")).value());
    ASSERT_TRUE(residual.ok()) << residual.status().to_string();
    const std::string residual_bytes(residual->get_value().raw());
    const std::vector<Slice> row_metadata(1, Slice(metadata_raw));

    // One row with three elements; the third has no value at all.
    ShreddedFieldNode node = make_object_array_node("arr", {3});
    ShreddedFieldNode o = make_node("o");
    o.value_column = make_binary_column({residual_bytes, std::nullopt, std::nullopt});
    o.children.emplace_back(
            make_bigint_child("o.k", {2, std::nullopt, std::nullopt}, {std::nullopt, std::nullopt, std::nullopt}));
    node.children.emplace_back(std::move(o));
    node.children.emplace_back(
            make_bigint_array_child("l", false, {std::vector<int64_t>{1, 2}, std::vector<int64_t>{3}, std::nullopt}));

    auto batch = VariantColumnReader::build_array_binding_column(node, row_metadata);
    ASSERT_TRUE(batch.ok()) << batch.status().to_string();
    const Column* elements = array_elements_of(batch.value().get());
    ASSERT_EQ(3, elements->size());
    const auto* element_variant =
            down_cast<const VariantColumn*>(down_cast<const NullableColumn*>(elements)->data_column().get());
    // Remain only: no typed paths in this batch.
    EXPECT_TRUE(element_variant->shredded_paths().empty());
    EXPECT_EQ(R"({"l":[1,2],"o":{"k":2,"r":3}})", nullable_variant_json_at(elements, 0));
    EXPECT_EQ(R"({"l":[3]})", nullable_variant_json_at(elements, 1));
    EXPECT_EQ("null", nullable_variant_json_at(elements, 2));
}

// Scalar-array layout: a null typed element is read from element.value, which must share the typed array offsets.
TEST(ParquetComplexColumnReaderTest, BuildArrayBindingColumnScalarArrayElementValue) {
    auto row = parse_variant_json(R"({"s":0})");
    const std::string metadata_raw(row.get_metadata().raw());
    const std::vector<Slice> row_metadata(2, Slice(metadata_raw));

    // Element value bytes as ARRAY<VARBINARY>: row 0 = [null, "x"], row 1 = [null].
    auto make_value_array = [](const std::vector<uint32_t>& offsets_data) {
        auto elements = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        elements->append_nulls(1);
        const std::string x = scalar_variant_bytes(R"("x")");
        elements->append_datum(Datum(Slice(x)));
        elements->append_nulls(1);
        auto offsets = UInt32Column::create();
        for (uint32_t o : offsets_data) {
            offsets->append(o);
        }
        return NullableColumn::create(ArrayColumn::create(std::move(elements), std::move(offsets)),
                                      NullColumn::create(offsets_data.size() - 1, 0));
    };

    // Typed: row 0 = [1, null], row 1 = [3].
    ShreddedFieldNode node = make_node("arr", ShreddedFieldNode::Kind::ARRAY);
    node.scalar_array_layout = true;
    node.typed_value_read_type = std::make_unique<TypeDescriptor>(TypeDescriptor::create_array_type(TYPE_BIGINT_DESC));
    {
        auto elements = ColumnHelper::create_column(TYPE_BIGINT_DESC, true);
        elements->append_datum(Datum(int64_t{1}));
        elements->append_nulls(1);
        elements->append_datum(Datum(int64_t{3}));
        auto offsets = UInt32Column::create();
        offsets->append(0);
        offsets->append(2);
        offsets->append(3);
        node.typed_value_column = NullableColumn::create(ArrayColumn::create(std::move(elements), std::move(offsets)),
                                                         NullColumn::create(2, 0));
    }
    node.array_element_value_column = make_value_array({0, 2, 3});

    auto batch = VariantColumnReader::build_array_binding_column(node, row_metadata);
    ASSERT_TRUE(batch.ok()) << batch.status().to_string();
    const Column* elements = array_elements_of(batch.value().get());
    ASSERT_EQ(3, elements->size());
    EXPECT_EQ("1", nullable_variant_json_at(elements, 0));
    EXPECT_EQ(R"("x")", nullable_variant_json_at(elements, 1));
    EXPECT_EQ("3", nullable_variant_json_at(elements, 2));

    // Same number of elements but different offsets: element.value would land on other elements.
    node.array_element_value_column = make_value_array({0, 1, 3});
    auto misaligned = VariantColumnReader::build_array_binding_column(node, row_metadata);
    ASSERT_FALSE(misaligned.ok());
    EXPECT_TRUE(misaligned.status().is_internal_error()) << misaligned.status().to_string();
}

} // namespace starrocks::parquet
