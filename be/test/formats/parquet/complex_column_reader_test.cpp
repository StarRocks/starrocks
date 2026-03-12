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

#include "column/column_helper.h"
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

} // namespace

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

} // namespace starrocks::parquet
