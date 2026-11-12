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

#include <limits>

#include <arrow/testing/builder.h>
#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "connector/hive/paimon/paimon_scanner.h"

namespace starrocks {

class PaimonScannerProjectionTest : public ::testing::Test {
protected:
    static void prepare(PaimonScanner* scanner, HdfsScannerContext* context, SlotDescriptor* slot,
                        const std::shared_ptr<arrow::DataType>& type, const std::shared_ptr<arrow::Array>& array) {
        context->format_scan_context.materialized_columns.push_back({0, slot});
        context->format_scan_context.stats = &scanner->_app_stats;
        scanner->_scanner_ctx = context;
        scanner->_arrow_record_batch =
                arrow::RecordBatch::Make(arrow::schema({arrow::field("v", type)}), array->length(), {array});
        scanner->_convert_functions.emplace_back(std::make_unique<ConvertFuncTree>());
        scanner->_cast_exprs.resize(1, nullptr);
        scanner->_max_chunk_size = 1;
    }
};

TEST_F(PaimonScannerProjectionTest, pruned_map_reads_actual_schema_across_chunks) {
    auto type = arrow::map(arrow::utf8(), arrow::int32());
    auto array = arrow::ArrayFromJSON(type, R"([[ ["a", 10], ["b", null] ], [], null])");
    for (int pruned_child : {0, 1}) {
        TypeDescriptor projected(TYPE_MAP);
        projected.children = {TYPE_VARCHAR_DESC, TYPE_INT_DESC};
        projected.children[pruned_child] = TYPE_UNKNOWN_DESC;
        SlotDescriptor slot(1, "v", projected);
        HdfsScannerContext context;
        PaimonScanner scanner;
        prepare(&scanner, &context, &slot, type, array);
        ChunkPtr first;
        ASSERT_TRUE(scanner.do_get_next(nullptr, &first).ok());
        auto column = first->get_column_by_slot_id(1);
        auto* map = down_cast<const MapColumn*>(ColumnHelper::get_data_column(column.get()));
        ASSERT_EQ(2, map->keys_column()->size());
        ASSERT_EQ(2, map->values_column()->size());
        auto pruned = pruned_child == 0 ? map->keys_column() : map->values_column();
        ASSERT_TRUE(pruned->is_null(0));
        ASSERT_TRUE(pruned->is_null(1));
        ASSERT_EQ(pruned_child == 0 ? "10" : "'a'",
                  (pruned_child == 0 ? map->values_column() : map->keys_column())->debug_item(0));
        ChunkPtr second;
        ASSERT_TRUE(scanner.do_get_next(nullptr, &second).ok());
        ASSERT_EQ("{}", second->get_column_by_slot_id(1)->debug_item(0));
        ChunkPtr third;
        ASSERT_TRUE(scanner.do_get_next(nullptr, &third).ok());
        ASSERT_TRUE(third->get_column_by_slot_id(1)->is_null(0));
        // Returning later chunks must not overwrite previously returned data.
        ASSERT_EQ(2, map->keys_column()->size());
        ASSERT_EQ(3, scanner._app_stats.raw_rows_read);
        ASSERT_EQ(0, scanner._read_chunk_template->num_rows());
    }
}

TEST_F(PaimonScannerProjectionTest, pruned_struct_uses_field_names) {
    auto type = arrow::struct_({arrow::field("unused", arrow::utf8()), arrow::field("score", arrow::int32())});
    auto array = arrow::ArrayFromJSON(type, R"([{"unused":"abc","score":7}])");
    TypeDescriptor projected(TYPE_STRUCT);
    projected.field_names = {"score"};
    projected.children.emplace_back(TYPE_INT);
    SlotDescriptor slot(1, "v", projected);
    HdfsScannerContext context;
    PaimonScanner scanner;
    prepare(&scanner, &context, &slot, type, array);
    ChunkPtr chunk;
    ASSERT_TRUE(scanner.do_get_next(nullptr, &chunk).ok());
    ASSERT_EQ("{score:7}", chunk->get_column_by_slot_id(1)->debug_item(0));
}

TEST_F(PaimonScannerProjectionTest, missing_arrow_column_reports_error) {
    SlotDescriptor slot(1, "v", TYPE_INT_DESC);
    HdfsScannerContext context;
    PaimonScanner scanner;
    prepare(&scanner, &context, &slot, arrow::int32(), arrow::ArrayFromJSON(arrow::int32(), "[7]"));
    scanner._arrow_record_batch =
            arrow::RecordBatch::Make(arrow::schema({}), 1, std::vector<std::shared_ptr<arrow::Array>>{});
    ChunkPtr chunk;
    auto status = scanner.do_get_next(nullptr, &chunk);
    ASSERT_FALSE(status.ok());
    ASSERT_NE(std::string::npos, status.message().find("missing column v"));
}

TEST_F(PaimonScannerProjectionTest, varchar_is_read_as_string) {
    auto type = arrow::utf8();
    auto array = arrow::ArrayFromJSON(type, R"(["中文", "中a", "😀文", "already stored longer text", null, ""])");
    for (int length : {2, std::numeric_limits<int>::max()}) {
        SlotDescriptor slot(1, "v", TypeDescriptor::create_varchar_type(length));
        HdfsScannerContext context;
        PaimonScanner scanner;
        prepare(&scanner, &context, &slot, type, array);
        for (int row = 0; row < array->length(); ++row) {
            ChunkPtr chunk;
            ASSERT_TRUE(scanner.do_get_next(nullptr, &chunk).ok());
            ASSERT_EQ(1, chunk->num_rows()) << "row " << row;
            auto value = chunk->get_column_by_slot_id(1)->get(0);
            if (array->IsNull(row)) {
                ASSERT_TRUE(value.is_null());
            } else {
                ASSERT_EQ(std::static_pointer_cast<arrow::StringArray>(array)->GetString(row),
                          value.get_slice().to_string());
            }
        }
        ASSERT_EQ(array->length(), scanner._app_stats.raw_rows_read);
    }
}

TEST_F(PaimonScannerProjectionTest, string_read_preserves_large_external_values) {
    const int characters = TypeDescriptor::MAX_VARCHAR_LENGTH / 3 + 1;
    std::string text;
    text.reserve(characters * 3);
    for (int i = 0; i < characters; ++i) text += "中";
    arrow::StringBuilder builder;
    ASSERT_TRUE(builder.Append(text).ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    SlotDescriptor slot(1, "v", TypeDescriptor::create_varchar_type(characters));
    HdfsScannerContext context;
    PaimonScanner scanner;
    prepare(&scanner, &context, &slot, arrow::utf8(), array);
    ChunkPtr chunk;
    ASSERT_TRUE(scanner.do_get_next(nullptr, &chunk).ok());
    ASSERT_EQ(1, chunk->num_rows());
    ASSERT_EQ(text, chunk->get_column_by_slot_id(1)->get(0).get_slice().to_string());
}

TEST_F(PaimonScannerProjectionTest, nested_multibyte_strings_preserve_values_and_nulls) {
    auto text = arrow::utf8();
    auto list = arrow::list(text);
    auto map = arrow::map(text, text);
    auto type = arrow::struct_({arrow::field("s", text), arrow::field("a", list), arrow::field("m", map)});
    auto array = arrow::ArrayFromJSON(type, R"([{"s":"中","a":["😀文",null],"m":[["中文","你好"]]},null])");
    TypeDescriptor projected(TYPE_STRUCT);
    projected.field_names = {"s", "a", "m"};
    TypeDescriptor varchar = TypeDescriptor::create_varchar_type(2);
    TypeDescriptor projected_list(TYPE_ARRAY);
    projected_list.children = {varchar};
    TypeDescriptor projected_map(TYPE_MAP);
    projected_map.children = {varchar, varchar};
    projected.children = {TypeDescriptor::create_varchar_type(1), projected_list, projected_map};
    SlotDescriptor slot(1, "v", projected);
    HdfsScannerContext context;
    PaimonScanner scanner;
    prepare(&scanner, &context, &slot, type, array);
    ChunkPtr first;
    ASSERT_TRUE(scanner.do_get_next(nullptr, &first).ok());
    ASSERT_EQ(1, first->num_rows());
    const auto row = first->get_column_by_slot_id(1)->get(0).get_struct();
    ASSERT_EQ("中", row[0].get_slice().to_string());
    const auto elements = row[1].get_array();
    ASSERT_EQ(2, elements.size());
    ASSERT_EQ("😀文", elements[0].get_slice().to_string());
    ASSERT_TRUE(elements[1].is_null());
    const auto entries = row[2].get_map();
    ASSERT_EQ(1, entries.size());
    ASSERT_EQ("中文", std::get<Slice>(entries.begin()->first).to_string());
    ASSERT_EQ("你好", entries.begin()->second.get_slice().to_string());
    ChunkPtr second;
    ASSERT_TRUE(scanner.do_get_next(nullptr, &second).ok());
    ASSERT_EQ(1, second->num_rows());
    ASSERT_TRUE(second->get_column_by_slot_id(1)->is_null(0));
    ASSERT_EQ(2, scanner._app_stats.raw_rows_read);
}

TEST_F(PaimonScannerProjectionTest, binary_bounds_remain_in_bytes) {
    TypeDescriptor binary = TYPE_VARBINARY_DESC;
    binary.len = 2;
    SlotDescriptor slot(1, "v", binary);
    HdfsScannerContext context;
    PaimonScanner scanner;
    auto array = arrow::ArrayFromJSON(arrow::binary(), R"(["中文","ab"])");
    prepare(&scanner, &context, &slot, arrow::binary(), array);
    ChunkPtr first;
    ASSERT_TRUE(scanner.do_get_next(nullptr, &first).ok());
    ASSERT_EQ(0, first->num_rows());
    ChunkPtr second;
    ASSERT_TRUE(scanner.do_get_next(nullptr, &second).ok());
    ASSERT_EQ(1, second->num_rows());
    ASSERT_EQ("ab", second->get_column_by_slot_id(1)->get(0).get_slice().to_string());
    ASSERT_EQ(2, scanner._convert_context.current_type_length);
}

} // namespace starrocks
