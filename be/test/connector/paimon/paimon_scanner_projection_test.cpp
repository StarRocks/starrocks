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
        projected.children = {TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_INT)};
        projected.children[pruned_child] = TypeDescriptor(TYPE_UNKNOWN);
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
    SlotDescriptor slot(1, "v", TypeDescriptor(TYPE_INT));
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

} // namespace starrocks
