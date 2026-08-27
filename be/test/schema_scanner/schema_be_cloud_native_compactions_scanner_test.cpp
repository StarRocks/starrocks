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

#include "schema_scanner/schema_be_cloud_native_compactions_scanner.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/column_helper.h"

namespace starrocks {

class SchemaBeCloudNativeCompactionsScannerTest : public ::testing::Test {
protected:
    static ChunkPtr create_chunk(const std::vector<SlotDescriptor*>& slot_descs) {
        auto chunk = std::make_shared<Chunk>();
        for (const auto* slot_desc : slot_descs) {
            auto column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
            chunk->append_column(std::move(column), slot_desc->id());
        }
        return chunk;
    }
};

TEST_F(SchemaBeCloudNativeCompactionsScannerTest, subtask_id) {
    SchemaBeCloudNativeCompactionsScanner scanner;
    SchemaScannerParam params;
    ObjectPool pool;
    ASSERT_OK(scanner.init(&params, &pool));

    lake::CompactionTaskInfo regular{};
    regular.txn_id = 1;
    regular.tablet_id = 2;
    regular.version = 3;
    regular.status = Status::OK();
    scanner._infos.emplace_back(std::move(regular));

    lake::CompactionTaskInfo parallel{};
    parallel.txn_id = 1;
    parallel.tablet_id = 2;
    parallel.version = 3;
    parallel.status = Status::OK();
    parallel.subtask_id = 7;
    scanner._infos.emplace_back(std::move(parallel));

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    ASSERT_OK(scanner.get_next(&chunk, &eos));
    ASSERT_FALSE(eos);
    ASSERT_EQ(1, chunk->num_rows());
    EXPECT_TRUE(chunk->get_column_by_index(11)->is_null(0));

    chunk->reset();
    ASSERT_OK(scanner.get_next(&chunk, &eos));
    ASSERT_FALSE(eos);
    ASSERT_EQ(1, chunk->num_rows());
    EXPECT_FALSE(chunk->get_column_by_index(11)->is_null(0));
    EXPECT_EQ(7, chunk->get_column_by_index(11)->get(0).get_int32());
}

} // namespace starrocks
