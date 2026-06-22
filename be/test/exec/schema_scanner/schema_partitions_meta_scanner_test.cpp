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

#include "exec/schema_scanner/schema_partitions_meta_scanner.h"

#include <gtest/gtest.h>

#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/object_pool.h"
#include "exec/schema_scanner.h"

namespace starrocks {

// Friend of SchemaPartitionsMetaScanner so the test can inject rows and invoke the private
// fill_chunk() directly, without standing up the FE RPC that start() performs.
class SchemaPartitionsMetaScannerTest : public ::testing::Test {
protected:
    void set_rows(SchemaPartitionsMetaScanner& scanner, std::vector<TPartitionMetaInfo> rows) {
        scanner._partitions_meta_vec = std::move(rows);
        scanner._partitions_meta_index = 0;
    }

    Status fill_chunk(SchemaPartitionsMetaScanner& scanner, ChunkPtr* chunk) { return scanner.fill_chunk(chunk); }
};

// MIN_VI_BUILT_VERSION (slot 32) / MAX_VI_BUILT_VERSION (slot 33) are filled from the
// per-partition thrift fields surfaced for async vector-index observability.
TEST_F(SchemaPartitionsMetaScannerTest, fill_vector_index_built_version_columns) {
    SchemaPartitionsMetaScanner scanner;
    SchemaScannerParam params;
    std::string ip = "127.0.0.1";
    params.ip = &ip;
    params.port = 9020;
    ObjectPool pool;
    ASSERT_OK(scanner.init(&params, &pool));

    TPartitionMetaInfo info;
    info.__set_min_vi_built_version(3);
    info.__set_max_vi_built_version(7);
    set_rows(scanner, {info});

    // Only request the two VI built-version columns so fill_chunk exercises just those cases.
    ChunkPtr chunk = std::make_shared<Chunk>();
    for (auto* slot : scanner.get_slot_descs()) {
        if (slot->id() == 32 || slot->id() == 33) {
            chunk->append_column(ColumnHelper::create_column(slot->type(), slot->is_nullable()), slot->id());
        }
    }
    ASSERT_EQ(2, chunk->num_columns());

    ASSERT_OK(fill_chunk(scanner, &chunk));

    EXPECT_EQ(3, chunk->get_column_by_slot_id(32)->get(0).get_int64());
    EXPECT_EQ(7, chunk->get_column_by_slot_id(33)->get(0).get_int64());
}

} // namespace starrocks
