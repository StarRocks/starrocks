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

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exec/partition/partition_hash_variant.h"
#include "runtime/mem_pool.h"
#include "types/logical_type.h"

namespace starrocks {

// A partition key column whose serialized size overflows uint32 (fake 5GB offset, no bytes allocated)
// must make LocalPartitionTopn's append_chunk return an error instead of under-allocating.
TEST(PartitionHashMapOverflowTest, AppendChunkGuardReturnsError) {
    SerializedKeyPartitionHashMap<PhmapSeed1> hm(4);

    auto fake = LargeBinaryColumn::create();
    auto* data_ptr = fake.get();
    fake->get_offset().push_back(5ULL * 1024 * 1024 * 1024);
    Columns key_columns;
    key_columns.emplace_back(std::move(fake));

    auto probe = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false);
    probe->append_datum(Datum(int32_t(1)));
    Columns chunk_cols;
    chunk_cols.emplace_back(std::move(probe));
    Chunk::SlotHashMap map;
    map[0] = 0;
    auto chunk = std::make_shared<Chunk>(std::move(chunk_cols), map);

    MemPool mem_pool;
    ObjectPool obj_pool;
    auto res = hm.append_chunk<false>(chunk, key_columns, &mem_pool, &obj_pool, nullptr, nullptr);
    EXPECT_FALSE(res.ok());
    EXPECT_NE(res.status().to_string().find("exceeds the 4GB"), std::string::npos);
    data_ptr->reset_column(); // restore bytes/offsets consistency before destruction
}

} // namespace starrocks
