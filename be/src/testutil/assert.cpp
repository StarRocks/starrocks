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

#include "testutil/assert.h"

#include "column/chunk.h"
#include "column/column.h"

namespace starrocks {

void assert_chunk_equals(const Chunk& chunk1, const Chunk& chunk2) {
    CHECK_EQ(chunk1.num_columns(), chunk2.num_columns());
    CHECK_EQ(chunk1.num_rows(), chunk2.num_rows());
    for (size_t i = 0, m = chunk1.num_columns(); i < m; i++) {
        const auto& col1 = chunk1.get_column_by_index(i);
        const auto& col2 = chunk2.get_column_by_index(i);
        for (size_t j = 0, n = chunk1.num_rows(); j < n; j++) {
            CHECK(col1->equals(j, *col2, j)) << "different at column " << i << " row " << j << ":\n"
                                             << col1->debug_string() << "\nvs\n"
                                             << col2->debug_string();
        }
    }
}

} // namespace starrocks
