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

#pragma once

#include <cstdint>
#include <vector>

#include "column/vectorized_fwd.h"
#include "exec/schema_scanner.h"

namespace starrocks {

class SchemaBeBvarsScanner : public SchemaScanner {
public:
    SchemaBeBvarsScanner();
    ~SchemaBeBvarsScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    static SchemaScanner::ColumnDesc _s_columns[];

    Status fill_chunk(ChunkPtr* chunk);

    ColumnPtr _columns[2];
    int64_t _be_id{0};
    size_t _cur_idx{0};
    size_t _chunk_size;
};

} // namespace starrocks
