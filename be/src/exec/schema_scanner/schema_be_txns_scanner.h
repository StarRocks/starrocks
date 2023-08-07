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

#include "exec/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks {

struct TxnInfo {
    UniqueId load_id;
    int64_t txn_id{0};
    int64_t partition_id{0};
    int64_t tablet_id{0};
    int64_t create_time{0};
    int64_t commit_time{0};
    int64_t publish_time{0};
    std::string rowset_id;
    int64_t num_segment{0};
    int64_t num_delfile{0};
    int64_t num_row{0};
    int64_t data_size{0};
    int64_t version{0};
};

class SchemaBeTxnsScanner : public SchemaScanner {
public:
    SchemaBeTxnsScanner();
    ~SchemaBeTxnsScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    Status fill_chunk(ChunkPtr* chunk);

    int64_t _be_id{0};
    std::vector<TxnInfo> _infos;
    size_t _cur_idx{0};
    static SchemaScanner::ColumnDesc _s_columns[];
};

} // namespace starrocks
