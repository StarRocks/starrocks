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

<<<<<<< HEAD:be/src/exec/vectorized/schema_scanner/schema_be_logs_scanner.h
#include "common/greplog.h"
#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"
=======
#include <cstddef>
#include <vector>
>>>>>>> 2a51cf89f9 ([BugFix] Fix select ORC MapVector's filter nullptr bug (#34267)):be/src/storage/lake/key_index.h

namespace starrocks::vectorized {

class SchemaBeLogsScanner : public SchemaScanner {
public:
    SchemaBeLogsScanner();
    ~SchemaBeLogsScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    Status fill_chunk(ChunkPtr* chunk);
    int64_t _be_id{0};
    std::deque<GrepLogEntry> _infos;
    size_t _cur_idx{0};
    static SchemaScanner::ColumnDesc _s_columns[];
};

} // namespace starrocks::vectorized
