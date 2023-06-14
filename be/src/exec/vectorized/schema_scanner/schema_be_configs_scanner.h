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

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks {

class SchemaBeConfigsScanner : public vectorized::SchemaScanner {
public:
    SchemaBeConfigsScanner();
    ~SchemaBeConfigsScanner() override;

<<<<<<< HEAD:be/src/exec/vectorized/schema_scanner/schema_be_configs_scanner.h
    Status start(RuntimeState* state) override;
    Status get_next(vectorized::ChunkPtr* chunk, bool* eos) override;
=======
    recorder.update_interval();
    int cpu_used_permille = recorder.cpu_used_permille();
    ASSERT_GE(cpu_used_permille, 0);
}
>>>>>>> 2d8daa75a ([BugFix] Fix CpuUsageRecorderTest failure (#25289)):be/test/util/cpu_usage_info_test.cpp

private:
    Status fill_chunk(vectorized::ChunkPtr* chunk);

    int64_t _be_id{0};
    std::vector<std::pair<std::string, std::string>> _infos;
    size_t _cur_idx{0};
    static SchemaScanner::ColumnDesc _s_columns[];
};

} // namespace starrocks
