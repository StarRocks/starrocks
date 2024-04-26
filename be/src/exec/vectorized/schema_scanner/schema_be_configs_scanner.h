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

private:
    Status fill_chunk(vectorized::ChunkPtr* chunk);

    int64_t _be_id{0};
    std::vector<std::pair<std::string, std::string>> _infos;
    size_t _cur_idx{0};
    static SchemaScanner::ColumnDesc _s_columns[];
=======
    static void append_int_conjunct(TExprOpcode::type opcode, SlotId slot_id, int value, std::vector<TExpr>* tExprs);
    static void append_string_conjunct(TExprOpcode::type opcode, SlotId slot_id, std::string value,
                                       std::vector<TExpr>* tExprs);
>>>>>>> 7fe278ca54 ([BugFix] Fix parquet footer not have min max statistics caused inaccurate query results (#44489)):be/test/formats/parquet/parquet_ut_base.h
};

} // namespace starrocks
