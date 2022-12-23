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

#include <map>
#include <string>

#include "exec/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks {

class SchemaVariablesScanner : public SchemaScanner {
public:
    SchemaVariablesScanner(TVarType::type type);
    ~SchemaVariablesScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    Status fill_chunk(ChunkPtr* chunk);

    Status _fill_chunk_for_verbose(ChunkPtr* chunk);

    static SchemaScanner::ColumnDesc _s_vars_columns[];

    static SchemaScanner::ColumnDesc _s_verbose_vars_columns[];

    TShowVariableResult _var_result;
    TVarType::type _type;
    std::map<std::string, std::string>::iterator _begin;
    std::vector<starrocks::TVerboseVariableRecord>::iterator _verbose_iter;
};

} // namespace starrocks
