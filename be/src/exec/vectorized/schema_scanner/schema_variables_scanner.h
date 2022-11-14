// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <map>
#include <string>

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks::vectorized {

class SchemaVariablesScanner : public SchemaScanner {
public:
    SchemaVariablesScanner(TVarType::type type);
    ~SchemaVariablesScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    Status fill_chunk(ChunkPtr* chunk);

    static SchemaScanner::ColumnDesc _s_vars_columns[];

    TShowVariableResult _var_result;
    TVarType::type _type;
    std::map<std::string, std::string>::iterator _begin;
};

} // namespace starrocks::vectorized
