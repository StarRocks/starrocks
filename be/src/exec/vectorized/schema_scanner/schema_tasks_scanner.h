// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks::vectorized {

class SchemaTasksScanner : public SchemaScanner {
public:
    SchemaTasksScanner();
    ~SchemaTasksScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    Status fill_chunk(ChunkPtr* chunk);

    int _task_index{0};
    TGetTaskInfoResult _task_result;
    static SchemaScanner::ColumnDesc _s_tbls_columns[];
};

} // namespace starrocks::vectorized
