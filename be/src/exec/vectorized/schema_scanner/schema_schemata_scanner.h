// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks::vectorized {

class SchemaSchemataScanner : public SchemaScanner {
public:
    SchemaSchemataScanner();
    ~SchemaSchemataScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    Status fill_chunk(ChunkPtr* chunk);

    int _db_index{0};
    TGetDbsResult _db_result;
    static SchemaScanner::ColumnDesc _s_columns[];
};

} // namespace starrocks::vectorized
