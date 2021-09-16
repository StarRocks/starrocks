// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks::vectorized {

class SchemaSchemataScanner : public SchemaScanner {
public:
    SchemaSchemataScanner();
    virtual ~SchemaSchemataScanner();

    virtual Status start(RuntimeState* state);
    virtual Status get_next(ChunkPtr* chunk, bool* eos);

private:
    Status fill_chunk(ChunkPtr* chunk);

    int _db_index;
    TGetDbsResult _db_result;
    static SchemaScanner::ColumnDesc _s_columns[];
};

} // namespace starrocks::vectorized
