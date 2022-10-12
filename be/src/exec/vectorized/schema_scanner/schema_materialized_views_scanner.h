// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks::vectorized {

class SchemaMaterializedViewsScanner : public SchemaScanner {
public:
    SchemaMaterializedViewsScanner();
    ~SchemaMaterializedViewsScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    Status get_new_table();
    Status fill_chunk(ChunkPtr* chunk);

    int _db_index{0};
    int _table_index{0};
    TGetDbsResult _db_result;
    TListTableStatusResult _table_result;
    static SchemaScanner::ColumnDesc _s_tbls_columns[];
};

} // namespace starrocks::vectorized