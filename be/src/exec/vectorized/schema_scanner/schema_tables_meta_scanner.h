// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks::vectorized {

class SchemaTablesMetaScanner : public SchemaScanner {
public:
    SchemaTablesMetaScanner();
    ~SchemaTablesMetaScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    Status get_new_table();
    Status fill_chunk(ChunkPtr* chunk);

    int _tables_meta_index{0};
    TGetTablesMetaResponse _tables_meta_response;
    static SchemaScanner::ColumnDesc _s_table_tables_meta_columns[];
};

} // namespace starrocks::vectorized