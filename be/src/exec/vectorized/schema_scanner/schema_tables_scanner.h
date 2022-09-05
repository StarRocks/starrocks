// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks::vectorized {

class SchemaTablesScanner : public SchemaScanner {
public:
    SchemaTablesScanner();
    ~SchemaTablesScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    Status get_new_table();
    Status fill_chunk(ChunkPtr* chunk);

    int _tables_info_index{0};
    TGetTablesInfoResponse _tabls_info_response;
    static SchemaScanner::ColumnDesc _s_tbls_columns[];
};

} // namespace starrocks::vectorized
