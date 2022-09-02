// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks::vectorized {

class SchemaTablesConfigScanner : public SchemaScanner {
public:
    SchemaTablesConfigScanner();
    ~SchemaTablesConfigScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    Status get_new_table();
    Status fill_chunk(ChunkPtr* chunk);

    int _tables_config_index{0};
    TGetTablesConfigResponse _tables_config_response;
    static SchemaScanner::ColumnDesc _s_table_tables_config_columns[];
};

} // namespace starrocks::vectorized