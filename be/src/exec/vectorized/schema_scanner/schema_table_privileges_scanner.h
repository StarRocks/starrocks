// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <string>

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks::vectorized {

class SchemaTablePrivilegesScanner : public SchemaScanner {
public:
    SchemaTablePrivilegesScanner();
    ~SchemaTablePrivilegesScanner() override;
    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    Status fill_chunk(ChunkPtr* chunk);

    int _table_priv_index;
    TGetTablePrivsResult _table_privs_result;
    static SchemaScanner::ColumnDesc _s_table_privs_columns[];
};

} // namespace starrocks::vectorized