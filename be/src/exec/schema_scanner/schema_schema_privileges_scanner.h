// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <string>

#include "exec/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks {

class SchemaSchemaPrivilegesScanner : public SchemaScanner {
public:
    SchemaSchemaPrivilegesScanner();
    virtual ~SchemaSchemaPrivilegesScanner();
    Status start(RuntimeState* state) override;
    Status get_next_row(Tuple* tuple, MemPool* pool, bool* eos) override;

private:
    Status fill_one_row(Tuple* tuple, MemPool* pool);

    int _db_priv_index;
    TGetDBPrivsResult _db_privs_result;
    static SchemaScanner::ColumnDesc _s_db_privs_columns[];
};

} // namespace starrocks