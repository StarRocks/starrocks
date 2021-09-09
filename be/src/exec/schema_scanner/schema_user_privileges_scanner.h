// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <string>

#include "exec/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks {

class SchemaUserPrivilegesScanner : public SchemaScanner {
public:
    SchemaUserPrivilegesScanner();
    virtual ~SchemaUserPrivilegesScanner();
    Status start(RuntimeState* state) override;
    Status get_next_row(Tuple* tuple, MemPool* pool, bool* eos) override;

private:
    Status fill_one_row(Tuple* tuple, MemPool* pool);

    int _user_priv_index;
    TGetUserPrivsResult _user_privs_result;
    static SchemaScanner::ColumnDesc _s_user_privs_columns[];
};

} // namespace starrocks