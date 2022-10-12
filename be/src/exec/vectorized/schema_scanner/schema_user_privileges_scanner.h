// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <string>

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks::vectorized {

class SchemaUserPrivilegesScanner : public SchemaScanner {
public:
    SchemaUserPrivilegesScanner();
    ~SchemaUserPrivilegesScanner() override;
    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    Status fill_chunk(ChunkPtr* chunk);

    int _user_priv_index{0};
    TGetUserPrivsResult _user_privs_result;
    static SchemaScanner::ColumnDesc _s_user_privs_columns[];
};

} // namespace starrocks::vectorized