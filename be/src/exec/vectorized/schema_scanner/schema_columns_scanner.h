// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <string>

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks::vectorized {

class SchemaColumnsScanner : public SchemaScanner {
public:
    SchemaColumnsScanner();
    ~SchemaColumnsScanner() override;
    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;
    std::string to_mysql_data_type_string(TColumnDesc& desc);
    std::string type_to_string(TColumnDesc& desc);

private:
    Status get_new_table();
    Status fill_chunk(ChunkPtr* chunk);
    Status get_new_desc();
    Status get_create_table(std::string* result);

    int _db_index{0};
    int _table_index{0};
    int _column_index{0};
    TGetDbsResult _db_result;
    TGetTablesResult _table_result;
    TDescribeTableResult _desc_result;
    static SchemaScanner::ColumnDesc _s_col_columns[];
};

} // namespace starrocks::vectorized
