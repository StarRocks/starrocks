// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cstdint>

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks::vectorized {

class SchemaCollationsScanner : public SchemaScanner {
public:
    SchemaCollationsScanner();
    ~SchemaCollationsScanner() override;

    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    struct CollationStruct {
        const char* name;
        const char* charset;
        int64_t id;
        const char* is_default;
        const char* is_compile;
        int64_t sortlen;
    };

    Status fill_chunk(ChunkPtr* chunk);

    int _index{0};
    static SchemaScanner::ColumnDesc _s_cols_columns[];
    static CollationStruct _s_collations[];
};

} // namespace starrocks::vectorized
