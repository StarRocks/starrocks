// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cstdint>

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks::vectorized {

class SchemaCharsetsScanner : public SchemaScanner {
public:
    SchemaCharsetsScanner();
    ~SchemaCharsetsScanner() override;

    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    struct CharsetStruct {
        const char* charset;
        const char* default_collation;
        const char* description;
        int64_t maxlen;
    };

    Status fill_chunk(ChunkPtr* chunk);

    int _index{0};
    static SchemaScanner::ColumnDesc _s_css_columns[];
    static CharsetStruct _s_charsets[];
};

} // namespace starrocks::vectorized
