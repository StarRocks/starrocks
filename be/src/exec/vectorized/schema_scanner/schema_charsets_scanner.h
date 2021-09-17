// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <stdint.h>

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks::vectorized {

class SchemaCharsetsScanner : public SchemaScanner {
public:
    SchemaCharsetsScanner();
    virtual ~SchemaCharsetsScanner();

    virtual Status get_next(ChunkPtr* chunk, bool* eos);

private:
    struct CharsetStruct {
        const char* charset;
        const char* default_collation;
        const char* description;
        int64_t maxlen;
    };

    Status fill_chunk(ChunkPtr* chunk);

    int _index;
    static SchemaScanner::ColumnDesc _s_css_columns[];
    static CharsetStruct _s_charsets[];
};

} // namespace starrocks::vectorized
