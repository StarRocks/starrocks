// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>

#include "exec/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks {

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

} // namespace starrocks
