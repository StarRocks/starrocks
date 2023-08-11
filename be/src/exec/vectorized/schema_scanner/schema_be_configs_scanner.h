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

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

#include "common/compiler_util.h"

namespace starrocks {

class SchemaBeConfigsScanner : public vectorized::SchemaScanner {
public:
    SchemaBeConfigsScanner();
    ~SchemaBeConfigsScanner() override;

<<<<<<< HEAD:be/src/exec/vectorized/schema_scanner/schema_be_configs_scanner.h
    Status start(RuntimeState* state) override;
    Status get_next(vectorized::ChunkPtr* chunk, bool* eos) override;
=======
    char* malloc(uint64_t size) override {
        // Return nullptr if size is 0, otherwise debug-enabled jemalloc would fail non-zero size assertion.
        // See https://github.com/jemalloc/jemalloc/issues/2514
        if (UNLIKELY(size == 0)) {
            return nullptr;
        }
        auto p = static_cast<char*>(std::malloc(size));
        if (UNLIKELY(p == nullptr)) {
            LOG(WARNING) << "malloc failed, size=" << size;
            throw std::bad_alloc();
        }
        return p;
    }
>>>>>>> 0e4994e572 ([BugFix] Avoid jemalloc non-zero assertion failure (#29062)):be/src/formats/orc/orc_memory_pool.cpp

private:
    Status fill_chunk(vectorized::ChunkPtr* chunk);

    int64_t _be_id{0};
    std::vector<std::pair<std::string, std::string>> _infos;
    size_t _cur_idx{0};
    static SchemaScanner::ColumnDesc _s_columns[];
};

} // namespace starrocks
