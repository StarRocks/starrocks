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

namespace starrocks {

struct MetricsInfo {
    std::string name;
    std::string labels;
    int64_t value{0};
};

class SchemaBeMetricsScanner : public vectorized::SchemaScanner {
public:
    SchemaBeMetricsScanner();
    ~SchemaBeMetricsScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next(vectorized::ChunkPtr* chunk, bool* eos) override;

private:
<<<<<<< HEAD:be/src/exec/vectorized/schema_scanner/schema_be_metrics_scanner.h
    Status fill_chunk(vectorized::ChunkPtr* chunk);

    int64_t _be_id{0};
    std::vector<MetricsInfo> _infos;
    size_t _cur_idx{0};
    static SchemaScanner::ColumnDesc _s_columns[];
=======
    bool _maybe_duplicated_keys;
    std::vector<SlotId> _arguments_ids;
    bool _is_prepared = false;
>>>>>>> 0d83a4174e ([BugFix] Expr prepare() should do only once (#34060)):be/src/exprs/map_apply_expr.h
};

} // namespace starrocks
