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

#include "exec/paimon/paimon_vector_data_scanner.h"

#include <glog/logging.h>

namespace starrocks {

Status PaimonVectorDataScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    _vector_condition = scanner_params.paimon_vector_search_condition;
    if (_vector_condition == nullptr) {
        return Status::InvalidArgument("PaimonVectorDataScanner requires a vector search condition");
    }
    LOG(INFO) << "PaimonVectorDataScanner::do_init shard_id=" << _vector_condition->shard_id
              << " function=" << _vector_condition->score_function_name
              << " column=" << _vector_condition->vector_column_name
              << " limit_per_shard=" << _vector_condition->limit_per_shard;
    return Status::OK();
}

Status PaimonVectorDataScanner::do_open(RuntimeState* state) {
    // TODO: open Paimon native vector index and execute ANN search for this shard
    return Status::OK();
}

void PaimonVectorDataScanner::do_close(RuntimeState* runtime_state) noexcept {
    // TODO: release Paimon native index resources
}

Status PaimonVectorDataScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    if (_is_finished) {
        return Status::EndOfFile("PaimonVectorDataScanner finished");
    }
    // TODO: iterate ANN search results, read corresponding rows, fill chunk
    _is_finished = true;
    return Status::EndOfFile("PaimonVectorDataScanner finished");
}

} // namespace starrocks
