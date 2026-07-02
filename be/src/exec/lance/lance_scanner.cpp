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

#include "exec/lance/lance_scanner.h"

#include "runtime/runtime_state.h"

namespace starrocks {

Status LanceScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    return Status::OK();
}

Status LanceScanner::do_open(RuntimeState* runtime_state) {
    _reader = std::make_unique<LanceNativeReader>(_scanner_params, _scanner_ctx, runtime_state, &_app_stats);
    return _reader->open();
}

Status LanceScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    ASSIGN_OR_RETURN(size_t row_count, _reader->get_next(chunk));
    RETURN_IF_ERROR(_scanner_ctx.append_or_update_not_existed_columns_to_chunk(chunk, row_count));
    _scanner_ctx.append_or_update_partition_column_to_chunk(chunk, row_count);
    RETURN_IF_ERROR(_scanner_ctx.evaluate_on_conjunct_ctxs_by_slot(chunk, &_chunk_filter));
    return Status::OK();
}

void LanceScanner::do_close(RuntimeState* runtime_state) noexcept {
    _reader.reset();
}

} // namespace starrocks
