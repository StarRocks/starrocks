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

#include "exec/hdfs_scanner_partition.h"

#include "column/column_helper.h"

namespace starrocks {

Status HdfsPartitionScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    (*chunk) = std::make_shared<Chunk>();
    for (const HdfsScannerContext::ColumnInfo& c : _scanner_ctx.materialized_columns) {
        ColumnPtr column = ColumnHelper::create_column(c.col_type, true);
        column->append_default(1);
        (*chunk)->append_column(column, c.slot_id);
    }
    _scanner_ctx.append_not_existed_columns_to_chunk(chunk, 1);
    return Status::OK();
}

Status HdfsPartitionScanner::do_open(RuntimeState* runtime_state) {
    return Status::OK();
}
void HdfsPartitionScanner::do_close(RuntimeState* runtime_state) noexcept {}

Status HdfsPartitionScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    return Status::OK();
}

} // namespace starrocks
