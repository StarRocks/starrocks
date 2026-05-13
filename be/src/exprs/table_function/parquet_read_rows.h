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

#include "exprs/table_function/table_function.h"

namespace starrocks {

// parquet_read_rows(source_info JSON) -> (file VARCHAR, row_in_file BIGINT, raw_record JSON)
//
// Lateral TVF that rehydrates the original Parquet row pointed to by a
// `source_info` anchor written by the rejected-records pipeline. Designed to be
// used as the right side of an implicit join with `_statistics_.rejected_records`:
//
//     SELECT ... FROM rejected_records r,
//                     TABLE(parquet_read_rows(r.source_info)) p
//     WHERE r.format = 'parquet';
//
// `source_info` must carry `file`, `row_in_file`, `file_size`, and `file_mtime_ms`.
// The BE re-opens the file via FileSystem, fail-closed checks the recorded size
// and modification time against the current file, then reads only the row group
// containing the anchor, projects out the target row, and emits it as a JSON
// object keyed by Parquet column names. Anchors whose file_size or mtime no
// longer match fail the whole query.
class ParquetReadRows final : public TableFunction {
public:
    Status init(const TFunction& fn, TableFunctionState** state) const override;
    Status prepare(TableFunctionState* state) const override;
    Status open(RuntimeState* runtime_state, TableFunctionState* state) const override;
    Status close(RuntimeState* runtime_state, TableFunctionState* state) const override;

    std::pair<Columns, UInt32Column::Ptr> process(RuntimeState* runtime_state,
                                                  TableFunctionState* state) const override;
};

} // namespace starrocks
