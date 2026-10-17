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
//                     parquet_read_rows(r.source_info) p
//     WHERE r.format = 'parquet';
//
// `source_info` must carry `file` and `row_in_file`. `file_size` and
// `file_mtime_ms` are optional anchors:
//   * When present, BE compares them against the live file and **fails the
//     whole query** on a mismatch (Status::Corruption). This is the strict
//     fail-closed path.
//   * When absent, the corresponding check is skipped — the rejected-records
//     writer omits `file_size` / `file_mtime_ms` whenever the upstream
//     scanner did not know them (see arrow_to_starrocks_converter.cpp).
//   * If `file_size` is present but the underlying filesystem cannot report
//     a size (S3 / OSS / Starlet), BE falls back to using the anchor's value
//     and skips that single check.
//   * If `file_mtime_ms` is present but the filesystem cannot report mtime
//     (S3 / OSS / Starlet), BE silently skips the mtime check (size check
//     above already catches the most common drift).
// After validation, BE re-opens the file via FileSystem, reads only the row
// group containing the anchor, projects out the target row, and emits it as
// a JSON object keyed by Parquet column names.
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
