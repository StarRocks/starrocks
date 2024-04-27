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

#include <stdint.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/global_types.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/expr_context.h"
#include "exprs/function_context.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/group_reader.h"
#include "formats/parquet/schema.h"
#include "gen_cpp/parquet_types.h"
#include "io/shared_buffered_input_stream.h"
#include "runtime/types.h"
#include "storage/range.h"

namespace starrocks {
class RandomAccessFile;

namespace parquet {
class ColumnReader;
class GroupReader;
struct ParquetField;
} // namespace parquet
struct TypeDescriptor;
} // namespace starrocks

namespace starrocks::parquet {

struct ColumnOffsetIndexCtx {
    tparquet::OffsetIndex offset_index;
    std::vector<bool> page_selected;
    uint64_t rg_first_row;

    void collect_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                          bool active);

    // be compatible with PARQUET-1850
    bool check_dictionary_page(int64_t data_page_offset) {
        return offset_index.page_locations.size() > 0 && offset_index.page_locations[0].offset > data_page_offset;
    }
};

class PageIndexReader {
public:
    PageIndexReader(GroupReader* group_reader, RandomAccessFile* file,
                    const std::unordered_map<SlotId, std::unique_ptr<ColumnReader>>& column_readers,
                    const tparquet::RowGroup* meta, const std::vector<ExprContext*> min_max_conjunct_ctxs)
            : _group_reader(group_reader),
              _file(file),
              _column_readers(column_readers),
              _row_group_metadata(meta),
              _min_max_conjunct_ctxs(min_max_conjunct_ctxs) {}

    StatusOr<bool> generate_read_range(SparseRange<uint64_t>& sparse_range);

    void select_column_offset_index();

private:
    Status _decode_value_into_column(ColumnPtr column, const std::vector<std::string>& values,
                                     const TypeDescriptor& type, const ParquetField* field,
                                     const std::string& timezone);

    GroupReader* _group_reader = nullptr;
    RandomAccessFile* _file = nullptr;
    // column readers for column chunk in row group
    const std::unordered_map<SlotId, std::unique_ptr<ColumnReader>>& _column_readers;
    // row group meta
    const tparquet::RowGroup* _row_group_metadata = nullptr;

    // min/max conjuncts
    std::vector<ExprContext*> _min_max_conjunct_ctxs;
};

} // namespace starrocks::parquet