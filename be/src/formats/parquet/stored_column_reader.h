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

#include <cstddef>
#include <cstdint>
#include <memory>

#include "column_reader.h"
#include "common/status.h"
#include "formats/parquet/column_chunk_reader.h"
#include "formats/parquet/schema.h"
#include "formats/parquet/types.h"
#include "formats/parquet/utils.h"
#include "gen_cpp/parquet_types.h"

namespace starrocks {
class Column;
} // namespace starrocks

namespace starrocks::parquet {

class ColumnChunkReader;
class StoredColumnReader {
public:
    static Status create(const ColumnReaderOptions& opts, const ParquetField* field,
                         const tparquet::ColumnChunk* _chunk_metadata, std::unique_ptr<StoredColumnReader>* out);
    StoredColumnReader(const ColumnReaderOptions& opts) : _opts(opts) {}

    virtual ~StoredColumnReader() = default;

    // Reset internal state and ready for next read_values
    virtual void reset() = 0;

    // If need_levels is set, client will get all levels through get_levels function.
    // If need_levels is not set, read_records may not records levels information, this will
    // improve performance. So set this flag when you only needs it.
    // TODO(zc): to recosiderate to move this flag to StoredColumnReaderOptions
    // StoredColumnReaderOptions is shared by all StoredColumnReader, but we only want set StoredColumnReader specifically,
    // so currently we can't put need_parse_levels into StoredColumnReaderOptions.
    virtual void set_need_parse_levels(bool need_parse_levels) {}

    // Return actually rows skipped or Status::InternalError()
    // If meet EOF, will return 0
    StatusOr<size_t> skip(size_t rows_to_skip) {
        reset();
        return do_read_or_skip_rows(rows_to_skip);
    }

    // Try to read values that can assemble up to num_rows rows. For example if we want to read
    // an array type, and stored value is [1, 2, 3], [4], [5, 6], when the input num_rows is 3,
    // this function will fill (1, 2, 3, 4, 5, 6) into 'dst'.
    // Return actually rows read or Status::InternalError()
    // If meet EOF, will return 0
    StatusOr<size_t> read(size_t rows_to_read, ColumnContentType* content_type, Column* dst) {
        reset();
        return do_read_or_skip_rows(rows_to_read, content_type, dst);
    }

    // This function can only be called after calling read_values. This function returns the
    // levels for last read_values.
    virtual void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) = 0;

    virtual Status get_dict_values(Column* column) { return _reader->get_dict_values(column); }

    virtual Status get_dict_values(const std::vector<int32_t>& dict_codes, Column* column) {
        return _reader->get_dict_values(dict_codes, column);
    }

    virtual Status get_dict_codes(const std::vector<Slice>& dict_values, std::vector<int32_t>* dict_codes) {
        return _reader->get_dict_codes(dict_values, dict_codes);
    }

protected:
    // Load next page
    // Will return values you skipped(values, not rows)
    // If meet EOF, will return Status::EndOfFile()
    StatusOr<size_t> load_next_page(size_t values_to_skip);
    // Skip and read have similar logic, so we combine it into a function
    virtual StatusOr<size_t> do_read_or_skip_rows(size_t rows_to_do, ColumnContentType* content_type = nullptr,
                                                  Column* dst = nullptr) = 0;

    std::unique_ptr<ColumnChunkReader> _reader;
    size_t _num_values_left_in_cur_page = 0;
    const ColumnReaderOptions& _opts;
};

} // namespace starrocks::parquet
