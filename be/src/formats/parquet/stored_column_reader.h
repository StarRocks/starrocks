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
#include <vector>

#include "column/vectorized_fwd.h"
#include "column_reader.h"
#include "common/status.h"
#include "common/statusor.h"
#include "formats/parquet/column_chunk_reader.h"
#include "formats/parquet/schema.h"
#include "formats/parquet/types.h"
#include "formats/parquet/utils.h"
#include "gen_cpp/parquet_types.h"
#include "storage_primitive/range.h"

namespace tparquet {
class ColumnChunk;
} // namespace tparquet

namespace starrocks {
class Column;
class NullableColumn;

namespace parquet {
struct ParquetField;
} // namespace parquet
} // namespace starrocks

namespace starrocks::parquet {

class ColumnChunkReader;

class StoredColumnReader {
public:
    static Status create(const ColumnReaderOptions& opts, const ParquetField* field,
                         const tparquet::ColumnChunk* _chunk_metadata, std::unique_ptr<StoredColumnReader>* out);
    virtual ~StoredColumnReader() = default;

    // If need_levels is set, client will get all levels through get_levels function.
    // If need_levels is not set, read_records may not records levels information, this will
    // improve performance. So set this flag when you only needs it.
    // TODO(zc): to recosiderate to move this flag to StoredColumnReaderOptions
    // StoredColumnReaderOptions is shared by all StoredColumnReader, but we only want set StoredColumnReader specifically,
    // so currently we can't put need_parse_levels into StoredColumnReaderOptions.
    virtual void set_need_parse_levels(bool need_parse_levels) {}

    virtual Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnContentType content_type,
                              Column* dst) = 0;

    // This function can only be called after calling read_values. This function returns the
    // levels for last read_values.
    virtual void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) = 0;

    virtual Status get_dict_values(Column* column) = 0;

    virtual Status get_dict_values(const Buffer<int32_t>& dict_codes, const NullableColumn& nulls, Column* column) = 0;

    virtual Status load_dictionary_page() { return Status::InternalError("Not supported load_dictionary_page"); }

    virtual Status load_specific_page(size_t cur_page_idx, uint64_t offset, uint64_t first_row) {
        return Status::InternalError("Not supported load_specific_page");
    }

    virtual void set_page_num(size_t page_num) {}

    virtual void set_page_change_on_record_boundry() {}
};

class StoredColumnReaderImpl : public StoredColumnReader {
public:
    StoredColumnReaderImpl(const ColumnReaderOptions& opts) : _opts(opts) {}

    ~StoredColumnReaderImpl() override = default;

    // Reset internal state and ready for next read_values
    virtual void reset_levels() = 0;

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnContentType content_type,
                      Column* dst) override;

    Status get_dict_values(Column* column) override { return _reader->get_dict_values(column); }

    Status get_dict_values(const Buffer<int32_t>& dict_codes, const NullableColumn& nulls, Column* column) override {
        return _reader->get_dict_values(dict_codes, nulls, column);
    }

    Status load_dictionary_page() override { return _reader->load_dictionary_page(); }

    Status load_specific_page(size_t cur_page_idx, uint64_t offset, uint64_t first_row) override;

    void set_page_num(size_t page_num) override { _reader->set_page_num(page_num); }

    static size_t count_not_null(level_t* def_levels, size_t num_parsed_levels, level_t max_def_level);

protected:
    virtual Status _next_page();
    virtual bool _cur_page_selected(size_t row_readed, const Filter* filter, size_t to_read);

    // for RequiredColumn, there is no need to get levels.
    // for RepeatedColumn, there is no possible to get default levels.
    // for OptionalColumn, we will override it.
    virtual void _append_default_levels(size_t row_nums) {}

    // Record that `num_levels` levels of the current page (already delimited by _convert_row_to_value) are passed
    // over without being materialized. Decoders are not moved here; the next selected read skips them in
    // _skip_deferred(). The caller must call this before decreasing _num_values_left_in_cur_page.
    virtual void _defer_skip(size_t num_levels) { _levels_to_skip += num_levels; }

    // Convert _levels_to_skip into _values_to_skip by consuming the levels from the level decoder.
    // The page must be loaded. For RequiredColumn, levels are values.
    virtual Status _skip_deferred_levels() {
        _values_to_skip += _levels_to_skip;
        _levels_to_skip = 0;
        return Status::OK();
    }

    // Position the decoders of the current page at the read cursor: load the page if needed and skip
    // everything recorded by _defer_skip().
    Status _skip_deferred();

    void _reset_deferred_skip() {
        _levels_to_skip = 0;
        _values_to_skip = 0;
    }

    std::unique_ptr<ColumnChunkReader> _reader;
    size_t _num_values_left_in_cur_page = 0;
    // Pending skips of the current page, see _defer_skip(). Both are reset when a new page is entered.
    // Levels passed over but not consumed from the level decoder yet.
    size_t _levels_to_skip = 0;
    // Values whose levels are consumed but which are not skipped in the value decoder yet.
    size_t _values_to_skip = 0;
    const ColumnReaderOptions& _opts;
    bool _cur_page_loaded = false;
    uint64_t _read_cursor = _opts.first_row_index;
    static constexpr size_t BATCH_PROCESS_SIZE = 8192;

private:
    Status _next_selected_page(size_t records_to_read, ColumnContentType content_type, size_t* records_to_skip,
                               Column* dst);

    Status _lazy_load_page_rows(size_t batch_size, ColumnContentType content_type, Column* dst);

    Status _skip(uint64_t row_to_skip);
    Status _read(const Range<uint64_t>& range, const Filter* filter, ColumnContentType content_type, Column* dst);

    // input is target row, this function will convert row to values bases on _num_values_left_in_cur_page,
    // only convert in the current page, the return is the num of values that will be used and
    // the input target row pointer is changed to the result row that can be dealt in current page.
    virtual StatusOr<size_t> _convert_row_to_value(size_t* row);

    // Read `num_values` levels (and their values) of the current page into dst. If append_default is set, the
    // segment is not selected: only default values (and levels) are appended, the skip is recorded by the caller
    // with _defer_skip().
    virtual Status _read_values_on_levels(size_t num_values, starrocks::parquet::ColumnContentType content_type,
                                          starrocks::Column* dst, bool append_default,
                                          const FilterData* filter = nullptr) = 0;

    virtual const FilterData* _convert_filter_row_to_value(const Filter* filter, size_t row_readed);
};

} // namespace starrocks::parquet
