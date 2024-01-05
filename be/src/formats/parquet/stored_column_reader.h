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
class NullableColumn;
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

    virtual Status read_records(size_t* num_rows, ColumnContentType content_type, Column* dst) = 0;

    virtual Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnContentType content_type,
                              Column* dst) = 0;

    // This function can only be called after calling read_values. This function returns the
    // levels for last read_values.
    virtual void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) = 0;

    virtual Status get_dict_values(Column* column) = 0;

    virtual Status get_dict_values(const std::vector<int32_t>& dict_codes, const NullableColumn& nulls,
                                   Column* column) = 0;

    virtual Status try_load_dictionary() { return Status::InternalError("Not supported try_load_dictionary"); }

    virtual Status load_specific_page(size_t cur_page_idx, uint64_t offset, uint64_t first_row) {
        return Status::InternalError("Not supported load_specific_page");
    }

    virtual void set_page_num(size_t page_num) {}
};

class StoredColumnReaderImpl : public StoredColumnReader {
public:
    StoredColumnReaderImpl(const ColumnReaderOptions& opts) : _opts(opts) {}

    virtual ~StoredColumnReaderImpl() = default;

    // Reset internal state and ready for next read_values
    virtual void reset() = 0;

    // Try to read values that can assemble up to num_rows rows. For example if we want to read
    // an array type, and stored value is [1, 2, 3], [4], [5, 6], when the input num_rows is 3,
    // this function will fill (1, 2, 3, 4, 5, 6) into 'dst'.
    Status read_records(size_t* num_rows, ColumnContentType content_type, Column* dst) override {
        reset();
        return do_read_records(num_rows, content_type, dst);
    }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnContentType content_type,
                      Column* dst) override;

    virtual Status get_dict_values(Column* column) override { return _reader->get_dict_values(column); }

    virtual Status get_dict_values(const std::vector<int32_t>& dict_codes, const NullableColumn& nulls,
                                   Column* column) override {
        return _reader->get_dict_values(dict_codes, nulls, column);
    }

    Status try_load_dictionary() override {
        RETURN_IF_ERROR(_reader->try_load_dictionary());
        // maybe there is no dictionary page, try_load_dictionary just loaded header
        _cur_page_loaded = false;
        _num_values_left_in_cur_page = _reader->num_values();
        return Status::OK();
    }

    Status load_specific_page(size_t cur_page_idx, uint64_t offset, uint64_t first_row) override;

    void set_page_num(size_t page_num) override { _reader->set_page_num(page_num); }

    static size_t get_level_to_decode_batch_size(size_t row, size_t num_values_left_in_cur_page, size_t decoded,
                                                 size_t parsed);

    static size_t count_not_null(level_t* def_levels, size_t num_parsed_levels, level_t max_def_level);

protected:
    virtual bool page_selected(size_t num_values);

    virtual Status do_read_records(size_t* num_rows, ColumnContentType content_type, Column* dst) = 0;

    Status next_page(size_t records_to_read, ColumnContentType content_type, size_t* records_read, Column* dst);

    virtual Status _next_page();
    virtual bool _cur_page_selected(size_t row_readed, const Filter* filter, size_t to_read);

    void update_read_context(size_t records_read);

    // for RequiredColumn, there is no need to get levels.
    // for RepeatedColumn, there is no possible to get default levels.
    // for OptionalColumn, we will override it.
    virtual void append_default_levels(size_t row_nums) {}

    std::unique_ptr<ColumnChunkReader> _reader;
    size_t _num_values_left_in_cur_page = 0;
    size_t _num_values_skip_in_cur_page = 0;
    const ColumnReaderOptions& _opts;
    bool _cur_page_loaded = false;
    uint64_t _read_cursor = _opts.first_row_index;

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
    // to skip values bases no levels we need to collect_not_null_values when decoding levels,
    // for required column, we don't need levels information,
    // for optional column, we collect levels information lazily,
    // for repeated column, we collect levels information after _convert_row_to_value which had decoded the levels.
    virtual void _collect_not_null_values(size_t num_levels, bool lazy_flag) {}

    virtual Status _lazy_skip_values(uint64_t begin) = 0;
    virtual Status _read_values_on_levels(size_t num_values, starrocks::parquet::ColumnContentType content_type,
                                          starrocks::Column* dst, bool append_default) = 0;
};

} // namespace starrocks::parquet
