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

#include "formats/parquet/stored_column_reader.h"

#include <fmt/core.h>
#include <glog/logging.h>

#include <algorithm>
#include <sstream>
#include <string>
#include <utility>

#include "column/column.h"
#include "column_reader.h"
#include "common/compiler_util.h"
#include "common/logging.h"
#include "common/status.h"
#include "formats/parquet/level_codec.h"
#include "formats/parquet/schema.h"
#include "formats/parquet/types.h"
#include "formats/parquet/utils.h"
#include "simd/simd.h"

namespace tparquet {
class ColumnChunk;
} // namespace tparquet

namespace starrocks::parquet {

class RepeatedStoredColumnReader : public StoredColumnReaderImpl {
public:
    RepeatedStoredColumnReader(const ColumnReaderOptions& opts) : StoredColumnReaderImpl(opts) {}
    ~RepeatedStoredColumnReader() override = default;

    Status init(const ParquetField* field, const tparquet::ColumnChunk* chunk_metadata) {
        _field = field;
        _reader = std::make_unique<ColumnChunkReader>(_field->max_def_level(), _field->max_rep_level(),
                                                      _field->type_length, chunk_metadata, _opts);
        RETURN_IF_ERROR(_reader->init(_opts.chunk_size));
        return Status::OK();
    }

    void reset_levels() override;

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        _reader->def_level_decoder().get_levels(def_levels, num_levels);
        _reader->rep_level_decoder().get_levels(rep_levels, num_levels);
    }

    Status load_specific_page(size_t cur_page_idx, uint64_t offset, uint64_t first_row) override;

    void set_page_change_on_record_boundry() override { _page_change_on_record_boundry = true; }

private:
    // Try to decode enough levels in levels buffer, if there are no enough levels, will throw InternalError msg.
    Status _decode_levels(size_t* num_rows, size_t* num_levels_parsed, level_t** def_levels);

    void _delimit_rows(const level_t* rep_levels, size_t* num_rows, size_t* num_levels_parsed);

    void _consume_levels(size_t num_values) {
        _reader->def_level_decoder().consume_levels(num_values);
        _reader->rep_level_decoder().consume_levels(num_values);
    }

    Status _next_page() override;

    StatusOr<size_t> _convert_row_to_value(size_t* row) override;
    void _collect_not_null_values(size_t num_levels, bool lazy_flag) override;

    Status _lazy_skip_values(uint64_t begin) override;
    Status _read_values_on_levels(size_t num_values, starrocks::parquet::ColumnContentType content_type,
                                  starrocks::Column* dst, bool append_default) override;

    bool _cur_page_selected(size_t row_readed, const Filter* filter, size_t to_read) override;

private:
    const ParquetField* _field = nullptr;

    bool _meet_first_record = false;

    size_t _not_null_to_skip = 0;

    // Use uint16_t instead of uint8_t to make it auto simd by compiler.
    std::vector<uint16_t> _is_nulls;

    // default is false, but if there is page index, it's true.
    // so that we don't need check next page to know the last record in current page is finished.
    bool _page_change_on_record_boundry = false;
};

class OptionalStoredColumnReader : public StoredColumnReaderImpl {
public:
    OptionalStoredColumnReader(const ColumnReaderOptions& opts) : StoredColumnReaderImpl(opts) {}
    ~OptionalStoredColumnReader() override = default;

    Status init(const ParquetField* field, const tparquet::ColumnChunk* chunk_metadata) {
        _field = field;
        _reader = std::make_unique<ColumnChunkReader>(_field->max_def_level(), _field->max_rep_level(),
                                                      _field->type_length, chunk_metadata, _opts);
        RETURN_IF_ERROR(_reader->init(_opts.chunk_size));
        return Status::OK();
    }

    // Reset internal state and ready for next read_values
    void reset_levels() override;

    // If need_levels is set, client will get all levels through get_levels function.
    // If need_levels is not set, read_records may not records levels information, this will
    // improve performance. So set this flag when you only needs it.
    void set_need_parse_levels(bool needs_levels) override { _need_parse_levels = needs_levels; }

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        // _needs_levels must be true
        DCHECK(_need_parse_levels);

        *rep_levels = nullptr;
        _reader->def_level_decoder().get_levels(def_levels, num_levels);
    }

private:
    Status _decode_levels(size_t* num_rows, size_t* num_levels_parsed, level_t** def_levels);

    void _consume_levels(size_t num_values) { _reader->def_level_decoder().consume_levels(num_values); }
    Status _lazy_skip_values(uint64_t begin) override;
    Status _read_values_on_levels(size_t num_values, starrocks::parquet::ColumnContentType content_type,
                                  starrocks::Column* dst, bool append_default) override;

    void _append_default_levels(size_t row_nums) override {
        if (_need_parse_levels) {
            _reader->def_level_decoder().append_default_levels(row_nums);
        }
    }

    const ParquetField* _field = nullptr;

    // When the flag is false, the information of levels does not need to be materialized,
    // so that the advantages of RLE encoding can be fully utilized and a lot of overhead
    // can be saved in decoding.
    bool _need_parse_levels = false;

    // Use uint16_t instead of uint8_t to make it auto simd by compiler.
    std::vector<uint16_t> _is_nulls;
};

class RequiredStoredColumnReader : public StoredColumnReaderImpl {
public:
    RequiredStoredColumnReader(const ColumnReaderOptions& opts) : StoredColumnReaderImpl(opts) {}
    ~RequiredStoredColumnReader() override = default;

    Status init(const ParquetField* field, const tparquet::ColumnChunk* chunk_metadata) {
        _field = field;
        _reader = std::make_unique<ColumnChunkReader>(_field->max_def_level(), _field->max_rep_level(),
                                                      _field->type_length, chunk_metadata, _opts);
        RETURN_IF_ERROR(_reader->init(_opts.chunk_size));
        return Status::OK();
    }

    void reset_levels() override {}

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        *def_levels = nullptr;
        *rep_levels = nullptr;
        *num_levels = 0;
    }

private:
    Status _lazy_skip_values(uint64_t begin) override;
    Status _read_values_on_levels(size_t num_values, starrocks::parquet::ColumnContentType content_type,
                                  starrocks::Column* dst, bool append_default) override;
    const ParquetField* _field = nullptr;
};

void RepeatedStoredColumnReader::reset_levels() {
    _reader->def_level_decoder().reset();
    _reader->rep_level_decoder().reset();
}

void RepeatedStoredColumnReader::_delimit_rows(const level_t* rep_levels, size_t* num_rows, size_t* num_levels_parsed) {
    size_t levels_pos = 0;
    size_t avail_levels = *num_levels_parsed;

#ifndef NDEBUG
    std::stringstream ss;
    ss << "rep=[";
    for (int i = levels_pos; i < avail_levels; ++i) {
        ss << ", " << rep_levels[i];
    }
    ss << "]";
    VLOG_FILE << ss.str();
#endif

    if (!_meet_first_record) {
        _meet_first_record = true;
        DCHECK_EQ(rep_levels[levels_pos], 0);
        levels_pos++;
    }

    size_t rows_read = 0;
    for (; levels_pos < avail_levels && rows_read < *num_rows; ++levels_pos) {
        rows_read += rep_levels[levels_pos] == 0;
    }

    if (rows_read == *num_rows) {
        // Notice, ++levels_pos in for-loop will take one step forward, so we need -1
        levels_pos--;
        // Had read enough rows, reset _meet_first_record for next read
        _meet_first_record = false;
        DCHECK_EQ(0, rep_levels[levels_pos]);
    } // else {
      //  means  rows_read < *num_rows, levels_pos >= _levels_decoded,
      //  so we need to decode more levels to obtain a complete line or
      //  we have read all the records in this column chunk.
    // }

    VLOG_FILE << "rows_reader=" << rows_read << ", level_parsed=" << levels_pos;
    *num_rows = rows_read;
    *num_levels_parsed = levels_pos;
}

Status RepeatedStoredColumnReader::_decode_levels(size_t* num_rows, size_t* num_levels_parsed, level_t** def_levels) {
    level_t* rep_levels = nullptr;
    size_t avail_levels = _reader->rep_level_decoder().get_avail_levels(*num_rows, &rep_levels);
    if (UNLIKELY(avail_levels == 0)) {
        return Status::InternalError(fmt::format("num values left in cur page: {}, but no available rep levels",
                                                 _num_values_left_in_cur_page));
    }
    if (UNLIKELY(avail_levels != _reader->def_level_decoder().get_avail_levels(*num_rows, def_levels))) {
        return Status::InternalError("rep/def levels' length do not match");
    }
    _delimit_rows(rep_levels, num_rows, &avail_levels);
    *num_levels_parsed = avail_levels;
    _consume_levels(*num_levels_parsed);
    return Status::OK();
}

void OptionalStoredColumnReader::reset_levels() {
    _reader->def_level_decoder().reset();
}

Status OptionalStoredColumnReader::_decode_levels(size_t* num_rows, size_t* num_levels_parsed, level_t** def_levels) {
    size_t avail_levels = _reader->def_level_decoder().get_avail_levels(*num_rows, def_levels);
    if (UNLIKELY(avail_levels == 0 || *num_rows > avail_levels)) {
        return Status::InternalError(
                fmt::format("def levels need to parsed: {}, def levels parsed: {}", *num_rows, avail_levels));
    }
    *num_levels_parsed = std::min(*num_rows, avail_levels);
    _consume_levels(*num_levels_parsed);
    return Status::OK();
}

Status StoredColumnReader::create(const ColumnReaderOptions& opts, const ParquetField* field,
                                  const tparquet::ColumnChunk* chunk_metadata,
                                  std::unique_ptr<StoredColumnReader>* out) {
    if (field->max_rep_level() > 0) {
        std::unique_ptr<RepeatedStoredColumnReader> reader(new RepeatedStoredColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, chunk_metadata));
        *out = std::move(reader);
    } else if (field->max_def_level() > 0) {
        std::unique_ptr<OptionalStoredColumnReader> reader(new OptionalStoredColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, chunk_metadata));
        *out = std::move(reader);
    } else {
        std::unique_ptr<RequiredStoredColumnReader> reader(new RequiredStoredColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, chunk_metadata));
        *out = std::move(reader);
    }
    return Status::OK();
}

size_t StoredColumnReaderImpl::count_not_null(level_t* def_levels, size_t num_parsed_levels, level_t max_def_level) {
    size_t count = 0;
    for (int i = 0; i < num_parsed_levels; ++i) {
        level_t def_level = def_levels[i];
        if (def_level == max_def_level) {
            count++;
        }
    }
    return count;
}

Status StoredColumnReaderImpl::read_range(const Range<uint64_t>& range, const Filter* filter,
                                          ColumnContentType content_type, Column* dst) {
    // reset_levels() to prepare levels for new reading
    reset_levels();
    if (_read_cursor < range.begin()) {
        RETURN_IF_ERROR(_skip(range.begin() - _read_cursor));
    }
    return _read(range, filter, content_type, dst);
}

Status StoredColumnReaderImpl::_skip(uint64_t rows_to_skip) {
    uint64_t skipped_row = 0;
    while (skipped_row < rows_to_skip) {
        uint64_t batch_to_skip = std::min(rows_to_skip - skipped_row, (uint64_t)BATCH_PROCESS_SIZE);
        size_t batch_skipped = 0;
        while (batch_skipped < batch_to_skip) {
            if (_num_values_left_in_cur_page > 0) {
                size_t skip_row = static_cast<size_t>(batch_to_skip) - batch_skipped;
                ASSIGN_OR_RETURN(size_t num_values, _convert_row_to_value(&skip_row));
                DCHECK_LE(num_values, _num_values_left_in_cur_page);
                _collect_not_null_values(num_values, num_values != _num_values_left_in_cur_page);
                _num_values_left_in_cur_page -= num_values;
                batch_skipped += skip_row;
            } else {
                RETURN_IF_ERROR(_next_page());
            }
        }
        skipped_row += batch_to_skip;
        // reset_levels() when batch_skipped completed, avoiding use too much memory
        reset_levels();
    }
    return Status::OK();
}

Status StoredColumnReaderImpl::_read(const Range<uint64_t>& range, const starrocks::Filter* filter,
                                     starrocks::parquet::ColumnContentType content_type, starrocks::Column* dst) {
    uint64_t row_count = range.span_size();
    size_t row_readed = 0;
    while (row_readed < row_count) {
        if (_num_values_left_in_cur_page > 0) {
            size_t to_read = static_cast<size_t>(row_count) - row_readed;
            ASSIGN_OR_RETURN(size_t num_values, _convert_row_to_value(&to_read));
            DCHECK_LE(num_values, _num_values_left_in_cur_page);
            if (_cur_page_selected(row_readed, filter, to_read)) {
                RETURN_IF_ERROR(_lazy_skip_values(range.begin()));
                RETURN_IF_ERROR(_read_values_on_levels(num_values, content_type, dst, false));
            } else {
                RETURN_IF_ERROR(_read_values_on_levels(num_values, content_type, dst, true));
            }
            row_readed += to_read;
            _num_values_left_in_cur_page -= num_values;
        } else {
            RETURN_IF_ERROR(_next_page());
        }
    }
    _read_cursor = range.end();
    return Status::OK();
}

StatusOr<size_t> StoredColumnReaderImpl::_convert_row_to_value(size_t* row) {
    *row = std::min(*row, _num_values_left_in_cur_page);
    return *row;
}

StatusOr<size_t> RepeatedStoredColumnReader::_convert_row_to_value(size_t* row) {
    size_t num_parsed_levels = 0;
    size_t parsed_row = 0;
    size_t target_row = *row;
    while (num_parsed_levels < _num_values_left_in_cur_page && parsed_row < target_row) {
        size_t row_to_parse = target_row - parsed_row;
        level_t* def_levels = nullptr;
        size_t level_parsed = 0;
        RETURN_IF_ERROR(_decode_levels(&row_to_parse, &level_parsed, &def_levels));
        num_parsed_levels += level_parsed;
        if (num_parsed_levels == _num_values_left_in_cur_page &&
            (_page_change_on_record_boundry || _reader->is_last_page())) {
            row_to_parse += 1;
            _meet_first_record = false;
        }
        parsed_row += row_to_parse;
    }
    *row = parsed_row;
    return num_parsed_levels;
}

Status RequiredStoredColumnReader::_lazy_skip_values(uint64_t begin) {
    if (_cur_page_loaded) {
        return _reader->skip_values(static_cast<size_t>(begin - _read_cursor));
    } else {
        RETURN_IF_ERROR(_reader->load_page());
        _cur_page_loaded = true;
        return _reader->skip_values(_reader->num_values() - _num_values_left_in_cur_page);
    }
}

Status OptionalStoredColumnReader::_lazy_skip_values(uint64_t begin) {
    size_t row_to_skip = 0;
    if (_cur_page_loaded) {
        row_to_skip = static_cast<size_t>(begin - _read_cursor);
    } else {
        RETURN_IF_ERROR(_reader->load_page());
        _cur_page_loaded = true;
        row_to_skip = _reader->num_values() - _num_values_left_in_cur_page;
    }
    size_t values_to_skip = 0;
    size_t row_skipped = 0;
    while (row_skipped < row_to_skip) {
        level_t* def_levels = nullptr;
        size_t level_parsed = 0;
        size_t cur_to_skip = row_to_skip - row_skipped;
        // skip batch by batch to avoid big memory alloc
        size_t batch_to_skip = std::min(cur_to_skip, (size_t)BATCH_PROCESS_SIZE);
        RETURN_IF_ERROR(_decode_levels(&batch_to_skip, &level_parsed, &def_levels));
        values_to_skip += count_not_null(&def_levels[0], level_parsed, _field->max_def_level());
        row_skipped += level_parsed;
        // reset_levels() to avoiding using too much memory and prepare levels for new reading.
        reset_levels();
    }
    return _reader->skip_values(values_to_skip);
}

Status RepeatedStoredColumnReader::_lazy_skip_values(uint64_t begin) {
    size_t to_skip = _not_null_to_skip;
    _not_null_to_skip = 0;
    return _reader->skip_values(to_skip);
}

Status RequiredStoredColumnReader::_read_values_on_levels(size_t num_values,
                                                          starrocks::parquet::ColumnContentType content_type,
                                                          starrocks::Column* dst, bool append_default) {
    if (append_default) {
        dst->append_default(num_values);
        return Status::OK();
    }
    return _reader->decode_values(num_values, content_type, dst);
}

Status OptionalStoredColumnReader::_read_values_on_levels(size_t num_values,
                                                          starrocks::parquet::ColumnContentType content_type,
                                                          starrocks::Column* dst, bool append_default) {
    if (append_default) {
        _append_default_levels(num_values);
        dst->append_default(num_values);
        return Status::OK();
    } else {
        level_t* def_levels = nullptr;
        size_t level_parsed = 0;
        RETURN_IF_ERROR(_decode_levels(&num_values, &level_parsed, &def_levels));
        DCHECK_EQ(num_values, level_parsed);
        _is_nulls.resize(num_values);
        // decode def levels
        for (size_t i = 0; i < num_values; ++i) {
            _is_nulls[i] = def_levels[i] < _field->max_def_level();
        }
        return _reader->decode_values(num_values, &_is_nulls[0], content_type, dst);
    }
}

Status RepeatedStoredColumnReader::_read_values_on_levels(size_t num_values,
                                                          starrocks::parquet::ColumnContentType content_type,
                                                          starrocks::Column* dst, bool append_default) {
    _is_nulls.resize(num_values);
    int null_pos = 0;
    level_t* def_levels = _reader->def_level_decoder().get_forward_levels(num_values);
    for (int i = 0; i < num_values; ++i) {
        level_t def_level = def_levels[i];
        _is_nulls[null_pos] = (def_level < _field->max_def_level());
        // if current def level < ancestor def level, the ancestor will be not defined too, so that we don't
        // need to add null value to this column. Otherwise, we need to add null value to this column.
        null_pos += (def_level >= _field->level_info.immediate_repeated_ancestor_def_level);
    }
    if (append_default) {
        _collect_not_null_values(num_values, num_values != _num_values_left_in_cur_page);
        dst->append_default(null_pos);
        return Status::OK();
    } else if (null_pos != 0) {
        return _reader->decode_values(null_pos, &_is_nulls[0], content_type, dst);
    } else {
        return Status::OK();
    }
}

void RepeatedStoredColumnReader::_collect_not_null_values(size_t num_levels, bool lazy_flag) {
    if (lazy_flag) {
        _not_null_to_skip += count_not_null(_reader->def_level_decoder().get_forward_levels(num_levels), num_levels,
                                            _field->max_def_level());
    }
}

Status StoredColumnReaderImpl::_next_page() {
    RETURN_IF_ERROR(_reader->next_page());
    RETURN_IF_ERROR(_reader->load_header());
    if (_reader->current_page_is_dict()) {
        RETURN_IF_ERROR(_reader->load_page());
        return _next_page();
    } else {
        _cur_page_loaded = false;
        _num_values_left_in_cur_page = _reader->num_values();
    }
    return Status::OK();
}

Status RepeatedStoredColumnReader::_next_page() {
    RETURN_IF_ERROR(StoredColumnReaderImpl::_next_page());
    _not_null_to_skip = 0;
    return _reader->load_page();
}

bool StoredColumnReaderImpl::_cur_page_selected(size_t row_readed, const Filter* filter, size_t to_read) {
    if (!filter) {
        return true;
    }
    size_t start_row = row_readed;
    int end_row = std::min(start_row + to_read, filter->size()) - 1;
    return SIMD::find_nonzero(*filter, start_row) <= end_row;
}

bool RepeatedStoredColumnReader::_cur_page_selected(size_t row_readed, const Filter* filter, size_t to_read) {
    // for repeated column we can't judge the last row in the current page,
    // to_read is all complete row, row + 1 contains the last incomplete row
    return StoredColumnReaderImpl::_cur_page_selected(row_readed, filter, to_read + 1);
}

Status StoredColumnReaderImpl::load_specific_page(size_t cur_page_idx, uint64_t offset, uint64_t first_row) {
    _reader->set_next_read_page_idx(cur_page_idx);
    RETURN_IF_ERROR(_reader->seek_to_offset(offset));
    RETURN_IF_ERROR(_reader->load_header());
    _cur_page_loaded = false;
    _num_values_left_in_cur_page = _reader->num_values();
    _read_cursor = first_row;
    return Status::OK();
}

Status RepeatedStoredColumnReader::load_specific_page(size_t cur_page_idx, uint64_t offset, uint64_t first_row) {
    RETURN_IF_ERROR(StoredColumnReaderImpl::load_specific_page(cur_page_idx, offset, first_row));
    _not_null_to_skip = 0;
    return _reader->load_page();
}
} // namespace starrocks::parquet
