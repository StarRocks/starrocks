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

#include "column/column.h"
#include "column_reader.h"
#include "common/status.h"
#include "exec/hdfs_scanner.h"
#include "formats/parquet/types.h"
#include "formats/parquet/utils.h"
#include "simd/simd.h"
#include "util/runtime_profile.h"
#include "gutil/strings/substitute.h"

namespace starrocks::parquet {

class RepeatedStoredColumnReader : public StoredColumnReader {
public:
    RepeatedStoredColumnReader(const ColumnReaderOptions& opts) : StoredColumnReader(opts) {}

    ~RepeatedStoredColumnReader() override = default;

    Status init(const ParquetField *field, const tparquet::ColumnChunk *chunk_metadata) {
        _field = field;
        _reader = std::make_unique<ColumnChunkReader>(_field->max_def_level(), _field->max_rep_level(),
                                                      _field->type_length, chunk_metadata, _opts);
        RETURN_IF_ERROR(_reader->init(_opts.chunk_size));
        return Status::OK();
    }

    void reset() override {
        _meet_first_record = false;
        size_t num_levels = _levels_decoded - _levels_parsed;
        if (_levels_parsed == 0 || num_levels == 0) {
            _levels_parsed = _levels_decoded = 0;
            return;
        }

        memmove(&_def_levels[0], &_def_levels[_levels_parsed], num_levels * sizeof(level_t));
        memmove(&_rep_levels[0], &_rep_levels[_levels_parsed], num_levels * sizeof(level_t));
        _levels_decoded -= _levels_parsed;
        _levels_parsed = 0;
    }

    Status do_skip(size_t rows_to_skip) override {
        size_t rows_skip = 0;
        while (rows_skip < rows_to_skip) {
            if (_num_values_left_in_cur_page == 0) {
                // For repeated columns, it is difficult to skip rows according the num_values in header because
                // there may be multiple values in one row. So we don't realize it util the page index is ready later.
                auto st = load_next_page();
                if (!st.ok()) {
                    if (st.status().is_end_of_file()) {
                        if (_meet_first_record) {
                            rows_skip++;
                        }
                        break;
                    } else {
                        return st.status();
                    }
                }
                continue;
            }

            size_t remain_rows_to_skip = rows_to_skip - rows_skip;
            _decode_levels(remain_rows_to_skip);

            auto res = _new_delimit_rows(remain_rows_to_skip);
            remain_rows_to_skip = res.first;
            size_t levels_parsed = res.second;


            // decode value read from reader
            // TODO(zc): optimized here
            {
                int null_pos = 0;
                for (int i = 0; i < levels_parsed; ++i) {
                    level_t def_level = _def_levels[_levels_parsed + i];
                    // if current def level < ancestor def level, the ancestor will be not defined too, so that we don't
                    // need to add null value to this column. Otherwise, we need to add null value to this column.
                    null_pos += (def_level >= _field->level_info.immediate_repeated_ancestor_def_level);
                }
                RETURN_IF_ERROR(_reader->skip(null_pos));
            }
            rows_skip += remain_rows_to_skip;
            _levels_parsed += levels_parsed;
            _num_values_left_in_cur_page -= levels_parsed;
        }
        if ((UNLIKELY(rows_skip != rows_to_skip))) {
            return Status::InternalError(
                    strings::Substitute("Skip num_rows failed, already skipped: $0, required skip: $1",
                                        rows_skip, rows_to_skip));
        }
        return Status::OK();
    }

    StatusOr<size_t> do_read_records(size_t rows_to_read, ColumnContentType content_type, Column *dst) override {
        size_t rows_read = 0;
        while (rows_read < rows_to_read) {
            if (_num_values_left_in_cur_page == 0) {
                auto st = load_next_page();
                if (!st.ok()) {
                    if (st.status().is_end_of_file()) {
                        if (_meet_first_record) {
                            rows_read++;
                        }
                        break;
                    } else {
                        return st.status();
                    }
                }
                continue;
            }

            // NOTE: must have values in current page.
            DCHECK_GT(_num_values_left_in_cur_page, 0);

            size_t remain_rows_to_read = rows_to_read - rows_read;
            _decode_levels(remain_rows_to_read);

            auto res = _new_delimit_rows(remain_rows_to_read);
            remain_rows_to_read = res.first;
            size_t levels_parsed = res.second;

            // decode value read from reader
            // TODO(zc): optimized here
            {
                // ensure enough capacity
                _is_nulls.resize(levels_parsed);
                int null_pos = 0;
                for (int i = 0; i < levels_parsed; ++i) {
                    level_t def_level = _def_levels[i + _levels_parsed];
                    _is_nulls[null_pos] = (def_level < _field->max_def_level());
                    // if current def level < ancestor def level, the ancestor will be not defined too, so that we don't
                    // need to add null value to this column. Otherwise, we need to add null value to this column.
                    null_pos += (def_level >= _field->level_info.immediate_repeated_ancestor_def_level);
                }
                RETURN_IF_ERROR(_reader->decode_values(null_pos, &_is_nulls[0], content_type, dst));
            }

            rows_read += remain_rows_to_read;
            _levels_parsed += levels_parsed;
            _num_values_left_in_cur_page -= levels_parsed;
        }
        return rows_read;
    }

    void get_levels(level_t **def_levels, level_t **rep_levels, size_t *num_levels) override {
        *def_levels = &_def_levels[0];
        *rep_levels = &_rep_levels[0];
        *num_levels = _levels_parsed;
    }

private:
    // Try to decode enough levels in levels buffer, except there is no enough levels in current
    void _decode_levels(size_t num_levels) {
        constexpr size_t min_level_batch_size = 4096;
        size_t levels_remaining = _levels_decoded - _levels_parsed;
        if (num_levels <= levels_remaining) {
            return;
        }
        size_t levels_to_decode = std::max(min_level_batch_size, num_levels - levels_remaining);
        levels_to_decode = std::min(levels_to_decode, _num_values_left_in_cur_page - levels_remaining);

        size_t new_capacity = _levels_decoded + levels_to_decode;
        if (new_capacity > _levels_capacity) {
            new_capacity = BitUtil::next_power_of_two(new_capacity);
            _def_levels.resize(new_capacity);
            _rep_levels.resize(new_capacity);

            _levels_capacity = new_capacity;
        }

        size_t res_def = _reader->decode_def_levels(levels_to_decode, &_def_levels[_levels_decoded]);
        size_t res_rep = _reader->decode_rep_levels(levels_to_decode, &_rep_levels[_levels_decoded]);
        DCHECK_EQ(res_def, res_rep);

        _levels_decoded += levels_to_decode;
    }

    std::pair<size_t, size_t> _new_delimit_rows(size_t rows_to_read) {
        DCHECK_GT(_levels_decoded - _levels_parsed, 0);
        size_t levels_pos = _levels_parsed;

#ifndef NDEBUG
        std::stringstream ss;
        ss << "rep=[";
        for (int i = levels_pos; i < _levels_decoded; ++i) {
            ss << ", " << _rep_levels[i];
        }
        ss << "], def=[";
        for (int i = levels_pos; i < _levels_decoded; ++i) {
            ss << ", " << _def_levels[i];
        }
        ss << "]";
        VLOG_FILE << ss.str();
#endif

        if (!_meet_first_record) {
            _meet_first_record = true;
            DCHECK_EQ(_rep_levels[levels_pos], 0);
            levels_pos++;
        }

        size_t rows_read = 0;
        for (; levels_pos < _levels_decoded && rows_read < rows_to_read; ++levels_pos) {
            rows_read += _rep_levels[levels_pos] == 0;
        }

        if (rows_read == rows_to_read) {
            // Notice, ++levels_pos in for-loop will take one step forward, so we need -1
            levels_pos--;
            DCHECK_EQ(0, _rep_levels[levels_pos]);
        } // else {
        //  means  rows_read < *num_rows, levels_pos >= _levels_decoded,
        //  so we need to decode more levels to obtain a complete line or
        //  we have read all the records in this column chunk.
        // }

        VLOG_FILE << "rows_reader=" << rows_read << ", level_parsed=" << levels_pos - _levels_parsed;
        return std::make_pair(rows_read, levels_pos - _levels_parsed);
    }

private:
    const ParquetField *_field = nullptr;

    bool _meet_first_record = false;

    size_t _levels_parsed = 0;
    size_t _levels_decoded = 0;
    size_t _levels_capacity = 0;

    std::vector<level_t> _def_levels;
    std::vector<level_t> _rep_levels;

    std::vector<uint8_t> _is_nulls;
};

class OptionalStoredColumnReader : public StoredColumnReader {
public:
    OptionalStoredColumnReader(const ColumnReaderOptions& opts) : StoredColumnReader(opts) {}
    ~OptionalStoredColumnReader() override = default;

    Status init(const ParquetField* field, const tparquet::ColumnChunk* chunk_metadata) {
        _field = field;
        _reader = std::make_unique<ColumnChunkReader>(_field->max_def_level(), _field->max_rep_level(),
                                                      _field->type_length, chunk_metadata, _opts);
        RETURN_IF_ERROR(_reader->init(_opts.chunk_size));
        return Status::OK();
    }

    // Reset internal state and ready for next read_values
    void reset() override {
        size_t num_levels = _levels_decoded - _levels_parsed;
        if (_levels_parsed == 0 || num_levels == 0) {
            _levels_parsed = _levels_decoded = 0;
            return;
        }

        memmove(&_def_levels[0], &_def_levels[_levels_parsed], num_levels * sizeof(level_t));
        _levels_decoded -= _levels_parsed;
        _levels_parsed = 0;
    }

    Status do_skip(size_t rows_to_skip) override {
        size_t rows_skip = 0;
        while (rows_skip < rows_to_skip) {
            if (_num_values_left_in_cur_page == 0) {
                auto st = load_next_page(rows_to_skip - rows_skip);
                if (st.ok()) {
                    rows_skip += st.value();
                } else {
                    if (st.status().is_end_of_file()) {
                        break;
                    } else {
                        return st.status();
                    }
                }
                continue;
            }

            size_t remain_rows_to_skip = std::min(rows_to_skip - rows_skip, _num_values_left_in_cur_page);
            DCHECK(remain_rows_to_skip > 0);

            if (_need_parse_levels) {
                {
                    SCOPED_RAW_TIMER(&_opts.stats->level_decode_ns);
                    // TODO try to skip decode
                    _decode_levels(remain_rows_to_skip);
                }
                _levels_parsed += remain_rows_to_skip;
                RETURN_IF_ERROR(_reader->skip(remain_rows_to_skip));
                _num_values_left_in_cur_page -= remain_rows_to_skip;
                rows_skip += remain_rows_to_skip;
            } else {
                size_t repeated_count = _reader->def_level_decoder().next_repeated_count();
                if (repeated_count > 0) {
                    remain_rows_to_skip = std::min(remain_rows_to_skip, repeated_count);
                    level_t def_level = 0;
                    {
                        // just advance cursor
                        SCOPED_RAW_TIMER(&_opts.stats->level_decode_ns);
                        def_level = _reader->def_level_decoder().get_repeated_value(remain_rows_to_skip);
                    }
                    SCOPED_RAW_TIMER(&_opts.stats->value_decode_ns);
                    if (def_level >= _field->max_def_level()) {
                        RETURN_IF_ERROR(_reader->skip(remain_rows_to_skip));
                    }
                } else {
                    {
                        SCOPED_RAW_TIMER(&_opts.stats->level_decode_ns);

                        size_t new_capacity = remain_rows_to_skip;
                        if (new_capacity > _levels_capacity) {
                            new_capacity = BitUtil::next_power_of_two(new_capacity);
                            _def_levels.resize(new_capacity);

                            _levels_capacity = new_capacity;
                        }
                        _reader->decode_def_levels(remain_rows_to_skip, &_def_levels[0]);
                    }
                    SCOPED_RAW_TIMER(&_opts.stats->value_decode_ns);
                    size_t i = 0;
                    while (i < remain_rows_to_skip) {
                        size_t j = i;
                        bool is_null = _def_levels[j] < _field->max_def_level();
                        j++;
                        while (j < remain_rows_to_skip && is_null == (_def_levels[j] < _field->max_def_level())) {
                            j++;
                        }
                        if (!is_null) {
                            RETURN_IF_ERROR(_reader->skip(j - i));
                        }
                        i = j;
                    }
                }
                _num_values_left_in_cur_page -= remain_rows_to_skip;
                rows_skip += remain_rows_to_skip;
            }
        }
        if ((UNLIKELY(rows_skip != rows_to_skip))) {
            return Status::InternalError(
                    strings::Substitute("Skip num_rows failed, already skipped: $0, required skip: $1",
                                        rows_skip, rows_to_skip));
        }
        return Status::OK();
    }


    StatusOr<size_t> do_read_records(size_t rows_to_read, ColumnContentType content_type, Column *dst) override {
        SCOPED_RAW_TIMER(&_opts.stats->column_read_ns);
        size_t rows_read = 0;
        while (rows_read < rows_to_read) {
            if (_num_values_left_in_cur_page == 0) {
                auto st = load_next_page();
                if (!st.ok()) {
                    if (st.status().is_end_of_file()) {
                        break;
                    } else {
                        return st.status();
                    }
                }
                continue;
            }

            size_t remain_rows_to_read = std::min(rows_to_read - rows_read, _num_values_left_in_cur_page);
            DCHECK(remain_rows_to_read > 0);

            if (_need_parse_levels) {
                {
                    SCOPED_RAW_TIMER(&_opts.stats->level_decode_ns);
                    _decode_levels(remain_rows_to_read);
                }
                {
                    // TODO(zc): make it better
                    _is_nulls.resize(remain_rows_to_read);
                    // decode def levels
                    for (size_t i = 0; i < remain_rows_to_read; ++i) {
                        _is_nulls[i] = _def_levels[_levels_parsed + i] < _field->max_def_level();
                    }
                    SCOPED_RAW_TIMER(&_opts.stats->value_decode_ns);
                    RETURN_IF_ERROR(_reader->decode_values(remain_rows_to_read, &_is_nulls[0], content_type, dst));
                }
                _levels_parsed += remain_rows_to_read;
                _num_values_left_in_cur_page -= remain_rows_to_read;
                rows_read += remain_rows_to_read;
            } else {
                size_t repeated_count = _reader->def_level_decoder().next_repeated_count();
                if (repeated_count > 0) {
                    remain_rows_to_read = std::min(remain_rows_to_read, repeated_count);
                    level_t def_level = 0;
                    {
                        // just advance cursor
                        SCOPED_RAW_TIMER(&_opts.stats->level_decode_ns);
                        def_level = _reader->def_level_decoder().get_repeated_value(remain_rows_to_read);
                    }
                    SCOPED_RAW_TIMER(&_opts.stats->value_decode_ns);
                    if (def_level >= _field->max_def_level()) {
                        RETURN_IF_ERROR(_reader->decode_values(remain_rows_to_read, content_type, dst));
                    } else {
                        dst->append_nulls(remain_rows_to_read);
                    }
                } else {
                    {
                        SCOPED_RAW_TIMER(&_opts.stats->level_decode_ns);

                        size_t new_capacity = remain_rows_to_read;
                        if (new_capacity > _levels_capacity) {
                            new_capacity = BitUtil::next_power_of_two(new_capacity);
                            _def_levels.resize(new_capacity);

                            _levels_capacity = new_capacity;
                        }
                        _reader->decode_def_levels(remain_rows_to_read, &_def_levels[0]);
                    }
                    SCOPED_RAW_TIMER(&_opts.stats->value_decode_ns);
                    size_t i = 0;
                    while (i < remain_rows_to_read) {
                        size_t j = i;
                        bool is_null = _def_levels[j] < _field->max_def_level();
                        j++;
                        while (j < remain_rows_to_read && is_null == (_def_levels[j] < _field->max_def_level())) {
                            j++;
                        }
                        if (is_null) {
                            dst->append_nulls(j - i);
                        } else {
                            RETURN_IF_ERROR(_reader->decode_values(j - i, content_type, dst));
                        }
                        i = j;
                    }
                }
                _num_values_left_in_cur_page -= remain_rows_to_read;
                rows_read += remain_rows_to_read;
            }
        }
        return rows_read;
    }

    // If need_levels is set, client will get all levels through get_levels function.
    // If need_levels is not set, read_records may not record levels information, this will
    // improve performance. So set this flag when you only needs it.
    void set_need_parse_levels(bool needs_levels) override { _need_parse_levels = needs_levels; }

    void get_levels(level_t **def_levels, level_t **rep_levels, size_t *num_levels) override {
        // _needs_levels must be true
        DCHECK(_need_parse_levels);

        *def_levels = &_def_levels[0];
        *rep_levels = nullptr;
        *num_levels = _levels_parsed;
    }

private:
    void _decode_levels(size_t num_levels) {
        constexpr size_t min_level_batch_size = 4096;
        size_t levels_remaining = _levels_decoded - _levels_parsed;
        if (num_levels <= levels_remaining) {
            return;
        }
        size_t levels_to_decode = std::max(min_level_batch_size, num_levels - levels_remaining);
        levels_to_decode = std::min(levels_to_decode, _num_values_left_in_cur_page - levels_remaining);

        size_t new_capacity = _levels_decoded + levels_to_decode;
        if (new_capacity > _levels_capacity) {
            new_capacity = BitUtil::next_power_of_two(new_capacity);
            _def_levels.resize(new_capacity);

            _levels_capacity = new_capacity;
        }

        _reader->decode_def_levels(levels_to_decode, &_def_levels[_levels_decoded]);

        _levels_decoded += levels_to_decode;
    }

    const ParquetField *_field = nullptr;

    // When the flag is false, the information of levels does not need to be materialized,
    // so that the advantages of RLE encoding can be fully utilized and a lot of overhead
    // can be saved in decoding.
    bool _need_parse_levels = false;

    size_t _levels_parsed = 0;
    size_t _levels_decoded = 0;
    size_t _levels_capacity = 0;

    std::vector<uint8_t> _is_nulls;
    std::vector<level_t> _def_levels;
};

class RequiredStoredColumnReader : public StoredColumnReader {
public:
    RequiredStoredColumnReader(const ColumnReaderOptions &opts) : StoredColumnReader(opts) {}

    ~RequiredStoredColumnReader() override = default;

    Status init(const ParquetField *field, const tparquet::ColumnChunk *chunk_metadata) {
        _field = field;
        _reader = std::make_unique<ColumnChunkReader>(_field->max_def_level(), _field->max_rep_level(),
                                                      _field->type_length, chunk_metadata, _opts);
        RETURN_IF_ERROR(_reader->init(_opts.chunk_size));
        return Status::OK();
    }

    Status do_skip(size_t rows_to_skip) override {
        size_t rows_skip = 0;
        while (rows_skip < rows_to_skip) {
            if (_num_values_left_in_cur_page == 0) {
                auto st = load_next_page(rows_to_skip - rows_skip);
                if (st.ok()) {
                    rows_skip += st.value();
                } else {
                    if (st.status().is_end_of_file()) {
                        break;
                    } else {
                        return st.status();
                    }
                }
                continue;
            }

            size_t remain_rows_to_skip = std::min(rows_to_skip - rows_skip, _num_values_left_in_cur_page);
            RETURN_IF_ERROR(_reader->skip(remain_rows_to_skip));
            _num_values_left_in_cur_page -= remain_rows_to_skip;
            rows_skip += remain_rows_to_skip;
        }
        if ((UNLIKELY(rows_skip != rows_to_skip))) {
            return Status::InternalError(
                    strings::Substitute("Skip num_rows failed, already skipped: $0, required skip: $1",
                                        rows_skip, rows_to_skip));
        }
        return Status::OK();
    }

    StatusOr<size_t> do_read_records(size_t rows_to_read, ColumnContentType content_type, Column *dst) override {
        SCOPED_RAW_TIMER(&_opts.stats->column_read_ns);
        size_t rows_read = 0;
        while (rows_read < rows_to_read) {
            if (_num_values_left_in_cur_page == 0) {
                auto st = load_next_page();
                if (!st.ok()) {
                    if (st.status().is_end_of_file()) {
                        break;
                    } else {
                        return st;
                    }
                }
                continue;
            }

            size_t remain_rows_to_read = std::min(rows_to_read - rows_read, _num_values_left_in_cur_page);
            RETURN_IF_ERROR(_reader->decode_values(remain_rows_to_read, content_type, dst));
            rows_read += remain_rows_to_read;
            _num_values_left_in_cur_page -= remain_rows_to_read;
        }
        return rows_read;
    }

    void reset() override {}

    void get_levels(level_t **def_levels, level_t **rep_levels, size_t *num_levels) override {
        *def_levels = nullptr;
        *rep_levels = nullptr;
        *num_levels = 0;
    }

private:
    // TODO(zc): No need copy
    const tparquet::ColumnChunk _chunk_metadata;
    const ParquetField *_field = nullptr;
};

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

// Load next page
// You need make sure _num_values_left_in_cur_page is 0
// Will return the values your skipped, Status::EndOfFile() or Status::InternalError()
StatusOr<size_t> StoredColumnReader::load_next_page(size_t values_to_skip) {
    SCOPED_RAW_TIMER(&_opts.stats->page_read_ns);
    // Only cur page's num values are consumed can load next page
    DCHECK(_num_values_left_in_cur_page == 0);
    // may return EOF here
    RETURN_IF_ERROR(_reader->load_header());
    size_t num_values = _reader->num_values();
    if (num_values == 0) {
        if (_reader->current_page_is_dict()) {
            // load dict page
            RETURN_IF_ERROR(_reader->load_page());
        } else {
            RETURN_IF_ERROR(_reader->skip_page());
        }
        return 0;
    }

    if (values_to_skip >= num_values) {
        RETURN_IF_ERROR(_reader->skip_page());
        return num_values;
    }

    // start to read data page
    RETURN_IF_ERROR(_reader->load_page());
    _num_values_left_in_cur_page = num_values;
    return 0;
}

} // namespace starrocks::parquet
