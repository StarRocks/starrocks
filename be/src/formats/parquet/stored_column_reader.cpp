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

namespace starrocks::parquet {

class RepeatedStoredColumnReader : public StoredColumnReader {
public:
    RepeatedStoredColumnReader(const ColumnReaderOptions& opts) : StoredColumnReader(opts) {}
    ~RepeatedStoredColumnReader() override = default;

    Status init(const ParquetField* field, const tparquet::ColumnChunk* chunk_metadata) {
        _field = field;
        _reader = std::make_unique<ColumnChunkReader>(_field->max_def_level(), _field->max_rep_level(),
                                                      _field->type_length, chunk_metadata, _opts);
        RETURN_IF_ERROR(_reader->init(_opts.chunk_size));
        return Status::OK();
    }

    void reset() override;

    Status do_read_records(size_t* num_rows, ColumnContentType content_type, Column* dst) override;

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        *def_levels = &_def_levels[0];
        *rep_levels = &_rep_levels[0];
        *num_levels = _levels_parsed;
    }

protected:
    bool page_selected(size_t num_values) override;

private:
    // Try to deocde enough levels in levels buffer, except there is no enough levels in current
    void _decode_levels(size_t num_levels);

    void _delimit_rows(size_t* num_rows, size_t* num_levels_parsed);

private:
    const ParquetField* _field = nullptr;

    bool _eof = false;
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
    void reset() override;

    Status do_read_records(size_t* num_records, ColumnContentType content_type, Column* dst) override {
        if (_need_parse_levels) {
            return _read_records_and_levels(num_records, content_type, dst);
        } else {
            return _read_records_only(num_records, content_type, dst);
        }
    }

    // If need_levels is set, client will get all levels through get_levels function.
    // If need_levels is not set, read_records may not records levels information, this will
    // improve performance. So set this flag when you only needs it.
    void set_need_parse_levels(bool needs_levels) override { _need_parse_levels = needs_levels; }

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        // _needs_levels must be true
        DCHECK(_need_parse_levels);

        *def_levels = &_def_levels[0];
        *rep_levels = nullptr;
        *num_levels = _levels_parsed;
    }

    void append_default_levels(size_t row_nums) {
        if (_need_parse_levels) {
            size_t new_capacity = _levels_parsed + row_nums;
            if (new_capacity > _levels_capacity) {
                _def_levels.resize(new_capacity);
                _levels_capacity = new_capacity;
            }
            memset(&_def_levels[_levels_parsed], 0x0, row_nums * sizeof(level_t));
            _levels_parsed += row_nums;
            _levels_decoded = _levels_parsed;
        }
    }

private:
    void _decode_levels(size_t num_levels);
    Status _read_records_only(size_t* num_records, ColumnContentType content_type, Column* dst);
    Status _read_records_and_levels(size_t* num_records, ColumnContentType content_type, Column* dst);

    const ParquetField* _field = nullptr;

    // When the flag is false, the information of levels does not need to be materialized,
    // so that the advantages of RLE encoding can be fully utilized and a lot of overhead
    // can be saved in decoding.
    bool _need_parse_levels = false;

    bool _eof = false;

    size_t _levels_parsed = 0;
    size_t _levels_decoded = 0;
    size_t _levels_capacity = 0;

    std::vector<uint8_t> _is_nulls;
    std::vector<level_t> _def_levels;
};

class RequiredStoredColumnReader : public StoredColumnReader {
public:
    RequiredStoredColumnReader(const ColumnReaderOptions& opts) : StoredColumnReader(opts) {}
    ~RequiredStoredColumnReader() override = default;

    Status init(const ParquetField* field, const tparquet::ColumnChunk* chunk_metadata) {
        _field = field;
        _reader = std::make_unique<ColumnChunkReader>(_field->max_def_level(), _field->max_rep_level(),
                                                      _field->type_length, chunk_metadata, _opts);
        RETURN_IF_ERROR(_reader->init(_opts.chunk_size));
        return Status::OK();
    }

    void reset() override {}

    Status do_read_records(size_t* num_rows, ColumnContentType content_type, Column* dst) override;

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        *def_levels = nullptr;
        *rep_levels = nullptr;
        *num_levels = 0;
    }

private:
    // TODO(zc): No need copy
    const tparquet::ColumnChunk _chunk_metadata;
    const ParquetField* _field = nullptr;
};

void RepeatedStoredColumnReader::reset() {
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

Status RepeatedStoredColumnReader::do_read_records(size_t* num_records, ColumnContentType content_type, Column* dst) {
    if (_eof) {
        *num_records = 0;
        return Status::EndOfFile("");
    }

    size_t records_read = 0;
    do {
        if (_num_values_left_in_cur_page == 0) {
            size_t read_count = 0;
            auto st = next_page(*num_records - records_read, content_type, &read_count, dst);
            DCHECK(read_count <= (*num_records - records_read));
            records_read += read_count;
            if (!st.ok()) {
                if (st.is_end_of_file()) {
                    if (_meet_first_record) {
                        records_read++;
                    }
                    _eof = true;
                    break;
                } else {
                    return st;
                }
            }
        }

        // NOTE: must have values in current page.
        DCHECK_GT(_num_values_left_in_cur_page, 0);

        size_t records_to_read = *num_records - records_read;
        _decode_levels(records_to_read);

        size_t num_parsed_levels = 0;
        _delimit_rows(&records_to_read, &num_parsed_levels);

        // decode value read from reader
        // TODO(zc): optimized here
        {
            // ensure enough capacity
            _is_nulls.resize(num_parsed_levels);
            int null_pos = 0;
            for (int i = 0; i < num_parsed_levels; ++i) {
                level_t def_level = _def_levels[i + _levels_parsed];
                _is_nulls[null_pos] = (def_level < _field->max_def_level());
                // if current def level < ancestor def level, the ancestor will be not defined too, so that we don't
                // need to add null value to this column. Otherwise, we need to add null value to this column.
                null_pos += (def_level >= _field->level_info.immediate_repeated_ancestor_def_level);
            }
            RETURN_IF_ERROR(_reader->decode_values(null_pos, &_is_nulls[0], content_type, dst));
        }

        records_read += records_to_read;
        _levels_parsed += num_parsed_levels;
        _num_values_left_in_cur_page -= num_parsed_levels;
        update_read_context(records_to_read);
    } while (records_read < *num_records);

    DCHECK(records_read <= *num_records);

    *num_records = records_read;
    return Status::OK();
}

// For repeated columns, it is difficult to skip rows according the num_values in header because
// there may be multiple values in one row. So we don't realize it util the page index is ready later.
bool RepeatedStoredColumnReader::page_selected(size_t num_values) {
    return true;
}

void RepeatedStoredColumnReader::_delimit_rows(size_t* num_rows, size_t* num_levels_parsed) {
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
    for (; levels_pos < _levels_decoded && rows_read < *num_rows; ++levels_pos) {
        rows_read += _rep_levels[levels_pos] == 0;
    }

    if (rows_read == *num_rows) {
        // Notice, ++levels_pos in for-loop will take one step forward, so we need -1
        levels_pos--;
        DCHECK_EQ(0, _rep_levels[levels_pos]);
    } // else {
      //  means  rows_read < *num_rows, levels_pos >= _levels_decoded,
      //  so we need to decode more levels to obtain a complete line or
      //  we have read all the records in this column chunk.
    // }

    VLOG_FILE << "rows_reader=" << rows_read << ", level_parsed=" << levels_pos - _levels_parsed;
    *num_rows = rows_read;
    *num_levels_parsed = levels_pos - _levels_parsed;
}

void RepeatedStoredColumnReader::_decode_levels(size_t num_levels) {
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

void OptionalStoredColumnReader::reset() {
    size_t num_levels = _levels_decoded - _levels_parsed;
    if (_levels_parsed == 0 || num_levels == 0) {
        _levels_parsed = _levels_decoded = 0;
        return;
    }

    memmove(&_def_levels[0], &_def_levels[_levels_parsed], num_levels * sizeof(level_t));
    _levels_decoded -= _levels_parsed;
    _levels_parsed = 0;
}

Status OptionalStoredColumnReader::_read_records_and_levels(size_t* num_records, ColumnContentType content_type,
                                                            Column* dst) {
    SCOPED_RAW_TIMER(&_opts.stats->column_read_ns);
    if (_eof) {
        *num_records = 0;
        return Status::EndOfFile("");
    }
    size_t records_read = 0;
    do {
        if (_num_values_left_in_cur_page == 0) {
            SCOPED_RAW_TIMER(&_opts.stats->page_read_ns);
            size_t read_count = 0;
            auto st = next_page(*num_records - records_read, content_type, &read_count, dst);
            records_read += read_count;
            if (!st.ok()) {
                if (st.is_end_of_file()) {
                    _eof = true;
                    break;
                } else {
                    return st;
                }
            }
        }

        size_t records_to_read = std::min(*num_records - records_read, _num_values_left_in_cur_page);
        if (records_to_read == 0) {
            break;
        }

        {
            SCOPED_RAW_TIMER(&_opts.stats->level_decode_ns);
            _decode_levels(records_to_read);
        }

        {
            // TODO(zc): make it better
            _is_nulls.resize(records_to_read);
            // decode def levels
            for (size_t i = 0; i < records_to_read; ++i) {
                _is_nulls[i] = _def_levels[_levels_parsed + i] < _field->max_def_level();
            }
            SCOPED_RAW_TIMER(&_opts.stats->value_decode_ns);
            RETURN_IF_ERROR(_reader->decode_values(records_to_read, &_is_nulls[0], content_type, dst));
        }
        _num_values_left_in_cur_page -= records_to_read;
        _levels_parsed += records_to_read;
        records_read += records_to_read;
        update_read_context(records_to_read);
    } while (records_read < *num_records);
    *num_records = records_read;
    return Status::OK();
}

Status OptionalStoredColumnReader::_read_records_only(size_t* num_records, ColumnContentType content_type,
                                                      Column* dst) {
    SCOPED_RAW_TIMER(&_opts.stats->column_read_ns);
    if (_eof) {
        return Status::EndOfFile("");
    }
    size_t records_read = 0;
    do {
        if (_num_values_left_in_cur_page == 0) {
            SCOPED_RAW_TIMER(&_opts.stats->page_read_ns);
            size_t read_count = 0;
            auto st = next_page(*num_records - records_read, content_type, &read_count, dst);
            records_read += read_count;
            if (!st.ok()) {
                if (st.is_end_of_file()) {
                    _eof = true;
                    break;
                } else {
                    return st;
                }
            }
        }

        size_t records_to_read = std::min(*num_records - records_read, _num_values_left_in_cur_page);
        if (records_to_read == 0) {
            break;
        }

        size_t repeated_count = _reader->def_level_decoder().next_repeated_count();
        if (repeated_count > 0) {
            records_to_read = std::min(records_to_read, repeated_count);
            level_t def_level = 0;
            {
                SCOPED_RAW_TIMER(&_opts.stats->level_decode_ns);
                def_level = _reader->def_level_decoder().get_repeated_value(records_to_read);
            }
            SCOPED_RAW_TIMER(&_opts.stats->value_decode_ns);
            if (def_level >= _field->max_def_level()) {
                RETURN_IF_ERROR(_reader->decode_values(records_to_read, content_type, dst));
            } else {
                dst->append_nulls(records_to_read);
            }
        } else {
            {
                SCOPED_RAW_TIMER(&_opts.stats->level_decode_ns);

                size_t new_capacity = records_to_read;
                if (new_capacity > _levels_capacity) {
                    new_capacity = BitUtil::next_power_of_two(new_capacity);
                    _def_levels.resize(new_capacity);

                    _levels_capacity = new_capacity;
                }
                _reader->decode_def_levels(records_to_read, &_def_levels[0]);
            }

            SCOPED_RAW_TIMER(&_opts.stats->value_decode_ns);
            size_t i = 0;
            while (i < records_to_read) {
                size_t j = i;
                bool is_null = _def_levels[j] < _field->max_def_level();
                j++;
                while (j < records_to_read && is_null == (_def_levels[j] < _field->max_def_level())) {
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

        _num_values_left_in_cur_page -= records_to_read;
        records_read += records_to_read;
        update_read_context(records_to_read);
    } while (records_read < *num_records);
    *num_records = records_read;
    return Status::OK();
}

void OptionalStoredColumnReader::_decode_levels(size_t num_levels) {
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

Status RequiredStoredColumnReader::do_read_records(size_t* num_records, ColumnContentType content_type, Column* dst) {
    size_t records_read = 0;
    while (records_read < *num_records) {
        if (_num_values_left_in_cur_page == 0) {
            size_t read_count = 0;
            auto st = next_page(*num_records - records_read, content_type, &read_count, dst);
            records_read += read_count;
            if (!st.ok()) {
                if (st.is_end_of_file()) {
                    break;
                } else {
                    return st;
                }
            }
        }

        size_t records_to_read = std::min(*num_records - records_read, _num_values_left_in_cur_page);
        if (records_to_read == 0) {
            break;
        }
        RETURN_IF_ERROR(_reader->decode_values(records_to_read, content_type, dst));
        records_read += records_to_read;
        _num_values_left_in_cur_page -= records_to_read;
        update_read_context(records_to_read);
    }
    *num_records = records_read;
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

Status StoredColumnReader::next_page(size_t records_to_read, ColumnContentType content_type, size_t* records_read,
                                     Column* dst) {
    *records_read = 0;
    size_t records_to_skip = 0;
    RETURN_IF_ERROR(_next_selected_page(records_to_read, content_type, &records_to_skip, dst));
    if (records_to_skip == 0) {
        return Status::OK();
    }

    if (_opts.context->rows_to_skip > 0) {
        _opts.context->rows_to_skip -= records_to_skip;
    }
    if (_opts.context->filter) {
        dst->append_default(records_to_skip);
        append_default_levels(records_to_skip);
        *records_read = records_to_skip;
    }
    return Status::OK();
}

Status StoredColumnReader::_next_selected_page(size_t records_to_read, ColumnContentType content_type,
                                               size_t* records_to_skip, Column* dst) {
    *records_to_skip = 0;
    do {
        size_t remain_values =
                _num_values_skip_in_cur_page > 0 ? _reader->num_values() - _num_values_skip_in_cur_page : 0;
        if (remain_values == 0) {
            RETURN_IF_ERROR(_reader->load_header());
            size_t num_values = _reader->num_values();
            if (num_values == 0) {
                if (_reader->current_page_is_dict()) {
                    RETURN_IF_ERROR(_reader->load_page());
                } else {
                    RETURN_IF_ERROR(_reader->skip_page());
                }
                continue;
            }
            _num_values_skip_in_cur_page = 0;
            remain_values = num_values;
        }

        size_t to_read = records_to_read - *records_to_skip;
        if (page_selected(remain_values)) {
            RETURN_IF_ERROR(_reader->load_page());
            _num_values_left_in_cur_page = _reader->num_values();
            size_t batch_size = records_to_read;
            _lazy_load_page_rows(batch_size, content_type, dst);
            _num_values_skip_in_cur_page = 0;
            break;
        }

        if (_opts.context->filter) {
            _opts.context->advance(std::min(to_read, remain_values));
        }
        if (to_read < remain_values) {
            _num_values_skip_in_cur_page += to_read;
            *records_to_skip += to_read;
            break;
        }
        RETURN_IF_ERROR(_reader->skip_page());
        *records_to_skip += remain_values;
        _num_values_skip_in_cur_page = 0;
    } while (*records_to_skip < records_to_read);

    return Status::OK();
}

Status StoredColumnReader::_lazy_load_page_rows(size_t batch_size, ColumnContentType content_type, Column* dst) {
    size_t load_rows = _num_values_skip_in_cur_page;
    if (load_rows == 0) {
        return Status::OK();
    }

    // TODO: We simply use reading records to decode and seek rows position now, it may cause some extra
    // memory copy. A more efficient way requires major changes to the current code, but it is necessary.
    auto filter = _opts.context->filter;
    auto rows_to_skip = _opts.context->rows_to_skip;
    _opts.context->filter = nullptr;
    _opts.context->rows_to_skip = 0;
    while (load_rows > 0) {
        size_t to_read = std::min(load_rows, batch_size);
        auto temp_column = dst->clone_empty();
        RETURN_IF_ERROR(do_read_records(&to_read, content_type, temp_column.get()));
        // TODO(SmithCruise) Refactor it
        // We need reset def/rep cursor after lazy load
        reset();
        load_rows -= to_read;
    }
    _opts.context->filter = filter;
    _opts.context->rows_to_skip = rows_to_skip;
    return Status::OK();
}

bool StoredColumnReader::page_selected(size_t num_values) {
    auto filter = _opts.context->filter;
    if (!filter) {
        return true;
    }
    size_t start_row = _opts.context->next_row;
    int end_row = std::min(start_row + num_values, filter->size()) - 1;
    return SIMD::find_nonzero(*filter, start_row) <= end_row;
}

void StoredColumnReader::update_read_context(size_t records_read) {
    if (_opts.context->rows_to_skip > 0) {
        _opts.context->rows_to_skip -= records_read;
    }
    if (_opts.context->filter) {
        _opts.context->advance(records_read);
    }
}

} // namespace starrocks::parquet
