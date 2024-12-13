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

<<<<<<< HEAD
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    ~RepeatedStoredColumnReader() override = default;

    Status init(const ParquetField* field, const tparquet::ColumnChunk* chunk_metadata) {
        _field = field;
        _reader = std::make_unique<ColumnChunkReader>(_field->max_def_level(), _field->max_rep_level(),
                                                      _field->type_length, chunk_metadata, _opts);
        RETURN_IF_ERROR(_reader->init(_opts.chunk_size));
        return Status::OK();
    }

<<<<<<< HEAD
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

private:
    const ParquetField* _field = nullptr;

<<<<<<< HEAD
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    ~OptionalStoredColumnReader() override = default;

    Status init(const ParquetField* field, const tparquet::ColumnChunk* chunk_metadata) {
        _field = field;
        _reader = std::make_unique<ColumnChunkReader>(_field->max_def_level(), _field->max_rep_level(),
                                                      _field->type_length, chunk_metadata, _opts);
        RETURN_IF_ERROR(_reader->init(_opts.chunk_size));
        return Status::OK();
    }

    // Reset internal state and ready for next read_values
<<<<<<< HEAD
    void reset() override;

    Status do_read_records(size_t* num_records, ColumnContentType content_type, Column* dst) override {
        if (_need_parse_levels) {
            return _read_records_and_levels(num_records, content_type, dst);
        } else {
            return _read_records_only(num_records, content_type, dst);
        }
    }
=======
    void reset_levels() override;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    // If need_levels is set, client will get all levels through get_levels function.
    // If need_levels is not set, read_records may not records levels information, this will
    // improve performance. So set this flag when you only needs it.
    void set_need_parse_levels(bool needs_levels) override { _need_parse_levels = needs_levels; }

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        // _needs_levels must be true
        DCHECK(_need_parse_levels);

<<<<<<< HEAD
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    const ParquetField* _field = nullptr;

    // When the flag is false, the information of levels does not need to be materialized,
    // so that the advantages of RLE encoding can be fully utilized and a lot of overhead
    // can be saved in decoding.
    bool _need_parse_levels = false;

<<<<<<< HEAD
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
=======
    // Use uint16_t instead of uint8_t to make it auto simd by compiler.
    std::vector<uint16_t> _is_nulls;
};

class RequiredStoredColumnReader : public StoredColumnReaderImpl {
public:
    RequiredStoredColumnReader(const ColumnReaderOptions& opts) : StoredColumnReaderImpl(opts) {}
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    ~RequiredStoredColumnReader() override = default;

    Status init(const ParquetField* field, const tparquet::ColumnChunk* chunk_metadata) {
        _field = field;
        _reader = std::make_unique<ColumnChunkReader>(_field->max_def_level(), _field->max_rep_level(),
                                                      _field->type_length, chunk_metadata, _opts);
        RETURN_IF_ERROR(_reader->init(_opts.chunk_size));
        return Status::OK();
    }

<<<<<<< HEAD
    void reset() override {}

    Status do_read_records(size_t* num_rows, ColumnContentType content_type, Column* dst) override;
=======
    void reset_levels() override {}
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        *def_levels = nullptr;
        *rep_levels = nullptr;
        *num_levels = 0;
    }

private:
<<<<<<< HEAD
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

#ifndef NDEBUG
    std::stringstream ss;
    ss << "rep=[";
<<<<<<< HEAD
    for (int i = levels_pos; i < _levels_decoded; ++i) {
        ss << ", " << _rep_levels[i];
    }
    ss << "], def=[";
    for (int i = levels_pos; i < _levels_decoded; ++i) {
        ss << ", " << _def_levels[i];
=======
    for (int i = levels_pos; i < avail_levels; ++i) {
        ss << ", " << rep_levels[i];
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }
    ss << "]";
    VLOG_FILE << ss.str();
#endif

    if (!_meet_first_record) {
        _meet_first_record = true;
<<<<<<< HEAD
        DCHECK_EQ(_rep_levels[levels_pos], 0);
=======
        DCHECK_EQ(rep_levels[levels_pos], 0);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        levels_pos++;
    }

    size_t rows_read = 0;
<<<<<<< HEAD
    for (; levels_pos < _levels_decoded && rows_read < *num_rows; ++levels_pos) {
        rows_read += _rep_levels[levels_pos] == 0;
=======
    for (; levels_pos < avail_levels && rows_read < *num_rows; ++levels_pos) {
        rows_read += rep_levels[levels_pos] == 0;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    if (rows_read == *num_rows) {
        // Notice, ++levels_pos in for-loop will take one step forward, so we need -1
        levels_pos--;
<<<<<<< HEAD
        DCHECK_EQ(0, _rep_levels[levels_pos]);
=======
        // Had read enough rows, reset _meet_first_record for next read
        _meet_first_record = false;
        DCHECK_EQ(0, rep_levels[levels_pos]);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    } // else {
      //  means  rows_read < *num_rows, levels_pos >= _levels_decoded,
      //  so we need to decode more levels to obtain a complete line or
      //  we have read all the records in this column chunk.
    // }

<<<<<<< HEAD
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

<<<<<<< HEAD
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
            RETURN_IF_ERROR(_lazy_load_page_rows(batch_size, content_type, dst));
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

=======
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
    } else {
        return _reader->decode_values(null_pos, &_is_nulls[0], content_type, dst);
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
} // namespace starrocks::parquet
