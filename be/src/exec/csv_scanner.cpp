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

#include "exec/csv_scanner.h"

#include "column/adaptive_nullable_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/hash_set.h"
#include "fs/fs.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"
#include "util/utf8_check.h"

namespace starrocks {

static std::string string_2_asc(const std::string& input) {
    std::stringstream oss;
    oss << "'";
    for (char c : input) {
        if (c == '\n') {
            oss << "\\n";
        } else if (c == '\t') {
            oss << "\\t";
        } else if (std::isprint(static_cast<unsigned char>(c))) {
            oss << c;
        } else {
            oss << "0x" << std::hex << (static_cast<unsigned int>(c) & 0xFF);
        }
    }
    oss << "'";
    return oss.str();
}

static std::string make_column_count_not_matched_error_message(int expected_count, int actual_count,
                                                               CSVParseOptions& parse_options) {
    std::stringstream error_msg;
    error_msg << "Value count does not match column count: "
              << "expected = " << expected_count << ", actual = " << actual_count << ". "
              << "Column separator: " << string_2_asc(parse_options.column_delimiter) << ", "
              << "Row delimiter: " << string_2_asc(parse_options.row_delimiter);
    return error_msg.str();
}

static std::string make_value_type_not_matched_error_message(int field_pos, const Slice& field,
                                                             const SlotDescriptor* slot) {
    std::stringstream error_msg;
    error_msg << "The field (name = " << slot->col_name() << ", pos = " << field_pos << ") is out of range. "
              << "Type: " << slot->type().debug_string() << ", Value: " << field.to_string();
    return error_msg.str();
}

static constexpr int REPORT_ERROR_MAX_NUMBER = 50;

const std::string& CSVScanner::ScannerCSVReader::filename() {
    return _file->filename();
}

Status CSVScanner::ScannerCSVReader::_fill_buffer() {
    SCOPED_RAW_TIMER(&_counter->file_read_ns);

    DCHECK(_buff.free_space() > 0);
    Slice s(_buff.limit(), _buff.free_space());
    auto res = _file->read(s.data, s.size);
    // According to the specification of `FileSystem::read`, when reached the end of
    // a file, the returned status will be OK instead of EOF, but here we check
    // EOF also for safety.
    if (res.status().is_end_of_file()) {
        s.size = 0;
    } else if (!res.ok()) {
        return res.status();
    } else {
        s.size = *res;
    }
    _buff.add_limit(s.size);
    auto n = _buff.available();
    if (s.size == 0) {
        if (n < _row_delimiter_length ||
            _buff.find(_parse_options.row_delimiter, n - _row_delimiter_length) == nullptr) {
            // Has reached the end of file but still no record delimiter found, which
            // is valid, according the RFC, add the record delimiter ourself.
            if (_buff.free_space() < _row_delimiter_length) {
                return Status::InternalError("CSV line length exceed limit " + std::to_string(_buff.capacity()));
            }
            for (char ch : _parse_options.row_delimiter) {
                _buff.append(ch);
            }
        }
        if (n == 0) {
            _buff.skip(_row_delimiter_length);
            // Has reached the end of file and the buffer is empty.
            return Status::EndOfFile(_file->filename());
        }
    } else {
        _state->update_num_bytes_scan_from_source(s.size);
    }
    return Status::OK();
}

CSVScanner::CSVScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                       ScannerCounter* counter)
        : FileScanner(state, profile, scan_range.params, counter), _scan_range(scan_range) {
    if (scan_range.params.__isset.multi_column_separator) {
        _parse_options.column_delimiter = scan_range.params.multi_column_separator;
    } else {
        _parse_options.column_delimiter = scan_range.params.column_separator;
    }
    if (scan_range.params.__isset.multi_row_delimiter) {
        _parse_options.row_delimiter = scan_range.params.multi_row_delimiter;
    } else {
        _parse_options.row_delimiter = scan_range.params.row_delimiter;
    }
    if (scan_range.params.__isset.skip_header) {
        _parse_options.skip_header = scan_range.params.skip_header;
    } else {
        _parse_options.skip_header = 0;
    }
    if (scan_range.params.__isset.trim_space) {
        _parse_options.trim_space = scan_range.params.trim_space;
    } else {
        _parse_options.trim_space = false;
    }
    if (scan_range.params.__isset.enclose) {
        _parse_options.enclose = scan_range.params.enclose;
    } else {
        _parse_options.enclose = 0;
    }
    if (scan_range.params.__isset.escape) {
        _parse_options.escape = scan_range.params.escape;
    } else {
        _parse_options.escape = 0;
    }
    if (_parse_options.enclose == 0 && _parse_options.escape == 0) {
        _use_v2 = false;
    } else {
        _use_v2 = true;
    }
}

void CSVScanner::close() {
    FileScanner::close();
};

Status CSVScanner::open() {
    RETURN_IF_ERROR(FileScanner::open());

    if (_scan_range.ranges.empty()) {
        return Status::OK();
    }

    const auto& first_range = _scan_range.ranges[0];
    if (!first_range.__isset.num_of_columns_from_file) {
        return Status::InternalError("'num_of_columns_from_file' not set");
    }

    for (const auto& rng : _scan_range.ranges) {
        if (rng.columns_from_path.size() != first_range.columns_from_path.size()) {
            return Status::InvalidArgument("path column count of range mismatch");
        }
        if (rng.num_of_columns_from_file != first_range.num_of_columns_from_file) {
            return Status::InvalidArgument("CSV column count of range mismatch");
        }
        if (rng.num_of_columns_from_file + rng.columns_from_path.size() != _src_slot_descriptors.size()) {
            return Status::InvalidArgument("slot descriptor and column count mismatch");
        }
    }

    _num_fields_in_csv = first_range.num_of_columns_from_file;

    for (int i = _num_fields_in_csv; i < _src_slot_descriptors.size(); i++) {
        if (_src_slot_descriptors[i] != nullptr && _src_slot_descriptors[i]->type().type != TYPE_VARCHAR) {
            auto t = _src_slot_descriptors[i]->type();
            return Status::InvalidArgument("Incorrect path column type '" + t.debug_string() + "'");
        }
    }

    for (int i = 0; i < _num_fields_in_csv; i++) {
        auto slot = _src_slot_descriptors[i];
        if (slot == nullptr) {
            // This means the i-th field in CSV file should be ignored.
            continue;
        }
        // NOTE: Here we always create a nullable converter, even if |slot->is_nullable()| is false,
        // since |slot->is_nullable()| is false does not guarantee that no NULL in the CSV files.
        // This implies that the input column of |conv| must be a `NullableColumn`.
        //
        // For those columns defined as non-nullable, NULL records will be filtered out by the
        // `TabletSink`.
        ConverterPtr conv = csv::get_converter(slot->type(), true);
        if (conv == nullptr) {
            auto msg = strings::Substitute("Unsupported CSV type $0", slot->type().debug_string());
            return Status::InternalError(msg);
        }
        _converters.emplace_back(std::move(conv));
    }

    return Status::OK();
}

void CSVScanner::_materialize_src_chunk_adaptive_nullable_column(ChunkPtr& chunk) {
    chunk->materialized_nullable();
    for (int i = 0; i < chunk->num_columns(); i++) {
        AdaptiveNullableColumn* adaptive_column =
                down_cast<AdaptiveNullableColumn*>(chunk->get_column_by_index(i).get());
        chunk->update_column_by_index(NullableColumn::create(adaptive_column->materialized_raw_data_column(),
                                                             adaptive_column->materialized_raw_null_column()),
                                      i);
    }
}

StatusOr<ChunkPtr> CSVScanner::get_next() {
    SCOPED_RAW_TIMER(&_counter->total_ns);

    ChunkPtr chunk;
    auto src_chunk = _create_chunk(_src_slot_descriptors);

    do {
        if (_curr_reader == nullptr && ++_curr_file_index < _scan_range.ranges.size()) {
            std::shared_ptr<SequentialFile> file;
            const TBrokerRangeDesc& range_desc = _scan_range.ranges[_curr_file_index];
            Status st = create_sequential_file(range_desc, _scan_range.broker_addresses[0], _scan_range.params, &file);
            if (!st.ok()) {
                LOG(WARNING) << "Failed to create sequential files. status: " << st.to_string();
                return st;
            }

            _curr_reader = std::make_unique<ScannerCSVReader>(file, _state, _parse_options);
            _curr_reader->set_counter(_counter);
            if (_scan_range.ranges[_curr_file_index].size > 0 &&
                _scan_range.ranges[_curr_file_index].format_type == TFileFormatType::FORMAT_CSV_PLAIN) {
                // Does not set limit for compressed file.
                _curr_reader->set_limit(_scan_range.ranges[_curr_file_index].size);
            }
            if (_scan_range.ranges[_curr_file_index].start_offset > 0) {
                // Skip the first record started from |start_offset|.
                auto status = file->skip(_scan_range.ranges[_curr_file_index].start_offset);
                if (status.is_time_out()) {
                    // open this file next time
                    --_curr_file_index;
                    _curr_reader.reset();
                    return status;
                }
                CSVReader::Record dummy;
                RETURN_IF_ERROR(_curr_reader->next_record(&dummy));
            }

            if (_parse_options.skip_header) {
                for (int64_t i = 0; i < _parse_options.skip_header; i++) {
                    CSVReader::Record dummy;
                    RETURN_IF_ERROR(_curr_reader->next_record(&dummy));
                }
            }
        } else if (_curr_reader == nullptr) {
            return Status::EndOfFile("CSVScanner");
        }

        src_chunk->set_num_rows(0);
        Status status = Status::OK();
        if (!_use_v2) {
            status = _parse_csv(src_chunk.get());
        } else {
            status = _parse_csv_v2(src_chunk.get());
        }
        if (!status.ok()) {
            if (status.is_end_of_file()) {
                _curr_reader = nullptr;
                DCHECK_EQ(0, src_chunk->num_rows());
            } else if (status.is_time_out()) {
                // if timeout happens at the beginning of reading src_chunk, we return the error state
                // else we will _materialize the lines read before timeout
                if (src_chunk->num_rows() == 0) {
                    return status;
                }
            } else {
                return status;
            }
        }

        if (src_chunk->num_rows() > 0) {
            _materialize_src_chunk_adaptive_nullable_column(src_chunk);
        }
    } while ((src_chunk)->num_rows() == 0);

    fill_columns_from_path(src_chunk, _num_fields_in_csv, _scan_range.ranges[_curr_file_index].columns_from_path,
                           src_chunk->num_rows());
    ASSIGN_OR_RETURN(chunk, materialize(nullptr, src_chunk));

    return std::move(chunk);
}

Status CSVScanner::_parse_csv_v2(Chunk* chunk) {
    const int capacity = _state->chunk_size();
    DCHECK_EQ(0, chunk->num_rows());
    Status status;

    int num_columns = chunk->num_columns();
    _column_raw_ptrs.resize(num_columns);
    for (int i = 0; i < num_columns; i++) {
        _column_raw_ptrs[i] = chunk->get_column_by_index(i).get();
    }

    csv::Converter::Options options{.invalid_field_as_null = !_strict_mode};
    for (size_t num_rows = chunk->num_rows(); num_rows < capacity; /**/) {
        status = _curr_reader->next_record(row);
        if (!status.ok() && !status.is_end_of_file()) {
            return status;
        }

        // skip empty row
        if (row.columns.size() == 0) {
            if (status.is_end_of_file()) {
                break;
            }
            continue;
        }

        const char* data = _curr_reader->buffBasePtr() + row.parsed_start;
        CSVReader::Record record(data, row.parsed_end - row.parsed_start);
        if (row.columns.size() != _num_fields_in_csv) {
            if (status.is_end_of_file()) {
                break;
            }
            if (_counter->num_rows_filtered++ < REPORT_ERROR_MAX_NUMBER) {
                std::string error_msg = make_column_count_not_matched_error_message(_num_fields_in_csv,
                                                                                    row.columns.size(), _parse_options);
                _report_error(record, error_msg);
            }
            if (_state->enable_log_rejected_record()) {
                std::string error_msg = make_column_count_not_matched_error_message(_num_fields_in_csv,
                                                                                    row.columns.size(), _parse_options);
                _report_rejected_record(record, error_msg);
            }
            continue;
        }
        if (!validate_utf8(record.data, record.size)) {
            if (_counter->num_rows_filtered++ < REPORT_ERROR_MAX_NUMBER) {
                _report_error(record, "Invalid UTF-8 row");
            }
            if (_state->enable_log_rejected_record()) {
                _state->append_rejected_record_to_file(record.to_string(), "Invalid UTF-8 row",
                                                       _curr_reader->filename());
            }
            continue;
        }

        SCOPED_RAW_TIMER(&_counter->fill_ns);
        bool has_error = false;
        for (int j = 0, k = 0; j < _num_fields_in_csv; j++) {
            auto slot = _src_slot_descriptors[j];
            if (slot == nullptr) {
                continue;
            }
            const CSVColumn& column = row.columns[j];
            char* basePtr = nullptr;
            if (column.is_escaped_column) {
                basePtr = _curr_reader->escapeDataPtr();
            } else {
                basePtr = _curr_reader->buffBasePtr();
            }

            const Slice data(basePtr + column.start_pos, column.length);
            options.type_desc = &(slot->type());
            if (!_converters[k]->read_string_for_adaptive_null_column(_column_raw_ptrs[k], data, options)) {
                chunk->set_num_rows(num_rows);
                if (_counter->num_rows_filtered++ < REPORT_ERROR_MAX_NUMBER) {
                    std::string error_msg = make_value_type_not_matched_error_message(j, data, slot);
                    _report_error(record, error_msg);
                }
                if (_state->enable_log_rejected_record()) {
                    std::string error_msg = make_value_type_not_matched_error_message(j, data, slot);
                    _report_rejected_record(record, error_msg);
                }
                has_error = true;
                break;
            }
            k++;
        }
        num_rows += !has_error;
        if (status.is_end_of_file()) {
            break;
        }
    }
    row.columns.clear();
    return chunk->num_rows() > 0 ? Status::OK() : Status::EndOfFile("");
}

Status CSVScanner::_parse_csv(Chunk* chunk) {
    const int capacity = _state->chunk_size();
    DCHECK_EQ(0, chunk->num_rows());
    Status status;
    CSVReader::Record record;

    int num_columns = chunk->num_columns();
    _column_raw_ptrs.resize(num_columns);
    for (int i = 0; i < num_columns; i++) {
        _column_raw_ptrs[i] = chunk->get_column_by_index(i).get();
    }

    csv::Converter::Options options{.invalid_field_as_null = !_strict_mode};

    for (size_t num_rows = chunk->num_rows(); num_rows < capacity; /**/) {
        status = _curr_reader->next_record(&record);
        if (status.is_end_of_file()) {
            break;
        } else if (!status.ok()) {
            return status;
        } else if (record.empty()) {
            // always skip blank rows.
            continue;
        }

        fields.clear();
        _curr_reader->split_record(record, &fields);

        if (fields.size() != _num_fields_in_csv) {
            if (_counter->num_rows_filtered++ < REPORT_ERROR_MAX_NUMBER) {
                std::string error_msg =
                        make_column_count_not_matched_error_message(_num_fields_in_csv, fields.size(), _parse_options);
                _report_error(record, error_msg);
            }
            if (_state->enable_log_rejected_record()) {
                std::string error_msg =
                        make_column_count_not_matched_error_message(_num_fields_in_csv, fields.size(), _parse_options);
                _report_rejected_record(record, error_msg);
            }
            continue;
        }
        if (!validate_utf8(record.data, record.size)) {
            if (_counter->num_rows_filtered++ < REPORT_ERROR_MAX_NUMBER) {
                _report_error(record, "Invalid UTF-8 row");
            }
            if (_state->enable_log_rejected_record()) {
                _report_rejected_record(record, "Invalid UTF-8 row");
            }
            continue;
        }

        SCOPED_RAW_TIMER(&_counter->fill_ns);
        bool has_error = false;
        for (int j = 0, k = 0; j < _num_fields_in_csv; j++) {
            auto slot = _src_slot_descriptors[j];
            if (slot == nullptr) {
                continue;
            }
            const Slice& field = fields[j];
            options.type_desc = &(slot->type());
            if (!_converters[k]->read_string_for_adaptive_null_column(_column_raw_ptrs[k], field, options)) {
                chunk->set_num_rows(num_rows);
                if (_counter->num_rows_filtered++ < REPORT_ERROR_MAX_NUMBER) {
                    std::string error_msg = make_value_type_not_matched_error_message(j, field, slot);
                    _report_error(record, error_msg);
                }
                if (_state->enable_log_rejected_record()) {
                    std::string error_msg = make_value_type_not_matched_error_message(j, field, slot);
                    _report_rejected_record(record, error_msg);
                }
                has_error = true;
                break;
            }
            k++;
        }
        num_rows += !has_error;
    }
    fields.clear();
    return chunk->num_rows() > 0 ? Status::OK() : Status::EndOfFile("");
}

ChunkPtr CSVScanner::_create_chunk(const std::vector<SlotDescriptor*>& slots) {
    SCOPED_RAW_TIMER(&_counter->init_chunk_ns);

    auto chunk = std::make_shared<Chunk>();
    for (int i = 0; i < _num_fields_in_csv; ++i) {
        if (slots[i] == nullptr) {
            continue;
        }
        // NOTE: Always create a nullable column, even if |slot->is_nullable()| is false.
        // See the comment in `CSVScanner::Open` for reference.
        // here we optimize it through adaptive nullable column
        auto column = ColumnHelper::create_column(slots[i]->type(), true, false, 0, true);

        chunk->append_column(std::move(column), slots[i]->id());
    }
    return chunk;
}

void CSVScanner::_report_error(const CSVReader::Record& record, const std::string& err_msg) {
    _state->append_error_msg_to_file(record.to_string(), err_msg);
}

void CSVScanner::_report_rejected_record(const CSVReader::Record& record, const std::string& err_msg) {
    _state->append_rejected_record_to_file(record.to_string(), err_msg, _curr_reader->filename());
}

} // namespace starrocks
