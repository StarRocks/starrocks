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

#include "exec/hdfs_scanner_text.h"

#include <unordered_map>

#include "column/column_helper.h"
#include "exec/exec_node.h"
#include "gutil/strings/substitute.h"
#include "util/compression/compression_utils.h"
#include "util/compression/stream_compression.h"
#include "util/utf8_check.h"

namespace starrocks {

static CompressionTypePB return_compression_type_from_filename(const std::string& filename) {
    ssize_t end = filename.size() - 1;
    while (end >= 0 && filename[end] != '.' && filename[end] != '/') end--;
    if (end == -1 || filename[end] == '/') return NO_COMPRESSION;
    const std::string& ext = filename.substr(end + 1);
    return CompressionUtils::to_compression_pb(ext);
}

class HdfsScannerCSVReader : public CSVReader {
public:
    // |file| must outlive HdfsScannerCSVReader
    HdfsScannerCSVReader(RandomAccessFile* file, const std::string& row_delimiter, const std::string& column_separator,
                         size_t file_length)
            : CSVReader(CSVParseOptions(row_delimiter, column_separator)) {
        _file = file;
        _offset = 0;
        _remain_length = file_length;
        _file_length = file_length;
        _row_delimiter_length = row_delimiter.size();
        _column_delimiter_length = column_separator.size();
    }

    Status reset(size_t offset, size_t remain_length);

    Status next_record(Record* record);

protected:
    Status _fill_buffer() override;

    void _trim_row_delimeter(Record* record);

private:
    RandomAccessFile* _file;
    size_t _offset = 0;
    size_t _remain_length = 0;
    size_t _file_length = 0;
    bool _should_stop_scan = false;
    bool _should_stop_next = false;
};

Status HdfsScannerCSVReader::reset(size_t offset, size_t remain_length) {
    RETURN_IF_ERROR(_file->seek(offset));
    _offset = offset;
    _remain_length = remain_length;
    _should_stop_scan = false;
    _should_stop_next = false;
    _buff.skip(_buff.limit() - _buff.position());
    return Status::OK();
}

Status HdfsScannerCSVReader::next_record(Record* record) {
    if (_should_stop_next) {
        return Status::EndOfFile("");
    }
    RETURN_IF_ERROR(CSVReader::next_record(record));
    // We should still read if remain_length is zero(we stop right at row delimiter)
    // because next scan range will skip a record till row delimiter.
    // so it's current reader's responsibility to consume this record.
    size_t consume = record->size + _row_delimiter_length;
    if (_remain_length < consume) {
        _should_stop_next = true;
    } else {
        _remain_length -= consume;
    }
    _trim_row_delimeter(record);
    return Status::OK();
}

Status HdfsScannerCSVReader::_fill_buffer() {
    if (_should_stop_scan) {
        return Status::EndOfFile("");
    }

    DCHECK(_buff.free_space() > 0);
    Slice s = Slice(_buff.limit(), _buff.free_space());

    // It's very critical to call `read` here, because underneath of RandomAccessFile
    // maybe its a SequenceFile because we support to read compressed text file.
    // For uncompressed text file, we can split csv file into chunks and process chunks parallelly.
    // For compressed text file, we only can parse csv file in sequential way.
    ASSIGN_OR_RETURN(s.size, _file->read(s.data, std::min(s.size, _file_length - _offset)));
    _offset += s.size;
    _buff.add_limit(s.size);
    if (s.size == 0) {
        size_t n = _buff.available();
        _should_stop_scan = true;
        // if there is no linger data in buff at all, then we don't need to add a row delimiter. Otherwise we will add a new record.
        // For example, a single column table like "a\nb\nc\n". if we add a trailing row delimiter, then there will be a "" at last.
        if (n == 0) return Status::EndOfFile("");

        // Has reached the end of file but still no record delimiter found, which
        // is valid, according the RFC, add the record delimiter ourself, ONLY IF we have space.
        // But if we don't have any space, which means a single csv record size has exceed buffer max size.
        if (n >= _row_delimiter_length &&
            _buff.find(_parse_options.row_delimiter, n - _row_delimiter_length) == nullptr) {
            if (_buff.free_space() >= _row_delimiter_length) {
                for (char ch : _parse_options.row_delimiter) {
                    _buff.append(ch);
                }
            } else {
                return Status::InternalError("CSV line length exceed limit " + std::to_string(_buff.capacity()) +
                                             " when padding row delimiter");
            }
        }
    }

    return Status::OK();
}

void HdfsScannerCSVReader::_trim_row_delimeter(Record* record) {
    // For default row delemiter which is line break, we need to trim the windows line break
    // if the file was written in windows platfom.
    if (_parse_options.row_delimiter == "\n") {
        while (record->size > 0 && record->data[record->size - 1] == '\r') {
            record->size--;
        }
    }
}

Status HdfsTextScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    TTextFileDesc text_file_desc = _scanner_params.scan_ranges[0]->text_file_desc;

    // All delimiters will not be empty.
    // Even if the user has not set it, there will be a default value.
    if (text_file_desc.field_delim.empty() || text_file_desc.line_delim.empty() ||
        text_file_desc.collection_delim.empty() || text_file_desc.mapkey_delim.empty()) {
        return Status::Corruption("Hive TEXTFILE's delimiters is missing");
    }

    // _field_delimiter and _record_delimiter should use std::string,
    // because the CSVReader is using std::string type as delimiter.
    _field_delimiter = text_file_desc.field_delim;
    // we should cast string to char now since csv reader only support record delimiter by char.
    _record_delimiter = text_file_desc.line_delim.front();

    // In Hive, users can specify collection delimiter and mapkey delimiter as string type,
    // but in fact, only the first character of the delimiter will take effect.
    // So here, we only use the first character of collection_delim and mapkey_delim.
    _collection_delimiter = text_file_desc.collection_delim.front();
    _mapkey_delimiter = text_file_desc.mapkey_delim.front();

    // by default it's unknown compression. we will synthesise informaiton from FE and BE(file extension)
    // parse compression type from FE first.
    _compression_type = CompressionTypePB::UNKNOWN_COMPRESSION;
    if (text_file_desc.__isset.compression_type) {
        _compression_type = CompressionUtils::to_compression_pb(text_file_desc.compression_type);
    }

    return Status::OK();
}

Status HdfsTextScanner::do_open(RuntimeState* runtime_state) {
    const std::string& path = _scanner_params.path;
    // if FE does not specify compress type, we choose it by looking at filename.
    if (_compression_type == CompressionTypePB::UNKNOWN_COMPRESSION) {
        _compression_type = return_compression_type_from_filename(path);
        if (_compression_type == CompressionTypePB::UNKNOWN_COMPRESSION) {
            _compression_type = CompressionTypePB::NO_COMPRESSION;
        }
    }
    RETURN_IF_ERROR(open_random_access_file());
    RETURN_IF_ERROR(_setup_io_ranges());
    RETURN_IF_ERROR(_create_or_reinit_reader());
    SCOPED_RAW_TIMER(&_app_stats.reader_init_ns);
    RETURN_IF_ERROR(_build_hive_column_name_2_index());
    for (const auto slot : _scanner_params.materialize_slots) {
        DCHECK(slot != nullptr);
        // We don't care about _invalid_field_as_null here, if get converter failed,
        // we use DefaultValueConverter instead.
        auto converter = csv::get_hive_converter(slot->type(), true);
        DCHECK(converter != nullptr);
        _converters.emplace_back(std::move(converter));
    }
    return Status::OK();
}

void HdfsTextScanner::do_close(RuntimeState* runtime_state) noexcept {
    _reader.reset();
}

Status HdfsTextScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    if (_no_data) {
        return Status::EndOfFile("");
    }
    CHECK(chunk != nullptr);
    RETURN_IF_ERROR(parse_csv(runtime_state->chunk_size(), chunk));

    ChunkPtr ck = *chunk;
    // do stats before we filter rows which does not match.
    _app_stats.raw_rows_read += ck->num_rows();
    for (auto& it : _scanner_ctx.conjunct_ctxs_by_slot) {
        // do evaluation.
        SCOPED_RAW_TIMER(&_app_stats.expr_filter_ns);
        RETURN_IF_ERROR(ExecNode::eval_conjuncts(it.second, ck.get()));
        if (ck->num_rows() == 0) {
            break;
        }
    }
    return Status::OK();
}

Status HdfsTextScanner::parse_csv(int chunk_size, ChunkPtr* chunk) {
    DCHECK_EQ(0, chunk->get()->num_rows());

    int num_columns = chunk->get()->num_columns();
    _column_raw_ptrs.resize(num_columns);
    for (int i = 0; i < num_columns; i++) {
        _column_raw_ptrs[i] = chunk->get()->get_column_by_index(i).get();
    }

    csv::Converter::Options options;
    // Use to custom Hive array format
    options.array_format_type = csv::ArrayFormatType::kHive;
    options.array_hive_collection_delimiter = _collection_delimiter;
    options.array_hive_mapkey_delimiter = _mapkey_delimiter;
    options.array_hive_nested_level = 1;
    options.invalid_field_as_null = _invalid_field_as_null;

    size_t rows_read = 0;

    CSVReader::Fields fields{};
    for (; rows_read < chunk_size; rows_read++) {
        CSVReader::Record record{};
        Status status = down_cast<HdfsScannerCSVReader*>(_reader.get())->next_record(&record);
        if (status.is_end_of_file()) {
            if (_current_range_index == _scanner_params.scan_ranges.size() - 1) {
                break;
            }
            // End of file status indicate:
            // 1. read end of file
            // 2. should stop scan
            _current_range_index++;
            RETURN_IF_ERROR(_create_or_reinit_reader());
            continue;
        } else if (!status.ok()) {
            LOG(WARNING) << strings::Substitute("Parse csv file $0 failed: $1", _file->filename(),
                                                status.get_error_msg());
            return status;
        }

        bool validate_res = validate_utf8(record.data, record.size);
        if (!validate_res && options.invalid_field_as_null) {
            VLOG_ROW << "Face csv invalidate UTF-8 character line, append default value for this line";
            chunk->get()->append_default();
            continue;
        } else if (!validate_res) {
            return Status::InternalError("Face csv invalidate UTF-8 character line.");
        }

        fields.resize(0);
        _reader->split_record(record, &fields);

        size_t num_materialize_columns = _scanner_params.materialize_slots.size();

        // Fill materialize columns first, then fill partition column
        for (int j = 0; j < num_materialize_columns; j++) {
            const auto& slot = _scanner_params.materialize_slots[j];
            DCHECK(slot != nullptr);

            size_t chunk_index = _scanner_params.materialize_index_in_chunk[j];
            size_t csv_index = _materialize_slots_index_2_csv_column_index[j];
            Column* column = _column_raw_ptrs[chunk_index];
            if (csv_index < fields.size()) {
                const Slice& field = fields[csv_index];
                options.type_desc = &(_scanner_params.materialize_slots[j]->type());
                if (!_converters[j]->read_string(column, field, options)) {
                    return Status::InternalError(
                            strings::Substitute("CSV converter encountered an error for field: $0, column name is: $1",
                                                field.to_string(), slot->col_name()));
                }
            } else {
                // The size of hive_column_names may be larger than fields when new columns are added.
                // The default value should be filled when querying the extra columns that
                // do not exist in the text file.
                column->append_default();
            }
        }
    }

    // TODO Try to reuse HdfsScannerContext::append_partition_column_to_chunk() function
    // Start to append partition column
    for (size_t p = 0; p < _scanner_ctx.partition_columns.size(); ++p) {
        size_t chunk_index = _scanner_params.partition_index_in_chunk[p];
        Column* column = _column_raw_ptrs[chunk_index];
        ColumnPtr partition_value = _scanner_ctx.partition_values[p];
        DCHECK(partition_value->is_constant());
        auto* const_column = ColumnHelper::as_raw_column<ConstColumn>(partition_value);
        const ColumnPtr& data_column = const_column->data_column();

        if (data_column->is_nullable()) {
            column->append_default(1);
        } else {
            column->append(*data_column, 0, 1);
        }

        column->assign(rows_read, 0);
    }

    // Check chunk's row number for each column
    chunk->get()->check_or_die();

    return rows_read > 0 ? Status::OK() : Status::EndOfFile("");
}

Status HdfsTextScanner::_create_or_reinit_reader() {
    if (_compression_type != NO_COMPRESSION) {
        // Since we can not parse compressed file in pieces, we only handle scan range whose offset == 0.
        size_t index = 0;
        for (; index < _scanner_params.scan_ranges.size(); index++) {
            const THdfsScanRange* scan_range = _scanner_params.scan_ranges[index];
            if (scan_range->offset == 0) {
                break;
            }
        }
        if (index == _scanner_params.scan_ranges.size()) {
            _no_data = true;
            return Status::OK();
        }
        // set current range index to the last one, so next time we reach EOF.
        _current_range_index = _scanner_params.scan_ranges.size() - 1;
        // we don't know real stream size in adavance, so we set a very large stream size
        auto file_size = static_cast<size_t>(-1);
        _reader = std::make_unique<HdfsScannerCSVReader>(_file.get(), _record_delimiter, _field_delimiter, file_size);
        return Status::OK();
    }

    // no compressed file, splittable.
    const THdfsScanRange* scan_range = _scanner_params.scan_ranges[_current_range_index];
    if (_current_range_index == 0) {
        _reader = std::make_unique<HdfsScannerCSVReader>(_file.get(), _record_delimiter, _field_delimiter,
                                                         scan_range->file_length);
    }
    {
        auto* reader = down_cast<HdfsScannerCSVReader*>(_reader.get());

        // if reading start of file, skipping UTF-8 BOM
        bool has_bom = false;
        if (scan_range->offset == 0) {
            CSVReader::Record first_line;
            RETURN_IF_ERROR(reader->next_record(&first_line));
            if (first_line.size >= 3 && (unsigned char)first_line.data[0] == 0xEF &&
                (unsigned char)first_line.data[1] == 0xBB && (unsigned char)first_line.data[2] == 0xBF) {
                has_bom = true;
            }
        }
        if (has_bom) {
            RETURN_IF_ERROR(reader->reset(scan_range->offset + 3, scan_range->length - 3));
        } else {
            RETURN_IF_ERROR(reader->reset(scan_range->offset, scan_range->length));
        }
        if (scan_range->offset != 0) {
            // Always skip first record of scan range with non-zero offset.
            // Notice that the first record will read by previous scan range.
            CSVReader::Record dummy;
            RETURN_IF_ERROR(reader->next_record(&dummy));
        }
    }
    return Status::OK();
}

Status HdfsTextScanner::_setup_io_ranges() const {
    if (_shared_buffered_input_stream != nullptr) {
        std::vector<io::SharedBufferedInputStream::IORange> ranges{};
        for (int64_t offset = 0; offset < _scanner_params.file_size;) {
            const int64_t remain_length = std::min(config::text_io_range_size, _scanner_params.file_size - offset);
            ranges.emplace_back(offset, remain_length);
            offset += remain_length;
        }
        RETURN_IF_ERROR(_shared_buffered_input_stream->set_io_ranges(ranges));
    }
    return Status::OK();
}

Status HdfsTextScanner::_build_hive_column_name_2_index() {
    // For some table like file table, there is no hive_column_names at all.
    // So we use slot order defined in table schema.
    if (_scanner_params.hive_column_names->empty()) {
        _materialize_slots_index_2_csv_column_index.resize(_scanner_params.materialize_slots.size());
        for (size_t i = 0; i < _scanner_params.materialize_slots.size(); i++) {
            _materialize_slots_index_2_csv_column_index[i] = i;
        }
        return Status::OK();
    }

    const bool case_sensitive = _scanner_params.case_sensitive;

    // The map's value is the position of column name in hive's table(Not in StarRocks' table)
    std::unordered_map<std::string, size_t> formatted_hive_column_name_2_index;

    for (size_t i = 0; i < _scanner_params.hive_column_names->size(); i++) {
        const std::string formatted_column_name =
                case_sensitive ? (*_scanner_params.hive_column_names)[i]
                               : boost::algorithm::to_lower_copy((*_scanner_params.hive_column_names)[i]);
        formatted_hive_column_name_2_index.emplace(formatted_column_name, i);
    }

    // Assign csv column index
    _materialize_slots_index_2_csv_column_index.resize(_scanner_params.materialize_slots.size());
    for (size_t i = 0; i < _scanner_params.materialize_slots.size(); i++) {
        const auto& slot = _scanner_params.materialize_slots[i];
        const std::string& formatted_slot_name =
                case_sensitive ? slot->col_name() : boost::algorithm::to_lower_copy(slot->col_name());
        const auto& it = formatted_hive_column_name_2_index.find(formatted_slot_name);
        if (it == formatted_hive_column_name_2_index.end()) {
            return Status::InternalError("Can not get index of column name: " + formatted_slot_name);
        }
        _materialize_slots_index_2_csv_column_index[i] = it->second;
    }
    return Status::OK();
}

} // namespace starrocks
