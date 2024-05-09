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
    HdfsScannerCSVReader(RandomAccessFile* file, const std::string& row_delimiter, bool need_probe_line_delimiter,
                         const std::string& column_separator, size_t file_length)
            : CSVReader(CSVParseOptions(row_delimiter, column_separator)) {
        _file = file;
        _offset = 0;
        _remain_length = file_length;
        _file_length = file_length;
        _row_delimiter_length = row_delimiter.size();
        _column_delimiter_length = column_separator.size();
        _need_probe_line_delimiter = need_probe_line_delimiter;
    }

    Status reset(size_t offset, size_t remain_length);

    Status next_record(Record* record);

protected:
    Status _fill_buffer() override;

    void _trim_row_delimeter(Record* record);

    char* _find_line_delimiter(starrocks::CSVBuffer& buffer, size_t pos) override;

private:
    RandomAccessFile* _file;
    size_t _offset = 0;
    size_t _remain_length = 0;
    size_t _file_length = 0;
    bool _should_stop_scan = false;
    bool _should_stop_next = false;
    // Hive TextFile's line delimiter maybe \n, \r or \r\n, we need to probe it
    bool _need_probe_line_delimiter = false;
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

char* HdfsScannerCSVReader::_find_line_delimiter(starrocks::CSVBuffer& buffer, size_t pos) {
    // If we can get explicit line.delim from hms, we don't need to probe it
    if (LIKELY(!_need_probe_line_delimiter)) {
        return buffer.find(_parse_options.row_delimiter, pos);
    } else {
        // If we didn't get explicit line.delim from hms, we need to probe it.
        // We will probe '\n' first, most of TextFile's line.delim is '\n'
        // Then we will try to probe '\r'
        // TODO(Smith)
        // We didn't support to treat '\r\n' as line.delim,
        // because our code does not support line separator's length larger than one char.
        char* p = buffer.find(LINE_DELIM_LF, pos);
        if (p != nullptr) {
            _need_probe_line_delimiter = false;
            _parse_options.row_delimiter = LINE_DELIM_LF;
            return p;
        }
        p = buffer.find(LINE_DELIM_CR, pos);
        if (p != nullptr) {
            _need_probe_line_delimiter = false;
            _parse_options.row_delimiter = LINE_DELIM_CR;
            return p;
        }
        return nullptr;
    }
}

Status HdfsTextScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    TTextFileDesc text_file_desc = _scanner_params.scan_range->text_file_desc;

    // _field_delimiter and _line_delimiter should use std::string,
    // because the CSVReader is using std::string type as delimiter.
    if (text_file_desc.__isset.field_delim) {
        if (text_file_desc.field_delim.empty()) {
            // Just a piece of defense code
            return Status::Corruption("Hive TextFile's field delim is empty");
        }
        _field_delimiter = text_file_desc.field_delim;
    } else {
        _field_delimiter = DEFAULT_FIELD_DELIM;
    }

    // we should cast string to char now since csv reader only support record delimiter by char.
    if (text_file_desc.__isset.line_delim) {
        if (text_file_desc.line_delim.empty()) {
            // Just a piece of defense code
            return Status::Corruption("Hive TextFile's line delim is empty");
        }
        _line_delimiter = text_file_desc.line_delim.front();
    } else {
        _line_delimiter = DEFAULT_LINE_DELIM;
        // We didn't get explicit line delimiter from hms, so we need to probe it by ourselves
        _need_probe_line_delimiter = true;
    }

    // In Hive, users can specify collection delimiter and mapkey delimiter as string type,
    // but in fact, only the first character of the delimiter will take effect.
    // So here, we only use the first character of collection_delim and mapkey_delim.
    if (text_file_desc.__isset.collection_delim) {
        if (text_file_desc.collection_delim.empty()) {
            // Just a piece of defense code
            return Status::Corruption("Hive TextFile's collection delim is empty");
        }
        _collection_delimiter = text_file_desc.collection_delim.front();
    } else {
        _collection_delimiter = DEFAULT_COLLECTION_DELIM.front();
    }
    if (text_file_desc.__isset.mapkey_delim) {
        if (text_file_desc.mapkey_delim.empty()) {
            // Just a piece of defense code
            return Status::Corruption("Hive TextFile's mapkey delim is empty");
        }
        _mapkey_delimiter = text_file_desc.mapkey_delim.front();
    } else {
        _mapkey_delimiter = DEFAULT_MAPKEY_DELIM.front();
    }

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
    RETURN_IF_ERROR(_create_or_reinit_reader());
    SCOPED_RAW_TIMER(&_app_stats.reader_init_ns);

    // update materialized columns.
    {
        std::unordered_set<std::string> names;
        for (const auto& column : _scanner_ctx.materialized_columns) {
            if (column.name() == "___count___") continue;
            names.insert(column.name());
        }
        RETURN_IF_ERROR(_scanner_ctx.update_materialized_columns(names));
    }

    RETURN_IF_ERROR(_build_hive_column_name_2_index());
    for (const auto& column : _scanner_ctx.materialized_columns) {
        // We don't care about _invalid_field_as_null here, if get converter failed,
        // we use DefaultValueConverter instead.
        auto converter = csv::get_hive_converter(column.slot_type(), true);
        DCHECK(converter != nullptr);
        _converters.emplace_back(std::move(converter));
    }
    return Status::OK();
}

void HdfsTextScanner::do_update_counter(HdfsScanProfile* profile) {
    profile->runtime_profile->add_info_string("TextCompression", CompressionTypePB_Name(_compression_type));
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
        _column_raw_ptrs[i]->reserve(chunk_size);
    }

    csv::Converter::Options options;
    // Use to custom Hive array format
    options.is_hive = true;
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
            break;
        } else if (!status.ok()) {
            LOG(WARNING) << strings::Substitute("Parse csv file $0 failed: $1", _file->filename(), status.message());
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

        size_t num_materialize_columns = _scanner_ctx.materialized_columns.size();

        // Fill materialize columns first, then fill partition column
        for (int j = 0; j < num_materialize_columns; j++) {
            const auto& column_info = _scanner_ctx.materialized_columns[j];

            size_t chunk_index = column_info.idx_in_chunk;
            size_t csv_index = _materialize_slots_index_2_csv_column_index[j];
            Column* column = _column_raw_ptrs[chunk_index];
            if (csv_index < fields.size()) {
                const Slice& field = fields[csv_index];
                options.type_desc = &(column_info.slot_type());
                if (!_converters[j]->read_string(column, field, options)) {
                    return Status::InternalError(
                            strings::Substitute("CSV converter encountered an error for field: $0, column name is: $1",
                                                field.to_string(), column_info.name()));
                }
            } else {
                // The size of hive_column_names may be larger than fields when new columns are added.
                // The default value should be filled when querying the extra columns that
                // do not exist in the text file.
                column->append_default();
            }
        }
    }

    RETURN_IF_ERROR(_scanner_ctx.append_or_update_not_existed_columns_to_chunk(chunk, rows_read));
    _scanner_ctx.append_or_update_partition_column_to_chunk(chunk, rows_read);

    // Check chunk's row number for each column
    chunk->get()->check_or_die();

    return rows_read > 0 ? Status::OK() : Status::EndOfFile("");
}

Status HdfsTextScanner::_create_or_reinit_reader() {
    const THdfsScanRange* scan_range = _scanner_ctx.scan_range;

    if (_compression_type != NO_COMPRESSION) {
        // Since we can not parse compressed file in pieces, we only handle scan range whose offset == 0.
        if (scan_range->offset != 0) {
            _no_data = true;
            return Status::OK();
        }
        // we don't know real stream size in adavance, so we set a very large stream size
        auto file_size = static_cast<size_t>(-1);
        _reader = std::make_unique<HdfsScannerCSVReader>(_file.get(), _line_delimiter, _need_probe_line_delimiter,
                                                         _field_delimiter, file_size);
        return Status::OK();
    }

    // no compressed file, splittable.
    {
        _reader = std::make_unique<HdfsScannerCSVReader>(_file.get(), _line_delimiter, _need_probe_line_delimiter,
                                                         _field_delimiter, scan_range->file_length);
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

Status HdfsTextScanner::_build_hive_column_name_2_index() {
    // For some table like file table, there is no hive_column_names at all.
    // So we use slot order defined in table schema.
    if (_scanner_ctx.hive_column_names->empty()) {
        _materialize_slots_index_2_csv_column_index.resize(_scanner_ctx.materialized_columns.size());
        for (size_t i = 0; i < _scanner_ctx.materialized_columns.size(); i++) {
            _materialize_slots_index_2_csv_column_index[i] = i;
        }
        return Status::OK();
    }

    const bool case_sensitive = _scanner_ctx.case_sensitive;

    // The map's value is the position of column name in hive's table(Not in StarRocks' table)
    std::unordered_map<std::string, size_t> formatted_hive_column_name_2_index;

    for (size_t i = 0; i < _scanner_ctx.hive_column_names->size(); i++) {
        const std::string& name = (*_scanner_ctx.hive_column_names)[i];
        const std::string formatted_column_name = _scanner_ctx.formatted_name(name);
        formatted_hive_column_name_2_index.emplace(formatted_column_name, i);
    }

    // Assign csv column index
    _materialize_slots_index_2_csv_column_index.resize(_scanner_ctx.materialized_columns.size());
    for (size_t i = 0; i < _scanner_ctx.materialized_columns.size(); i++) {
        const auto& column = _scanner_ctx.materialized_columns[i];
        const std::string formatted_slot_name = column.formatted_name(case_sensitive);
        const auto& it = formatted_hive_column_name_2_index.find(formatted_slot_name);
        if (it == formatted_hive_column_name_2_index.end()) {
            return Status::InternalError("Can not get index of column name: " + formatted_slot_name);
        }
        _materialize_slots_index_2_csv_column_index[i] = it->second;
    }
    return Status::OK();
}

int64_t HdfsTextScanner::estimated_mem_usage() const {
    int64_t value = HdfsScanner::estimated_mem_usage();
    if (value != 0) return value;
    return _reader->buff_capacity() * 3 / 2;
}

} // namespace starrocks
