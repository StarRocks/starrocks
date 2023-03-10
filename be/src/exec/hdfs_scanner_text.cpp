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

#include "column/column_helper.h"
#include "exec/exec_node.h"
#include "gen_cpp/Descriptors_types.h"
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
    ASSIGN_OR_RETURN(s.size, _file->read(s.data, s.size));
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

Status HdfsTextScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    TTextFileDesc text_file_desc = _scanner_params.scan_ranges[0]->text_file_desc;

    // All delimiters will not be empty.
    // Even if the user has not set it, there will be a default value.
    DCHECK(!text_file_desc.field_delim.empty());
    DCHECK(!text_file_desc.line_delim.empty());
    DCHECK(!text_file_desc.collection_delim.empty());
    DCHECK(!text_file_desc.mapkey_delim.empty());

    // _field_delimiter and _record_delimiter should use std::string,
    // because the CSVReader is using std::string type as delimiter.
    _field_delimiter = text_file_desc.field_delim;
    // we should cast string to char now since csv reader only support record delimiter by char.
    _record_delimiter = text_file_desc.line_delim.front();

    // In Hive, users can specify collection delimiter and mapkey delimiter as string type,
    // but in fact, only the first character of the delimiter will take effect.
    // So here, we only use the first character of collection_delim and mapkey_delim.
    if (text_file_desc.collection_delim.empty() || text_file_desc.mapkey_delim.empty()) {
        // During the StarRocks upgrade process, collection_delim and mapkey_delim may be empty,
        // in order to prevent crash, we set _collection_delimiter
        // and _mapkey_delimiter a default value here.
        _collection_delimiter = '\002';
        _mapkey_delimiter = '\003';
    } else {
        _collection_delimiter = text_file_desc.collection_delim.front();
        _mapkey_delimiter = text_file_desc.mapkey_delim.front();
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
    SCOPED_RAW_TIMER(&_stats.reader_init_ns);
    for (const auto slot : _scanner_params.materialize_slots) {
        // TODO slot maybe null?
        DCHECK(slot != nullptr);
        ConverterPtr conv = csv::get_converter(slot->type(), true);
        RETURN_IF_ERROR(_get_hive_column_index(slot->col_name()));
        if (conv == nullptr) {
            return Status::InternalError(strings::Substitute("Unsupported CSV type $0", slot->type().debug_string()));
        }
        _converters.emplace_back(std::move(conv));
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
    _stats.raw_rows_read += ck->num_rows();
    for (auto& it : _scanner_ctx.conjunct_ctxs_by_slot) {
        // do evaluation.
        SCOPED_RAW_TIMER(&_stats.expr_filter_ns);
        RETURN_IF_ERROR(ExecNode::eval_conjuncts(it.second, ck.get()));
        if (ck->num_rows() == 0) {
            break;
        }
    }
    return Status::OK();
}

Status HdfsTextScanner::parse_csv(int chunk_size, ChunkPtr* chunk) {
    DCHECK_EQ(0, chunk->get()->num_rows());
    Status status;
    CSVReader::Record record;
    CSVReader::Fields fields;

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
    options.invalid_field_as_null = true;

    for (size_t num_rows = chunk->get()->num_rows(); num_rows < chunk_size; /**/) {
        status = down_cast<HdfsScannerCSVReader*>(_reader.get())->next_record(&record);
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
            LOG(WARNING) << "Status is not ok " << status.get_error_msg();
            return status;
        }

        fields.clear();
        _reader->split_record(record, &fields);

        if (!validate_utf8(record.data, record.size)) {
            continue;
        }

        bool has_error = false;
        std::string error_msg;
        int num_materialize_columns = _scanner_params.materialize_slots.size();
        int field_size = fields.size();
        if (_scanner_params.hive_column_names->size() != field_size) {
            VLOG(7) << strings::Substitute("Size mismatch between hive column $0 names and fields $1!",
                                           _scanner_params.hive_column_names->size(), fields.size());
        }
        for (int j = 0; j < num_materialize_columns; j++) {
            // TODO slot maybe null?
            const auto slot = _scanner_params.materialize_slots[j];
            DCHECK(slot != nullptr);

            std::string col_name = _scanner_params.materialize_slots[j]->col_name();
            if (!_scanner_params.case_sensitive) {
                std::transform(col_name.begin(), col_name.end(), col_name.begin(), ::tolower);
            }
            int index = _scanner_params.materialize_index_in_chunk[j];
            int column_field_index = _columns_index[_scanner_params.materialize_slots[j]->col_name()];
            Column* column = _column_raw_ptrs[index];
            if (column_field_index < field_size) {
                const Slice& field = fields[column_field_index];
                options.type_desc = &(_scanner_params.materialize_slots[j]->type());
                if (!_converters[j]->read_string(column, field, options)) {
                    LOG(WARNING) << "Converter encountered an error for field " << field.to_string() << ", index "
                                 << index << ", column " << _scanner_params.materialize_slots[j]->debug_string();
                    error_msg = strings::Substitute("CSV parse column [$0] failed, more details please see be log.",
                                                    _scanner_params.materialize_slots[j]->debug_string());
                    chunk->get()->set_num_rows(num_rows);
                    has_error = true;
                    break;
                }
            } else {
                // The size of hive_column_names may be larger than fields when new columns are added.
                // The default value should be filled when querying the extra columns that
                // do not exist in the text file.
                // hive only support null column
                // TODO: support not null
                column->append_nulls(1);
            }
        }
        num_rows += !has_error;
        if (!has_error) {
            // Partition column not stored in text file, we should append these columns
            // when we select partition column.
            int num_part_columns = _scanner_ctx.partition_columns.size();
            for (int p = 0; p < num_part_columns; ++p) {
                int index = _scanner_params.partition_index_in_chunk[p];
                Column* column = _column_raw_ptrs[index];
                ColumnPtr partition_value = _scanner_ctx.partition_values[p];
                DCHECK(partition_value->is_constant());
                auto* const_column = ColumnHelper::as_raw_column<ConstColumn>(partition_value);
                const ColumnPtr& data_column = const_column->data_column();
                if (data_column->is_nullable()) {
                    column->append_nulls(1);
                } else {
                    column->append(*data_column, 0, 1);
                }
            }
        } else {
            return Status::InternalError(error_msg);
        }
    }
    return chunk->get()->num_rows() > 0 ? Status::OK() : Status::EndOfFile("");
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
        RETURN_IF_ERROR(reader->reset(scan_range->offset, scan_range->length));
        if (scan_range->offset != 0) {
            // Always skip first record of scan range with non-zero offset.
            // Notice that the first record will read by previous scan range.
            CSVReader::Record dummy;
            RETURN_IF_ERROR(reader->next_record(&dummy));
        }
    }
    return Status::OK();
}

Status HdfsTextScanner::_get_hive_column_index(const std::string& column_name) {
    const bool case_sensitive = _scanner_params.case_sensitive;
    for (int i = 0; i < _scanner_params.hive_column_names->size(); i++) {
        const std::string& name = _scanner_params.hive_column_names->at(i);
        const bool found = case_sensitive ? name == column_name : 0 == strcasecmp(name.c_str(), column_name.c_str());

        if (found) {
            _columns_index[column_name] = i;
            return Status::OK();
        }
    }
    return Status::InvalidArgument("Can not get index of column name " + column_name);
}

} // namespace starrocks
