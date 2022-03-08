// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/hdfs_scanner_text.h"

#include "exec/vectorized/hdfs_scan_node.h"
#include "gen_cpp/Descriptors_types.h"
#include "gutil/strings/substitute.h"
#include "util/utf8_check.h"

namespace starrocks::vectorized {

class HdfsScannerCSVReader : public CSVReader {
public:
    // |file| must outlive HdfsScannerCSVReader
    HdfsScannerCSVReader(RandomAccessFile* file, const string& row_delimiter, const string& column_separator,
                         size_t offset, size_t remain_length, size_t file_length)
            : CSVReader(row_delimiter, column_separator) {
        _file = file;
        _offset = offset;
        _remain_length = remain_length;
        _file_length = file_length;
        _row_delimiter_length = row_delimiter.size();
        _column_separator_length = column_separator.size();
    }

    void reset(size_t offset, size_t remain_length);

    Status next_record(Record* record);

protected:
    Status _fill_buffer() override;

private:
    RandomAccessFile* _file;
    size_t _offset = 0;
    int32_t _remain_length = 0;
    size_t _file_length = 0;
    bool _should_stop_scan = false;
};

void HdfsScannerCSVReader::reset(size_t offset, size_t remain_length) {
    _offset = offset;
    _remain_length = remain_length;
    _should_stop_scan = false;
    _buff.skip(_buff.limit() - _buff.position());
}

Status HdfsScannerCSVReader::next_record(Record* record) {
    if (_should_stop_scan) {
        return Status::EndOfFile("Should stop for this reader!");
    }
    return CSVReader::next_record(record);
}

Status HdfsScannerCSVReader::_fill_buffer() {
    if (_should_stop_scan || _offset >= _file_length) {
        return Status::EndOfFile("HdfsScannerCSVReader");
    }

    DCHECK(_buff.free_space() > 0);
    Slice s;
    if (_remain_length <= 0) {
        s = Slice(_buff.limit(), _buff.free_space());
    } else {
        size_t slice_len = _remain_length;
        s = Slice(_buff.limit(), std::min(_buff.free_space(), slice_len));
    }
    ASSIGN_OR_RETURN(s.size, _file->read_at(_offset, s.data, s.size));
    _offset += s.size;
    _remain_length -= s.size;
    _buff.add_limit(s.size);
    auto n = _buff.available();
    if (s.size == 0) {
        if (n == 0) {
            // Has reached the end of file and the buffer is empty.
            _should_stop_scan = true;
            LOG(INFO) << "Reach end of file!";
            return Status::EndOfFile(_file->filename());
        } else if (n < _row_delimiter_length || _buff.find(_row_delimiter, n - _row_delimiter_length) == nullptr) {
            // Has reached the end of file but still no record delimiter found, which
            // is valid, according the RFC, add the record delimiter ourself.
            for (char ch : _row_delimiter) {
                _buff.append(ch);
            }
        }
    }

    // For each scan range we always read the first record of next scan range,so _remain_length
    // may be negative here. Once we have read the first record of next scan range we
    // should stop scan in the next round.
    if ((_remain_length < 0 && _buff.find(_row_delimiter, 0) != nullptr)) {
        _should_stop_scan = true;
    }

    return Status::OK();
}

Status HdfsTextScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    TTextFileDesc text_file_desc = _scanner_params.scan_ranges[0]->text_file_desc;
    _field_delimiter = text_file_desc.field_delim;
    // we should cast string to char now since csv reader only support record delimiter by char
    _record_delimiter = text_file_desc.line_delim.front();
    return Status::OK();
}

Status HdfsTextScanner::do_open(RuntimeState* runtime_state) {
    RETURN_IF_ERROR(_create_or_reinit_reader());
    SCOPED_RAW_TIMER(&_stats.reader_init_ns);
    for (int i = 0; i < _scanner_params.materialize_slots.size(); i++) {
        auto slot = _scanner_params.materialize_slots[i];
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
    CHECK(chunk != nullptr);
    RETURN_IF_ERROR(parse_csv(runtime_state->chunk_size(), chunk));

    ChunkPtr ck = *chunk;
    // do stats before we filter rows which does not match.
    _stats.raw_rows_read += ck->num_rows();
    for (auto& it : _file_read_param.conjunct_ctxs_by_slot) {
        // do evaluation.
        SCOPED_RAW_TIMER(&_stats.expr_filter_ns);
        ExecNode::eval_conjuncts(it.second, ck.get());
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
        } else if (record.empty()) {
            // always skip blank lines.
            continue;
        }

        fields.clear();
        _reader->split_record(record, &fields);

        if (!validate_utf8(record.data, record.size)) {
            continue;
        }

        bool has_error = false;
        int num_materialize_columns = _scanner_params.materialize_slots.size();
        int field_size = fields.size();
        if (_scanner_params.hive_column_names->size() != field_size) {
            VLOG(7) << strings::Substitute("Size mismatch between hive column $0 names and fields $1!",
                                           _scanner_params.hive_column_names->size(), fields.size());
        }
        for (int j = 0; j < num_materialize_columns; j++) {
            int index = _scanner_params.materialize_index_in_chunk[j];
            int column_field_index = _columns_index[_scanner_params.materialize_slots[j]->col_name()];
            Column* column = _column_raw_ptrs[index];
            if (column_field_index < field_size) {
                const Slice& field = fields[column_field_index];
                options.type_desc = &(_scanner_params.materialize_slots[j]->type());
                if (!_converters[j]->read_string(column, field, options)) {
                    LOG(WARNING) << "Converter encountered an error for field " << field.to_string() << ", index "
                                 << index << ", column " << _scanner_params.materialize_slots[j]->debug_string();
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
            int num_part_columns = _file_read_param.partition_columns.size();
            for (int p = 0; p < num_part_columns; ++p) {
                int index = _scanner_params.partition_index_in_chunk[p];
                Column* column = _column_raw_ptrs[index];
                ColumnPtr partition_value = _file_read_param.partition_values[p];
                DCHECK(partition_value->is_constant());
                auto* const_column = vectorized::ColumnHelper::as_raw_column<vectorized::ConstColumn>(partition_value);
                ColumnPtr data_column = const_column->data_column();
                if (data_column->is_nullable()) {
                    column->append_nulls(1);
                } else {
                    column->append(*data_column, 0, 1);
                }
            }
        }
    }
    return chunk->get()->num_rows() > 0 ? Status::OK() : Status::EndOfFile("");
}

Status HdfsTextScanner::_create_or_reinit_reader() {
    const THdfsScanRange* scan_range = _scanner_params.scan_ranges[_current_range_index];
    if (_current_range_index == 0) {
        _reader =
                std::make_unique<HdfsScannerCSVReader>(_file.get(), _record_delimiter, _field_delimiter,
                                                       scan_range->offset, scan_range->length, scan_range->file_length);
    } else {
        down_cast<HdfsScannerCSVReader*>(_reader.get())->reset(scan_range->offset, scan_range->length);
    }
    if (scan_range->offset != 0) {
        // Always skip first record of scan range with non-zero offset.
        // Notice that the first record will read by previous scan range.
        CSVReader::Record dummy;
        RETURN_IF_ERROR(down_cast<HdfsScannerCSVReader*>(_reader.get())->next_record(&dummy));
    }
    return Status::OK();
}

Status HdfsTextScanner::_get_hive_column_index(const std::string& column_name) {
    for (int i = 0; i < _scanner_params.hive_column_names->size(); i++) {
        const std::string& name = _scanner_params.hive_column_names->at(i);
        if (name == column_name) {
            _columns_index[name] = i;
            return Status::OK();
        }
    }
    return Status::InvalidArgument("Can not get index of column name " + column_name);
}

} // namespace starrocks::vectorized
