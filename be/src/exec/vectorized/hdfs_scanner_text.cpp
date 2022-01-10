// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/hdfs_scanner_text.h"

#include "exec/vectorized/hdfs_scan_node.h"
#include "gen_cpp/Descriptors_types.h"
#include "gutil/strings/substitute.h"
#include "util/utf8_check.h"

namespace starrocks::vectorized {

Status HdfsTextScanner::HdfsScannerCSVReader::_fill_buffer() {
    DCHECK(_buff.free_space() > 0);
    Slice s(_buff.limit(), _buff.free_space());
    Status st = _file->read(_offset, &s);
    _offset += s.size;
    // According to the specification of `Env::read`, when reached the end of
    // a file, the returned status will be OK instead of EOF, but here we check
    // EOF also for safety.
    if (st.is_end_of_file()) {
        s.size = 0;
    } else if (!st.ok()) {
        return st;
    }
    _buff.add_limit(s.size);
    auto n = _buff.available();
    if (s.size == 0 && n == 0) {
        // Has reached the end of file and the buffer is empty.
        return Status::EndOfFile(_file->file_name());
    } else if (s.size == 0 && _buff.position()[n - 1] != _record_delimiter) {
        // Has reached the end of file but still no record delimiter found, which
        // is valid, according the RFC, add the record delimiter ourself.
        _buff.append(_record_delimiter);
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
    _reader = std::make_unique<HdfsScannerCSVReader>(_scanner_params.fs, _record_delimiter, _field_delimiter,
                                                     _scanner_params.scan_ranges[0]->offset);
    for (int i = 0; i < _scanner_params.materialize_slots.size(); i++) {
        auto slot = _scanner_params.materialize_slots[i];
        ConverterPtr conv = csv::get_converter(slot->type(), true);
        if (conv == nullptr) {
            auto msg = strings::Substitute("Unsupported CSV type $0", slot->type().debug_string());
            return Status::InternalError(msg);
        }
        _converters.emplace_back(std::move(conv));
    }
    return Status::OK();
}

void HdfsTextScanner::do_close(RuntimeState* runtime_state) noexcept {
    update_counter();
    _reader.reset();
}

Status HdfsTextScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    return _parse_csv(runtime_state->chunk_size(), chunk);
}

Status HdfsTextScanner::_parse_csv(int chunk_size, ChunkPtr* chunk) {
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
        status = _reader->next_record(&record);
        if (status.is_end_of_file()) {
            break;
        } else if (!status.ok()) {
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
        for (int j = 0, k = 0; j < _scanner_params.materialize_slots.size(); j++) {
            const Slice& field = fields[_scanner_params.materialize_slots[j]->id() - 1];
            options.type_desc = &(_scanner_params.materialize_slots[j]->type());
            if (!_converters[k]->read_string(_column_raw_ptrs[k], field, options)) {
                chunk->get()->set_num_rows(num_rows);
                has_error = true;
                break;
            }
            k++;
        }
        num_rows += !has_error;
    }
    return chunk->get()->num_rows() > 0 ? Status::OK() : Status::EndOfFile("");
}

} // namespace starrocks::vectorized
