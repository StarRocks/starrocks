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

#include "exec/parquet_reader.h"

#include <arrow/array.h>
#include <arrow/status.h>
#include <gutil/strings/substitute.h>

#include <utility>

#include "common/logging.h"
#include "fmt/format.h"
#include "runtime/descriptors.h"

namespace starrocks {
// ====================================================================================================================

ParquetReaderWrap::~ParquetReaderWrap() {
    close();
}

ParquetReaderWrap::ParquetReaderWrap(std::shared_ptr<arrow::io::RandomAccessFile>&& parquet_file,
                                     int32_t num_of_columns_from_file, int64_t read_offset, int64_t read_size)

        : _num_of_columns_from_file(num_of_columns_from_file),
          _total_groups(0),
          _current_group(0),
          _rows_of_group(0),
          _current_line_of_group(0),
          _current_line_of_batch(0),
          _read_offset(read_offset),
          _read_size(read_size) {
    _parquet = std::move(parquet_file);
    _properties = parquet::ReaderProperties();
    _properties.enable_buffered_stream();
    _properties.set_buffer_size(8 * 1024 * 1024);
    _filename = (reinterpret_cast<ParquetChunkFile*>(_parquet.get()))->filename();
}

Status ParquetReaderWrap::next_selected_row_group() {
    while (_current_group < _total_groups) {
        int64_t row_group_start = _file_metadata->RowGroup(_current_group)->file_offset();

        if (_read_size == 0) {
            return Status::EndOfFile("End of row group");
        }

        int64_t scan_start = _read_offset;
        int64_t scan_end = _read_size + scan_start;

        if (row_group_start >= scan_start && row_group_start < scan_end) {
            return Status::OK();
        }

        _current_group++;
    }

    return Status::EndOfFile("End of row group");
}

Status ParquetReaderWrap::init_parquet_reader(const std::vector<SlotDescriptor*>& tuple_slot_descs,
                                              const std::string& timezone) {
    try {
        // new file reader for parquet file
        auto st = parquet::arrow::FileReader::Make(arrow::default_memory_pool(),
                                                   parquet::ParquetFileReader::Open(_parquet, _properties), &_reader);
        if (!st.ok()) {
            LOG(WARNING) << "Failed to create parquet file reader. error: " << st.ToString()
                         << ", filename: " << _filename;
            return Status::InternalError(fmt::format("Failed to create file reader. filename: {}", _filename));
        }

        if (!_reader || !_reader->parquet_reader()) {
            LOG(INFO) << "Ignore the parquet file because of unexpected nullptr ParquetReader";
            return Status::EndOfFile("Unexpected nullptr ParquetReader");
        }
        _file_metadata = _reader->parquet_reader()->metadata();
        if (!_file_metadata) {
            LOG(INFO) << "Ignore the parquet file because of unexpected nullptr FileMetaData";
            return Status::EndOfFile("Unexpected nullptr FileMetaData");
        }
        // initial members
        _total_groups = _file_metadata->num_row_groups();
        if (_total_groups == 0) {
            return Status::EndOfFile("Empty Parquet File");
        }
        RETURN_IF_ERROR(next_selected_row_group());

        _rows_of_group = _file_metadata->RowGroup(_current_group)->num_rows();

        {
            // Initialize _map_column, map column name to it's index
            // For nested type, it has multiple column, we need to map field_name to multiple indices
            auto parquet_schema = _file_metadata->schema();
            for (int i = 0; i < parquet_schema->num_columns(); i++) {
                auto column_desc = parquet_schema->Column(i);
                std::string field_name = column_desc->path()->ToDotVector()[0];
                _map_column_nested[field_name].push_back(i);
            }
        }

        _timezone = timezone;

        if (_current_line_of_group == 0) { // the first read
            RETURN_IF_ERROR(column_indices(tuple_slot_descs));
            arrow::Status status = _reader->GetRecordBatchReader({_current_group}, _parquet_column_ids, &_rb_batch);

            if (!status.ok()) {
                LOG(WARNING) << "Get RecordBatch Failed. error: " << status.ToString() << ", filename: " << _filename;
                return Status::InternalError(fmt::format("{}. filename: {}", status.ToString(), _filename));
            }
            if (!_rb_batch) {
                LOG(INFO) << "Ignore the parquet file because of an unexpected nullptr "
                             "RecordBatchReader";
                return Status::EndOfFile("Unexpected nullptr RecordBatchReader");
            }
            status = _rb_batch->ReadNext(&_batch);
            if (!status.ok()) {
                LOG(WARNING) << "The first read record. error: " << status.ToString() << ", filename: " << _filename;
                return Status::InternalError(fmt::format("{}. filename: {}", status.ToString(), _filename));
            }
            if (!_batch) {
                LOG(INFO) << "Ignore the parquet file because of an expected nullptr RecordBatch";
                return Status::EndOfFile("Unexpected nullptr RecordBatch");
            }
            _current_line_of_batch = 0;
            //save column type
            std::shared_ptr<arrow::Schema> field_schema = _batch->schema();
            if (!field_schema) {
                LOG(INFO) << "Ignore the parquet file because of an expected nullptr Schema";
                return Status::EndOfFile("Unexpected nullptr RecordBatch");
            }
        }
        return Status::OK();
    } catch (parquet::ParquetException& e) {
        std::stringstream str_error;
        str_error << "Init parquet reader fail. " << e.what();
        LOG(WARNING) << str_error.str() << " filename: " << _filename;
        return Status::InternalError(fmt::format("{}. filename: {}", str_error.str(), _filename));
    }
}

void ParquetReaderWrap::close() {
    [[maybe_unused]] auto st = _parquet->Close();
}

Status ParquetReaderWrap::size(int64_t* size) {
    auto size_res = _parquet->GetSize();
    if (!size_res.ok()) {
        return Status::InternalError(size_res.status().ToString());
    }
    *size = size_res.ValueOrDie();
    return Status::OK();
}

Status ParquetReaderWrap::column_indices(const std::vector<SlotDescriptor*>& tuple_slot_descs) {
    _parquet_column_ids.clear();
    for (int i = 0; i < _num_of_columns_from_file; i++) {
        auto* slot_desc = tuple_slot_descs.at(i);
        if (slot_desc == nullptr) {
            continue;
        }
        std::string col_name = slot_desc->col_name();

        auto iter = _map_column_nested.find(col_name);
        if (iter != _map_column_nested.end()) {
            for (auto index : iter->second) {
                _parquet_column_ids.emplace_back(index);
            }
        } else {
            std::stringstream str_error;
            str_error << "Invalid Column Name:" << slot_desc->col_name();
            LOG(WARNING) << str_error.str();
            return Status::InvalidArgument(str_error.str());
        }
    }
    return Status::OK();
}

Status ParquetReaderWrap::read_record_batch(const std::vector<SlotDescriptor*>& tuple_slot_descs, bool* eof) {
    if (_current_line_of_group >= _rows_of_group) { // read next row group
        VLOG(7) << "read_record_batch, current group id:" << _current_group
                << " current line of group:" << _current_line_of_group
                << " is larger than rows group size:" << _rows_of_group << ". start to read next row group";
        _current_group++;
        auto st = next_selected_row_group();
        if (!st.ok()) { // read completed.
            _parquet_column_ids.clear();
            *eof = true;
            return Status::OK();
        }
        _current_line_of_group = 0;
        _rows_of_group = _file_metadata->RowGroup(_current_group)->num_rows(); //get rows of the current row group
        // read batch
        arrow::Status status = _reader->GetRecordBatchReader({_current_group}, _parquet_column_ids, &_rb_batch);
        if (!status.ok()) {
            return Status::InternalError(fmt::format("Get RecordBatchReader Failed. filename: {}", _filename));
        }
        status = _rb_batch->ReadNext(&_batch);
        if (!status.ok()) {
            return Status::InternalError(fmt::format("Read Batch Error With Libarrow. filename: {}", _filename));
        }

        // arrow::RecordBatchReader::ReadNext returns null at end of stream.
        // Since we count the batches read, EOF implies reader source failure.
        if (_batch == nullptr) {
            LOG(WARNING) << "Unexpected EOF. Row groups less than expected. expected: " << _total_groups
                         << " got: " << _current_group << ", filename" << _filename;
            return Status::InternalError(fmt::format("Unexpected EOF. filename: {}", _filename));
        }

        _current_line_of_batch = 0;
    } else if (_current_line_of_batch >= _batch->num_rows()) {
        VLOG(7) << "read_record_batch, current group id:" << _current_group
                << " current line of batch:" << _current_line_of_batch
                << " is larger than batch size:" << _batch->num_rows() << ". start to read next batch";
        arrow::Status status = _rb_batch->ReadNext(&_batch);
        if (!status.ok()) {
            return Status::InternalError(fmt::format("Read Batch Error With Libarrow. filename: {}", _filename));
        }

        // arrow::RecordBatchReader::ReadNext returns null at end of stream.
        // Since we count the batches read, EOF implies reader source failure.
        if (_batch == nullptr) {
            LOG(WARNING) << "Unexpected EOF. Row groups less than expected. expected: " << _total_groups
                         << " got: " << _current_group << ", filename: " << _filename;
            return Status::InternalError(fmt::format("Unexpected EOF. filename: {}", _filename));
        }

        _current_line_of_batch = 0;
    }
    return Status::OK();
}

const std::shared_ptr<arrow::RecordBatch>& ParquetReaderWrap::get_batch() {
    _current_line_of_batch += _batch->num_rows();
    _current_line_of_group += _batch->num_rows();
    return _batch;
}

// ====================================================================================================================
ParquetChunkReader::ParquetChunkReader(std::shared_ptr<ParquetReaderWrap>&& parquet_reader,
                                       const std::vector<SlotDescriptor*>& src_slot_desc, std::string time_zone)
        : _parquet_reader(std::move(parquet_reader)),
          _src_slot_descs(src_slot_desc),
          _time_zone(std::move(time_zone)),
          _state(State::UNINITIALIZED) {}

ParquetChunkReader::~ParquetChunkReader() {
    _parquet_reader->close();
}

Status ParquetChunkReader::next_batch(RecordBatchPtr* batch) {
    switch (_state) {
    case State::UNINITIALIZED: {
        RETURN_IF_ERROR(_parquet_reader->init_parquet_reader(_src_slot_descs, _time_zone));
        _state = INITIALIZED;
        break;
    }
    case State::INITIALIZED: {
        bool eof = false;
        auto status = _parquet_reader->read_record_batch(_src_slot_descs, &eof);
        if (status.is_end_of_file() || eof) {
            *batch = nullptr;
            _state = END_OF_FILE;
            return Status::EndOfFile(Slice());
        } else if (!status.ok()) {
            return status;
        }
        break;
    }
    case State::END_OF_FILE: {
        *batch = nullptr;
        return Status::EndOfFile(Slice());
    }
    }
    *batch = _parquet_reader->get_batch();
    return Status::OK();
}

using StarRocksStatusCode = ::starrocks::TStatusCode::type;
using ArrowStatusCode = ::arrow::StatusCode;
using StarRocksStatus = ::starrocks::Status;
using ArrowStatus = ::arrow::Status;

ParquetChunkFile::ParquetChunkFile(std::shared_ptr<starrocks::RandomAccessFile> file, uint64_t pos)
        : _file(std::move(file)), _pos(pos) {}

ParquetChunkFile::~ParquetChunkFile() {
    [[maybe_unused]] auto st = Close();
}

arrow::Status ParquetChunkFile::Close() {
    _file.reset();
    return ArrowStatus::OK();
}

bool ParquetChunkFile::closed() const {
    return false;
}

arrow::Result<int64_t> ParquetChunkFile::Read(int64_t nbytes, void* buffer) {
    return ReadAt(_pos, nbytes, buffer);
}

arrow::Result<int64_t> ParquetChunkFile::ReadAt(int64_t position, int64_t nbytes, void* out) {
    _pos += nbytes;
    auto status = _file->read_at_fully(position, out, nbytes);
    return status.ok() ? nbytes
                       : arrow::Result<int64_t>(arrow::Status(arrow::StatusCode::IOError, status.get_error_msg()));
}

arrow::Result<int64_t> ParquetChunkFile::GetSize() {
    const StatusOr<uint64_t> status_or = _file->get_size();
    return status_or.ok() ? status_or.value()
                          : arrow::Result<int64_t>(
                                    arrow::Status(arrow::StatusCode::IOError, status_or.status().get_error_msg()));
}

arrow::Status ParquetChunkFile::Seek(int64_t position) {
    _pos = position;
    return ArrowStatus::OK();
}

arrow::Result<int64_t> ParquetChunkFile::Tell() const {
    return _pos;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ParquetChunkFile::Read(int64_t nbytes) {
    auto buffer_res = arrow::AllocateBuffer(nbytes, arrow::default_memory_pool());
    ARROW_RETURN_NOT_OK(buffer_res);
    std::shared_ptr<arrow::Buffer> read_buf = std::move(buffer_res.ValueOrDie());
    arrow::Result<int64_t> bytes_read_res = ReadAt(_pos, nbytes, read_buf->mutable_data());
    ARROW_RETURN_NOT_OK(bytes_read_res);
    // If bytes_read is equal with read_buf's capacity, we just assign
    if (bytes_read_res.ValueOrDie() == nbytes) {
        return std::move(read_buf);
    } else {
        return arrow::SliceBuffer(read_buf, 0, bytes_read_res.ValueOrDie());
    }
}

const std::string& ParquetChunkFile::filename() const {
    return _file->filename();
}

} // namespace starrocks
