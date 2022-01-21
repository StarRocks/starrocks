// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/parquet_reader.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#include "exec/parquet_reader.h"

#include <arrow/array.h>
#include <arrow/status.h>

#include <ctime>

#include "common/logging.h"
#include "exec/file_reader.h"
#include "gen_cpp/FileBrokerService_types.h"
#include "gen_cpp/TFileBrokerService.h"
#include "runtime/broker_mgr.h"
#include "runtime/client_cache.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "util/thrift_util.h"

namespace starrocks {

// Broker

ParquetReaderWrap::ParquetReaderWrap(FileReader* file_reader, int32_t num_of_columns_from_file)
        : ParquetReaderWrap(std::shared_ptr<arrow::io::RandomAccessFile>(new ParquetFile(file_reader)),
                            num_of_columns_from_file) {}

ParquetReaderWrap::ParquetReaderWrap(std::shared_ptr<arrow::io::RandomAccessFile>&& parquet_file,
                                     int32_t num_of_columns_from_file)
        : _num_of_columns_from_file(num_of_columns_from_file),
          _total_groups(0),
          _current_group(0),
          _rows_of_group(0),
          _current_line_of_group(0),
          _current_line_of_batch(0) {
    _parquet = std::move(parquet_file);
    _properties = parquet::ReaderProperties();
    _properties.enable_buffered_stream();
    _properties.set_buffer_size(8 * 1024 * 1024);
}

ParquetReaderWrap::~ParquetReaderWrap() {
    close();
}
Status ParquetReaderWrap::init_parquet_reader(const std::vector<SlotDescriptor*>& tuple_slot_descs,
                                              const std::string& timezone) {
    try {
        // new file reader for parquet file
        auto st = parquet::arrow::FileReader::Make(arrow::default_memory_pool(),
                                                   parquet::ParquetFileReader::Open(_parquet, _properties), &_reader);
        if (!st.ok()) {
            LOG(WARNING) << "failed to create parquet file reader, errmsg=" << st.ToString();
            return Status::InternalError("Failed to create file reader");
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
        _rows_of_group = _file_metadata->RowGroup(0)->num_rows();

        // map
        auto* schemaDescriptor = _file_metadata->schema();
        for (int i = 0; i < _file_metadata->num_columns(); ++i) {
            // Get the Column Reader for the boolean column
            _map_column.emplace(schemaDescriptor->Column(i)->name(), i);
        }

        _timezone = timezone;

        if (_current_line_of_group == 0) { // the first read
            RETURN_IF_ERROR(column_indices(tuple_slot_descs));
            // read batch
            arrow::Status status = _reader->GetRecordBatchReader({_current_group}, _parquet_column_ids, &_rb_batch);
            if (!status.ok()) {
                LOG(WARNING) << "Get RecordBatch Failed. " << status.ToString();
                return Status::InternalError(status.ToString());
            }
            if (!_rb_batch) {
                LOG(INFO) << "Ignore the parquet file because of an unexpected nullptr "
                             "RecordBatchReader";
                return Status::EndOfFile("Unexpected nullptr RecordBatchReader");
            }
            status = _rb_batch->ReadNext(&_batch);
            if (!status.ok()) {
                LOG(WARNING) << "The first read record. " << status.ToString();
                return Status::InternalError(status.ToString());
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
            for (int i = 0; i < _parquet_column_ids.size(); i++) {
                std::shared_ptr<arrow::Field> field = field_schema->field(i);
                if (!field) {
                    LOG(WARNING) << "Get field schema failed. Column order:" << i;
                    return Status::InternalError(status.ToString());
                }
                _parquet_column_type.emplace_back(field->type());
            }
        }
        return Status::OK();
    } catch (parquet::ParquetException& e) {
        std::stringstream str_error;
        str_error << "Init parquet reader fail. " << e.what();
        LOG(WARNING) << str_error.str();
        return Status::InternalError(str_error.str());
    }
}

void ParquetReaderWrap::close() {
    _parquet->Close();
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
        // Get the Column Reader for the boolean column
        auto iter = _map_column.find(slot_desc->col_name());
        if (iter != _map_column.end()) {
            _parquet_column_ids.emplace_back(iter->second);
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
        if (_current_group >= _total_groups) { // read completed.
            _parquet_column_ids.clear();
            *eof = true;
            return Status::OK();
        }
        _current_line_of_group = 0;
        _rows_of_group = _file_metadata->RowGroup(_current_group)->num_rows(); //get rows of the current row group
        // read batch
        arrow::Status status = _reader->GetRecordBatchReader({_current_group}, _parquet_column_ids, &_rb_batch);
        if (!status.ok()) {
            return Status::InternalError("Get RecordBatchReader Failed.");
        }
        status = _rb_batch->ReadNext(&_batch);
        if (!status.ok()) {
            return Status::InternalError("Read Batch Error With Libarrow.");
        }
        _current_line_of_batch = 0;
    } else if (_current_line_of_batch >= _batch->num_rows()) {
        VLOG(7) << "read_record_batch, current group id:" << _current_group
                << " current line of batch:" << _current_line_of_batch
                << " is larger than batch size:" << _batch->num_rows() << ". start to read next batch";
        arrow::Status status = _rb_batch->ReadNext(&_batch);
        if (!status.ok()) {
            return Status::InternalError("Read Batch Error With Libarrow.");
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

const std::vector<std::shared_ptr<arrow::DataType>>& ParquetReaderWrap::get_column_types() {
    return _parquet_column_type;
}

Status ParquetReaderWrap::handle_timestamp(const std::shared_ptr<arrow::TimestampArray>& ts_array, uint8_t* buf,
                                           int32_t* wbytes) {
    const auto type = std::dynamic_pointer_cast<arrow::TimestampType>(ts_array->type());
    // StarRocks only supports seconds
    int64_t timestamp = 0;
    switch (type->unit()) {
    case arrow::TimeUnit::type::NANO: {                                    // INT96
        timestamp = ts_array->Value(_current_line_of_batch) / 1000000000L; // convert to Second
        break;
    }
    case arrow::TimeUnit::type::SECOND: {
        timestamp = ts_array->Value(_current_line_of_batch);
        break;
    }
    case arrow::TimeUnit::type::MILLI: {
        timestamp = ts_array->Value(_current_line_of_batch) / 1000; // convert to Second
        break;
    }
    case arrow::TimeUnit::type::MICRO: {
        timestamp = ts_array->Value(_current_line_of_batch) / 1000000; // convert to Second
        break;
    }
    default:
        return Status::InternalError("Invalid Time Type.");
    }

    DateTimeValue dtv;
    if (!dtv.from_unixtime(timestamp, _timezone)) {
        std::stringstream str_error;
        str_error << "Parse timestamp (" + std::to_string(timestamp) + ") error";
        LOG(WARNING) << str_error.str();
        return Status::InternalError(str_error.str());
    }
    char* buf_end = (char*)buf;
    buf_end = dtv.to_string((char*)buf_end);
    *wbytes = buf_end - (char*)buf - 1;
    return Status::OK();
}

ParquetFile::ParquetFile(FileReader* file) : _file(file) {}

ParquetFile::~ParquetFile() {
    Close();
}

arrow::Status ParquetFile::Close() {
    if (_file != nullptr) {
        _file->close();
        delete _file;
        _file = nullptr;
    }
    return arrow::Status::OK();
}

bool ParquetFile::closed() const {
    if (_file != nullptr) {
        return _file->closed();
    } else {
        return true;
    }
}

arrow::Result<int64_t> ParquetFile::Read(int64_t nbytes, void* buffer) {
    return ReadAt(_pos, nbytes, buffer);
}

arrow::Result<int64_t> ParquetFile::ReadAt(int64_t position, int64_t nbytes, void* out) {
    int64_t reads = 0;
    int64_t bytes_read = 0;
    _pos = position;
    while (nbytes > 0) {
        Status result = _file->readat(_pos, nbytes, &reads, out);
        if (!result.ok()) {
            bytes_read = 0;
            return arrow::Status::IOError("Readat failed.");
        }
        if (reads == 0) {
            break;
        }
        bytes_read += reads; // total read bytes
        nbytes -= reads;     // remained bytes
        _pos += reads;
        out = (char*)out + reads;
    }
    return bytes_read;
}

arrow::Result<int64_t> ParquetFile::GetSize() {
    int64_t size = _file->size();
    return size;
}

arrow::Status ParquetFile::Seek(int64_t position) {
    _pos = position;
    // NOTE: Only readat operation is used, so _file seek is not called here.
    return arrow::Status::OK();
}

arrow::Result<int64_t> ParquetFile::Tell() const {
    return _pos;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ParquetFile::Read(int64_t nbytes) {
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

} // namespace starrocks
