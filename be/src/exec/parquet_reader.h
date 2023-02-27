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

#pragma once

#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include <cstdint>
#include <map>
#include <string>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "fs/fs.h"
#include "runtime/types.h"

namespace starrocks {

using RecordBatch = ::arrow::RecordBatch;
using RecordBatchPtr = std::shared_ptr<RecordBatch>;
class ParquetChunkFile : public arrow::io::RandomAccessFile {
public:
    ParquetChunkFile(std::shared_ptr<starrocks::RandomAccessFile> file, uint64_t pos);
    ~ParquetChunkFile() override;
    arrow::Result<int64_t> Read(int64_t nbytes, void* buffer) override;
    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;
    arrow::Result<int64_t> GetSize() override;
    arrow::Status Seek(int64_t position) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;
    arrow::Result<int64_t> Tell() const override;
    arrow::Status Close() override;
    bool closed() const override;
    const std::string& filename() const;

private:
    std::shared_ptr<starrocks::RandomAccessFile> _file;
    uint64_t _pos = 0;
};

class ParquetReaderWrap {
public:
    ParquetReaderWrap(std::shared_ptr<arrow::io::RandomAccessFile>&& parquet_file, int32_t num_of_columns_from_file,
                      int64_t read_offset, int64_t read_size);
    virtual ~ParquetReaderWrap();

    void close();
    Status size(int64_t* size);
    Status init_parquet_reader(const std::vector<SlotDescriptor*>& tuple_slot_descs, const std::string& timezone);
    Status read_record_batch(const std::vector<SlotDescriptor*>& tuple_slot_descs, bool* eof);
    const std::shared_ptr<arrow::RecordBatch>& get_batch();

private:
    Status column_indices(const std::vector<SlotDescriptor*>& tuple_slot_descs);
    Status handle_timestamp(const std::shared_ptr<arrow::TimestampArray>& ts_array, uint8_t* buf, int32_t* wbtyes);
    Status next_selected_row_group();

    const int32_t _num_of_columns_from_file;
    parquet::ReaderProperties _properties;
    std::shared_ptr<arrow::io::RandomAccessFile> _parquet;

    // parquet file reader object
    std::shared_ptr<::arrow::RecordBatchReader> _rb_batch;
    std::shared_ptr<arrow::RecordBatch> _batch;
    std::unique_ptr<parquet::arrow::FileReader> _reader;
    std::shared_ptr<parquet::FileMetaData> _file_metadata;

    // For nested column type, it's consisting of multiple physical-columns
    std::map<std::string, std::vector<int>> _map_column_nested;
    std::vector<int> _parquet_column_ids;
    int _total_groups; // groups in a parquet file
    int _current_group;

    int _rows_of_group; // rows in a group.
    int _current_line_of_group;
    int _current_line_of_batch;

    int64_t _read_offset;
    int64_t _read_size;

    std::string _timezone;
    std::string _filename;
};

// Reader of broker parquet file
class ParquetChunkReader {
public:
    enum State { UNINITIALIZED, INITIALIZED, END_OF_FILE };

    ParquetChunkReader(std::shared_ptr<ParquetReaderWrap>&& parquet_reader,
                       const std::vector<SlotDescriptor*>& src_slot_descs, std::string time_zone);
    ~ParquetChunkReader();
    Status next_batch(RecordBatchPtr* batch);

private:
    std::shared_ptr<ParquetReaderWrap> _parquet_reader;
    const std::vector<SlotDescriptor*>& _src_slot_descs;
    std::string _time_zone;
    State _state;
};

} // namespace starrocks
