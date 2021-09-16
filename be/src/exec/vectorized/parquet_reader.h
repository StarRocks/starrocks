// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <column/vectorized_fwd.h>
#include <env/env.h>
#include <exec/file_reader.h>
#include <exec/parquet_reader.h>
#include <exprs/expr.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <runtime/types.h>
#include <stdint.h>

#include <map>
#include <string>

#include "common/status.h"

namespace starrocks::vectorized {

using RecordBatch = ::arrow::RecordBatch;
using RecordBatchPtr = std::shared_ptr<RecordBatch>;
class ParquetChunkFile : public arrow::io::RandomAccessFile {
public:
    ParquetChunkFile(std::shared_ptr<starrocks::RandomAccessFile> file, uint64_t pos);
    virtual ~ParquetChunkFile();
    arrow::Result<int64_t> Read(int64_t nbytes, void* buffer) override;
    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;
    arrow::Result<int64_t> GetSize() override;
    arrow::Status Seek(int64_t position) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;
    arrow::Result<int64_t> Tell() const override;
    arrow::Status Close() override;
    bool closed() const override;

private:
    std::shared_ptr<starrocks::RandomAccessFile> _file;
    uint64_t _pos = 0;
};

// Reader of broker parquet file
class ParquetChunkReader {
public:
    enum State { UNINITIALIZED, INITIALIZED, END_OF_FILE };

    ParquetChunkReader(std::shared_ptr<ParquetReaderWrap>&& parquet_reader,
                       const std::vector<SlotDescriptor*>& src_slot_descs, const std::string& time_zone);
    ~ParquetChunkReader();
    Status next_batch(RecordBatchPtr* batch);

private:
    std::shared_ptr<ParquetReaderWrap> _parquet_reader;
    const std::vector<SlotDescriptor*>& _src_slot_descs;
    std::string _time_zone;
    State _state;
};

} // namespace starrocks::vectorized
