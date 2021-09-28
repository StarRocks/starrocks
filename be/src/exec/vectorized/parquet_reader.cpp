// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
#include "exec/vectorized/parquet_reader.h"

#include <arrow/array.h>
#include <arrow/status.h>
#include <column/array_column.h>
#include <column/column_helper.h>
#include <gutil/strings/substitute.h>

#include <utility>

#include "column/chunk.h"
#include "common/logging.h"
#include "exec/file_reader.h"
#include "exec/vectorized/arrow_to_starrocks_converter.h"
#include "runtime/client_cache.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"

namespace starrocks::vectorized {

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

static inline ArrowStatusCode convert_status_code(StarRocksStatusCode code) {
    switch (code) {
    case StarRocksStatusCode::OK:
        return ArrowStatusCode::OK;
    case StarRocksStatusCode::NOT_FOUND:
    case StarRocksStatusCode::END_OF_FILE:
        return ArrowStatusCode::IOError;
    case StarRocksStatusCode::NOT_IMPLEMENTED_ERROR:
        return ArrowStatusCode::NotImplemented;
    case StarRocksStatusCode::MEM_ALLOC_FAILED:
    case StarRocksStatusCode::BUFFER_ALLOCATION_FAILED:
    case StarRocksStatusCode::MEM_LIMIT_EXCEEDED:
        return ArrowStatusCode::OutOfMemory;
    default:
        return ArrowStatusCode::ExecutionError;
    }
}

static inline ArrowStatus convert_status(const StarRocksStatus& status) {
    if (LIKELY(status.ok())) {
        return ArrowStatus::OK();
    } else {
        return ArrowStatus(convert_status_code(status.code()), status.get_error_msg());
    }
}

ParquetChunkFile::ParquetChunkFile(std::shared_ptr<starrocks::RandomAccessFile> file, uint64_t pos)
        : _file(std::move(file)), _pos(pos) {}

ParquetChunkFile::~ParquetChunkFile() {
    Close();
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
    Slice s;
    s.data = (char*)out;
    s.size = nbytes;
    auto status = _file->read_at(position, s);
    return status.ok() ? nbytes : -1;
}

arrow::Result<int64_t> ParquetChunkFile::GetSize() {
    int64_t size = 0;
    _file->size((uint64_t*)&size);
    return size;
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

} // namespace starrocks::vectorized
