
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
#include <gen_cpp/DataSinks_types.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include <utility>

#include "column/chunk.h"
#include "column/nullable_column.h"
#include "exec/pipeline/sink/rolling_file_writer.h"
#include "formats/parquet/chunk_writer.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::parquet {

struct FileColumnId {
    int32_t field_id = -1;
    std::vector<FileColumnId> children;
};

class ParquetOutputStream : public arrow::io::OutputStream {
public:
    ParquetOutputStream(std::unique_ptr<starrocks::WritableFile> wfile);

    ~ParquetOutputStream() override;

    arrow::Status Write(const void* data, int64_t nbytes) override;

    arrow::Status Write(const std::shared_ptr<arrow::Buffer>& data) override;

    arrow::Status Close() override;

    arrow::Result<int64_t> Tell() const override;

    bool closed() const override { return _is_closed; };

private:
    std::unique_ptr<starrocks::WritableFile> _wfile;
    bool _is_closed = false;

    enum HEADER_STATE {
        INITED = 1,
        CACHED = 2,
        WRITEN = 3,
    };
    HEADER_STATE _header_state = INITED;
};

struct ParquetBuilderOptions {
    TCompressionType::type compression_type = TCompressionType::SNAPPY;
    bool use_dict = true;
    int64_t row_group_max_size = 128 * 1024 * 1024;
};

class ParquetBuildHelper {
public:
    static arrow::Result<std::shared_ptr<::parquet::schema::GroupNode>> make_schema(
            const std::vector<std::string>& file_column_names, const std::vector<ExprContext*>& output_expr_ctxs,
            const std::vector<FileColumnId>& file_column_ids);

    static arrow::Result<std::shared_ptr<::parquet::schema::GroupNode>> make_schema(
            const std::vector<std::string>& file_column_names, const std::vector<TypeDescriptor>& type_descs,
            const std::vector<FileColumnId>& file_column_ids);

    static StatusOr<std::shared_ptr<::parquet::WriterProperties>> make_properties(const ParquetBuilderOptions& options);

    static StatusOr<::parquet::Compression::type> convert_compression_type(
            const TCompressionType::type& compression_type);

private:
    static arrow::Result<::parquet::schema::NodePtr> _make_schema_node(const std::string& name,
                                                                       const TypeDescriptor& type_desc,
                                                                       ::parquet::Repetition::type rep_type,
                                                                       FileColumnId file_column_ids = FileColumnId());
};

class FileWriterBase {
public:
    FileWriterBase(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<::parquet::WriterProperties> properties,
                   std::shared_ptr<::parquet::schema::GroupNode> schema,
                   const std::vector<ExprContext*>& output_expr_ctxs, int64_t _max_file_size);

    FileWriterBase(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<::parquet::WriterProperties> properties,
                   std::shared_ptr<::parquet::schema::GroupNode> schema, std::vector<TypeDescriptor> type_descs);

    virtual ~FileWriterBase() = default;

    Status init();

    Status write(Chunk* chunk);

    std::size_t file_size() const;

    void set_max_row_group_size(int64_t rg_size) { _max_row_group_size = rg_size; }

    std::shared_ptr<::parquet::FileMetaData> metadata() const { return _file_metadata; }

    Status split_offsets(std::vector<int64_t>& splitOffsets) const;

    virtual bool closed() const = 0;

protected:
    void _generate_chunk_writer();

    virtual Status _flush_row_group() = 0;

private:
    bool is_last_row_group() {
        return _max_file_size - _writer->num_row_groups() * _max_row_group_size < 2 * _max_row_group_size;
    }

protected:
    std::shared_ptr<ParquetOutputStream> _outstream;
    std::shared_ptr<::parquet::WriterProperties> _properties;
    std::shared_ptr<::parquet::schema::GroupNode> _schema;
    std::unique_ptr<::parquet::ParquetFileWriter> _writer;
    std::unique_ptr<ChunkWriter> _chunk_writer;

    std::vector<TypeDescriptor> _type_descs;
    std::function<StatusOr<ColumnPtr>(Chunk*, size_t)> _eval_func;
    std::shared_ptr<::parquet::FileMetaData> _file_metadata;

    const static int64_t kDefaultMaxRowGroupSize = 128 * 1024 * 1024; // 128MB
    int64_t _max_row_group_size = kDefaultMaxRowGroupSize;
    int64_t _max_file_size = 512 * 1024 * 1024; // 512MB
};

class SyncFileWriter : public FileWriterBase {
public:
    SyncFileWriter(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<::parquet::WriterProperties> properties,
                   std::shared_ptr<::parquet::schema::GroupNode> schema,
                   const std::vector<ExprContext*>& output_expr_ctxs, int64_t max_file_size)
            : FileWriterBase(std::move(writable_file), std::move(properties), std::move(schema), output_expr_ctxs,
                             max_file_size) {}

    SyncFileWriter(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<::parquet::WriterProperties> properties,
                   std::shared_ptr<::parquet::schema::GroupNode> schema, std::vector<TypeDescriptor> type_descs)
            : FileWriterBase(std::move(writable_file), std::move(properties), std::move(schema),
                             std::move(type_descs)) {}

    ~SyncFileWriter() override = default;

    Status close();

    bool closed() const override { return _closed; }

private:
    Status _flush_row_group() override;

    bool _closed = false;
};

class AsyncFileWriter : public FileWriterBase {
public:
    AsyncFileWriter(std::unique_ptr<WritableFile> writable_file, std::string file_location,
                    std::string partition_location, std::shared_ptr<::parquet::WriterProperties> properties,
                    std::shared_ptr<::parquet::schema::GroupNode> schema,
                    const std::vector<ExprContext*>& output_expr_ctxs, PriorityThreadPool* executor_pool,
                    RuntimeProfile* parent_profile, int64_t max_file_size, RuntimeState* state);

    ~AsyncFileWriter() override = default;

    Status close(RuntimeState* state,
                 const std::function<void(starrocks::parquet::AsyncFileWriter*, RuntimeState*)>& cb = nullptr);

    bool writable() {
        auto lock = std::unique_lock(_m);
        return !_rg_writer_closing;
    }

    bool closed() const override { return _closed.load(); }

    std::string file_location() const { return _file_location; }

    std::string partition_location() const { return _partition_location; }

    void set_io_status(const Status& status) {
        std::unique_lock l(_io_status_mutex);
        if (_io_status.ok()) {
            _io_status = status;
        }
    }

    Status get_io_status() const {
        std::shared_lock l(_io_status_mutex);
        return _io_status;
    }

private:
    Status _flush_row_group() override;

    std::string _file_location;
    std::string _partition_location;
    std::atomic<bool> _closed = false;

    mutable std::shared_mutex _io_status_mutex;
    Status _io_status;

    PriorityThreadPool* _executor_pool;

    RuntimeProfile* _parent_profile = nullptr;
    RuntimeProfile::Counter* _io_timer = nullptr;

    RuntimeState* _state;

    std::condition_variable _cv;
    bool _rg_writer_closing = false;
    std::mutex _m;
};

template <typename T>
std::future<T> make_completed_future(T&& t) {
    std::promise<T> p;
    p.set_value(std::forward<T>(t));
    return p.get_future();
}

class ParquetFileWriter final : public pipeline::FileWriter {
public:
    ParquetFileWriter(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<::parquet::WriterProperties> properties,
                   std::shared_ptr<::parquet::schema::GroupNode> schema,
                   const std::vector<ExprContext*>& output_expr_ctxs, int64_t _max_file_size);

    ~ParquetFileWriter() override;

    std::future<Status> write(ChunkPtr chunk) override;

    void commitAsync(std::function<void(CommitResult)> callback) override;

    void rollback() override;

    Status init() override;

    int64_t getWrittenBytes() override;

private:
    std::future<Status> _flush_row_group();

    void _set_metrics(::parquet::FileMetaData* meta_data);

private:
    std::shared_ptr<::parquet::WriterProperties> _properties;
    std::shared_ptr<::parquet::schema::GroupNode> _schema;

    std::vector<TypeDescriptor> _type_descs;
    std::function<StatusOr<ColumnPtr>(Chunk*, size_t)> _eval_func;
    std::shared_ptr<::parquet::FileMetaData> _file_metadata;

    std::shared_ptr<::parquet::ParquetFileWriter> _writer;
    std::shared_ptr<ChunkWriter> _rowgroup_writer;
    std::shared_ptr<ParquetOutputStream> _output_stream;
    std::optional<FileMetrics> _metrics; // set metrics after commit
    PriorityThreadPool* _executors;

    inline static std::future<Status> NON_BLOCKED_OK = make_completed_future(Status::OK());
};


} // namespace starrocks::parquet
