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

#include <formats/parquet/file_writer.h>

#include <functional>

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/sink/rolling_file_writer.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "formats/orc/utils.h"
#include "fs/fs.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks {

class OrcColumnWriter;

// OrcChunkWriter is a bridge between apache/orc file and chunk, wraps orc::writer
// Write chunks into buffer. Flush on closing.
class OrcOutputStream : public orc::OutputStream {
public:
    OrcOutputStream(std::unique_ptr<starrocks::WritableFile> wfile);

    ~OrcOutputStream() override;

    uint64_t getLength() const override;

    uint64_t getNaturalWriteSize() const override;

    void write(const void* buf, size_t length) override;

    void close() override;

    const std::string& getName() const override;

private:
    std::unique_ptr<starrocks::WritableFile> _wfile;
    bool _is_closed = false;
};

class OrcChunkWriter {
public:
    OrcChunkWriter(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<orc::WriterOptions> writer_options,
                   std::shared_ptr<orc::Type> schema, const std::vector<ExprContext*>& output_expr_ctxs);

    // For UT
    OrcChunkWriter(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<orc::WriterOptions> writer_options,
                   std::vector<TypeDescriptor>& type_descs, std::unique_ptr<orc::Type> schema)
            : _type_descs(type_descs),
              _writer_options(writer_options),
              _schema(std::move(schema)),
              _output_stream(std::move(writable_file)){};

    virtual ~OrcChunkWriter() = default;

    Status set_compression(const TCompressionType::type& compression_type);

    Status write(Chunk* chunk);

    void close();

    size_t file_size() { return _output_stream.getLength(); };

    static StatusOr<std::unique_ptr<orc::Type>> make_schema(const std::vector<std::string>& file_column_names,
                                                            const std::vector<TypeDescriptor>& type_descs);

private:
    virtual Status _flush_batch();

    static StatusOr<std::unique_ptr<orc::Type>> _get_orc_type(const TypeDescriptor& type_desc);

    Status _write_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, const TypeDescriptor& type_desc);

    template <LogicalType Type, typename VectorBatchType>
    void _write_number(orc::ColumnVectorBatch& orc_column, ColumnPtr& column);

    void _write_string(orc::ColumnVectorBatch& orc_column, ColumnPtr& column);

    void _write_decimal(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, int precision, int scale);

    template <LogicalType DecimalType, typename VectorBatchType, typename T>
    void _write_decimal32or64or128(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, int precision, int scale);

    void _write_date(orc::ColumnVectorBatch& orc_column, ColumnPtr& column);

    void _write_datetime(orc::ColumnVectorBatch& orc_column, ColumnPtr& column);

    Status _write_array_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, const TypeDescriptor& type);

    Status _write_struct_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, const TypeDescriptor& type);

    Status _write_map_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, const TypeDescriptor& type);

protected:
    std::unique_ptr<orc::Writer> _writer;
    std::vector<TypeDescriptor> _type_descs;
    std::shared_ptr<orc::WriterOptions> _writer_options;
    std::shared_ptr<orc::Type> _schema;
    std::unique_ptr<orc::ColumnVectorBatch> _batch = nullptr;
    OrcOutputStream _output_stream;
};

class AsyncOrcChunkWriter : public OrcChunkWriter {
public:
    AsyncOrcChunkWriter(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<orc::WriterOptions> writer_options,
                        std::shared_ptr<orc::Type> schema, const std::vector<ExprContext*>& output_expr_ctxs,
                        PriorityThreadPool* executor_pool, RuntimeProfile* parent_profile);

    // For UT
    AsyncOrcChunkWriter(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<orc::WriterOptions> writer_options,
                        std::vector<TypeDescriptor>& type_descs, std::unique_ptr<orc::Type> schema,
                        PriorityThreadPool* executor_pool, RuntimeProfile* parent_profile)
            : OrcChunkWriter(std::move(writable_file), writer_options, type_descs, std::move(schema)),
              _executor_pool(executor_pool),
              _parent_profile(parent_profile){};

    Status close(RuntimeState* state, const std::function<void(AsyncOrcChunkWriter*, RuntimeState*)>& cb = nullptr);

    bool writable();

    bool closed() { return _closed.load(); }

private:
    Status _flush_batch() override;

    PriorityThreadPool* _executor_pool;
    RuntimeProfile* _parent_profile = nullptr;
    RuntimeProfile::Counter* _io_timer = nullptr;

    std::atomic<bool> _closed = false;
    std::condition_variable _cv;
    std::mutex _lock;
    bool _batch_closing = false;
};

// class OrcFileWriter final : public pipeline::FileWriter {
// public:
//     OrcFileWriter();
//
//     ~OrcFileWriter() override;
//
//     std::future<Status> write(ChunkPtr chunk) override;
//
//     void commitAsync(std::function<void(CommitResult)> callback) override;
//
//     void rollback() override;
//
//     void close() override;
//
// private:
//
//     std::unique_ptr<OrcChunkWriter> _writer;
//     std::unique_ptr<OrcOutputStream> _output_stream;
//     std::optional<pipeline::FileMetrics> _metrics; // set metrics after commit
//     PriorityThreadPool* _executors;
//
//     inline static std::future<Status> NON_BLOCKED_OK = parquet::make_completed_future(Status::OK());
// };

} // namespace starrocks
