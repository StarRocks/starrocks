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

#include "column/chunk.h"
#include "common/status.h"
#include "connector/utils.h"
#include "formats/file_writer.h"
#include "fs/fs.h"
#include "runtime/exec_env.h"
#include "storage/load_chunk_spiller.h"
#include "util/threadpool.h"
#include "util/uid_util.h"

namespace starrocks::connector {

using CommitResult = formats::FileWriter::CommitResult;
using CommitFunc = std::function<void(const CommitResult& result)>;
using ErrorHandleFunc = std::function<void(const Status& status)>;

class AsyncFlushStreamPoller;

struct SortOrdering {
    std::vector<uint32_t> sort_key_idxes;
    SortDescs sort_descs;
};

struct PartitionChunkWriterContext {
    std::shared_ptr<formats::FileWriterFactory> file_writer_factory;
    std::shared_ptr<LocationProvider> location_provider;
    int64_t max_file_size = 0;
    bool is_default_partition = false;
};

struct BufferPartitionChunkWriterContext : public PartitionChunkWriterContext {};

struct SpillPartitionChunkWriterContext : public PartitionChunkWriterContext {
    std::shared_ptr<FileSystem> fs;
    pipeline::FragmentContext* fragment_context = nullptr;
    TupleDescriptor* tuple_desc = nullptr;
    std::shared_ptr<std::vector<std::unique_ptr<ColumnEvaluator>>> column_evaluators;
    std::shared_ptr<SortOrdering> sort_ordering;
};

class PartitionChunkWriter {
public:
    PartitionChunkWriter(std::string partition, std::vector<int8_t> partition_field_null_list,
                         const std::shared_ptr<PartitionChunkWriterContext>& ctx);

    virtual ~PartitionChunkWriter() = default;

    virtual Status init() = 0;

    virtual Status write(const ChunkPtr& chunk) = 0;

    virtual Status flush() = 0;

    virtual Status wait_flush() = 0;

    virtual Status finish() = 0;

    virtual bool is_finished() = 0;

    virtual int64_t get_written_bytes() = 0;

    virtual int64_t get_flushable_bytes() = 0;

    const std::string& partition() const { return _partition; }

    const std::vector<int8_t>& partition_field_null_list() const { return _partition_field_null_list; }

    std::shared_ptr<formats::FileWriter> file_writer() { return _file_writer; }

    std::shared_ptr<io::AsyncFlushOutputStream> out_stream() { return _out_stream; }

    void set_io_poller(AsyncFlushStreamPoller* io_poller) { _io_poller = io_poller; }

    void set_commit_callback(const CommitFunc& commit_callback) { _commit_callback = commit_callback; }

    void set_error_handler(const ErrorHandleFunc& error_handler) { _error_handler = error_handler; }

protected:
    Status create_file_writer_if_needed();

    void commit_file();

protected:
    std::string _partition;
    std::vector<int8_t> _partition_field_null_list;
    std::shared_ptr<formats::FileWriterFactory> _file_writer_factory;
    std::shared_ptr<LocationProvider> _location_provider;
    int64_t _max_file_size = 0;
    bool _is_default_partition = false;
    AsyncFlushStreamPoller* _io_poller = nullptr;

    std::shared_ptr<formats::FileWriter> _file_writer;
    std::shared_ptr<io::AsyncFlushOutputStream> _out_stream;
    CommitFunc _commit_callback;
    std::string _commit_extra_data;
    ErrorHandleFunc _error_handler = nullptr;
};

class BufferPartitionChunkWriter : public PartitionChunkWriter {
public:
    BufferPartitionChunkWriter(std::string partition, std::vector<int8_t> partition_field_null_list,
                               const std::shared_ptr<BufferPartitionChunkWriterContext>& ctx)
            : PartitionChunkWriter(std::move(partition), std::move(partition_field_null_list), ctx) {}

    Status init() override;

    Status write(const ChunkPtr& chunk) override;

    Status flush() override;

    Status wait_flush() override;

    Status finish() override;

    bool is_finished() override { return true; }

    int64_t get_written_bytes() override { return _file_writer ? _file_writer->get_written_bytes() : 0; }

    int64_t get_flushable_bytes() override { return _file_writer ? _file_writer->get_written_bytes() : 0; }
};

class SpillPartitionChunkWriter : public PartitionChunkWriter {
public:
    SpillPartitionChunkWriter(std::string partition, std::vector<int8_t> partition_field_null_list,
                              const std::shared_ptr<SpillPartitionChunkWriterContext>& ctx);

    ~SpillPartitionChunkWriter();

    Status init() override;

    Status write(const ChunkPtr& chunk) override;

    Status flush() override;

    Status wait_flush() override;

    Status finish() override;

    bool is_finished() override;

    int64_t get_written_bytes() override {
        if (!_file_writer) {
            return 0;
        }
        return _chunk_bytes_usage + _spilling_bytes_usage.load(std::memory_order_relaxed) +
               _file_writer->get_written_bytes();
    }

    int64_t get_flushable_bytes() override {
        if (!_spill_mode) {
            return _file_writer ? _file_writer->get_written_bytes() : 0;
        }
        return _chunk_bytes_usage;
    }

    Status merge_blocks();

private:
    Status _sort();

    Status _spill();

    Status _flush_to_file();

    Status _flush_chunk(Chunk* chunk, bool split);

    Status _write_chunk(Chunk* chunk);

    Status _merge_chunks();

    SchemaPtr _make_schema();

    ChunkPtr _create_schema_chunk(const ChunkPtr& base_chunk, size_t row_nums);

    bool _mem_insufficent();

    void _handle_err(const Status& st);

private:
    std::shared_ptr<FileSystem> _fs = nullptr;
    pipeline::FragmentContext* _fragment_context = nullptr;
    TupleDescriptor* _tuple_desc = nullptr;
    std::shared_ptr<std::vector<std::unique_ptr<ColumnEvaluator>>> _column_evaluators;
    std::shared_ptr<SortOrdering> _sort_ordering;
    std::unique_ptr<ThreadPoolToken> _chunk_spill_token;
    std::unique_ptr<ThreadPoolToken> _block_merge_token;
    std::unique_ptr<LoadSpillBlockManager> _load_spill_block_mgr;
    std::shared_ptr<LoadChunkSpiller> _load_chunk_spiller;
    TUniqueId _writer_id;

    std::list<ChunkPtr> _chunks;
    int64_t _chunk_bytes_usage = 0;
    std::atomic<int64_t> _spilling_bytes_usage = 0;
    ChunkPtr _result_chunk;
    ChunkPtr _base_chunk;
    SchemaPtr _schema;
    std::unordered_map<int, int> _col_index_map; // result chunk index -> chunk index
    bool _spill_mode = false;

    static const int64_t kWaitMilliseconds;
};

using PartitionChunkWriterPtr = std::shared_ptr<PartitionChunkWriter>;

class PartitionChunkWriterFactory {
public:
    virtual ~PartitionChunkWriterFactory() = default;

    virtual Status init() = 0;

    virtual PartitionChunkWriterPtr create(std::string partition,
                                           std::vector<int8_t> partition_field_null_list) const = 0;
};

class BufferPartitionChunkWriterFactory : public PartitionChunkWriterFactory {
public:
    BufferPartitionChunkWriterFactory(std::shared_ptr<BufferPartitionChunkWriterContext> ctx) : _ctx(ctx) {}

    ~BufferPartitionChunkWriterFactory() = default;

    Status init() override { return _ctx->file_writer_factory->init(); }

    PartitionChunkWriterPtr create(std::string partition,
                                   std::vector<int8_t> partition_field_null_list) const override {
        return std::make_shared<BufferPartitionChunkWriter>(std::move(partition), std::move(partition_field_null_list),
                                                            _ctx);
    }

private:
    std::shared_ptr<BufferPartitionChunkWriterContext> _ctx;
};

class SpillPartitionChunkWriterFactory : public PartitionChunkWriterFactory {
public:
    SpillPartitionChunkWriterFactory(std::shared_ptr<SpillPartitionChunkWriterContext> ctx) : _ctx(ctx) {}

    ~SpillPartitionChunkWriterFactory() = default;

    Status init() override { return _ctx->file_writer_factory->init(); }

    PartitionChunkWriterPtr create(std::string partition,
                                   std::vector<int8_t> partition_field_null_list) const override {
        return std::make_shared<SpillPartitionChunkWriter>(std::move(partition), std::move(partition_field_null_list),
                                                           _ctx);
    }

private:
    std::shared_ptr<SpillPartitionChunkWriterContext> _ctx;
};

} // namespace starrocks::connector
