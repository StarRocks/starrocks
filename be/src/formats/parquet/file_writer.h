
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
#include "fs/fs.h"
#include "runtime/runtime_state.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::parquet {

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
    static void build_compression_type(::parquet::WriterProperties::Builder& builder,
                                       const TCompressionType::type& compression_type);

    static arrow::Result<std::shared_ptr<::parquet::schema::GroupNode>> make_schema(
            const std::vector<std::string>& file_column_names, const std::vector<ExprContext*>& output_expr_ctxs);

    static arrow::Result<std::shared_ptr<::parquet::schema::GroupNode>> make_schema(
            const std::vector<std::string>& file_column_names, const std::vector<TypeDescriptor>& type_descs);

    static std::shared_ptr<::parquet::WriterProperties> make_properties(const ParquetBuilderOptions& options);

private:
    static arrow::Result<::parquet::schema::NodePtr> _make_schema_node(const std::string& name,
                                                                       const TypeDescriptor& type_desc,
                                                                       ::parquet::Repetition::type rep_type);
};

class ChunkWriter {
public:
    ChunkWriter(::parquet::RowGroupWriter* rg_writer, const std::vector<TypeDescriptor>& type_descs,
                const std::shared_ptr<::parquet::schema::GroupNode>& schema);

    Status write(Chunk* chunk);

    void close();

    int64_t estimated_buffered_bytes() const;

private:
    class Context {
    public:
        Context(int16_t max_def_level, int16_t max_rep_level, size_t estimated_size = 0)
                : _max_def_level(max_def_level), _max_rep_level(max_rep_level) {
            _idx2subcol.reserve(estimated_size);
            _def_levels.reserve(estimated_size);
            _rep_levels.reserve(estimated_size);
        }

        void append(int idx, int16_t def_level, int16_t rep_level) {
            _idx2subcol.push_back(idx);
            _def_levels.push_back(def_level);
            _rep_levels.push_back(rep_level);
        }

        std::tuple<int, int16_t, int16_t> get(int i) const {
            DCHECK_LT(i, size());
            return std::make_tuple(_idx2subcol[i], _def_levels[i], _rep_levels[i]);
        }

        int size() const { return _def_levels.size(); }

        std::string debug_string() const;

    public:
        const int16_t _max_def_level;
        const int16_t _max_rep_level;
        constexpr static int kNULL = -1;

        std::vector<int> _idx2subcol;
        std::vector<int16_t> _def_levels;
        std::vector<int16_t> _rep_levels;
    };

    Status _add_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                             const ::parquet::schema::NodePtr& node, const ColumnPtr& col);

    Status _add_boolean_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                                     const ::parquet::schema::NodePtr& node, const ColumnPtr& col);

    template <LogicalType lt, ::parquet::Type::type pt>
    Status _add_int_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                                 const ::parquet::schema::NodePtr& node, const ColumnPtr& col);

    Status _add_decimal128_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                                        const ::parquet::schema::NodePtr& node, const ColumnPtr& col);

    Status _add_varchar_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                                     const ::parquet::schema::NodePtr& node, const ColumnPtr& col);

    Status _add_date_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                                  const ::parquet::schema::NodePtr& node, const ColumnPtr& col);

    Status _add_datetime_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                                      const ::parquet::schema::NodePtr& node, const ColumnPtr& col);

    Status _add_array_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                                   const ::parquet::schema::NodePtr& node, const ColumnPtr& col);

    Status _add_map_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                                 const ::parquet::schema::NodePtr& node, const ColumnPtr& col);

    Status _add_struct_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                                    const ::parquet::schema::NodePtr& node, const ColumnPtr& col);

    std::vector<uint8_t> _make_null_bitset(size_t n, const uint8_t* nulls) const;

    void _populate_def_levels(std::vector<int16_t>& def_levels, const Context& ctx, const uint8_t* nulls) const;

    ::parquet::RowGroupWriter* _rg_writer;
    std::vector<TypeDescriptor> _type_descs;
    std::shared_ptr<::parquet::schema::GroupNode> _schema;
    std::vector<int64_t> _estimated_buffered_bytes;
    int _col_idx;
};

class FileWriterBase {
public:
    FileWriterBase(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<::parquet::WriterProperties> properties,
                   std::shared_ptr<::parquet::schema::GroupNode> schema,
                   const std::vector<ExprContext*>& output_expr_ctxs);

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

    virtual void _flush_row_group() = 0;

protected:
    std::shared_ptr<ParquetOutputStream> _outstream;
    std::shared_ptr<::parquet::WriterProperties> _properties;
    std::shared_ptr<::parquet::schema::GroupNode> _schema;
    std::unique_ptr<::parquet::ParquetFileWriter> _writer;
    std::unique_ptr<ChunkWriter> _chunk_writer;

    std::vector<TypeDescriptor> _type_descs;
    std::shared_ptr<::parquet::FileMetaData> _file_metadata;

    const static int64_t kDefaultMaxRowGroupSize = 128 * 1024 * 1024; // 128MB
    int64_t _max_row_group_size = kDefaultMaxRowGroupSize;
};

class SyncFileWriter : public FileWriterBase {
public:
    SyncFileWriter(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<::parquet::WriterProperties> properties,
                   std::shared_ptr<::parquet::schema::GroupNode> schema,
                   const std::vector<ExprContext*>& output_expr_ctxs)
            : FileWriterBase(std::move(writable_file), std::move(properties), std::move(schema), output_expr_ctxs) {}

    SyncFileWriter(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<::parquet::WriterProperties> properties,
                   std::shared_ptr<::parquet::schema::GroupNode> schema, std::vector<TypeDescriptor> type_descs)
            : FileWriterBase(std::move(writable_file), std::move(properties), std::move(schema),
                             std::move(type_descs)) {}

    ~SyncFileWriter() override = default;

    Status close();

    bool closed() const override { return _closed; }

private:
    void _flush_row_group() override;

    bool _closed = false;
};

class AsyncFileWriter : public FileWriterBase {
public:
    AsyncFileWriter(std::unique_ptr<WritableFile> writable_file, std::string file_name, std::string& file_dir,
                    std::shared_ptr<::parquet::WriterProperties> properties,
                    std::shared_ptr<::parquet::schema::GroupNode> schema,
                    const std::vector<ExprContext*>& output_expr_ctxs, PriorityThreadPool* executor_pool,
                    RuntimeProfile* parent_profile);

    ~AsyncFileWriter() override = default;

    Status close(RuntimeState* state,
                 std::function<void(starrocks::parquet::AsyncFileWriter*, RuntimeState*)> cb = nullptr);

    bool writable() {
        auto lock = std::unique_lock(_m);
        return !_rg_writer_closing;
    }

    bool closed() const override { return _closed.load(); }

    std::string file_name() const { return _file_name; }

    std::string file_dir() const { return _file_dir; }

private:
    void _flush_row_group() override;

    std::string _file_name;
    std::string _file_dir;

    std::atomic<bool> _closed = false;

    PriorityThreadPool* _executor_pool;

    RuntimeProfile* _parent_profile = nullptr;
    RuntimeProfile::Counter* _io_timer = nullptr;

    std::condition_variable _cv;
    bool _rg_writer_closing = false;
    std::mutex _m;
};

} // namespace starrocks::parquet
