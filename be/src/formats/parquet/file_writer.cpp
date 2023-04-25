
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

#include "formats/parquet/file_writer.h"

#include <arrow/buffer.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <parquet/arrow/writer.h>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "runtime/exec_env.h"
#include "util/defer_op.h"
#include "util/priority_thread_pool.hpp"
#include "util/runtime_profile.h"
#include "util/slice.h"

namespace starrocks::parquet {

ParquetOutputStream::ParquetOutputStream(std::unique_ptr<starrocks::WritableFile> wfile) : _wfile(std::move(wfile)) {
    set_mode(arrow::io::FileMode::WRITE);
}

ParquetOutputStream::~ParquetOutputStream() {
    arrow::Status st = ParquetOutputStream::Close();
    if (!st.ok()) {
        LOG(WARNING) << "close parquet output stream failed: " << st;
    }
}

arrow::Status ParquetOutputStream::Write(const std::shared_ptr<arrow::Buffer>& data) {
    arrow::Status st = Write(data->data(), data->size());
    if (!st.ok()) {
        LOG(WARNING) << "Failed to write data to output stream, err msg: " << st.message();
    }
    return st;
}

arrow::Status ParquetOutputStream::Write(const void* data, int64_t nbytes) {
    if (_is_closed) {
        return arrow::Status::IOError("The output stream is closed but there are still inputs");
    }

    const char* ch = reinterpret_cast<const char*>(data);

    if (_header_state == INITED) {
        _header_state = CACHED;
    } else {
        if (_header_state == CACHED) {
            _header_state = WRITEN;
            Status st = _wfile->append(Slice("PAR1"));
            if (!st.ok()) {
                return arrow::Status::IOError(st.to_string());
            }
        }
        Status st = _wfile->append(Slice(ch, nbytes));
        if (!st.ok()) {
            return arrow::Status::IOError(st.to_string());
        }
    }

    return arrow::Status::OK();
}

arrow::Result<int64_t> ParquetOutputStream::Tell() const {
    if (_header_state == CACHED) {
        return 4;
    } else {
        return _wfile->size();
    }
}

arrow::Status ParquetOutputStream::Close() {
    if (_is_closed) {
        return arrow::Status::OK();
    }
    Status st = _wfile->close();
    if (!st.ok()) {
        LOG(WARNING) << "close parquet output stream failed: " << st;
        return arrow::Status::IOError(st.to_string());
    }
    _is_closed = true;
    return arrow::Status::OK();
}

void ParquetBuildHelper::build_compression_type(::parquet::WriterProperties::Builder& builder,
                                                const TCompressionType::type& compression_type) {
    switch (compression_type) {
    case TCompressionType::SNAPPY: {
        builder.compression(::parquet::Compression::SNAPPY);
        break;
    }
    case TCompressionType::GZIP: {
        builder.compression(::parquet::Compression::GZIP);
        break;
    }
    case TCompressionType::BROTLI: {
        builder.compression(::parquet::Compression::BROTLI);
        break;
    }
    case TCompressionType::ZSTD: {
        builder.compression(::parquet::Compression::ZSTD);
        break;
    }
    case TCompressionType::LZ4: {
        builder.compression(::parquet::Compression::LZ4);
        break;
    }
    case TCompressionType::LZO: {
        builder.compression(::parquet::Compression::LZO);
        break;
    }
    case TCompressionType::BZIP2: {
        builder.compression(::parquet::Compression::BZ2);
        break;
    }
    default:
        builder.compression(::parquet::Compression::UNCOMPRESSED);
    }
}

arrow::Result<std::shared_ptr<::parquet::schema::GroupNode>> ParquetBuildHelper::make_schema(
        const std::vector<std::string>& file_column_names, const std::vector<ExprContext*>& output_expr_ctxs) {
    ::parquet::schema::NodeVector fields;

    for (int i = 0; i < output_expr_ctxs.size(); i++) {
        auto column_expr = output_expr_ctxs[i]->root();
        ARROW_ASSIGN_OR_RAISE(auto node,
                              _make_schema_node(file_column_names[i], column_expr->type(),
                                                column_expr->is_nullable() ? ::parquet::Repetition::OPTIONAL
                                                                           : ::parquet::Repetition::REQUIRED));
        DCHECK(node != nullptr);
        fields.push_back(std::move(node));
    }

    return std::static_pointer_cast<::parquet::schema::GroupNode>(
            ::parquet::schema::GroupNode::Make("table", ::parquet::Repetition::REQUIRED, std::move(fields)));
}

arrow::Result<std::shared_ptr<::parquet::schema::GroupNode>> ParquetBuildHelper::make_schema(
        const std::vector<std::string>& file_column_names, const std::vector<TypeDescriptor>& type_descs) {
    ::parquet::schema::NodeVector fields;

    for (int i = 0; i < type_descs.size(); i++) {
        ARROW_ASSIGN_OR_RAISE(auto node,
                              _make_schema_node(file_column_names[i], type_descs[i], ::parquet::Repetition::OPTIONAL));
        DCHECK(node != nullptr);
        fields.push_back(std::move(node));
    }

    return std::static_pointer_cast<::parquet::schema::GroupNode>(
            ::parquet::schema::GroupNode::Make("table", ::parquet::Repetition::REQUIRED, std::move(fields)));
}

std::shared_ptr<::parquet::WriterProperties> ParquetBuildHelper::make_properties(const ParquetBuilderOptions& options) {
    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_0);
    options.use_dict ? builder.enable_dictionary() : builder.disable_dictionary();
    starrocks::parquet::ParquetBuildHelper::build_compression_type(builder, options.compression_type);
    return builder.build();
}

// Repetition of subtype in nested type is set by default now, due to type descriptor has no nullable field.
arrow::Result<::parquet::schema::NodePtr> ParquetBuildHelper::_make_schema_node(const std::string& name,
                                                                                const TypeDescriptor& type_desc,
                                                                                ::parquet::Repetition::type rep_type) {
    switch (type_desc.type) {
    case TYPE_BOOLEAN: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::None(),
                                                      ::parquet::Type::BOOLEAN);
    }
    case TYPE_TINYINT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(8, true),
                                                      ::parquet::Type::INT32);
    }
    case TYPE_SMALLINT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(16, true),
                                                      ::parquet::Type::INT32);
    }
    case TYPE_INT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(32, true),
                                                      ::parquet::Type::INT32);
    }
    case TYPE_BIGINT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(64, true),
                                                      ::parquet::Type::INT64);
    }
    case TYPE_FLOAT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::None(),
                                                      ::parquet::Type::FLOAT);
    }
    case TYPE_DOUBLE: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::None(),
                                                      ::parquet::Type::DOUBLE);
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::String(),
                                                      ::parquet::Type::BYTE_ARRAY);
    }
    case TYPE_DATE: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Date(),
                                                      ::parquet::Type::INT32);
    }
    case TYPE_DATETIME: {
        return ::parquet::schema::PrimitiveNode::Make(
                name, rep_type, ::parquet::LogicalType::Timestamp(true, ::parquet::LogicalType::TimeUnit::unit::MILLIS),
                ::parquet::Type::INT64);
    }
    case TYPE_DECIMAL32: {
        return ::parquet::schema::PrimitiveNode::Make(
                name, rep_type, ::parquet::LogicalType::Decimal(type_desc.precision, type_desc.scale),
                ::parquet::Type::INT32);
    }

    case TYPE_DECIMAL64: {
        return ::parquet::schema::PrimitiveNode::Make(
                name, rep_type, ::parquet::LogicalType::Decimal(type_desc.precision, type_desc.scale),
                ::parquet::Type::INT64);
    }
    case TYPE_DECIMAL128: {
        return ::parquet::schema::PrimitiveNode::Make(
                name, rep_type, ::parquet::LogicalType::Decimal(type_desc.precision, type_desc.scale),
                ::parquet::Type::FIXED_LEN_BYTE_ARRAY, 16);
    }
    case TYPE_STRUCT: {
        DCHECK(type_desc.children.size() == type_desc.field_names.size());
        ::parquet::schema::NodeVector fields;
        for (size_t i = 0; i < type_desc.children.size(); i++) {
            ARROW_ASSIGN_OR_RAISE(auto child,
                                  _make_schema_node(type_desc.field_names[i], type_desc.children[i],
                                                    ::parquet::Repetition::OPTIONAL)); // use optional as default
            fields.push_back(std::move(child));
        }
        return ::parquet::schema::GroupNode::Make(name, rep_type, fields);
    }
    case TYPE_ARRAY: {
        DCHECK(type_desc.children.size() == 1);
        ARROW_ASSIGN_OR_RAISE(auto element,
                              _make_schema_node("element", type_desc.children[0],
                                                ::parquet::Repetition::OPTIONAL)); // use optional as default
        auto list = ::parquet::schema::GroupNode::Make("list", ::parquet::Repetition::REPEATED, {element});
        return ::parquet::schema::GroupNode::Make(name, rep_type, {list}, ::parquet::LogicalType::List());
    }
    case TYPE_MAP: {
        DCHECK(type_desc.children.size() == 2);
        ARROW_ASSIGN_OR_RAISE(auto key,
                              _make_schema_node("key", type_desc.children[0], ::parquet::Repetition::REQUIRED));
        ARROW_ASSIGN_OR_RAISE(auto value,
                              _make_schema_node("value", type_desc.children[1], ::parquet::Repetition::OPTIONAL));
        auto key_value = ::parquet::schema::GroupNode::Make("key_value", ::parquet::Repetition::REPEATED, {key, value});
        return ::parquet::schema::GroupNode::Make(name, rep_type, {key_value}, ::parquet::LogicalType::Map());
    }
    default: {
        return arrow::Status::TypeError(fmt::format("Type {} is not supported", type_desc.debug_string()));
    }
    }
}

FileWriterBase::FileWriterBase(std::unique_ptr<WritableFile> writable_file,
                               std::shared_ptr<::parquet::WriterProperties> properties,
                               std::shared_ptr<::parquet::schema::GroupNode> schema,
                               const std::vector<ExprContext*>& output_expr_ctxs)
        : _properties(std::move(properties)), _schema(std::move(schema)) {
    _outstream = std::make_shared<ParquetOutputStream>(std::move(writable_file));
    _type_descs.reserve(output_expr_ctxs.size());
    for (auto expr : output_expr_ctxs) {
        _type_descs.push_back(expr->root()->type());
    }
}

FileWriterBase::FileWriterBase(std::unique_ptr<WritableFile> writable_file,
                               std::shared_ptr<::parquet::WriterProperties> properties,
                               std::shared_ptr<::parquet::schema::GroupNode> schema,
                               std::vector<TypeDescriptor> type_descs)
        : _properties(std::move(properties)), _schema(std::move(schema)), _type_descs(std::move(type_descs)) {
    _outstream = std::make_shared<ParquetOutputStream>(std::move(writable_file));
}

Status FileWriterBase::init() {
    _writer = ::parquet::ParquetFileWriter::Open(_outstream, _schema, _properties);
    if (_writer == nullptr) {
        return Status::InternalError("Failed to create file writer");
    }
    return Status::OK();
}

void FileWriterBase::_generate_chunk_writer() {
    DCHECK(_writer != nullptr);
    if (_chunk_writer == nullptr) {
        auto rg_writer = _writer->AppendRowGroup();
        _chunk_writer = std::make_unique<ChunkWriter>(rg_writer, _type_descs, _schema);
    }
}

Status FileWriterBase::write(Chunk* chunk) {
    if (!chunk->has_rows()) {
        return Status::OK();
    }

    _generate_chunk_writer();
    _chunk_writer->write(chunk);

//    if (_chunk_writer->estimated_buffered_bytes() > _max_row_group_size) {
        _flush_row_group();
//    }

    return Status::OK();
}

std::size_t FileWriterBase::file_size() const {
    DCHECK(_outstream != nullptr);
    if (_chunk_writer == nullptr) {
        return _outstream->Tell().MoveValueUnsafe();
    }
    return _outstream->Tell().MoveValueUnsafe() + _chunk_writer->estimated_buffered_bytes();
}

Status FileWriterBase::split_offsets(std::vector<int64_t>& splitOffsets) const {
    if (_file_metadata == nullptr) {
        LOG(WARNING) << "file metadata null";
        return Status::InternalError("Get split offsets while the file metadata is null");
    }
    for (int i = 0; i < _file_metadata->num_row_groups(); i++) {
        auto first_column_meta = _file_metadata->RowGroup(i)->ColumnChunk(0);
        int64_t dict_page_offset = first_column_meta->dictionary_page_offset();
        int64_t first_data_page_offset = first_column_meta->data_page_offset();
        int64_t split_offset = dict_page_offset > 0 && dict_page_offset < first_data_page_offset
                                       ? dict_page_offset
                                       : first_data_page_offset;
        splitOffsets.emplace_back(split_offset);
    }
    return Status::OK();
}

void SyncFileWriter::_flush_row_group() {
    if (_chunk_writer != nullptr) {
        _chunk_writer->close();
        _chunk_writer = nullptr;
    }
}

Status SyncFileWriter::close() {
    if (_closed) {
        return Status::OK();
    }

    _flush_row_group();
    _writer->Close();

    auto st = _outstream->Close();
    if (!st.ok()) {
        return Status::InternalError("Close file failed!");
    }

    _closed = true;
    return Status::OK();
}

AsyncFileWriter::AsyncFileWriter(std::unique_ptr<WritableFile> writable_file, std::string file_name,
                                 std::string& file_dir, std::shared_ptr<::parquet::WriterProperties> properties,
                                 std::shared_ptr<::parquet::schema::GroupNode> schema,
                                 const std::vector<ExprContext*>& output_expr_ctxs, PriorityThreadPool* executor_pool,
                                 RuntimeProfile* parent_profile)
        : FileWriterBase(std::move(writable_file), std::move(properties), std::move(schema), output_expr_ctxs),
          _file_name(std::move(file_name)),
          _file_dir(file_dir),
          _executor_pool(executor_pool),
          _parent_profile(parent_profile) {
    _io_timer = ADD_TIMER(_parent_profile, "FileWriterIoTimer");
}

void AsyncFileWriter::_flush_row_group() {
    {
        auto lock = std::unique_lock(_m);
        _rg_writer_closing = true;
    }
    bool ret = _executor_pool->try_offer([&]() {
        SCOPED_TIMER(_io_timer);
        if (_chunk_writer != nullptr) {
            _chunk_writer->close();
            _chunk_writer = nullptr;
        }
        {
            auto lock = std::unique_lock(_m);
            _rg_writer_closing = false;
            lock.unlock();
            _cv.notify_one();
        }
    });
    if (!ret) {
        {
            auto lock = std::unique_lock(_m);
            _rg_writer_closing = false;
            lock.unlock();
            _cv.notify_one();
        }
    }
}

Status AsyncFileWriter::close(RuntimeState* state,
                              std::function<void(starrocks::parquet::AsyncFileWriter*, RuntimeState*)> cb) {
    bool ret = _executor_pool->try_offer([&, state, cb]() {
        SCOPED_TIMER(_io_timer);
        {
            auto lock = std::unique_lock(_m);
            _cv.wait(lock, [&] { return !_rg_writer_closing; });
        }
        _flush_row_group();
        _file_metadata = _writer->metadata();
        auto st = _outstream->Close();
        if (!st.ok()) {
            return Status::InternalError("Close outstream failed");
        }

        if (cb != nullptr) {
            cb(this, state);
        }
        _closed.store(true);
        return Status::OK();
    });

    if (ret) {
        return Status::OK();
    }
    return Status::InternalError("Submit close file error");
}

} // namespace starrocks::parquet
