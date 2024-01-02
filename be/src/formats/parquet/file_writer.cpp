
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

StatusOr<::parquet::Compression::type> ParquetBuildHelper::convert_compression_type(
        const TCompressionType::type& compression_type) {
    auto codec = ::parquet::Compression::UNCOMPRESSED;
    switch (compression_type) {
    case TCompressionType::NO_COMPRESSION: {
        codec = ::parquet::Compression::UNCOMPRESSED;
        break;
    }
    case TCompressionType::SNAPPY: {
        codec = ::parquet::Compression::SNAPPY;
        break;
    }
    case TCompressionType::GZIP: {
        codec = ::parquet::Compression::GZIP;
        break;
    }
    case TCompressionType::BROTLI: {
        codec = ::parquet::Compression::BROTLI;
        break;
    }
    case TCompressionType::ZSTD: {
        codec = ::parquet::Compression::ZSTD;
        break;
    }
    case TCompressionType::LZ4: {
        codec = ::parquet::Compression::LZ4_HADOOP;
        break;
    }
    case TCompressionType::LZO: {
        codec = ::parquet::Compression::LZO;
        break;
    }
    case TCompressionType::BZIP2: {
        codec = ::parquet::Compression::BZ2;
        break;
    }
    default: {
        return Status::NotSupported(fmt::format("not supported compression type {}", to_string(compression_type)));
    }
    }

    // Check if arrow supports indicated compression type
    if (!::parquet::IsCodecSupported(codec)) {
        return Status::NotSupported(fmt::format("not supported compression codec {}", to_string(compression_type)));
    }

    return codec;
}

arrow::Result<std::shared_ptr<::parquet::schema::GroupNode>> ParquetBuildHelper::make_schema(
        const std::vector<std::string>& file_column_names, const std::vector<ExprContext*>& output_expr_ctxs,
        const std::vector<FileColumnId>& file_column_ids) {
    ::parquet::schema::NodeVector fields;

    for (int i = 0; i < output_expr_ctxs.size(); i++) {
        auto* column_expr = output_expr_ctxs[i]->root();
        ARROW_ASSIGN_OR_RAISE(auto node, _make_schema_node(file_column_names[i], column_expr->type(),
                                                           column_expr->is_nullable() ? ::parquet::Repetition::OPTIONAL
                                                                                      : ::parquet::Repetition::REQUIRED,
                                                           file_column_ids[i]));
        DCHECK(node != nullptr);
        fields.push_back(std::move(node));
    }

    return std::static_pointer_cast<::parquet::schema::GroupNode>(
            ::parquet::schema::GroupNode::Make("table", ::parquet::Repetition::REQUIRED, std::move(fields)));
}

// for UT only
arrow::Result<std::shared_ptr<::parquet::schema::GroupNode>> ParquetBuildHelper::make_schema(
        const std::vector<std::string>& file_column_names, const std::vector<TypeDescriptor>& type_descs,
        const std::vector<FileColumnId>& file_column_ids) {
    ::parquet::schema::NodeVector fields;

    for (int i = 0; i < type_descs.size(); i++) {
        ARROW_ASSIGN_OR_RAISE(auto node, _make_schema_node(file_column_names[i], type_descs[i],
                                                           ::parquet::Repetition::OPTIONAL, file_column_ids[i]))
        DCHECK(node != nullptr);
        fields.push_back(std::move(node));
    }

    return std::static_pointer_cast<::parquet::schema::GroupNode>(
            ::parquet::schema::GroupNode::Make("table", ::parquet::Repetition::REQUIRED, std::move(fields)));
}

StatusOr<std::shared_ptr<::parquet::WriterProperties>> ParquetBuildHelper::make_properties(
        const ParquetBuilderOptions& options) {
    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_0);
    options.use_dict ? builder.enable_dictionary() : builder.disable_dictionary();
    ASSIGN_OR_RETURN(auto compression_codec,
                     parquet::ParquetBuildHelper::convert_compression_type(options.compression_type));
    builder.compression(compression_codec);
    return builder.build();
}

// Repetition of subtype in nested type is set by default now, due to type descriptor has no nullable field.
arrow::Result<::parquet::schema::NodePtr> ParquetBuildHelper::_make_schema_node(const std::string& name,
                                                                                const TypeDescriptor& type_desc,
                                                                                ::parquet::Repetition::type rep_type,
                                                                                FileColumnId file_column_id) {
    if (file_column_id.children.size() != type_desc.children.size()) {
        file_column_id.children = std::vector<FileColumnId>(type_desc.children.size());
    }

    switch (type_desc.type) {
    case TYPE_BOOLEAN: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::None(),
                                                      ::parquet::Type::BOOLEAN, -1, file_column_id.field_id);
    }
    case TYPE_TINYINT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(8, true),
                                                      ::parquet::Type::INT32, -1, file_column_id.field_id);
    }
    case TYPE_SMALLINT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(16, true),
                                                      ::parquet::Type::INT32, -1, file_column_id.field_id);
    }
    case TYPE_INT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(32, true),
                                                      ::parquet::Type::INT32, -1, file_column_id.field_id);
    }
    case TYPE_BIGINT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(64, true),
                                                      ::parquet::Type::INT64, -1, file_column_id.field_id);
    }
    case TYPE_FLOAT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::None(),
                                                      ::parquet::Type::FLOAT, -1, file_column_id.field_id);
    }
    case TYPE_DOUBLE: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::None(),
                                                      ::parquet::Type::DOUBLE, -1, file_column_id.field_id);
    }
    case TYPE_BINARY:
    case TYPE_VARBINARY:
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::None(),
                                                      ::parquet::Type::BYTE_ARRAY, -1, file_column_id.field_id);
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::String(),
                                                      ::parquet::Type::BYTE_ARRAY, -1, file_column_id.field_id);
    }
    case TYPE_DATE: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Date(),
                                                      ::parquet::Type::INT32, -1, file_column_id.field_id);
    }
    case TYPE_DATETIME: {
        // TODO(letian-jiang): set isAdjustedToUTC to true, and normalize datetime values
        return ::parquet::schema::PrimitiveNode::Make(
                name, rep_type,
                ::parquet::LogicalType::Timestamp(false, ::parquet::LogicalType::TimeUnit::unit::MILLIS),
                ::parquet::Type::INT64, -1, file_column_id.field_id);
    }
    case TYPE_DECIMAL32: {
        return ::parquet::schema::PrimitiveNode::Make(
                name, rep_type, ::parquet::LogicalType::Decimal(type_desc.precision, type_desc.scale),
                ::parquet::Type::INT32, -1, file_column_id.field_id);
    }

    case TYPE_DECIMAL64: {
        return ::parquet::schema::PrimitiveNode::Make(
                name, rep_type, ::parquet::LogicalType::Decimal(type_desc.precision, type_desc.scale),
                ::parquet::Type::INT64, -1, file_column_id.field_id);
    }
    case TYPE_DECIMAL128: {
        return ::parquet::schema::PrimitiveNode::Make(
                name, rep_type, ::parquet::LogicalType::Decimal(type_desc.precision, type_desc.scale),
                ::parquet::Type::FIXED_LEN_BYTE_ARRAY, 16, file_column_id.field_id);
    }
    case TYPE_STRUCT: {
        DCHECK(type_desc.children.size() == type_desc.field_names.size());
        ::parquet::schema::NodeVector fields;
        for (size_t i = 0; i < type_desc.children.size(); i++) {
            ARROW_ASSIGN_OR_RAISE(auto child, _make_schema_node(type_desc.field_names[i], type_desc.children[i],
                                                                ::parquet::Repetition::OPTIONAL,
                                                                file_column_id.children[i])); // use optional as default
            fields.push_back(std::move(child));
        }
        return ::parquet::schema::GroupNode::Make(name, rep_type, fields, ::parquet::ConvertedType::NONE,
                                                  file_column_id.field_id);
    }
    case TYPE_ARRAY: {
        DCHECK(type_desc.children.size() == 1);
        ARROW_ASSIGN_OR_RAISE(auto element,
                              _make_schema_node("element", type_desc.children[0], ::parquet::Repetition::OPTIONAL,
                                                file_column_id.children[0])); // use optional as default
        auto list = ::parquet::schema::GroupNode::Make("list", ::parquet::Repetition::REPEATED, {element});
        return ::parquet::schema::GroupNode::Make(name, rep_type, {list}, ::parquet::LogicalType::List(),
                                                  file_column_id.field_id);
    }
    case TYPE_MAP: {
        DCHECK(type_desc.children.size() == 2);
        ARROW_ASSIGN_OR_RAISE(auto key, _make_schema_node("key", type_desc.children[0], ::parquet::Repetition::REQUIRED,
                                                          file_column_id.children[0]))
        ARROW_ASSIGN_OR_RAISE(auto value,
                              _make_schema_node("value", type_desc.children[1], ::parquet::Repetition::OPTIONAL,
                                                file_column_id.children[1]));
        auto key_value = ::parquet::schema::GroupNode::Make("key_value", ::parquet::Repetition::REPEATED, {key, value});
        return ::parquet::schema::GroupNode::Make(name, rep_type, {key_value}, ::parquet::LogicalType::Map(),
                                                  file_column_id.field_id);
    }
    case TYPE_TIME: {
        return ::parquet::schema::PrimitiveNode::Make(
                name, rep_type, ::parquet::LogicalType::Time(false, ::parquet::LogicalType::TimeUnit::MICROS),
                ::parquet::Type::INT64, -1, file_column_id.field_id);
    }
    default: {
        return arrow::Status::TypeError(fmt::format("Doesn't support to write {} type data", type_desc.debug_string()));
    }
    }
}

FileWriterBase::FileWriterBase(std::unique_ptr<WritableFile> writable_file,
                               std::shared_ptr<::parquet::WriterProperties> properties,
                               std::shared_ptr<::parquet::schema::GroupNode> schema,
                               const std::vector<ExprContext*>& output_expr_ctxs, int64_t max_file_size)
        : _properties(std::move(properties)), _schema(std::move(schema)), _max_file_size(max_file_size) {
    _outstream = std::make_shared<ParquetOutputStream>(std::move(writable_file));
    _type_descs.reserve(output_expr_ctxs.size());
    for (auto expr : output_expr_ctxs) {
        _type_descs.push_back(expr->root()->type());
    }
    _eval_func = [output_expr_ctxs](Chunk* chunk, size_t col_idx) {
        return output_expr_ctxs[col_idx]->evaluate(chunk);
    };
}

// For UT only
FileWriterBase::FileWriterBase(std::unique_ptr<WritableFile> writable_file,
                               std::shared_ptr<::parquet::WriterProperties> properties,
                               std::shared_ptr<::parquet::schema::GroupNode> schema,
                               std::vector<TypeDescriptor> type_descs)
        : _properties(std::move(properties)), _schema(std::move(schema)), _type_descs(std::move(type_descs)) {
    _outstream = std::make_shared<ParquetOutputStream>(std::move(writable_file));
    _eval_func = [](Chunk* chunk, size_t col_idx) { return chunk->get_column_by_index(col_idx); };
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
        auto rg_writer = _writer->AppendBufferedRowGroup();
        _chunk_writer = std::make_unique<ChunkWriter>(rg_writer, _type_descs, _schema, _eval_func);
    }
}

Status FileWriterBase::write(Chunk* chunk) {
    if (!chunk->has_rows()) {
        return Status::OK();
    }

    _generate_chunk_writer();
    RETURN_IF_ERROR(_chunk_writer->write(chunk));

    if (_chunk_writer->estimated_buffered_bytes() > _max_row_group_size && !is_last_row_group()) {
        RETURN_IF_ERROR(_flush_row_group());
    }

    return Status::OK();
}

std::size_t FileWriterBase::file_size() const {
    DCHECK(_outstream != nullptr);
    if (_chunk_writer == nullptr) {
        return _outstream->Tell().MoveValueUnsafe();
    }
    return _outstream->Tell().MoveValueUnsafe() + _chunk_writer->estimated_buffered_bytes();
}

// TODO(stephen): we should use `RowGroupMetaData::file_offset()` of arrow to get file split_offset.
// However, the current arrow version 5.0.0 have bug in this interface and requires an upgrade.
// So we rewrite the correct logic for this.
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

Status SyncFileWriter::_flush_row_group() {
    if (_chunk_writer != nullptr) {
        try {
            _chunk_writer->close();
        } catch (const ::parquet::ParquetStatusException& e) {
            _chunk_writer.reset();
            _closed = true;
            auto st = Status::IOError(fmt::format("{}: {}", "flush rowgroup error", e.what()));
            LOG(WARNING) << st;
            return st;
        }
    }

    _chunk_writer.reset();
    return Status::OK();
}

Status SyncFileWriter::close() {
    if (_closed) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_flush_row_group());
    _writer->Close();

    auto arrow_st = _outstream->Close();
    if (!arrow_st.ok()) {
        auto st = Status::IOError(fmt::format("{}: {}", "close file error", arrow_st.message()));
        LOG(WARNING) << st;
        return st;
    }

    _closed = true;
    return Status::OK();
}

AsyncFileWriter::AsyncFileWriter(std::unique_ptr<WritableFile> writable_file, std::string file_location,
                                 std::string partition_location,
                                 std::shared_ptr<::parquet::WriterProperties> properties,
                                 std::shared_ptr<::parquet::schema::GroupNode> schema,
                                 const std::vector<ExprContext*>& output_expr_ctxs, PriorityThreadPool* executor_pool,
                                 RuntimeProfile* parent_profile, int64_t max_file_size)
        : FileWriterBase(std::move(writable_file), std::move(properties), std::move(schema), output_expr_ctxs,
                         max_file_size),
          _file_location(std::move(file_location)),
          _partition_location(std::move(partition_location)),
          _executor_pool(executor_pool),
          _parent_profile(parent_profile) {
    _io_timer = ADD_TIMER(_parent_profile, "FileWriterIoTimer");
}

Status AsyncFileWriter::_flush_row_group() {
    {
        auto lock = std::unique_lock(_m);
        _rg_writer_closing = true;
    }

    bool ok = _executor_pool->try_offer([&]() {
        SCOPED_TIMER(_io_timer);
        if (_chunk_writer != nullptr) {
            try {
                _chunk_writer->close();
            } catch (const ::parquet::ParquetStatusException& e) {
                LOG(WARNING) << "flush row group error: " << e.what();
                set_io_status(Status::IOError(fmt::format("{}: {}", "flush rowgroup error", e.what())));
            }
            _chunk_writer = nullptr;
        }
        {
            auto lock = std::unique_lock(_m);
            _rg_writer_closing = false;
        }
        _cv.notify_one();
    });

    if (!ok) {
        {
            auto lock = std::unique_lock(_m);
            _rg_writer_closing = false;
        }
        _cv.notify_one();
        auto st = Status::ResourceBusy("submit flush row group task fails");
        LOG(WARNING) << st;
        return st;
    }

    return Status::OK();
}

Status AsyncFileWriter::close(RuntimeState* state,
                              const std::function<void(starrocks::parquet::AsyncFileWriter*, RuntimeState*)>& cb) {
    bool ret = _executor_pool->try_offer([&, state, cb]() {
        SCOPED_TIMER(_io_timer);
        {
            auto lock = std::unique_lock(_m);
            _cv.wait(lock, [&] { return !_rg_writer_closing; });
        }

        DeferOp defer([&]() {
            // set closed to true anyway
            _closed.store(true);
        });
        try {
            _writer->Close();
        } catch (const ::parquet::ParquetStatusException& e) {
            LOG(WARNING) << "close writer error: " << e.what();
            set_io_status(Status::IOError(fmt::format("{}: {}", "close writer error", e.what())));
        }
        _chunk_writer = nullptr;
        _file_metadata = _writer->metadata();
        auto st = _outstream->Close();
        if (!st.ok()) {
            LOG(WARNING) << "close output stream error: " << st.message();
            set_io_status(Status::IOError(fmt::format("{}: {}", "close output stream error", st.message())));
        }

        if (cb != nullptr) {
            cb(this, state);
        }
    });

    if (!ret) {
        return Status::InternalError("Submit close file task error");
    }

    return Status::OK();
}

} // namespace starrocks::parquet
