
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

#include "formats/parquet/parquet_file_writer.h"

#include <arrow/buffer.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <parquet/arrow/writer.h>
#include <runtime/current_thread.h>

#include <future>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "formats/file_writer.h"
#include "formats/utils.h"
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
    PARQUET_THROW_NOT_OK(ParquetOutputStream::Close());
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
    _is_closed = true;

    Status st = _wfile->close();
    if (!st.ok()) {
        LOG(WARNING) << "close parquet output stream failed: " << st;
        return arrow::Status::IOError(st.to_string());
    }
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
    try {
        _writer->Close();
    } catch (const ::parquet::ParquetStatusException& e) {
        LOG(WARNING) << "close writer error: " << e.what();
        return Status::IOError(fmt::format("{}: {}", "close writer error", e.what()));
    }

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
                                 RuntimeProfile* parent_profile, int64_t max_file_size, RuntimeState* state)
        : FileWriterBase(std::move(writable_file), std::move(properties), std::move(schema), output_expr_ctxs,
                         max_file_size),
          _file_location(std::move(file_location)),
          _partition_location(std::move(partition_location)),
          _executor_pool(executor_pool),
          _parent_profile(parent_profile),
          _state(state) {
    _io_timer = ADD_TIMER(_parent_profile, "FileWriterIoTimer");
}

Status AsyncFileWriter::_flush_row_group() {
    {
        auto lock = std::unique_lock(_m);
        _rg_writer_closing = true;
    }

    bool ok = _executor_pool->try_offer([&] {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_state->instance_mem_tracker());
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
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_state->instance_mem_tracker());
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

namespace starrocks::formats {

std::future<Status> ParquetFileWriter::write(ChunkPtr chunk) {
    if (_rowgroup_writer == nullptr) {
        _rowgroup_writer = std::make_unique<parquet::ChunkWriter>(_writer->AppendBufferedRowGroup(), _type_descs,
                                                                  _schema, _eval_func);
    }
    if (auto status = _rowgroup_writer->write(chunk.get()); !status.ok()) {
        return make_ready_future(std::move(status));
    }
    if (_rowgroup_writer->estimated_buffered_bytes() > _writer_options->rowgroup_size) {
        return _flush_row_group();
    }
    return make_ready_future(Status::OK());
}

std::future<FileWriter::CommitResult> ParquetFileWriter::commit() {
    auto promise = std::make_shared<std::promise<FileWriter::CommitResult>>();
    std::future<FileWriter::CommitResult> future = promise->get_future();

    auto task = [writer = _writer, output_stream = _output_stream, p = promise,
                 has_field_id = _writer_options->column_ids.has_value(), rollback = _rollback_action,
                 location = _location] {
        // TODO(letian-jiang): check if there are any outstanding io task
        FileWriter::CommitResult result{.io_status = Status::OK(), .format = PARQUET, .location = location, .rollback_action = rollback};
        try {
            writer->Close();
        } catch (const ::parquet::ParquetStatusException& e) {
            result.io_status.update(Status::IOError(fmt::format("{}: {}", "close file error", e.what())));
        }

        if (auto status = output_stream->Close(); !status.ok()) {
            result.io_status.update(
                    Status::IOError(fmt::format("{}: {}", "close output stream error", status.message())));
        }

        if (result.io_status.ok()) {
            result.file_metrics = _metrics(writer->metadata().get(), has_field_id);
            result.file_metrics.file_size = output_stream->Tell().MoveValueUnsafe();
        }

        p->set_value(result);
    };

    if (_executors) {
        bool ok = _executors->try_offer(task);
        if (!ok) {
            Status exception = Status::ResourceBusy("submit close file task fails");
            LOG(WARNING) << exception;
            promise->set_value(FileWriter::CommitResult{.io_status = exception, .rollback_action = _rollback_action});
        }
    } else {
        task();
    }

    _writer = nullptr;
    return future;
}

int64_t ParquetFileWriter::get_written_bytes() {
    int n = _output_stream->Tell().MoveValueUnsafe();
    if (_rowgroup_writer != nullptr) {
        n += _rowgroup_writer->estimated_buffered_bytes();
    }
    return n;
}

std::future<Status> ParquetFileWriter::_flush_row_group() {
    DCHECK(_rowgroup_writer != nullptr);
    auto promise = std::make_shared<std::promise<Status>>();
    std::future<Status> future = promise->get_future();

    auto task = [rowgroup_writer = _rowgroup_writer, p = promise] {
        try {
            rowgroup_writer->close();
        } catch (const ::parquet::ParquetStatusException& e) {
            Status exception = Status::IOError(fmt::format("{}: {}", "flush rowgroup error", e.what()));
            LOG(WARNING) << exception;
            p->set_value(exception);
            return;
        }
        p->set_value(Status::OK());
    };

    if (_executors) {
        bool ok = _executors->try_offer(task);
        if (!ok) {
            Status exception = Status::ResourceBusy("submit close file task fails");
            LOG(WARNING) << exception;
            promise->set_value(exception);
        }
    } else {
        task();
    }

    _rowgroup_writer = nullptr;
    return future;
}

#define MERGE_STATS_CASE(ParquetType)                                                                              \
    case ParquetType: {                                                                                            \
        auto typed_left_stat =                                                                                     \
                std::static_pointer_cast<::parquet::TypedStatistics<::parquet::PhysicalType<ParquetType>>>(left);  \
        auto typed_right_stat =                                                                                    \
                std::static_pointer_cast<::parquet::TypedStatistics<::parquet::PhysicalType<ParquetType>>>(right); \
        typed_left_stat->Merge(*typed_right_stat);                                                                 \
        return;                                                                                                    \
    }

void merge_stats(const std::shared_ptr<::parquet::Statistics>& left,
                 const std::shared_ptr<::parquet::Statistics>& right) {
    DCHECK(left->physical_type() == right->physical_type());
    switch (left->physical_type()) {
        MERGE_STATS_CASE(::parquet::Type::BOOLEAN);
        MERGE_STATS_CASE(::parquet::Type::INT32);
        MERGE_STATS_CASE(::parquet::Type::INT64);
        MERGE_STATS_CASE(::parquet::Type::INT96);
        MERGE_STATS_CASE(::parquet::Type::FLOAT);
        MERGE_STATS_CASE(::parquet::Type::DOUBLE);
        MERGE_STATS_CASE(::parquet::Type::BYTE_ARRAY);
        MERGE_STATS_CASE(::parquet::Type::FIXED_LEN_BYTE_ARRAY);
    default: {
    }
    }
}

FileWriter::FileMetrics ParquetFileWriter::_metrics(const ::parquet::FileMetaData* meta, bool has_field_id) {
    DCHECK(meta != nullptr);
    FileWriter::FileMetrics file_metrics;
    file_metrics.record_count = meta->num_rows();

    if (!has_field_id) {
        return file_metrics;
    }

    // rowgroup split offsets
    std::vector<int64_t> split_offsets;
    for (int i = 0; i < meta->num_row_groups(); i++) {
        auto first_column_meta = meta->RowGroup(i)->ColumnChunk(0);
        int64_t dict_page_offset = first_column_meta->dictionary_page_offset();
        int64_t first_data_page_offset = first_column_meta->data_page_offset();
        int64_t split_offset = dict_page_offset > 0 && dict_page_offset < first_data_page_offset
                               ? dict_page_offset
                               : first_data_page_offset;
        split_offsets.push_back(split_offset);
    }
    file_metrics.split_offsets = split_offsets;

    // field_id -> column_stat
    std::map<int32_t, std::shared_ptr<::parquet::Statistics>> column_stats;
    std::map<int32_t, int64_t> column_sizes;
    std::map<int32_t, int64_t> value_counts;
    std::map<int32_t, int64_t> null_value_counts;
    std::map<int32_t, std::string> lower_bounds;
    std::map<int32_t, std::string> upper_bounds;
    bool has_null_count = false;
    bool has_min_max = false;

    // traverse stat of column chunk in each row group
    for (int col_idx = 0; col_idx < meta->num_columns(); col_idx++) {
        auto field_id = meta->schema()->Column(col_idx)->schema_node()->field_id();

        for (int rg_idx = 0; rg_idx < meta->num_row_groups(); rg_idx++) {
            auto column_chunk_meta = meta->RowGroup(rg_idx)->ColumnChunk(col_idx);
            column_sizes[field_id] += column_chunk_meta->total_compressed_size();

            if (column_chunk_meta->is_stats_set()) {
                auto column_stat = column_chunk_meta->statistics();
                if (!column_stats.count(field_id)) {
                    column_stats[field_id] = column_stat;
                } else {
                    merge_stats(column_stats[field_id], column_stat);
                }
            }
        }
    }

    for (auto& [field_id, column_stat] : column_stats) {
        value_counts[field_id] = column_stat->num_values();
        if (column_stat->HasNullCount()) {
            has_null_count = true;
            null_value_counts[field_id] = column_stat->null_count();
        }
        if (column_stat->HasMinMax()) {
            has_min_max = true;
            lower_bounds[field_id] = column_stat->EncodeMin();
            upper_bounds[field_id] = column_stat->EncodeMax();
        }
    }

    file_metrics.column_sizes = std::move(column_sizes);
    file_metrics.value_counts = std::move(value_counts);
    if (has_null_count) {
        file_metrics.null_value_counts = std::move(null_value_counts);
    }
    if (has_min_max) {
        file_metrics.lower_bounds = std::move(lower_bounds);
        file_metrics.upper_bounds = std::move(upper_bounds);
    }

    return file_metrics;
}

ParquetFileWriter::ParquetFileWriter(const std::string& location,
                                     std::unique_ptr<parquet::ParquetOutputStream> output_stream,
                                     const std::vector<std::string>& column_names,
                                     const std::vector<TypeDescriptor>& type_descs,
                                     std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                                     const std::shared_ptr<ParquetWriterOptions>& writer_options,
                                     const std::function<void()> rollback_action, PriorityThreadPool* executors)
        : _location(location),
          _output_stream(std::move(output_stream)),
          _column_names(column_names),
          _type_descs(type_descs),
          _column_evaluators(std::move(column_evaluators)),
          _writer_options(writer_options),
          _rollback_action(std::move(rollback_action)),
          _executors(executors) {}

arrow::Result<std::shared_ptr<::parquet::schema::GroupNode>> ParquetFileWriter::_make_schema(
        const vector<std::string>& column_names, const vector<TypeDescriptor>& type_descs,
        const std::vector<FileColumnId>& file_column_ids) {
    ::parquet::schema::NodeVector fields;
    for (int i = 0; i < type_descs.size(); i++) {
        ARROW_ASSIGN_OR_RAISE(auto node, _make_schema_node(column_names[i], type_descs[i],
                                                           ::parquet::Repetition::OPTIONAL, file_column_ids[i]))
        DCHECK(node != nullptr);
        fields.push_back(std::move(node));
    }
    return std::static_pointer_cast<::parquet::schema::GroupNode>(
            ::parquet::schema::GroupNode::Make("table", ::parquet::Repetition::REQUIRED, std::move(fields)));
}

arrow::Result<::parquet::schema::NodePtr> ParquetFileWriter::_make_schema_node(const std::string& name,
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

Status ParquetFileWriter::init() {
    for (auto& e : _column_evaluators) {
        RETURN_IF_ERROR(e->init());
    }
    _eval_func = [&](Chunk* chunk, size_t col_idx) { return _column_evaluators[col_idx]->evaluate(chunk); };

    auto status = [&]() {
        if (_writer_options->column_ids.has_value()) {
            ARROW_ASSIGN_OR_RAISE(_schema,
                                  _make_schema(_column_names, _type_descs, _writer_options->column_ids.value()));
        } else {
            std::vector<FileColumnId> column_ids(_type_descs.size());
            ARROW_ASSIGN_OR_RAISE(_schema, _make_schema(_column_names, _type_descs, column_ids));
        }
        return arrow::Status::OK();
    }();

    if (!status.ok()) {
        return Status::NotSupported(status.message());
    }

    _properties = std::make_unique<::parquet::WriterProperties::Builder>()
                          ->data_pagesize(_writer_options->page_size)
                          ->write_batch_size(_writer_options->write_batch_size)
                          ->dictionary_pagesize_limit(_writer_options->dictionary_pagesize)
                          ->build();

    _writer = ::parquet::ParquetFileWriter::Open(_output_stream, _schema, _properties);
    return Status::OK();
}

ParquetFileWriter::~ParquetFileWriter() {
    try {
        _output_stream->Close();
    } catch (...) {
    }
}

ParquetFileWriterFactory::ParquetFileWriterFactory(std::shared_ptr<FileSystem> fs, const string& format,
                                                   const std::map<std::string, std::string>& options,
                                                   const vector<std::string>& column_names,
                                                   vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                                                   std::optional<std::vector<formats::FileColumnId>> field_ids,
                                                   PriorityThreadPool* executors)
        : _fs(std::move(fs)),
          _format(format),
          _field_ids(field_ids),
          _options(options),
          _column_names(column_names),
          _column_evaluators(std::move(column_evaluators)),
          _executors(executors) {}

Status ParquetFileWriterFactory::_init() {
    RETURN_IF_ERROR(ColumnEvaluator::init(_column_evaluators));
    _parsed_options = std::make_shared<ParquetWriterOptions>();
    _parsed_options->column_ids = _field_ids;
    return Status::OK();
}

StatusOr<std::shared_ptr<FileWriter>> ParquetFileWriterFactory::create(const string& path) {
    if (_parsed_options == nullptr) {
        RETURN_IF_ERROR(_init());
    }

    ASSIGN_OR_RETURN(auto file, _fs->new_writable_file(path));
    auto rollback_action = [fs = _fs, path = path]() {
        WARN_IF_ERROR(ignore_not_found(fs->delete_file(path)), "fail to delete file");
    };
    auto column_evaluators = ColumnEvaluator::clone(_column_evaluators);
    auto types = ColumnEvaluator::types(_column_evaluators);
    auto output_stream = std::make_unique<parquet::ParquetOutputStream>(std::move(file));
    return std::make_shared<ParquetFileWriter>(path, std::move(output_stream), _column_names, types,
                                               std::move(column_evaluators), _parsed_options, rollback_action,
                                               _executors);
}

} // namespace starrocks::formats
