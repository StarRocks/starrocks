
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
#include "gutil/casts.h"
#include "gutil/endian.h"
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
                              _make_schema_node(file_column_names[i], type_descs[i],
                                                ::parquet::Repetition::OPTIONAL));
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
    case TYPE_FLOAT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::None(),
                                                      ::parquet::Type::FLOAT);
    }
    case TYPE_DOUBLE: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::None(),
                                                      ::parquet::Type::DOUBLE);
    }
    case TYPE_TINYINT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(8, true),
                                                      ::parquet::Type::INT32);
    }
    case TYPE_UNSIGNED_TINYINT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(8, false),
                                                      ::parquet::Type::INT32);
    }
    case TYPE_SMALLINT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(16, true),
                                                      ::parquet::Type::INT32);
    }
    case TYPE_UNSIGNED_SMALLINT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(16, false),
                                                      ::parquet::Type::INT32);
    }
    case TYPE_INT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(32, true),
                                                      ::parquet::Type::INT32);
    }
    case TYPE_UNSIGNED_INT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(32, false),
                                                      ::parquet::Type::INT32);
    }
    case TYPE_BIGINT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(64, true),
                                                      ::parquet::Type::INT64);
    }
    case TYPE_UNSIGNED_BIGINT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(64, false),
                                                      ::parquet::Type::INT64);
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::String(),
                                                      ::parquet::Type::BYTE_ARRAY);
    case TYPE_DATE: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Date(),
                                                      ::parquet::Type::INT32);
    }
    case TYPE_DATETIME: {
        return ::parquet::schema::PrimitiveNode::Make(
                name, rep_type, ::parquet::LogicalType::Timestamp(true, ::parquet::LogicalType::TimeUnit::unit::MILLIS),
                ::parquet::Type::INT64);
    }
    case TYPE_DECIMAL32:
        return ::parquet::schema::PrimitiveNode::Make(
                name, rep_type, ::parquet::LogicalType::Decimal(type_desc.precision, type_desc.scale),
                ::parquet::Type::INT32);
    case TYPE_DECIMAL64:
        return ::parquet::schema::PrimitiveNode::Make(
                name, rep_type, ::parquet::LogicalType::Decimal(type_desc.precision, type_desc.scale),
                ::parquet::Type::INT64);
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

std::string ChunkWriter::Context::debug_string() const {
    std::stringstream ss;
    for (size_t i = 0; i < size(); ++i) {
        ss << "(" << _idx2subcol[i] << ", " << _def_levels[i] << ", " << _rep_levels[i] << ")\n";
    }
    return ss.str();
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
    if (_chunk_writer == nullptr) {
        DCHECK(_writer != nullptr);
        auto rg_writer = _writer->AppendBufferedRowGroup();
        _chunk_writer = std::make_unique<ChunkWriter>(rg_writer, _type_descs, _schema);
    }
}

Status FileWriterBase::write(Chunk* chunk) {
    if (!chunk->has_rows()) {
        return Status::OK();
    }

    _generate_chunk_writer();
    _chunk_writer->write(chunk);

    if (_chunk_writer->estimated_buffered_bytes() > _max_row_group_size) {
        _flush_row_group();
    }

    return Status::OK();
}

ChunkWriter::ChunkWriter(::parquet::RowGroupWriter* rg_writer, const std::vector<TypeDescriptor>& type_descs,
                         const std::shared_ptr<::parquet::schema::GroupNode>& schema)
        : _rg_writer(rg_writer), _type_descs(type_descs), _schema(schema) {
    int num_columns = rg_writer->num_columns();
    _estimated_buffered_bytes.resize(num_columns);
    std::fill(_estimated_buffered_bytes.begin(), _estimated_buffered_bytes.end(), 0);
}

Status ChunkWriter::write(Chunk* chunk) {
    _col_idx = 0; // reset index upon every write

    Context ctx(0, 0, chunk->num_rows());
    for (int i = 0; i < chunk->num_rows(); i++) {
        ctx.append(i, 0, 0);
    }

    for (size_t i = 0; i < chunk->num_columns(); i++) {
        auto col = chunk->get_column_by_index(i);
        auto ret = _add_column_chunk(ctx, _type_descs[i], _schema->field(i), col);
        if (!ret.ok()) {
            return ret;
        }
    }

    return Status::OK();
}

void ChunkWriter::close() {
    _rg_writer->Close();
}

// The current row group written bytes = total_bytes_written + total_compressed_bytes + estimated_bytes.
// total_bytes_written: total bytes written by the page writer
// total_compressed_types: total bytes still compressed but not written
// estimated_bytes: estimated size of all column chunk uncompressed values that are not written to a page yet. it
// mainly includes value buffer size and repetition buffer size and definition buffer value for each column.
int64_t ChunkWriter::estimated_buffered_bytes() const {
    if (_rg_writer == nullptr) {
        return 0;
    }
    // TODO(letian-jiang): check this
    auto estimated_bytes = std::accumulate(_estimated_buffered_bytes.begin(), _estimated_buffered_bytes.end(), 0);
    return _rg_writer->total_bytes_written() + _rg_writer->total_compressed_bytes() + estimated_bytes;
}

Status ChunkWriter::_add_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                                      const ::parquet::schema::NodePtr& node, const ColumnPtr& col) {
    switch (type_desc.type) {
    case TYPE_BOOLEAN: {
        return _add_boolean_column_chunk(ctx, type_desc, node, col);
    }
    case TYPE_TINYINT: {
        return _add_int_column_chunk<TYPE_TINYINT, ::parquet::Type::INT32>(ctx, type_desc, node, col);
    }
    case TYPE_UNSIGNED_TINYINT: {
        return _add_int_column_chunk<TYPE_UNSIGNED_TINYINT, ::parquet::Type::INT32>(ctx, type_desc, node, col);
    }
    case TYPE_SMALLINT: {
        return _add_int_column_chunk<TYPE_SMALLINT, ::parquet::Type::INT32>(ctx, type_desc, node, col);
    }
    case TYPE_UNSIGNED_SMALLINT: {
        return _add_int_column_chunk<TYPE_UNSIGNED_SMALLINT, ::parquet::Type::INT32>(ctx, type_desc, node, col);
    }
    case TYPE_INT: {
        return _add_int_column_chunk<TYPE_INT, ::parquet::Type::INT32>(ctx, type_desc, node, col);
    }
    case TYPE_UNSIGNED_INT: {
        return _add_int_column_chunk<TYPE_UNSIGNED_INT, ::parquet::Type::INT32>(ctx, type_desc, node, col);
    }
    case TYPE_BIGINT: {
        return _add_int_column_chunk<TYPE_BIGINT, ::parquet::Type::INT64>(ctx, type_desc, node, col);
    }
    case TYPE_UNSIGNED_BIGINT: {
        return _add_int_column_chunk<TYPE_UNSIGNED_BIGINT, ::parquet::Type::INT64>(ctx, type_desc, node, col);
    }
    case TYPE_FLOAT: {
        return _add_int_column_chunk<TYPE_FLOAT, ::parquet::Type::FLOAT>(ctx, type_desc, node, col);
    }
    case TYPE_DOUBLE: {
        return _add_int_column_chunk<TYPE_DOUBLE, ::parquet::Type::DOUBLE>(ctx, type_desc, node, col);
    }
    case TYPE_DECIMAL32: {
        return _add_int_column_chunk<TYPE_DECIMAL32, ::parquet::Type::INT32>(ctx, type_desc, node, col);
    }
    case TYPE_DECIMAL64: {
        return _add_int_column_chunk<TYPE_DECIMAL64, ::parquet::Type::INT64>(ctx, type_desc, node, col);
    }
    case TYPE_DECIMAL128: {
        return _add_decimal128_column_chunk(ctx, type_desc, node, col);
    }
    case TYPE_DATE: {
        return _add_date_column_chunk(ctx, type_desc, node, col);
    }
    case TYPE_DATETIME: {
        return _add_datetime_column_chunk(ctx, type_desc, node, col);
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        return _add_varchar_column_chunk(ctx, type_desc, node, col);
    }
    case TYPE_ARRAY: {
        return _add_array_column_chunk(ctx, type_desc, node, col);
    }
    case TYPE_MAP: {
        return _add_map_column_chunk(ctx, type_desc, node, col);
    }
    case TYPE_STRUCT: {
        return _add_struct_column_chunk(ctx, type_desc, node, col);
    }
    default: {
        return Status::NotSupported(fmt::format("Type {} is not supported", type_desc.debug_string()));
    }
    }
}

Status ChunkWriter::_add_boolean_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                                              const ::parquet::schema::NodePtr& node, const ColumnPtr& col) {
    const auto data_column = ColumnHelper::get_data_column(col.get());
    const auto raw_col = down_cast<RunTimeColumnType<TYPE_BOOLEAN>*>(data_column)->get_data().data();

    uint8_t* nulls = nullptr;
    if (col->is_nullable()) {
        const auto null_column = down_cast<NullableColumn*>(col.get())->null_column();
        nulls = null_column->get_data().data();
    }

    auto col_writer = down_cast<::parquet::BoolWriter*>(_rg_writer->column(_col_idx));
    DCHECK(col_writer != nullptr);

    DeferOp defer([&] {
        _estimated_buffered_bytes[_col_idx] = col_writer->EstimatedBufferedValueBytes();
        _col_idx++;
    });

    // Use the rep_levels in the context from caller since node is primitive.
    auto rep_levels_ptr = ctx._rep_levels.data();

    const int16_t* def_levels_ptr = nullptr;
    std::vector<int16_t> def_levels;
    if (node->is_required()) {
        // For required node, we use the def_levels in the context from caller
        def_levels_ptr = ctx._def_levels.data();
    } else {
        // For optional node, we should increment def_levels of these defined values
        DCHECK(node->is_optional());
        _populate_def_levels(def_levels, ctx, nulls);
        def_levels_ptr = def_levels.data();
    }
    DCHECK(def_levels_ptr != nullptr);

    // sizeof(bool) depends on implementation, thus we copy values to ensure correctness
    // std::vector<bool> may not provide a contiguous section of memory
    auto values = new bool[col->size()];
    int j = 0; // ptr to the next values slot to populate
    for (size_t i = 0; i < col->size(); i++) {
        if (col->is_nullable() && nulls[i]) {
            continue;
        }
        values[j++] = static_cast<bool>(raw_col[i]);
    }

    col_writer->WriteBatch(ctx.size(), def_levels_ptr, rep_levels_ptr, values);
    return Status::OK();
}

template <LogicalType lt, ::parquet::Type::type pt>
Status ChunkWriter::_add_int_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                                          const ::parquet::schema::NodePtr& node, const ColumnPtr& col) {
    const auto data_column = ColumnHelper::get_data_column(col.get());
    const auto raw_col = down_cast<RunTimeColumnType<lt>*>(data_column)->get_data().data();

    uint8_t* nulls = nullptr;
    if (col->is_nullable()) {
        const auto null_column = down_cast<NullableColumn*>(col.get())->null_column();
        nulls = null_column->get_data().data();
    }

    auto col_writer =
            down_cast<::parquet::TypedColumnWriter<::parquet::PhysicalType<pt>>*>(_rg_writer->column(_col_idx));
    DCHECK(col_writer != nullptr);

    DeferOp defer([&] {
        _estimated_buffered_bytes[_col_idx] = col_writer->EstimatedBufferedValueBytes();
        _col_idx++;
    });

    // Use the rep_levels in the context from caller since node is primitive.
    auto rep_levels_ptr = ctx._rep_levels.data();

    const int16_t* def_levels_ptr = nullptr;
    std::vector<int16_t> def_levels;
    if (node->is_required()) {
        // For required node, we use the def_levels in the context from caller
        def_levels_ptr = ctx._def_levels.data();
    } else {
        // For optional node, we should increment def_levels of these defined values
        DCHECK(node->is_optional());
        _populate_def_levels(def_levels, ctx, nulls);
        def_levels_ptr = def_levels.data();
    }
    DCHECK(def_levels_ptr != nullptr);

    // Type aliases of type in StarRocks and physical type in Parquet
    using source_type = RunTimeCppType<lt>;
    using target_type = typename ::parquet::type_traits<pt>::value_type;

    if constexpr (std::is_same_v<source_type, target_type>) {
        // If two types are identical, invoke WriteBatch without copying values
        if (!col->is_nullable() || !col->has_null()) {
            col_writer->WriteBatch(ctx.size(), def_levels_ptr, rep_levels_ptr, raw_col);
        } else {
            // Make bitset to denote not-null values
            auto null_bitset = _make_null_bitset(col->size(), nulls);
            col_writer->WriteBatchSpaced(ctx.size(), def_levels_ptr, rep_levels_ptr, null_bitset.data(), 0, raw_col);
        }
    } else {
        // If two types are different, we have to copy values anyway
        std::vector<target_type> values;
        values.reserve(col->size());
        for (size_t i = 0; i < col->size(); i++) {
            if (col->is_nullable() && nulls[i]) {
                continue;
            }
            values.push_back(static_cast<target_type>(raw_col[i]));
        }

        col_writer->WriteBatch(ctx.size(), def_levels_ptr, rep_levels_ptr, values.data());
    }
    return Status::OK();
}

Status ChunkWriter::_add_decimal128_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                                                 const ::parquet::schema::NodePtr& node, const ColumnPtr& col) {
    const auto data_column = ColumnHelper::get_data_column(col.get());
    const auto raw_col = down_cast<RunTimeColumnType<TYPE_DECIMAL128>*>(data_column)->get_data().data();

    uint8_t* nulls = nullptr;
    if (col->is_nullable()) {
        const auto null_column = down_cast<NullableColumn*>(col.get())->null_column();
        nulls = null_column->get_data().data();
    }

    auto col_writer = down_cast<::parquet::FixedLenByteArrayWriter*>(_rg_writer->column(_col_idx));
    DCHECK(col_writer != nullptr);

    DeferOp defer([&] {
        _estimated_buffered_bytes[_col_idx] = col_writer->EstimatedBufferedValueBytes();
        _col_idx++;
    });

    // Use the rep_levels in the context from caller since node is primitive.
    auto rep_levels_ptr = ctx._rep_levels.data();

    const int16_t* def_levels_ptr = nullptr;
    std::vector<int16_t> def_levels;
    if (node->is_required()) {
        // For required node, we use the def_levels in the context from caller
        def_levels_ptr = ctx._def_levels.data();
    } else {
        // For optional node, we should increment def_levels of these defined values
        DCHECK(node->is_optional());
        _populate_def_levels(def_levels, ctx, nulls);
        def_levels_ptr = def_levels.data();
    }
    DCHECK(def_levels_ptr != nullptr);

    std::vector<unsigned __int128> values;
    values.reserve(ctx.size());
    for (auto i = 0; i < ctx.size(); i++) {
        auto [idx, def_level, rep_level] = ctx.get(i);
        if (idx == Context::kNULL || (nulls != nullptr && nulls[idx])) {
            continue;
        }
        auto big_endian_value = BigEndian::FromHost128(raw_col[idx]);
        values.push_back(big_endian_value);
    }

    std::vector<::parquet::FixedLenByteArray> flba_values;
    flba_values.reserve(values.size());
    for (size_t i = 0; i < values.size(); i++) {
        auto ptr = reinterpret_cast<const uint8_t*>(values.data() + i);
        flba_values.emplace_back(ptr);
    }

    col_writer->WriteBatch(ctx.size(), def_levels_ptr, rep_levels_ptr, flba_values.data());
    return Status::OK();
}

Status ChunkWriter::_add_date_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                                           const ::parquet::schema::NodePtr& node, const ColumnPtr& col) {
    const auto data_column = ColumnHelper::get_data_column(col.get());
    auto raw_col = down_cast<const RunTimeColumnType<TYPE_DATE>*>(data_column)->get_data().data();

    uint8_t* nulls = nullptr;
    if (col->is_nullable()) {
        const auto null_column = down_cast<NullableColumn*>(col.get())->null_column();
        nulls = null_column->get_data().data();
    }

    auto col_writer = down_cast<::parquet::Int32Writer*>(_rg_writer->column(_col_idx));
    DCHECK(col_writer != nullptr);

    DeferOp defer([&] {
        _estimated_buffered_bytes[_col_idx] = col_writer->EstimatedBufferedValueBytes();
        _col_idx++;
    });

    // Use the rep_levels in the context from caller since node is primitive.
    auto rep_levels_ptr = ctx._rep_levels.data();

    const int16_t* def_levels_ptr = nullptr;
    std::vector<int16_t> def_levels;
    if (node->is_required()) {
        // For required node, we use the def_levels in the context from caller
        def_levels_ptr = ctx._def_levels.data();
    } else {
        // For optional node, we should increment def_levels of these defined values
        DCHECK(node->is_optional());
        _populate_def_levels(def_levels, ctx, nulls);
        def_levels_ptr = def_levels.data();
    }
    DCHECK(def_levels_ptr != nullptr);

    std::vector<int32_t> values;
    values.reserve(ctx.size());
    auto unix_epoch_date = DateValue::create(1970, 1, 1);
    for (auto i = 0; i < ctx.size(); i++) {
        auto [idx, def_level, rep_level] = ctx.get(i);
        if (idx == Context::kNULL || (nulls != nullptr && nulls[idx])) {
            continue;
        }
        int32_t unix_days = raw_col[idx]._julian - unix_epoch_date._julian;
        values.push_back(unix_days);
    }

    col_writer->WriteBatch(ctx.size(), def_levels_ptr, rep_levels_ptr, values.data());
    return Status::OK();
}

Status ChunkWriter::_add_datetime_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                                               const ::parquet::schema::NodePtr& node, const ColumnPtr& col) {
    const auto data_column = ColumnHelper::get_data_column(col.get());
    auto raw_col = down_cast<const RunTimeColumnType<TYPE_DATETIME>*>(data_column)->get_data().data();

    uint8_t* nulls = nullptr;
    if (col->is_nullable()) {
        const auto null_column = down_cast<NullableColumn*>(col.get())->null_column();
        nulls = null_column->get_data().data();
    }

    auto col_writer = down_cast<::parquet::Int64Writer*>(_rg_writer->column(_col_idx));
    DCHECK(col_writer != nullptr);

    DeferOp defer([&] {
        _estimated_buffered_bytes[_col_idx] = col_writer->EstimatedBufferedValueBytes();
        _col_idx++;
    });

    // Use the rep_levels in the context from caller since node is primitive.
    auto rep_levels_ptr = ctx._rep_levels.data();

    const int16_t* def_levels_ptr = nullptr;
    std::vector<int16_t> def_levels;
    if (node->is_required()) {
        // For required node, we use the def_levels in the context from caller
        def_levels_ptr = ctx._def_levels.data();
    } else {
        // For optional node, we should increment def_levels of these defined values
        DCHECK(node->is_optional());
        _populate_def_levels(def_levels, ctx, nulls);
        def_levels_ptr = def_levels.data();
    }
    DCHECK(def_levels_ptr != nullptr);

    std::vector<int64_t> values;
    values.reserve(ctx.size());
    for (auto i = 0; i < ctx.size(); i++) {
        auto [idx, def_level, rep_level] = ctx.get(i);
        if (idx == Context::kNULL || (nulls != nullptr && nulls[idx])) {
            continue;
        }
        int64_t milliseconds = raw_col[idx].to_unix_second() * 1000;
        values.push_back(milliseconds);
    }

    col_writer->WriteBatch(ctx.size(), def_levels_ptr, rep_levels_ptr, values.data());
    return Status::OK();
}

Status ChunkWriter::_add_varchar_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                                              const ::parquet::schema::NodePtr& node, const ColumnPtr& col) {
    const auto data_column = ColumnHelper::get_data_column(col.get());
    auto raw_col = down_cast<const RunTimeColumnType<TYPE_VARCHAR>*>(data_column);
    auto& vo = raw_col->get_offset();
    auto& vb = raw_col->get_bytes();

    uint8_t* nulls = nullptr;
    if (col->is_nullable()) {
        const auto null_column = down_cast<NullableColumn*>(col.get())->null_column();
        nulls = null_column->get_data().data();
    }

    auto col_writer = down_cast<::parquet::ByteArrayWriter*>(_rg_writer->column(_col_idx));
    DCHECK(col_writer != nullptr);

    DeferOp defer([&] {
        _estimated_buffered_bytes[_col_idx] = col_writer->EstimatedBufferedValueBytes();
        _col_idx++;
    });

    // Use the rep_levels in the context from caller since node is primitive.
    auto rep_levels_ptr = ctx._rep_levels.data();

    const int16_t* def_levels_ptr = nullptr;
    std::vector<int16_t> def_levels;
    if (node->is_required()) {
        // For required node, we use the def_levels in the context from caller
        def_levels_ptr = ctx._def_levels.data();
    } else {
        // For optional node, we should increment def_levels of these defined values
        DCHECK(node->is_optional());
        _populate_def_levels(def_levels, ctx, nulls);
        def_levels_ptr = def_levels.data();
    }
    DCHECK(def_levels_ptr != nullptr);

    std::vector<::parquet::ByteArray> values;
    values.reserve(ctx.size());
    for (auto i = 0; i < ctx.size(); i++) {
        auto [idx, def_level, rep_level] = ctx.get(i);
        if (idx == Context::kNULL || (nulls != nullptr && nulls[idx])) {
            continue;
        }
        auto len = static_cast<uint32_t>(vo[idx + 1] - vo[idx]);
        auto ptr = reinterpret_cast<const uint8_t*>(vb.data() + vo[idx]);
        values.emplace_back(len, ptr);
    }

    col_writer->WriteBatch(ctx.size(), def_levels_ptr, rep_levels_ptr, values.data());
    return Status::OK();
}

Status ChunkWriter::_add_array_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                                            const ::parquet::schema::NodePtr& node, const ColumnPtr& col) {
    DCHECK(type_desc.type == TYPE_ARRAY);
    auto outer_node = std::static_pointer_cast<::parquet::schema::GroupNode>(node);
    auto mid_node = std::static_pointer_cast<::parquet::schema::GroupNode>(outer_node->field(0));
    auto inner_node = mid_node->field(0);

    unsigned char* nulls = nullptr;
    if (col->is_nullable()) {
        const auto null_column = down_cast<NullableColumn*>(col.get())->null_column();
        nulls = null_column->get_data().data();
    }

    const auto& array_column = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(col.get()));
    const auto& elements = array_column->elements_column();
    const auto& offsets = array_column->offsets_column()->get_data();
    Context derived_ctx(ctx._max_def_level + node->is_optional() + 1, ctx._max_rep_level + 1,
                        ctx.size() + elements->size());

    int subcol_size = 0;
    for (auto i = 0; i < ctx.size(); i++) {
        auto [idx, def_level, rep_level] = ctx.get(i);
        if (idx == Context::kNULL || nulls[idx]) {
            derived_ctx.append(Context::kNULL, def_level, rep_level);
            continue;
        }
        auto array_size = offsets[idx + 1] - offsets[idx];
        if (array_size == 0) {
            derived_ctx.append(Context::kNULL, def_level + node->is_optional(), rep_level);
            continue;
        }
        derived_ctx.append(subcol_size++, def_level + node->is_optional() + 1, rep_level);
        for (auto offset = 1; offset < array_size; offset++) {
            derived_ctx.append(subcol_size++, def_level + node->is_optional() + 1, derived_ctx._max_rep_level);
        }
    }

    DCHECK(elements->size() == subcol_size);
    return _add_column_chunk(derived_ctx, type_desc.children[0], inner_node, elements);
}

Status ChunkWriter::_add_map_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                                          const ::parquet::schema::NodePtr& node, const ColumnPtr& col) {
    DCHECK(type_desc.type == TYPE_MAP);
    auto outer_node = std::static_pointer_cast<::parquet::schema::GroupNode>(node);
    auto mid_node = std::static_pointer_cast<::parquet::schema::GroupNode>(outer_node->field(0));
    auto key_node = mid_node->field(0);
    auto value_node = mid_node->field(1);

    unsigned char* nulls = nullptr;
    if (col->is_nullable()) {
        const auto null_column = down_cast<NullableColumn*>(col.get())->null_column();
        nulls = null_column->get_data().data();
    }

    const auto map_column = down_cast<MapColumn*>(ColumnHelper::get_data_column(col.get()));
    const auto& keys = map_column->keys_column();
    const auto& values = map_column->values_column();
    const auto& offsets = map_column->offsets_column()->get_data();

    Context derived_ctx(ctx._max_def_level + node->is_optional(), ctx._max_rep_level + 1, ctx.size() + offsets.size());

    int subcol_size = 0;
    for (auto i = 0; i < ctx.size(); i++) {
        auto [idx, def_level, rep_level] = ctx.get(i);
        if (idx == Context::kNULL || nulls[idx]) {
            derived_ctx.append(Context::kNULL, def_level, rep_level);
            continue;
        }
        auto map_size = offsets[idx + 1] - offsets[idx];
        if (map_size == 0) {
            derived_ctx.append(Context::kNULL, def_level + outer_node->is_optional(), rep_level);
            continue;
        }
        derived_ctx.append(subcol_size, def_level + outer_node->is_optional() + 1, rep_level);
        subcol_size++;
        for (auto offset = 1; offset < map_size; offset++) {
            derived_ctx.append(subcol_size, def_level + outer_node->is_optional() + 1, derived_ctx._max_rep_level);
            subcol_size++;
        }
    }

    DCHECK(keys->size() == subcol_size);
    DCHECK(values->size() == subcol_size);
    auto ret = _add_column_chunk(derived_ctx, type_desc.children[0], key_node, keys);
    if (!ret.ok()) {
        return ret;
    }
    return _add_column_chunk(derived_ctx, type_desc.children[1], value_node, values);
}

Status ChunkWriter::_add_struct_column_chunk(const Context& ctx, const TypeDescriptor& type_desc,
                                             const ::parquet::schema::NodePtr& node, const ColumnPtr& col) {
    DCHECK(type_desc.type == TYPE_STRUCT);
    auto struct_node = std::static_pointer_cast<::parquet::schema::GroupNode>(node);

    unsigned char* nulls = nullptr;
    if (col->is_nullable()) {
        const auto null_column = down_cast<NullableColumn*>(col.get())->null_column();
        nulls = null_column->get_data().data();
    }

    const auto data_column = ColumnHelper::get_data_column(col.get());
    const auto struct_column = down_cast<StructColumn*>(data_column);
    Context derived_ctx(ctx._max_def_level + node->is_optional(), ctx._max_rep_level, ctx.size() + data_column->size());

    int subcol_size = 0;
    for (auto i = 0; i < ctx.size(); i++) {
        auto [idx, def_level, rep_level] = ctx.get(i);
        if (idx == Context::kNULL) {
            derived_ctx.append(Context::kNULL, def_level, rep_level);
            continue;
        }
        if (nulls != nullptr && nulls[idx]) {
            derived_ctx.append(Context::kNULL, def_level, rep_level);
            subcol_size++;
            continue;
        }
        derived_ctx.append(subcol_size++, def_level + node->is_optional(), rep_level);
    }

    for (size_t i = 0; i < type_desc.children.size(); i++) {
        auto sub_col = struct_column->field_column(type_desc.field_names[i]);
        DCHECK(sub_col->size() == subcol_size);
        auto ret = _add_column_chunk(derived_ctx, type_desc.children[i], struct_node->field(i), sub_col);
        if (!ret.ok()) {
            return ret;
        }
    }
    return Status::OK();
}

std::vector<uint8_t> ChunkWriter::_make_null_bitset(size_t n, const uint8_t* nulls) const {
    // TODO(letian-jiang): optimize
    DCHECK(nulls != nullptr);
    std::vector<uint8_t> bitset((n + 7) / 8);
    for (size_t i = 0; i < n; i++) {
        if (!nulls[i]) {
            bitset[i / 8] |= 1 << (i % 8);
        }
    }
    return bitset;
}

void ChunkWriter::_populate_def_levels(std::vector<int16_t>& def_levels, const Context& ctx,
                                       const uint8_t* nulls) const {
    DCHECK(def_levels.empty());
    def_levels.reserve(ctx.size());
    for (auto i = 0; i < ctx.size(); i++) {
        auto [idx, def_level, rep_level] = ctx.get(i);
        if (idx == Context::kNULL || (nulls != nullptr && nulls[idx])) {
            def_levels.push_back(def_level);
            continue;
        }
        def_levels.push_back(def_level + 1);
    }
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
