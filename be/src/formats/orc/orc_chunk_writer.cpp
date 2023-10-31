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

#include "formats/orc/orc_chunk_writer.h"

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

/*
** ORCOutputStream
*/
OrcOutputStream::OrcOutputStream(std::unique_ptr<starrocks::WritableFile> wfile) : _wfile(std::move(wfile)) {}

OrcOutputStream::~OrcOutputStream() {
    if (!_is_closed) {
        close();
    }
}

uint64_t OrcOutputStream::getLength() const {
    return _wfile->size();
}

uint64_t OrcOutputStream::getNaturalWriteSize() const {
    return config::vector_chunk_size;
}

const std::string& OrcOutputStream::getName() const {
    return _wfile->filename();
}

void OrcOutputStream::write(const void* buf, size_t length) {
    if (_is_closed) {
        throw "The output stream is closed but there are still inputs";
    }
    const char* ch = reinterpret_cast<const char*>(buf);
    Status st = _wfile->append(Slice(ch, length));
    if (!st.ok()) {
        throw "write to orc failed: " + st.to_string();
    }
    return;
}

void OrcOutputStream::close() {
    if (_is_closed) {
        throw "The output stream is already closed";
    }
    Status st = _wfile->close();
    if (!st.ok()) {
        throw "close orc output stream failed: " + st.to_string();
    }
    _is_closed = true;
    return;
}

OrcChunkWriter::OrcChunkWriter(std::unique_ptr<WritableFile> writable_file,
                               std::shared_ptr<orc::WriterOptions> writer_options, std::shared_ptr<orc::Type> schema,
                               const std::vector<ExprContext*>& output_expr_ctxs)
        : _output_stream(std::move(writable_file)) {
    _type_descs.reserve(output_expr_ctxs.size());
    for (auto expr : output_expr_ctxs) {
        _type_descs.push_back(expr->root()->type());
    }
    _writer_options = writer_options;
    _schema = schema;
}

Status OrcChunkWriter::set_compression(const TCompressionType::type& compression_type) {
    switch (compression_type) {
    case TCompressionType::SNAPPY: {
        _writer_options->setCompression(orc::CompressionKind::CompressionKind_SNAPPY);
        break;
    }
    case TCompressionType::ZLIB: {
        _writer_options->setCompression(orc::CompressionKind::CompressionKind_ZLIB);
        break;
    }
    case TCompressionType::ZSTD: {
        _writer_options->setCompression(orc::CompressionKind::CompressionKind_ZSTD);
        break;
    }
    case TCompressionType::LZ4: {
        _writer_options->setCompression(orc::CompressionKind::CompressionKind_LZ4);
        break;
    }
    case TCompressionType::LZO: {
        _writer_options->setCompression(orc::CompressionKind::CompressionKind_LZO);
        break;
    }
    case TCompressionType::NO_COMPRESSION: {
        _writer_options->setCompression(orc::CompressionKind::CompressionKind_NONE);
        break;
    }
    default:
        return Status::NotSupported("The Compression Type is not supported");
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<orc::Type>> OrcChunkWriter::_get_orc_type(const TypeDescriptor& type_desc) {
    switch (type_desc.type) {
    case TYPE_BOOLEAN: {
        return orc::createPrimitiveType(orc::TypeKind::BOOLEAN);
    }
    case TYPE_TINYINT:
        [[fallthrough]];
    case TYPE_UNSIGNED_TINYINT: {
        return orc::createPrimitiveType(orc::TypeKind::BYTE);
    }
    case TYPE_SMALLINT:
        [[fallthrough]];
    case TYPE_UNSIGNED_SMALLINT: {
        return orc::createPrimitiveType(orc::TypeKind::SHORT);
    }
    case TYPE_INT:
        [[fallthrough]];
    case TYPE_UNSIGNED_INT: {
        return orc::createPrimitiveType(orc::TypeKind::INT);
    }
    case TYPE_BIGINT:
        [[fallthrough]];
    case TYPE_UNSIGNED_BIGINT: {
        return orc::createPrimitiveType(orc::TypeKind::LONG);
    }
    case TYPE_FLOAT: {
        return orc::createPrimitiveType(orc::TypeKind::FLOAT);
    }
    case TYPE_DOUBLE: {
        return orc::createPrimitiveType(orc::TypeKind::DOUBLE);
    }
    case TYPE_CHAR:
        [[fallthrough]];
    case TYPE_VARCHAR: {
        return orc::createPrimitiveType(orc::TypeKind::STRING);
    }
    case TYPE_DECIMAL:
        [[fallthrough]];
    case TYPE_DECIMAL32:
        [[fallthrough]];
    case TYPE_DECIMAL64:
        [[fallthrough]];
    case TYPE_DECIMAL128: {
        return orc::createDecimalType(type_desc.precision, type_desc.scale);
    }
    case TYPE_DATE: {
        return orc::createPrimitiveType(orc::TypeKind::DATE);
    }
    case TYPE_DATETIME:
        [[fallthrough]];
    case TYPE_TIME: {
        return orc::createPrimitiveType(orc::TypeKind::TIMESTAMP);
    }
    case TYPE_STRUCT: {
        auto struct_type = orc::createStructType();
        for (size_t i = 0; i < type_desc.children.size(); ++i) {
            const TypeDescriptor& child_type = type_desc.children[i];
            ASSIGN_OR_RETURN(std::unique_ptr<orc::Type> child_orc_type, OrcChunkWriter::_get_orc_type(child_type));
            struct_type->addStructField(type_desc.field_names[i], std::move(child_orc_type));
        }
        return struct_type;
    }
    case TYPE_MAP: {
        const TypeDescriptor& key_type = type_desc.children[0];
        const TypeDescriptor& value_type = type_desc.children[1];
        if (key_type.is_unknown_type() || value_type.is_unknown_type()) {
            return Status::InternalError("This data type in MAP is not supported by ORC");
        }
        ASSIGN_OR_RETURN(std::unique_ptr<orc::Type> key_orc_type, OrcChunkWriter::_get_orc_type(key_type));
        ASSIGN_OR_RETURN(std::unique_ptr<orc::Type> value_orc_type, OrcChunkWriter::_get_orc_type(value_type));
        return orc::createMapType(std::move(key_orc_type), std::move(value_orc_type));
    }
    case TYPE_ARRAY: {
        const TypeDescriptor& child_type = type_desc.children[0];
        ASSIGN_OR_RETURN(std::unique_ptr<orc::Type> child_orc_type, OrcChunkWriter::_get_orc_type(child_type));
        return orc::createListType(std::move(child_orc_type));
    }
    default:
        return Status::InternalError("This data type is not supported by ORC");
    }
}

Status OrcChunkWriter::write(Chunk* chunk) {
    if (!_writer) {
        _writer = orc::createWriter(*_schema, &_output_stream, *_writer_options);
    }
    size_t num_rows = chunk->num_rows();
    size_t num_columns = chunk->num_columns();

    auto columns = chunk->columns();

    _batch = _writer->createRowBatch(num_rows);
    orc::StructVectorBatch& root = dynamic_cast<orc::StructVectorBatch&>(*_batch);

    for (size_t i = 0; i < num_columns; ++i) {
        RETURN_IF_ERROR(_write_column(*root.fields[i], columns[i], _type_descs[i]));
    }

    root.numElements = num_rows;
    RETURN_IF_ERROR(_flush_batch());
    return Status::OK();
}

Status OrcChunkWriter::_flush_batch() {
    if (!_writer) {
        return Status::InternalError("ORC Writer is not initialized");
    }
    if (!_batch) {
        return Status::InternalError("ORC Batch is empty");
    }
    _writer->add(*_batch);
    return Status::OK();
}

Status OrcChunkWriter::_write_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column,
                                     const TypeDescriptor& type_desc) {
    switch (type_desc.type) {
    case TYPE_BOOLEAN: {
        _write_number<TYPE_BOOLEAN, orc::LongVectorBatch>(orc_column, column);
        break;
    }
    case TYPE_TINYINT: {
        _write_number<TYPE_TINYINT, orc::LongVectorBatch>(orc_column, column);
        break;
    }
    case TYPE_SMALLINT: {
        _write_number<TYPE_SMALLINT, orc::LongVectorBatch>(orc_column, column);
        break;
    }
    case TYPE_INT: {
        _write_number<TYPE_INT, orc::LongVectorBatch>(orc_column, column);
        break;
    }
    case TYPE_BIGINT: {
        _write_number<TYPE_BIGINT, orc::LongVectorBatch>(orc_column, column);
        break;
    }
    case TYPE_FLOAT: {
        _write_number<TYPE_FLOAT, orc::DoubleVectorBatch>(orc_column, column);
    }
    case TYPE_DOUBLE: {
        _write_number<TYPE_DOUBLE, orc::DoubleVectorBatch>(orc_column, column);
    }
    case TYPE_CHAR:
        [[fallthrough]];
    case TYPE_VARCHAR: {
        _write_string(orc_column, column);
        break;
    }
    case TYPE_DECIMAL: {
        _write_decimal(orc_column, column, type_desc.precision, type_desc.scale);
        break;
    }
    case TYPE_DECIMAL32: {
        _write_decimal32or64or128<TYPE_DECIMAL32, orc::Decimal64VectorBatch, int64_t>(
                orc_column, column, type_desc.precision, type_desc.scale);
        break;
    }
    case TYPE_DECIMAL64: {
        _write_decimal32or64or128<TYPE_DECIMAL64, orc::Decimal64VectorBatch, int64_t>(
                orc_column, column, type_desc.precision, type_desc.scale);
        break;
    }
    case TYPE_DECIMAL128: {
        _write_decimal32or64or128<TYPE_DECIMAL128, orc::Decimal128VectorBatch, int128_t>(
                orc_column, column, type_desc.precision, type_desc.scale);
        break;
    }
    case TYPE_DATE: {
        _write_date(orc_column, column);
        break;
    }
    case TYPE_DATETIME: {
        _write_datetime(orc_column, column);
        break;
    }
    case TYPE_ARRAY: {
        RETURN_IF_ERROR(_write_array_column(orc_column, column, type_desc));
        break;
    }
    case TYPE_STRUCT: {
        RETURN_IF_ERROR(_write_struct_column(orc_column, column, type_desc));
        break;
    }
    case TYPE_MAP: {
        RETURN_IF_ERROR(_write_map_column(orc_column, column, type_desc));
        break;
    }
    default:
        return Status::NotSupported(strings::Substitute("Type $0 not supported", type_desc.type));
    }
    return Status::OK();
}

template <LogicalType Type, typename VectorBatchType>
void OrcChunkWriter::_write_number(orc::ColumnVectorBatch& orc_column, ColumnPtr& column) {
    auto& number_orc_column = dynamic_cast<VectorBatchType&>(orc_column);
    auto column_size = column->size();

    number_orc_column.resize(column_size);
    number_orc_column.notNull.resize(column_size);

    if (column->is_nullable()) {
        auto c = ColumnHelper::as_raw_column<NullableColumn>(column);
        auto* nulls = c->null_column()->get_data().data();
        auto* values = ColumnHelper::cast_to_raw<Type>(c->data_column())->get_data().data();

        for (size_t i = 0; i < column_size; ++i) {
            // redundant copy for auto-vectorization
            number_orc_column.notNull[i] = !nulls[i];
            number_orc_column.data[i] = values[i];
        }
        orc_column.hasNulls = true;
    } else {
        auto* values = ColumnHelper::cast_to_raw<Type>(column)->get_data().data();
        for (size_t i = 0; i < column_size; ++i) {
            number_orc_column.data[i] = values[i];
        }
        memset(number_orc_column.notNull.data(), 1, column_size * sizeof(char));
    }
    number_orc_column.numElements = column_size;
}

void OrcChunkWriter::_write_string(orc::ColumnVectorBatch& orc_column, ColumnPtr& column) {
    auto& string_orc_column = dynamic_cast<orc::StringVectorBatch&>(orc_column);
    auto column_size = column->size();

    string_orc_column.resize(column_size);
    string_orc_column.notNull.resize(column_size);

    if (column->is_nullable()) {
        auto* c = ColumnHelper::as_raw_column<NullableColumn>(column);
        auto* nulls = c->null_column()->get_data().data();
        auto* values = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(c->data_column());

        for (size_t i = 0; i < column_size; ++i) {
            if (nulls[i]) {
                string_orc_column.notNull[i] = 0;
                continue;
            }
            string_orc_column.notNull[i] = 1;
            auto slice = values->get_slice(i);
            string_orc_column.data[i] = const_cast<char*>(slice.get_data());
            string_orc_column.length[i] = slice.get_size();
        }
        orc_column.hasNulls = true;
    } else {
        auto* str_column = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(column);

        for (size_t i = 0; i < column_size; ++i) {
            auto slice = str_column->get_slice(i);

            string_orc_column.data[i] = const_cast<char*>(slice.get_data());
            string_orc_column.length[i] = string_orc_column.length[i] = slice.get_size();
        }
        memset(string_orc_column.notNull.data(), 1, column_size * sizeof(char));
    }
    string_orc_column.numElements = column_size;
}

template <LogicalType DecimalType, typename VectorBatchType, typename T>
void OrcChunkWriter::_write_decimal32or64or128(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, int precision,
                                               int scale) {
    auto& decimal_orc_column = dynamic_cast<VectorBatchType&>(orc_column);
    auto column_size = column->size();
    using Type = RunTimeCppType<DecimalType>;

    decimal_orc_column.resize(column_size);
    decimal_orc_column.notNull.resize(column_size);

    decimal_orc_column.precision = precision;
    decimal_orc_column.scale = scale;

    if (column->is_nullable()) {
        auto c = ColumnHelper::as_raw_column<NullableColumn>(column);
        auto* nulls = c->null_column()->get_data().data();
        auto* values = ColumnHelper::cast_to_raw<DecimalType>(c->data_column())->get_data().data();

        for (size_t i = 0; i < column_size; ++i) {
            if (nulls[i]) {
                decimal_orc_column.notNull[i] = 0;
                continue;
            }
            decimal_orc_column.notNull[i] = 1;
            T value;
            DecimalV3Cast::to_decimal_trivial<Type, T, false>(values[i], &value);
            if constexpr (std::is_same_v<T, int128_t>) {
                auto high64Bits = static_cast<int64_t>(value >> 64);
                auto low64Bits = static_cast<uint64_t>(value);
                decimal_orc_column.values[i] = orc::Int128{high64Bits, low64Bits};
            } else {
                decimal_orc_column.values[i] = value;
            }
        }
        orc_column.hasNulls = true;
    } else {
        auto* values = ColumnHelper::cast_to_raw<DecimalType>(column)->get_data().data();
        for (size_t i = 0; i < column_size; ++i) {
            T value;
            DecimalV3Cast::to_decimal_trivial<Type, T, false>(values[i], &value);
            if constexpr (std::is_same_v<T, int128_t>) {
                int64_t high64Bits = static_cast<int64_t>(value >> 64);
                uint64_t low64Bits = static_cast<uint64_t>(value);
                decimal_orc_column.values[i] = orc::Int128{high64Bits, low64Bits};
            } else {
                decimal_orc_column.values[i] = value;
            }
        }
        memset(decimal_orc_column.notNull.data(), 1, column_size * sizeof(char));
    }
    decimal_orc_column.numElements = column_size;
}

void OrcChunkWriter::_write_decimal(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, int precision, int scale) {
    auto& decimal_orc_column = dynamic_cast<orc::Decimal128VectorBatch&>(orc_column);
    auto column_size = column->size();

    decimal_orc_column.resize(column_size);
    decimal_orc_column.notNull.resize(column_size);

    decimal_orc_column.precision = precision;
    decimal_orc_column.scale = scale;

    if (column->is_nullable()) {
        auto c = ColumnHelper::as_raw_column<NullableColumn>(column);
        auto* nulls = c->null_column()->get_data().data();
        auto* values =
                reinterpret_cast<int128_t*>(down_cast<DecimalColumn*>(c->data_column().get())->get_data().data());

        for (size_t i = 0; i < column_size; ++i) {
            if (nulls[i]) {
                decimal_orc_column.notNull[i] = 0;
                continue;
            }
            decimal_orc_column.notNull[i] = 1;
            decimal_orc_column.values[i] = orc::Int128(values[i] >> 64, (values[i] << 64) >> 64);
        }
        orc_column.hasNulls = true;
    } else {
        auto* values = reinterpret_cast<int128_t*>(down_cast<DecimalColumn*>(column.get())->get_data().data());
        for (size_t i = 0; i < column_size; ++i) {
            decimal_orc_column.values[i] = orc::Int128(values[i] >> 64, (values[i] << 64) >> 64);
        }
        memset(decimal_orc_column.notNull.data(), 1, column_size * sizeof(char));
    }
    decimal_orc_column.numElements = column_size;
}

void OrcChunkWriter::_write_date(orc::ColumnVectorBatch& orc_column, ColumnPtr& column) {
    auto& date_orc_column = dynamic_cast<orc::LongVectorBatch&>(orc_column);
    auto column_size = column->size();

    date_orc_column.resize(column_size);
    date_orc_column.notNull.resize(column_size);

    if (column->is_nullable()) {
        auto c = ColumnHelper::as_raw_column<NullableColumn>(column);
        auto* nulls = c->null_column()->get_data().data();
        auto* values = ColumnHelper::cast_to_raw<TYPE_DATE>(c->data_column())->get_data().data();

        for (size_t i = 0; i < column_size; ++i) {
            if (nulls[i]) {
                date_orc_column.notNull[i] = 0;
                continue;
            }
            date_orc_column.notNull[i] = 1;
            date_orc_column.data[i] = OrcDateHelper::native_date_to_orc_date(values[i]);
        }
        orc_column.hasNulls = true;
    } else {
        auto* values = ColumnHelper::cast_to_raw<TYPE_DATE>(column)->get_data().data();
        for (size_t i = 0; i < column_size; ++i) {
            date_orc_column.data[i] = OrcDateHelper::native_date_to_orc_date(values[i]);
        }
        memset(date_orc_column.notNull.data(), 1, column_size * sizeof(char));
    }
    date_orc_column.numElements = column_size;
}

void OrcChunkWriter::_write_datetime(orc::ColumnVectorBatch& orc_column, ColumnPtr& column) {
    auto& timestamp_orc_column = dynamic_cast<orc::TimestampVectorBatch&>(orc_column);
    auto column_size = column->size();

    timestamp_orc_column.resize(column_size);
    timestamp_orc_column.notNull.resize(column_size);

    if (column->is_nullable()) {
        auto c = ColumnHelper::as_raw_column<NullableColumn>(column);
        auto* nulls = c->null_column()->get_data().data();
        auto* values = ColumnHelper::cast_to_raw<TYPE_DATETIME>(c->data_column())->get_data().data();

        for (size_t i = 0; i < column_size; ++i) {
            if (nulls[i]) {
                timestamp_orc_column.notNull[i] = 0;
                continue;
            }
            timestamp_orc_column.notNull[i] = 1;
            OrcTimestampHelper::native_ts_to_orc_ts(values[i], timestamp_orc_column.data[i],
                                                    timestamp_orc_column.nanoseconds[i]);
        }
        orc_column.hasNulls = true;
    } else {
        auto* values = ColumnHelper::cast_to_raw<TYPE_DATETIME>(column)->get_data().data();
        for (size_t i = 0; i < column_size; ++i) {
            OrcTimestampHelper::native_ts_to_orc_ts(values[i], timestamp_orc_column.data[i],
                                                    timestamp_orc_column.nanoseconds[i]);
        }
        memset(timestamp_orc_column.notNull.data(), 1, column_size * sizeof(char));
    }
    timestamp_orc_column.numElements = column_size;
}

Status OrcChunkWriter::_write_array_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column,
                                           const TypeDescriptor& type) {
    auto& array_orc_column = dynamic_cast<orc::ListVectorBatch&>(orc_column);
    auto column_size = column->size();

    array_orc_column.resize(column_size);
    array_orc_column.notNull.resize(column_size);
    auto& value_orc_column = *array_orc_column.elements;

    if (column->is_nullable()) {
        auto* col_nullable = down_cast<NullableColumn*>(column.get());
        auto* array_cols = down_cast<ArrayColumn*>(col_nullable->data_column().get());
        uint32_t* offsets = array_cols->offsets_column().get()->get_data().data();
        auto* nulls = col_nullable->null_column()->get_data().data();

        array_orc_column.offsets[0] = offsets[0];
        for (size_t i = 0; i < column_size; ++i) {
            array_orc_column.offsets[i + 1] = offsets[i + 1];
            array_orc_column.notNull[i] = !nulls[i];
        }

        RETURN_IF_ERROR(_write_column(value_orc_column, array_cols->elements_column(), type.children[0]));
        array_orc_column.hasNulls = true;
    } else {
        auto* array_cols = down_cast<ArrayColumn*>(column.get());
        uint32_t* offsets = array_cols->offsets_column().get()->get_data().data();

        array_orc_column.offsets[0] = offsets[0];
        for (size_t i = 0; i < column_size; ++i) {
            array_orc_column.offsets[i + 1] = offsets[i + 1];
        }
        memset(array_orc_column.notNull.data(), 1, column_size * sizeof(char));

        RETURN_IF_ERROR(_write_column(value_orc_column, array_cols->elements_column(), type.children[0]));
    }
    array_orc_column.numElements = column_size;
    return Status::OK();
}

Status OrcChunkWriter::_write_struct_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column,
                                            const TypeDescriptor& type) {
    auto& struct_orc_column = dynamic_cast<orc::StructVectorBatch&>(orc_column);
    auto column_size = column->size();

    struct_orc_column.resize(column_size);
    struct_orc_column.notNull.resize(column_size);

    if (column->is_nullable()) {
        auto* col_nullable = down_cast<NullableColumn*>(column.get());
        auto* struct_cols = down_cast<StructColumn*>(col_nullable->data_column().get());
        auto* nulls = col_nullable->null_column()->get_data().data();
        Columns& field_columns = struct_cols->fields_column();

        for (size_t i = 0; i < column_size; ++i) {
            struct_orc_column.notNull[i] = !nulls[i];
        }

        for (size_t i = 0; i < type.children.size(); ++i) {
            RETURN_IF_ERROR(_write_column(*struct_orc_column.fields[i], field_columns[i], type.children[i]));
        }
        struct_orc_column.hasNulls = true;
    } else {
        auto* struct_cols = down_cast<StructColumn*>(column.get());
        Columns& field_columns = struct_cols->fields_column();

        for (size_t i = 0; i < type.children.size(); ++i) {
            RETURN_IF_ERROR(_write_column(*struct_orc_column.fields[i], field_columns[i], type.children[i]));
        }
        memset(struct_orc_column.notNull.data(), 1, column_size * sizeof(char));
    }
    struct_orc_column.numElements = column_size;
    return Status::OK();
}

Status OrcChunkWriter::_write_map_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column,
                                         const TypeDescriptor& type) {
    auto& map_orc_column = dynamic_cast<orc::MapVectorBatch&>(orc_column);
    size_t column_size = column->size();

    orc::ColumnVectorBatch& keys_orc_column = *map_orc_column.keys;
    orc::ColumnVectorBatch& values_orc_column = *map_orc_column.elements;

    if (column->is_nullable()) {
        auto* col_nullable = down_cast<NullableColumn*>(column.get());
        auto* col_map = down_cast<MapColumn*>(col_nullable->data_column().get());
        uint32_t* offsets = col_map->offsets_column().get()->get_data().data();
        auto* nulls = col_nullable->null_column()->get_data().data();

        ColumnPtr& keys = col_map->keys_column();
        ColumnPtr& values = col_map->values_column();

        map_orc_column.resize(column_size);
        map_orc_column.offsets[0] = 0;

        for (size_t i = 0; i < column_size; ++i) {
            map_orc_column.offsets[i + 1] = offsets[i + 1];
            map_orc_column.notNull[i] = !nulls[i];
        }

        RETURN_IF_ERROR(_write_column(keys_orc_column, keys, type.children[0]));
        RETURN_IF_ERROR(_write_column(values_orc_column, values, type.children[1]));
        map_orc_column.hasNulls = true;
    } else {
        auto* col_map = down_cast<MapColumn*>(column.get());
        uint32_t* offsets = col_map->offsets_column().get()->get_data().data();

        ColumnPtr& keys = col_map->keys_column();
        ColumnPtr& values = col_map->values_column();

        for (size_t i = 0; i < column_size; ++i) {
            map_orc_column.offsets[i + 1] = offsets[i + 1];
        }
        memset(map_orc_column.notNull.data(), 1, column_size * sizeof(char));

        RETURN_IF_ERROR(_write_column(keys_orc_column, keys, type.children[0]));
        RETURN_IF_ERROR(_write_column(values_orc_column, values, type.children[1]));
    }
    map_orc_column.numElements = column->size();
    return Status::OK();
}

void OrcChunkWriter::close() {
    _writer->close();
}

StatusOr<std::unique_ptr<orc::Type>> OrcChunkWriter::make_schema(const std::vector<std::string>& file_column_names,
                                                                 const std::vector<TypeDescriptor>& type_descs) {
    auto schema = orc::createStructType();
    for (size_t i = 0; i < type_descs.size(); ++i) {
        ASSIGN_OR_RETURN(std::unique_ptr<orc::Type> field_type, OrcChunkWriter::_get_orc_type(type_descs[i]));
        schema->addStructField(file_column_names[i], std::move(field_type));
    }
    return schema;
}

/*
** AsyncOrcChunkWriter
*/
AsyncOrcChunkWriter::AsyncOrcChunkWriter(std::unique_ptr<WritableFile> writable_file,
                                         std::shared_ptr<orc::WriterOptions> writer_options,
                                         std::shared_ptr<orc::Type> schema,
                                         const std::vector<ExprContext*>& output_expr_ctxs,
                                         PriorityThreadPool* executor_pool, RuntimeProfile* parent_profile)
        : OrcChunkWriter(std::move(writable_file), writer_options, schema, output_expr_ctxs),
          _executor_pool(executor_pool),
          _parent_profile(parent_profile) {
    _io_timer = ADD_TIMER(_parent_profile, "OrcChunkWriterIoTimer");
};

Status AsyncOrcChunkWriter::_flush_batch() {
    {
        auto lock = std::unique_lock(_lock);
        _batch_closing = true;
    }

    bool finish = _executor_pool->try_offer([&]() {
        SCOPED_TIMER(_io_timer);
        if (_batch != nullptr) {
            _writer->add(*_batch);
            _batch = nullptr;
        }
        {
            auto lock = std::unique_lock(_lock);
            _batch_closing = false;
        }
        _cv.notify_one();
    });

    if (!finish) {
        {
            auto lock = std::unique_lock(_lock);
            _batch_closing = false;
        }
        _cv.notify_one();
        auto st = Status::ResourceBusy("submit flush batch task fails");
        return st;
    }
    return Status::OK();
}

Status AsyncOrcChunkWriter::close(RuntimeState* state,
                                  const std::function<void(AsyncOrcChunkWriter*, RuntimeState*)>& cb) {
    bool ret = _executor_pool->try_offer([&, state, cb]() {
        SCOPED_TIMER(_io_timer);
        {
            auto lock = std::unique_lock(_lock);
            _cv.wait(lock, [&] { return !_batch_closing; });
        }
        _writer->close();
        _batch = nullptr;
        if (cb != nullptr) {
            cb(this, state);
        }
        _closed.store(true);
        return Status::OK();
    });

    if (!ret) {
        return Status::InternalError("Submit close file error");
    }
    return Status::OK();
}

bool AsyncOrcChunkWriter::writable() {
    auto lock = std::unique_lock(_lock);
    return !_batch_closing;
}

} // namespace starrocks
