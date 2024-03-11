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

#include "orc_file_writer.h"

#include <fmt/format.h>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "formats/orc/utils.h"
#include "formats/utils.h"

namespace starrocks::formats {

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
        throw std::runtime_error("The output stream is closed but there are still inputs");
    }
    const char* ch = reinterpret_cast<const char*>(buf);
    Status st = _wfile->append(Slice(ch, length));
    if (!st.ok()) {
        throw std::runtime_error("write to orc failed: " + st.to_string());
    }
    return;
}

void OrcOutputStream::close() {
    if (_is_closed) {
        return;
    }
    _is_closed = true;

    if (auto st = _wfile->close(); !st.ok()) {
        throw std::runtime_error("close orc output stream failed: " + st.to_string());
    }
}

ORCFileWriter::ORCFileWriter(const std::string& location, std::unique_ptr<OrcOutputStream> output_stream,
                             const std::vector<std::string>& column_names,
                             const std::vector<TypeDescriptor>& type_descs,
                             std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                             const std::shared_ptr<ORCWriterOptions>& writer_options,
                             const std::function<void()> rollback_action, PriorityThreadPool* executors)
        : _location(location),
          _output_stream(std::move(output_stream)),
          _column_names(column_names),
          _type_descs(type_descs),
          _column_evaluators(std::move(column_evaluators)),
          _writer_options(writer_options),
          _rollback_action(rollback_action),
          _executors(executors) {}

Status ORCFileWriter::init() {
    RETURN_IF_ERROR(ColumnEvaluator::init(_column_evaluators));
    ASSIGN_OR_RETURN(_schema, _make_schema(_column_names, _type_descs));
    _writer = orc::createWriter(*_schema, _output_stream.get(), orc::WriterOptions());
    return Status::OK();
}

int64_t ORCFileWriter::get_written_bytes() {
    // TODO(letian-jiang): fixme
    return _output_stream->getLength();
}

std::future<Status> ORCFileWriter::write(ChunkPtr chunk) {
    auto cvb = _convert(chunk);
    if (!cvb.ok()) {
        return make_ready_future(cvb.status());
    }

    // TODO(letian-jiang): aware of flush operations and execute async
    _writer->add(*cvb.value());
    _row_counter += chunk->num_rows();
    return make_ready_future(Status::OK());
}

std::future<FileWriter::CommitResult> ORCFileWriter::commit() {
    auto promise = std::make_shared<std::promise<FileWriter::CommitResult>>();
    std::future<FileWriter::CommitResult> future = promise->get_future();

    auto task = [writer = _writer, output_stream = _output_stream, p = promise, rollback = _rollback_action,
                 row_counter = _row_counter, location = _location] {
        FileWriter::CommitResult result{
                .io_status = Status::OK(), .format = ORC, .location = location, .rollback_action = rollback};
        try {
            writer->close();
        } catch (const std::exception& e) {
            result.io_status.update(Status::IOError(fmt::format("{}: {}", "close file error", e.what())));
        }

        try {
            output_stream->close();
        } catch (const std::exception& e) {
            result.io_status.update(Status::IOError(fmt::format("{}: {}", "close output stream error", e.what())));
        }

        if (result.io_status.ok()) {
            result.file_statistics.record_count = row_counter;
            result.file_statistics.file_size = output_stream->getLength();
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

StatusOr<std::unique_ptr<orc::ColumnVectorBatch>> ORCFileWriter::_convert(ChunkPtr chunk) {
    auto cvb = _writer->createRowBatch(chunk->num_rows());
    auto root = down_cast<orc::StructVectorBatch*>(cvb.get());

    for (size_t i = 0; i < _column_evaluators.size(); ++i) {
        ASSIGN_OR_RETURN(auto column, _column_evaluators[i]->evaluate(chunk.get()));
        _write_column(*root->fields[i], column, _type_descs[i]);
    }

    root->numElements = chunk->num_rows();
    return cvb;
}

void ORCFileWriter::_write_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column,
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
        _write_array_column(orc_column, column, type_desc);
        break;
    }
    case TYPE_STRUCT: {
        _write_struct_column(orc_column, column, type_desc);
        break;
    }
    case TYPE_MAP: {
        _write_map_column(orc_column, column, type_desc);
        break;
    }
    default: {
    }
    }
}

inline uint8_t* get_raw_null_column(const ColumnPtr& col) {
    if (!col->has_null()) {
        return nullptr;
    }
    auto& null_column = down_cast<NullableColumn*>(col.get())->null_column();
    auto* raw_column = null_column->get_data().data();
    return raw_column;
}

template <LogicalType lt>
inline RunTimeCppType<lt>* get_raw_data_column(const ColumnPtr& col) {
    auto* data_column = ColumnHelper::get_data_column(col.get());
    auto* raw_column = down_cast<RunTimeColumnType<lt>*>(data_column)->get_data().data();
    return raw_column;
}

template <LogicalType Type, typename VectorBatchType>
void ORCFileWriter::_write_number(orc::ColumnVectorBatch& orc_column, ColumnPtr& column) {
    auto& number_orc_column = dynamic_cast<VectorBatchType&>(orc_column);
    auto column_size = column->size();
    orc_column.resize(column_size);
    orc_column.numElements = column_size;

    auto* null_col = get_raw_null_column(column);
    auto* data_col = get_raw_data_column<Type>(column);

    if (null_col != nullptr) {
        orc_column.hasNulls = true;
        orc_column.notNull.resize(column_size);
        for (size_t i = 0; i < column_size; i++) {
            orc_column.notNull[i] = 1 - null_col[i];
        }
    }

    for (size_t i = 0; i < column_size; ++i) {
        number_orc_column.data[i] = data_col[i];
    }
}

void ORCFileWriter::_write_string(orc::ColumnVectorBatch& orc_column, ColumnPtr& column) {
    auto& string_orc_column = dynamic_cast<orc::StringVectorBatch&>(orc_column);
    auto column_size = column->size();
    orc_column.resize(column_size);
    orc_column.numElements = column_size;

    auto* null_col = get_raw_null_column(column);
    auto* data_col = down_cast<const RunTimeColumnType<TYPE_VARCHAR>*>(ColumnHelper::get_data_column(column.get()));

    if (null_col != nullptr) {
        orc_column.hasNulls = true;
        orc_column.notNull.resize(column_size);
        for (size_t i = 0; i < column_size; i++) {
            orc_column.notNull[i] = 1 - null_col[i];
        }
    }

    for (size_t i = 0; i < column_size; ++i) {
        auto slice = data_col->get_slice(i);
        string_orc_column.data[i] = const_cast<char*>(slice.get_data());
        string_orc_column.length[i] = slice.get_size();
    }
}

template <LogicalType DecimalType, typename VectorBatchType, typename T>
void ORCFileWriter::_write_decimal32or64or128(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, int precision,
                                              int scale) {
    auto& decimal_orc_column = dynamic_cast<VectorBatchType&>(orc_column);
    auto column_size = column->size();
    using Type = RunTimeCppType<DecimalType>;

    decimal_orc_column.resize(column_size);
    decimal_orc_column.numElements = column_size;
    decimal_orc_column.precision = precision;
    decimal_orc_column.scale = scale;

    auto* null_col = get_raw_null_column(column);
    auto* data_col = get_raw_data_column<DecimalType>(column);

    if (null_col != nullptr) {
        orc_column.hasNulls = true;
        orc_column.notNull.resize(column_size);
        for (size_t i = 0; i < column_size; i++) {
            orc_column.notNull[i] = 1 - null_col[i];
        }
    }

    for (size_t i = 0; i < column_size; ++i) {
        T value;
        DecimalV3Cast::to_decimal_trivial<Type, T, false>(data_col[i], &value);
        if constexpr (std::is_same_v<T, int128_t>) {
            auto high64Bits = static_cast<int64_t>(value >> 64);
            auto low64Bits = static_cast<uint64_t>(value);
            decimal_orc_column.values[i] = orc::Int128{high64Bits, low64Bits};
        } else {
            decimal_orc_column.values[i] = value;
        }
    }
}

void ORCFileWriter::_write_date(orc::ColumnVectorBatch& orc_column, ColumnPtr& column) {
    auto& date_orc_column = dynamic_cast<orc::LongVectorBatch&>(orc_column);
    auto column_size = column->size();

    orc_column.resize(column_size);
    orc_column.numElements = column_size;

    auto* null_col = get_raw_null_column(column);
    auto* data_col = get_raw_data_column<TYPE_DATE>(column);

    if (null_col != nullptr) {
        orc_column.hasNulls = true;
        orc_column.notNull.resize(column_size);
        for (size_t i = 0; i < column_size; i++) {
            orc_column.notNull[i] = 1 - null_col[i];
        }
    }

    for (size_t i = 0; i < column_size; ++i) {
        date_orc_column.data[i] = OrcDateHelper::native_date_to_orc_date(data_col[i]);
    }
}

void ORCFileWriter::_write_datetime(orc::ColumnVectorBatch& orc_column, ColumnPtr& column) {
    auto& timestamp_orc_column = dynamic_cast<orc::TimestampVectorBatch&>(orc_column);
    auto column_size = column->size();

    orc_column.resize(column_size);
    orc_column.numElements = column_size;

    auto* null_col = get_raw_null_column(column);
    auto* data_col = get_raw_data_column<TYPE_DATETIME>(column);

    if (null_col != nullptr) {
        orc_column.hasNulls = true;
        orc_column.notNull.resize(column_size);
        for (size_t i = 0; i < column_size; i++) {
            orc_column.notNull[i] = 1 - null_col[i];
        }
    }

    for (size_t i = 0; i < column_size; ++i) {
        OrcTimestampHelper::native_ts_to_orc_ts(data_col[i], timestamp_orc_column.data[i],
                                                timestamp_orc_column.nanoseconds[i]);
    }
}

void ORCFileWriter::_write_array_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column,
                                        const TypeDescriptor& type) {
    auto& array_orc_column = dynamic_cast<orc::ListVectorBatch&>(orc_column);
    auto column_size = column->size();

    array_orc_column.resize(column_size);
    array_orc_column.numElements = column_size;
    auto& value_orc_column = *array_orc_column.elements;

    auto* null_col = get_raw_null_column(column);
    auto* array_col = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(column.get()));

    if (null_col != nullptr) {
        orc_column.hasNulls = true;
        orc_column.notNull.resize(column_size);
        for (size_t i = 0; i < column_size; i++) {
            orc_column.notNull[i] = 1 - null_col[i];
        }
    }

    auto* offsets = array_col->offsets_column().get()->get_data().data();
    for (size_t i = 0; i < column_size + 1; ++i) {
        array_orc_column.offsets[i] = offsets[i];
    }

    _write_column(value_orc_column, array_col->elements_column(), type.children[0]);
}

void ORCFileWriter::_write_struct_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column,
                                         const TypeDescriptor& type) {
    auto& struct_orc_column = dynamic_cast<orc::StructVectorBatch&>(orc_column);
    auto column_size = column->size();

    struct_orc_column.resize(column_size);
    struct_orc_column.numElements = column_size;

    auto* null_col = get_raw_null_column(column);
    auto* data_col = ColumnHelper::get_data_column(column.get());
    auto* struct_col = down_cast<StructColumn*>(data_col);
    Columns& field_columns = struct_col->fields_column();

    if (null_col != nullptr) {
        orc_column.hasNulls = true;
        orc_column.notNull.resize(column_size);
        for (size_t i = 0; i < column_size; i++) {
            orc_column.notNull[i] = 1 - null_col[i];
        }
    }

    for (size_t i = 0; i < type.children.size(); ++i) {
        _write_column(*struct_orc_column.fields[i], field_columns[i], type.children[i]);
    }
}

void ORCFileWriter::_write_map_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column,
                                      const TypeDescriptor& type) {
    auto& map_orc_column = dynamic_cast<orc::MapVectorBatch&>(orc_column);
    size_t column_size = column->size();

    map_orc_column.resize(column_size);
    map_orc_column.numElements = column->size();

    orc::ColumnVectorBatch& keys_orc_column = *map_orc_column.keys;
    orc::ColumnVectorBatch& values_orc_column = *map_orc_column.elements;

    auto* null_col = get_raw_null_column(column);
    auto* map_col = down_cast<MapColumn*>(ColumnHelper::get_data_column(column.get()));
    auto& keys = map_col->keys_column();
    auto& values = map_col->values_column();
    auto& offsets = map_col->offsets_column()->get_data();

    if (null_col != nullptr) {
        orc_column.hasNulls = true;
        orc_column.notNull.resize(column_size);
        for (size_t i = 0; i < column_size; i++) {
            orc_column.notNull[i] = 1 - null_col[i];
        }
    }

    for (size_t i = 0; i < column_size + 1; ++i) {
        map_orc_column.offsets[i] = offsets[i];
    }

    _write_column(keys_orc_column, keys, type.children[0]);
    _write_column(values_orc_column, values, type.children[1]);
}

StatusOr<std::unique_ptr<orc::Type>> ORCFileWriter::_make_schema(const std::vector<std::string>& column_names,
                                                                 const std::vector<TypeDescriptor>& type_descs) {
    auto schema = orc::createStructType();
    for (size_t i = 0; i < type_descs.size(); ++i) {
        ASSIGN_OR_RETURN(std::unique_ptr<orc::Type> field_type, _make_schema_node(type_descs[i]));
        schema->addStructField(column_names[i], std::move(field_type));
    }
    return schema;
}

StatusOr<std::unique_ptr<orc::Type>> ORCFileWriter::_make_schema_node(const TypeDescriptor& type_desc) {
    switch (type_desc.type) {
    case TYPE_BOOLEAN: {
        return orc::createPrimitiveType(orc::TypeKind::BOOLEAN);
    }
    case TYPE_TINYINT: {
        return orc::createPrimitiveType(orc::TypeKind::BYTE);
    }
    case TYPE_SMALLINT: {
        return orc::createPrimitiveType(orc::TypeKind::SHORT);
    }
    case TYPE_INT: {
        return orc::createPrimitiveType(orc::TypeKind::INT);
    }
    case TYPE_BIGINT: {
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
            ASSIGN_OR_RETURN(std::unique_ptr<orc::Type> child_orc_type, _make_schema_node(child_type));
            struct_type->addStructField(type_desc.field_names[i], std::move(child_orc_type));
        }
        return struct_type;
    }
    case TYPE_MAP: {
        const TypeDescriptor& key_type = type_desc.children[0];
        const TypeDescriptor& value_type = type_desc.children[1];
        ASSIGN_OR_RETURN(std::unique_ptr<orc::Type> key_orc_type, _make_schema_node(key_type));
        ASSIGN_OR_RETURN(std::unique_ptr<orc::Type> value_orc_type, _make_schema_node(value_type));
        return orc::createMapType(std::move(key_orc_type), std::move(value_orc_type));
    }
    case TYPE_ARRAY: {
        const TypeDescriptor& child_type = type_desc.children[0];
        ASSIGN_OR_RETURN(std::unique_ptr<orc::Type> child_orc_type, _make_schema_node(child_type));
        return orc::createListType(std::move(child_orc_type));
    }
    default:
        return Status::NotSupported(
                fmt::format("ORC writer does not support to write {} type yet", type_desc.debug_string()));
    }
}

ORCFileWriterFactory::ORCFileWriterFactory(std::shared_ptr<FileSystem> fs,
                                           const std::map<std::string, std::string>& options,
                                           const std::vector<std::string>& column_names,
                                           std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                                           PriorityThreadPool* executors)
        : _fs(std::move(fs)),
          _options(options),
          _column_names(column_names),
          _column_evaluators(std::move(column_evaluators)),
          _executors(executors) {}

Status ORCFileWriterFactory::_init() {
    for (auto& e : _column_evaluators) {
        RETURN_IF_ERROR(e->init());
    }
    _parsed_options = std::make_shared<ORCWriterOptions>();
    return Status::OK();
}

StatusOr<std::shared_ptr<FileWriter>> ORCFileWriterFactory::create(const std::string& path) {
    if (_parsed_options == nullptr) {
        RETURN_IF_ERROR(_init());
    }

    ASSIGN_OR_RETURN(auto file, _fs->new_writable_file(path));
    auto rollback_action = [fs = _fs, path = path]() {
        WARN_IF_ERROR(ignore_not_found(fs->delete_file(path)), "fail to delete file");
    };
    auto column_evaluators = ColumnEvaluator::clone(_column_evaluators);
    auto types = ColumnEvaluator::types(_column_evaluators);
    auto output_stream = std::make_unique<OrcOutputStream>(std::move(file));
    return std::make_shared<ORCFileWriter>(path, std::move(output_stream), _column_names, types,
                                           std::move(column_evaluators), _parsed_options, rollback_action, _executors);
}

} // namespace starrocks::formats
