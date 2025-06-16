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

#include <utility>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "formats/orc/orc_memory_pool.h"
#include "formats/orc/utils.h"
#include "formats/utils.h"
#include "runtime/current_thread.h"
#include "util/debug_util.h"

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

AsyncOrcOutputStream::AsyncOrcOutputStream(io::AsyncFlushOutputStream* stream) : _stream(stream) {}

uint64_t AsyncOrcOutputStream::getLength() const {
    return _stream->tell();
}

uint64_t AsyncOrcOutputStream::getNaturalWriteSize() const {
    return config::vector_chunk_size;
}

const std::string& AsyncOrcOutputStream::getName() const {
    return _stream->filename();
}

void AsyncOrcOutputStream::write(const void* buf, size_t length) {
    if (_is_closed) {
        throw std::runtime_error("The output stream is closed but there are still inputs");
    }
    const uint8_t* ch = static_cast<const uint8_t*>(buf);
    Status st = _stream->write(ch, length);
    if (!st.ok()) {
        throw std::runtime_error("write to orc failed: " + st.to_string());
    }
    return;
}

void AsyncOrcOutputStream::close() {
    if (_is_closed) {
        return;
    }
    _is_closed = true;

    if (auto st = _stream->close(); !st.ok()) {
        throw std::runtime_error("close orc output stream failed: " + st.to_string());
    }
}

ORCFileWriter::ORCFileWriter(std::string location, std::shared_ptr<orc::OutputStream> output_stream,
                             std::vector<std::string> column_names, std::vector<TypeDescriptor> type_descs,
                             std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                             TCompressionType::type compression_type, std::shared_ptr<ORCWriterOptions> writer_options,
                             std::function<void()> rollback_action)
        : _location(std::move(location)),
          _output_stream(std::move(output_stream)),
          _column_names(std::move(column_names)),
          _type_descs(std::move(type_descs)),
          _column_evaluators(std::move(column_evaluators)),
          _compression_type(compression_type),
          _writer_options(std::move(writer_options)),
          _rollback_action(std::move(rollback_action)) {}

Status ORCFileWriter::init() {
    RETURN_IF_ERROR(ColumnEvaluator::init(_column_evaluators));
    ASSIGN_OR_RETURN(_schema, _make_schema(_column_names, _type_descs));
    auto options = orc::WriterOptions();
    ASSIGN_OR_RETURN(auto compression, _convert_compression_type(_compression_type));
    options.setCompression(compression);
    options.setMemoryPool(&_memory_pool);
    _writer = orc::createWriter(*_schema, _output_stream.get(), options);
    _writer->addUserMetadata(STARROCKS_ORC_WRITER_VERSION_KEY, get_short_version());
    return Status::OK();
}

int64_t ORCFileWriter::get_written_bytes() {
    return _output_stream->getLength();
}

int64_t ORCFileWriter::get_allocated_bytes() {
    return _memory_pool.bytes_allocated();
}

Status ORCFileWriter::write(Chunk* chunk) {
    ASSIGN_OR_RETURN(auto cvb, _convert(chunk));
    _writer->add(*cvb);
    _row_counter += chunk->num_rows();
    return Status::OK();
}

FileWriter::CommitResult ORCFileWriter::commit() {
    FileWriter::CommitResult result{
            .io_status = Status::OK(), .format = ORC, .location = _location, .rollback_action = _rollback_action};
    try {
        _writer->close();
    } catch (const std::exception& e) {
        result.io_status.update(Status::IOError(fmt::format("{}: {}", "close file error", e.what())));
    }

    try {
        _output_stream->close();
    } catch (const std::exception& e) {
        result.io_status.update(Status::IOError(fmt::format("{}: {}", "close output stream error", e.what())));
    }

    if (result.io_status.ok()) {
        result.file_statistics.record_count = _row_counter;
        result.file_statistics.file_size = _output_stream->getLength();
    }

    auto promise = std::make_shared<std::promise<FileWriter::CommitResult>>();
    std::future<FileWriter::CommitResult> future = promise->get_future();

    _writer = nullptr;
    return result;
}

StatusOr<std::unique_ptr<orc::ColumnVectorBatch>> ORCFileWriter::_convert(Chunk* chunk) {
    auto cvb = _writer->createRowBatch(chunk->num_rows());
    auto root = down_cast<orc::StructVectorBatch*>(cvb.get());

    for (size_t i = 0; i < _column_evaluators.size(); ++i) {
        ASSIGN_OR_RETURN(auto column, _column_evaluators[i]->evaluate(chunk));
        RETURN_IF_ERROR(_write_column(*root->fields[i], column, _type_descs[i]));
    }

    root->numElements = chunk->num_rows();
    return cvb;
}

Status ORCFileWriter::_write_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column,
                                    const TypeDescriptor& type_desc) {
    switch (type_desc.type) {
    case TYPE_BOOLEAN: {
        return _write_number<TYPE_BOOLEAN, orc::LongVectorBatch>(orc_column, column);
    }
    case TYPE_TINYINT: {
        return _write_number<TYPE_TINYINT, orc::LongVectorBatch>(orc_column, column);
    }
    case TYPE_SMALLINT: {
        return _write_number<TYPE_SMALLINT, orc::LongVectorBatch>(orc_column, column);
    }
    case TYPE_INT: {
        return _write_number<TYPE_INT, orc::LongVectorBatch>(orc_column, column);
    }
    case TYPE_BIGINT: {
        return _write_number<TYPE_BIGINT, orc::LongVectorBatch>(orc_column, column);
    }
    case TYPE_FLOAT: {
        return _write_number<TYPE_FLOAT, orc::DoubleVectorBatch>(orc_column, column);
    }
    case TYPE_DOUBLE: {
        return _write_number<TYPE_DOUBLE, orc::DoubleVectorBatch>(orc_column, column);
    }
    case TYPE_CHAR:
        [[fallthrough]];
    case TYPE_VARCHAR: {
        return _write_string(orc_column, column);
    }
    case TYPE_DECIMAL32: {
        return _write_decimal32or64or128<TYPE_DECIMAL32, orc::Decimal64VectorBatch, int64_t>(
                orc_column, column, type_desc.precision, type_desc.scale);
    }
    case TYPE_DECIMAL64: {
        return _write_decimal32or64or128<TYPE_DECIMAL64, orc::Decimal64VectorBatch, int64_t>(
                orc_column, column, type_desc.precision, type_desc.scale);
    }
    case TYPE_DECIMAL128: {
        return _write_decimal32or64or128<TYPE_DECIMAL128, orc::Decimal128VectorBatch, int128_t>(
                orc_column, column, type_desc.precision, type_desc.scale);
    }
    case TYPE_DATE: {
        return _write_date(orc_column, column);
    }
    case TYPE_DATETIME: {
        return _write_datetime(orc_column, column);
    }
    default: {
        return Status::NotSupported(
                fmt::format("ORC writer does not support to write {} type yet", type_desc.debug_string()));
    }
    }
}

inline const uint8_t* get_raw_null_column(const ColumnPtr& col) {
    if (!col->has_null()) {
        return nullptr;
    }
    auto& null_column = down_cast<const NullableColumn*>(col.get())->null_column();
    auto* raw_column = null_column->get_data().data();
    return raw_column;
}

template <LogicalType lt>
inline const RunTimeCppType<lt>* get_raw_data_column(const ColumnPtr& col) {
    auto* data_column = ColumnHelper::get_data_column(col.get());
    auto* raw_column = down_cast<const RunTimeColumnType<lt>*>(data_column)->get_data().data();
    return raw_column;
}

template <LogicalType Type, typename VectorBatchType>
Status ORCFileWriter::_write_number(orc::ColumnVectorBatch& orc_column, ColumnPtr& column) {
    auto& number_orc_column = dynamic_cast<VectorBatchType&>(orc_column);
    auto column_size = column->size();
    orc_column.resize(column_size);
    orc_column.numElements = column_size;

    const auto* null_col = get_raw_null_column(column);
    const auto* data_col = get_raw_data_column<Type>(column);

    _populate_orc_notnull(orc_column, null_col, column_size);

    for (size_t i = 0; i < column_size; ++i) {
        number_orc_column.data[i] = data_col[i];
    }

    return Status::OK();
}

Status ORCFileWriter::_write_string(orc::ColumnVectorBatch& orc_column, ColumnPtr& column) {
    auto& string_orc_column = dynamic_cast<orc::StringVectorBatch&>(orc_column);
    auto column_size = column->size();
    orc_column.resize(column_size);
    orc_column.numElements = column_size;

    auto* null_col = get_raw_null_column(column);
    auto* data_col = down_cast<const RunTimeColumnType<TYPE_VARCHAR>*>(ColumnHelper::get_data_column(column.get()));

    _populate_orc_notnull(orc_column, null_col, column_size);

    for (size_t i = 0; i < column_size; ++i) {
        auto slice = data_col->get_slice(i);
        string_orc_column.data[i] = const_cast<char*>(slice.get_data());
        string_orc_column.length[i] = slice.get_size();
    }

    return Status::OK();
}

template <LogicalType DecimalType, typename VectorBatchType, typename T>
Status ORCFileWriter::_write_decimal32or64or128(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, int precision,
                                                int scale) {
    auto& decimal_orc_column = dynamic_cast<VectorBatchType&>(orc_column);
    auto column_size = column->size();
    using Type = RunTimeCppType<DecimalType>;

    decimal_orc_column.resize(column_size);
    decimal_orc_column.numElements = column_size;
    decimal_orc_column.precision = precision;
    decimal_orc_column.scale = scale;

    const auto* null_col = get_raw_null_column(column);
    const auto* data_col = get_raw_data_column<DecimalType>(column);

    _populate_orc_notnull(orc_column, null_col, column_size);

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

    return Status::OK();
}

Status ORCFileWriter::_write_date(orc::ColumnVectorBatch& orc_column, ColumnPtr& column) {
    auto& date_orc_column = dynamic_cast<orc::LongVectorBatch&>(orc_column);
    auto column_size = column->size();

    orc_column.resize(column_size);
    orc_column.numElements = column_size;

    const auto* null_col = get_raw_null_column(column);
    const auto* data_col = get_raw_data_column<TYPE_DATE>(column);

    _populate_orc_notnull(orc_column, null_col, column_size);

    for (size_t i = 0; i < column_size; ++i) {
        date_orc_column.data[i] = OrcDateHelper::native_date_to_orc_date(data_col[i]);
    }

    return Status::OK();
}

Status ORCFileWriter::_write_datetime(orc::ColumnVectorBatch& orc_column, ColumnPtr& column) {
    auto& timestamp_orc_column = dynamic_cast<orc::TimestampVectorBatch&>(orc_column);
    auto column_size = column->size();

    orc_column.resize(column_size);
    orc_column.numElements = column_size;

    const auto* null_col = get_raw_null_column(column);
    const auto* data_col = get_raw_data_column<TYPE_DATETIME>(column);

    _populate_orc_notnull(orc_column, null_col, column_size);

    for (size_t i = 0; i < column_size; ++i) {
        OrcTimestampHelper::native_ts_to_orc_ts(data_col[i], timestamp_orc_column.data[i],
                                                timestamp_orc_column.nanoseconds[i]);
    }

    return Status::OK();
}

StatusOr<orc::CompressionKind> ORCFileWriter::_convert_compression_type(TCompressionType::type type) {
    orc::CompressionKind converted_type;
    switch (type) {
    case TCompressionType::NO_COMPRESSION: {
        converted_type = orc::CompressionKind_NONE;
        break;
    }
    case TCompressionType::SNAPPY: {
        converted_type = orc::CompressionKind_SNAPPY;
        break;
    }
    case TCompressionType::GZIP: {
        converted_type = orc::CompressionKind_ZLIB;
        break;
    }
    case TCompressionType::ZSTD: {
        converted_type = orc::CompressionKind_ZSTD;
        break;
    }
    case TCompressionType::LZ4: {
        converted_type = orc::CompressionKind_LZ4;
        break;
    }
    default: {
        return Status::NotSupported(fmt::format("not supported compression type {}", to_string(type)));
    }
    }

    return converted_type;
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
    case TYPE_DATETIME: {
        return orc::createPrimitiveType(orc::TypeKind::TIMESTAMP);
    }
    // TODO(letian-jiang): support nested type
    default: {
        return Status::NotSupported(
                fmt::format("ORC writer does not support to write {} type yet", type_desc.debug_string()));
    }
    }
}

void ORCFileWriter::_populate_orc_notnull(orc::ColumnVectorBatch& orc_column, const uint8_t* null_column,
                                          size_t column_size) {
    orc_column.notNull.resize(column_size);
    if (null_column != nullptr) {
        orc_column.hasNulls = true;
        for (size_t i = 0; i < column_size; i++) {
            orc_column.notNull[i] = 1 - null_column[i];
        }
    } else {
        orc_column.hasNulls = false;
        memset(orc_column.notNull.data(), 1, column_size * sizeof(char));
    }
}

ORCFileWriterFactory::ORCFileWriterFactory(std::shared_ptr<FileSystem> fs, TCompressionType::type compression_type,
                                           std::map<std::string, std::string> options,
                                           std::vector<std::string> column_names,
                                           std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                                           PriorityThreadPool* executors, RuntimeState* runtime_state)
        : _fs(std::move(fs)),
          _compression_type(compression_type),
          _options(std::move(options)),
          _column_names(std::move(column_names)),
          _column_evaluators(std::move(column_evaluators)),
          _executors(executors),
          _runtime_state(runtime_state) {}

Status ORCFileWriterFactory::init() {
    for (auto& e : _column_evaluators) {
        RETURN_IF_ERROR(e->init());
    }
    _parsed_options = std::make_shared<ORCWriterOptions>();
    return Status::OK();
}

StatusOr<WriterAndStream> ORCFileWriterFactory::create(const string& path) const {
    ASSIGN_OR_RETURN(auto file, _fs->new_writable_file(WritableFileOptions{.direct_write = true}, path));
    auto rollback_action = [fs = _fs, path = path]() {
        WARN_IF_ERROR(ignore_not_found(fs->delete_file(path)), "fail to delete file");
    };
    auto column_evaluators = ColumnEvaluator::clone(_column_evaluators);
    auto types = ColumnEvaluator::types(_column_evaluators);
    auto async_output_stream =
            std::make_unique<io::AsyncFlushOutputStream>(std::move(file), _executors, _runtime_state);
    auto orc_output_stream = std::make_shared<AsyncOrcOutputStream>(async_output_stream.get());
    auto writer =
            std::make_unique<ORCFileWriter>(path, orc_output_stream, _column_names, types, std::move(column_evaluators),
                                            _compression_type, _parsed_options, rollback_action);
    return WriterAndStream{
            .writer = std::move(writer),
            .stream = std::move(async_output_stream),
    };
}

} // namespace starrocks::formats
