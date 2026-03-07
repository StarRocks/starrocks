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

#include "exec/adbc_scanner.h"

#include <arrow/c/bridge.h>
#include <glog/logging.h>

#include "base/time/time.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "exec/adbc_parallel_reader.h"
#include "exec/arrow_to_starrocks_converter.h"
#include "exec/arrow_type_traits.h"
#include "runtime/descriptors.h"
#include "storage/chunk_helper.h"

namespace starrocks {

// Macro to check ADBC C API return codes and convert to StarRocks Status.
#define RETURN_ADBC_NOT_OK(status_code, adbc_error)                                                  \
    do {                                                                                             \
        if ((status_code) != ADBC_STATUS_OK) {                                                       \
            std::string msg = (adbc_error).message ? (adbc_error).message : "Unknown ADBC error";    \
            if ((adbc_error).release) (adbc_error).release(&(adbc_error));                            \
            return Status::InternalError("ADBC error: " + msg);                                      \
        }                                                                                            \
    } while (0)

// ================================
// ADBCScanner
// ================================

ADBCScanner::ADBCScanner(std::string driver, std::string uri, std::string username, std::string password,
                         std::string token, std::string sql, const TupleDescriptor* tuple_desc)
        : _driver(std::move(driver)),
          _uri(std::move(uri)),
          _username(std::move(username)),
          _password(std::move(password)),
          _token(std::move(token)),
          _sql(std::move(sql)),
          _tuple_desc(tuple_desc) {}

ADBCScanner::~ADBCScanner() {
    if (!_closed) {
        close(nullptr);
    }
}

Status ADBCScanner::open(RuntimeState* state) {
    // Set max chunk size from runtime state if available
    if (state != nullptr && state->chunk_size() > 0) {
        _max_chunk_size = state->chunk_size();
    }

    auto start = MonotonicMillis();

    RETURN_IF_ERROR(_init_adbc());

    _connect_time_ms = MonotonicMillis() - start;

    // Try parallel reading first; fall back to single stream if it fails
    auto parallel_status = _try_parallel_read();
    if (!parallel_status.ok()) {
        RETURN_IF_ERROR(_fallback_single_stream_read());
    }

    _opened = true;
    return Status::OK();
}

Status ADBCScanner::_init_adbc() {
    AdbcError error = ADBC_ERROR_INIT;

    // Initialize database
    RETURN_ADBC_NOT_OK(AdbcDatabaseNew(&_database, &error), error);
    RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database, "driver", _driver.c_str(), &error), error);
    RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database, "uri", _uri.c_str(), &error), error);

    if (!_username.empty()) {
        RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database, "username", _username.c_str(), &error), error);
    }
    if (!_password.empty()) {
        RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database, "password", _password.c_str(), &error), error);
    }
    if (!_token.empty()) {
        std::string auth_header = "Bearer " + _token;
        RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database, "adbc.flight.sql.authorization_header",
                                                 auth_header.c_str(), &error),
                           error);
    }

    RETURN_ADBC_NOT_OK(AdbcDatabaseInit(&_database, &error), error);
    _database_initialized = true;

    // Initialize connection
    RETURN_ADBC_NOT_OK(AdbcConnectionNew(&_connection, &error), error);
    RETURN_ADBC_NOT_OK(AdbcConnectionInit(&_connection, &_database, &error), error);
    _connection_initialized = true;

    // Create statement and set SQL
    RETURN_ADBC_NOT_OK(AdbcStatementNew(&_connection, &_statement, &error), error);
    _statement_initialized = true;
    RETURN_ADBC_NOT_OK(AdbcStatementSetSqlQuery(&_statement, _sql.c_str(), &error), error);

    return Status::OK();
}

Status ADBCScanner::_try_parallel_read() {
    AdbcError error = ADBC_ERROR_INIT;
    struct ArrowSchema c_schema {};
    struct AdbcPartitions partitions {};
    int64_t rows_affected = -1;

    AdbcStatusCode sc =
            AdbcStatementExecutePartitions(&_statement, &c_schema, &partitions, &rows_affected, &error);
    if (sc != ADBC_STATUS_OK) {
        // Driver doesn't support partitions -- fall back
        if (error.release) error.release(&error);
        if (c_schema.release) c_schema.release(&c_schema);
        return Status::NotSupported("ADBC partitions not supported by driver");
    }

    if (partitions.num_partitions <= 1) {
        // Not worth parallel overhead for a single partition
        if (partitions.release) partitions.release(&partitions);
        if (c_schema.release) c_schema.release(&c_schema);
        return Status::NotSupported("Only one partition, falling back to single stream");
    }

    // Create parallel reader with capped thread count
    size_t num_threads = std::min(partitions.num_partitions, (size_t)4);
    _parallel_reader = std::make_unique<ADBCParallelReader>(&_database, partitions, num_threads);
    RETURN_IF_ERROR(_parallel_reader->start());
    _use_parallel = true;

    // Release partitions memory
    if (partitions.release) partitions.release(&partitions);
    if (c_schema.release) c_schema.release(&c_schema);

    return Status::OK();
}

Status ADBCScanner::_fallback_single_stream_read() {
    AdbcError error = ADBC_ERROR_INIT;
    struct ArrowArrayStream c_stream {};
    int64_t rows_affected = -1;

    RETURN_ADBC_NOT_OK(AdbcStatementExecuteQuery(&_statement, &c_stream, &rows_affected, &error), error);

    auto result = arrow::ImportRecordBatchReader(&c_stream);
    if (!result.ok()) {
        return Status::InternalError("Failed to import ArrowArrayStream: " + result.status().ToString());
    }
    _batch_reader = std::move(result).ValueUnsafe();
    _use_parallel = false;

    return Status::OK();
}

Status ADBCScanner::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    *eos = false;

    std::shared_ptr<arrow::RecordBatch> batch;

    // Check if we have a pending batch from re-chunking
    if (_pending_batch && _pending_offset < _pending_batch->num_rows()) {
        int64_t remaining = _pending_batch->num_rows() - _pending_offset;
        int64_t slice_len = std::min(remaining, _max_chunk_size);
        batch = _pending_batch->Slice(_pending_offset, slice_len);
        _pending_offset += slice_len;
        if (_pending_offset >= _pending_batch->num_rows()) {
            _pending_batch.reset();
            _pending_offset = 0;
        }
    } else {
        // Read next batch
        if (_use_parallel) {
            bool eos_flag = false;
            RETURN_IF_ERROR(_parallel_reader->get_next(&batch, &eos_flag));
            if (eos_flag) {
                *eos = true;
                return Status::OK();
            }
        } else {
            auto read_status = _batch_reader->ReadNext(&batch);
            if (!read_status.ok()) {
                return Status::InternalError("Arrow read error: " + read_status.ToString());
            }
            if (!batch) {
                *eos = true;
                return Status::OK();
            }
        }

        // Re-chunk if batch is larger than max_chunk_size
        if (batch->num_rows() > _max_chunk_size) {
            _pending_batch = batch;
            _pending_offset = _max_chunk_size;
            batch = _pending_batch->Slice(0, _max_chunk_size);
        }
    }

    RETURN_IF_ERROR(_convert_batch_to_chunk(batch, chunk));

    _rows_read += (*chunk)->num_rows();
    _bytes_read += (*chunk)->bytes_usage();

    return Status::OK();
}

Status ADBCScanner::_convert_batch_to_chunk(const std::shared_ptr<arrow::RecordBatch>& batch, ChunkPtr* chunk) {
    size_t num_rows = batch->num_rows();
    if (num_rows == 0) {
        *chunk = std::make_shared<Chunk>();
        return Status::OK();
    }

    const auto& slots = _tuple_desc->slots();
    Columns columns(slots.size());

    for (size_t i = 0; i < slots.size(); i++) {
        SlotDescriptor* slot = slots[i];
        if (!slot->is_materialized()) {
            continue;
        }

        auto arrow_column = batch->column(i);
        auto arrow_type = arrow_column->type();
        ArrowTypeId arrow_type_id = arrow_type->id();
        LogicalType sr_type = slot->type().type;
        bool is_nullable = slot->is_nullable();

        ConvertFunc converter = get_arrow_converter(arrow_type_id, sr_type, is_nullable, true);
        if (converter == nullptr) {
            return Status::InternalError(
                    fmt::format("No Arrow converter for arrow type {} to StarRocks type {}",
                                arrow_type->ToString(), type_to_string(sr_type)));
        }

        // Create column
        auto column = ColumnHelper::create_column(slot->type(), is_nullable);
        column->reserve(num_rows);

        // Handle nullable columns: fill null data first
        Filter chunk_filter;
        if (is_nullable) {
            auto* nullable = down_cast<NullableColumn*>(column.get());
            auto* null_column = nullable->null_column_raw_ptr();
            null_column->resize(num_rows);
            size_t null_count = fill_null_column(arrow_column.get(), 0, num_rows, null_column, 0);
            nullable->set_has_null(null_count != 0);
            uint8_t* null_data = &null_column->get_data().front();

            Column* data_col = nullable->data_column_raw_ptr();
            data_col->resize(num_rows);
            RETURN_IF_ERROR(converter(arrow_column.get(), 0, num_rows, data_col, 0, null_data, &chunk_filter,
                                      nullptr, nullptr));
        } else {
            column->resize(num_rows);
            RETURN_IF_ERROR(
                    converter(arrow_column.get(), 0, num_rows, column.get(), 0, nullptr, &chunk_filter, nullptr,
                              nullptr));
        }

        columns[i] = std::move(column);
    }

    // Build slot map: slot_id -> column index
    Chunk::SlotHashMap slot_map;
    for (size_t i = 0; i < slots.size(); i++) {
        if (slots[i]->is_materialized()) {
            slot_map[slots[i]->id()] = i;
        }
    }
    *chunk = std::make_shared<Chunk>(std::move(columns), std::move(slot_map));
    return Status::OK();
}

void ADBCScanner::close(RuntimeState* state) {
    if (_closed) return;
    _closed = true;

    // Close parallel reader first
    if (_parallel_reader) {
        _parallel_reader->close();
        _parallel_reader.reset();
    }

    // Release batch reader
    _batch_reader.reset();
    _pending_batch.reset();

    // Release ADBC resources in reverse order: statement -> connection -> database
    AdbcError error = ADBC_ERROR_INIT;

    if (_statement_initialized) {
        AdbcStatusCode sc = AdbcStatementRelease(&_statement, &error);
        if (sc != ADBC_STATUS_OK) {
            LOG(WARNING) << "Failed to release ADBC statement: "
                         << (error.message ? error.message : "Unknown error");
            if (error.release) error.release(&error);
        }
        _statement_initialized = false;
    }

    if (_connection_initialized) {
        error = ADBC_ERROR_INIT;
        AdbcStatusCode sc = AdbcConnectionRelease(&_connection, &error);
        if (sc != ADBC_STATUS_OK) {
            LOG(WARNING) << "Failed to release ADBC connection: "
                         << (error.message ? error.message : "Unknown error");
            if (error.release) error.release(&error);
        }
        _connection_initialized = false;
    }

    if (_database_initialized) {
        error = ADBC_ERROR_INIT;
        AdbcStatusCode sc = AdbcDatabaseRelease(&_database, &error);
        if (sc != ADBC_STATUS_OK) {
            LOG(WARNING) << "Failed to release ADBC database: "
                         << (error.message ? error.message : "Unknown error");
            if (error.release) error.release(&error);
        }
        _database_initialized = false;
    }
}

} // namespace starrocks
