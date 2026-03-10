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

#include <fstream>
#include <sstream>

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
                         std::string token, std::string sql, const TupleDescriptor* tuple_desc,
                         std::string ca_cert_file, std::string client_cert_file, std::string client_key_file,
                         bool tls_verify)
        : _driver(std::move(driver)),
          _uri(std::move(uri)),
          _username(std::move(username)),
          _password(std::move(password)),
          _token(std::move(token)),
          _sql(std::move(sql)),
          _tuple_desc(tuple_desc),
          _ca_cert_file(std::move(ca_cert_file)),
          _client_cert_file(std::move(client_cert_file)),
          _client_key_file(std::move(client_key_file)),
          _tls_verify(tls_verify) {}

Status ADBCScanner::_read_file_to_string(const std::string& path, std::string* content) {
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        return Status::InvalidArgument(fmt::format("Cannot open certificate file: {}", path));
    }
    std::ostringstream ss;
    ss << file.rdbuf();
    *content = ss.str();
    return Status::OK();
}

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

    LOG(INFO) << "ADBC: _init_adbc starting, driver=" << _driver << " uri=" << _uri;

    // Initialize database
    // The Flight SQL driver is statically linked (Go/cgo-based, crashes if loaded via dlopen).
    // Its .a provides all ADBC API functions directly, so no driver manager is needed.
    RETURN_ADBC_NOT_OK(AdbcDatabaseNew(&_database, &error), error);
    LOG(INFO) << "ADBC: AdbcDatabaseNew OK";
    RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database, "uri", _uri.c_str(), &error), error);
    LOG(INFO) << "ADBC: SetOption uri OK";

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

    // TLS options -- must be set before AdbcDatabaseInit
    if (!_ca_cert_file.empty()) {
        std::string pem_content;
        RETURN_IF_ERROR(_read_file_to_string(_ca_cert_file, &pem_content));
        RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database, "adbc.flight.sql.client_option.tls_root_certs",
                                                 pem_content.c_str(), &error),
                           error);
    }

    if (!_client_cert_file.empty() && !_client_key_file.empty()) {
        std::string cert_content, key_content;
        RETURN_IF_ERROR(_read_file_to_string(_client_cert_file, &cert_content));
        RETURN_IF_ERROR(_read_file_to_string(_client_key_file, &key_content));
        RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database, "adbc.flight.sql.client_option.mtls_cert_chain",
                                                 cert_content.c_str(), &error),
                           error);
        RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database, "adbc.flight.sql.client_option.mtls_private_key",
                                                 key_content.c_str(), &error),
                           error);
    }

    if (!_tls_verify) {
        RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database, "adbc.flight.sql.client_option.tls_skip_verify", "true",
                                                 &error),
                           error);
        LOG(WARNING) << "ADBC TLS certificate verification DISABLED (insecure mode)";
    }

    LOG(INFO) << "ADBC: calling AdbcDatabaseInit...";
    RETURN_ADBC_NOT_OK(AdbcDatabaseInit(&_database, &error), error);
    _database_initialized = true;
    LOG(INFO) << "ADBC: AdbcDatabaseInit OK";

    // Initialize connection
    RETURN_ADBC_NOT_OK(AdbcConnectionNew(&_connection, &error), error);
    LOG(INFO) << "ADBC: AdbcConnectionNew OK";
    RETURN_ADBC_NOT_OK(AdbcConnectionInit(&_connection, &_database, &error), error);
    _connection_initialized = true;
    LOG(INFO) << "ADBC: AdbcConnectionInit OK";

    // Create statement and set SQL
    RETURN_ADBC_NOT_OK(AdbcStatementNew(&_connection, &_statement, &error), error);
    _statement_initialized = true;
    LOG(INFO) << "ADBC: AdbcStatementNew OK";
    RETURN_ADBC_NOT_OK(AdbcStatementSetSqlQuery(&_statement, _sql.c_str(), &error), error);
    LOG(INFO) << "ADBC: SetSqlQuery OK, sql=" << _sql;

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
    LOG(INFO) << "ADBC: _fallback_single_stream_read starting...";
    AdbcError error = ADBC_ERROR_INIT;
    int64_t rows_affected = -1;

    RETURN_ADBC_NOT_OK(AdbcStatementExecuteQuery(&_statement, &_c_stream, &rows_affected, &error), error);
    _stream_initialized = true;
    LOG(INFO) << "ADBC: ExecuteQuery OK, rows_affected=" << rows_affected;

    // Get schema from the C stream
    struct ArrowSchema c_schema {};
    if (_c_stream.get_schema(&_c_stream, &c_schema) != 0) {
        const char* err = _c_stream.get_last_error(&_c_stream);
        return Status::InternalError(
                fmt::format("Failed to get schema from ADBC stream: {}", err ? err : "unknown"));
    }
    auto schema_result = arrow::ImportSchema(&c_schema);
    if (!schema_result.ok()) {
        return Status::InternalError("Failed to import Arrow schema: " + schema_result.status().ToString());
    }
    _arrow_schema = std::move(schema_result).ValueUnsafe();

    _use_parallel = false;
    LOG(INFO) << "ADBC: stream initialized OK, schema=" << _arrow_schema->ToString();

    return Status::OK();
}

Status ADBCScanner::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    *eos = false;
    LOG(INFO) << "ADBC: get_next called, _use_parallel=" << _use_parallel
              << " _tuple_desc=" << (void*)_tuple_desc
              << " chunk=" << (void*)chunk
              << " *chunk=" << (chunk ? (void*)chunk->get() : nullptr);

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
            // Read directly from the C stream (avoids Arrow's assertion on Go driver's release)
            struct ArrowArray c_array {};
            int rc = _c_stream.get_next(&_c_stream, &c_array);
            if (rc != 0) {
                const char* err = _c_stream.get_last_error(&_c_stream);
                return Status::InternalError(
                        fmt::format("Arrow stream read error: {}", err ? err : "unknown"));
            }
            if (c_array.release == nullptr) {
                *eos = true;
                return Status::OK();
            }
            auto batch_result = arrow::ImportRecordBatch(&c_array, _arrow_schema);
            if (!batch_result.ok()) {
                return Status::InternalError("Failed to import record batch: " +
                                             batch_result.status().ToString());
            }
            batch = std::move(batch_result).ValueUnsafe();
        }

        // Re-chunk if batch is larger than max_chunk_size
        if (batch->num_rows() > _max_chunk_size) {
            _pending_batch = batch;
            _pending_offset = _max_chunk_size;
            batch = _pending_batch->Slice(0, _max_chunk_size);
        }
    }

    LOG(INFO) << "ADBC: got batch with " << batch->num_rows() << " rows, " << batch->num_columns() << " cols"
              << ", schema=" << batch->schema()->ToString();

    RETURN_IF_ERROR(_convert_batch_to_chunk(batch, chunk));
    LOG(INFO) << "ADBC: _convert_batch_to_chunk OK, chunk rows=" << (*chunk)->num_rows();

    _rows_read += (*chunk)->num_rows();
    _bytes_read += (*chunk)->bytes_usage();

    return Status::OK();
}

Status ADBCScanner::_convert_batch_to_chunk(const std::shared_ptr<arrow::RecordBatch>& batch, ChunkPtr* chunk) {
    size_t num_rows = batch->num_rows();
    LOG(INFO) << "ADBC: _convert_batch_to_chunk num_rows=" << num_rows
              << " batch_cols=" << batch->num_columns()
              << " _tuple_desc=" << (void*)_tuple_desc;
    if (num_rows == 0) {
        *chunk = std::make_shared<Chunk>();
        return Status::OK();
    }

    const auto& slots = _tuple_desc->slots();
    LOG(INFO) << "ADBC: tuple_desc has " << slots.size() << " slots";
    Columns columns(slots.size());

    // Initialize chunk filter (1 = valid). The arrow converter accesses it unconditionally.
    Filter chunk_filter(num_rows, 1);

    for (size_t i = 0; i < slots.size(); i++) {
        SlotDescriptor* slot = slots[i];
        if (!slot->is_materialized()) {
            LOG(INFO) << "ADBC: slot[" << i << "] not materialized, skipping";
            continue;
        }

        if (i >= (size_t)batch->num_columns()) {
            return Status::InternalError(
                    fmt::format("ADBC: slot index {} >= batch columns {}", i, batch->num_columns()));
        }

        auto arrow_column = batch->column(i);
        auto arrow_type = arrow_column->type();
        ArrowTypeId arrow_type_id = arrow_type->id();
        LogicalType sr_type = slot->type().type;
        bool is_nullable = slot->is_nullable();

        LOG(INFO) << "ADBC: converting slot[" << i << "] name=" << slot->col_name()
                  << " arrow_type=" << arrow_type->ToString()
                  << " sr_type=" << type_to_string(sr_type)
                  << " nullable=" << is_nullable;

        ConvertFunc converter = get_arrow_converter(arrow_type_id, sr_type, is_nullable, true);
        if (converter == nullptr) {
            return Status::InternalError(
                    fmt::format("No Arrow converter for arrow type {} to StarRocks type {}",
                                arrow_type->ToString(), type_to_string(sr_type)));
        }

        // Create column with reserved capacity but size 0.
        // The converter internally calls resize(size + num_elements), so we must NOT pre-resize.
        auto column = ColumnHelper::create_column(slot->type(), is_nullable);
        column->reserve(num_rows);

        if (is_nullable) {
            auto* nullable = down_cast<NullableColumn*>(column.get());
            auto* null_column = nullable->null_column_raw_ptr();
            // fill_null_column internally resizes the null column
            size_t null_count = fill_null_column(arrow_column.get(), 0, num_rows, null_column, 0);
            nullable->set_has_null(null_count != 0);
            uint8_t* null_data = &null_column->get_data().front();

            Column* data_col = nullable->data_column_raw_ptr();
            // Do NOT resize data_col -- the converter handles sizing
            RETURN_IF_ERROR(converter(arrow_column.get(), 0, num_rows, data_col, 0, null_data, &chunk_filter,
                                      nullptr, nullptr));
        } else {
            // Do NOT resize column -- the converter handles sizing
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

    // Release C stream manually — the Go ADBC driver doesn't null out release after
    // calling it, which violates the Arrow C Data Interface protocol and triggers an
    // assertion abort in Arrow's ArrowArrayStreamRelease helper.
    if (_stream_initialized && _c_stream.release) {
        _c_stream.release(&_c_stream);
        _c_stream.release = nullptr; // workaround for Go driver protocol violation
        _stream_initialized = false;
    }
    _arrow_schema.reset();
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
