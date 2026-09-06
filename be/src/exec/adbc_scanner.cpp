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

#include <algorithm>

#include "base/time/time.h"
#include "column/arrow/arrow_to_starrocks_converter.h"
#include "column/arrow/arrow_type_traits.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "runtime/descriptors.h"

namespace starrocks {

// Macro to check ADBC C API return codes and convert to StarRocks Status.
#define RETURN_ADBC_NOT_OK(status_code, adbc_error)                                               \
    do {                                                                                          \
        if ((status_code) != ADBC_STATUS_OK) {                                                    \
            std::string msg = (adbc_error).message ? (adbc_error).message : "Unknown ADBC error"; \
            if ((adbc_error).release) (adbc_error).release(&(adbc_error));                        \
            return Status::InternalError("ADBC error: " + msg);                                   \
        }                                                                                         \
    } while (0)

// ================================
// ADBCScanner
// ================================

ADBCScanner::ADBCScanner(const ADBCScanContext& ctx, const TupleDescriptor* tuple_desc, RuntimeProfile* runtime_profile)
        : _ctx(ctx), _tuple_desc(tuple_desc), _runtime_profile(runtime_profile) {
    _init_profile();
}

void ADBCScanner::_init_profile() {
    if (_runtime_profile == nullptr) {
        return;
    }
    _profile.rows_read_counter = ADD_COUNTER(_runtime_profile, "RowsRead", TUnit::UNIT);
    _profile.io_timer = ADD_TIMER(_runtime_profile, "IOTime");
    _profile.io_counter = ADD_COUNTER(_runtime_profile, "IOCounter", TUnit::UNIT);
    _profile.fill_chunk_timer = ADD_TIMER(_runtime_profile, "FillChunkTime");
    _profile.connect_timer = ADD_TIMER(_runtime_profile, "ConnectTime");
    _runtime_profile->add_info_string("Query", _ctx.sql);
    _runtime_profile->add_info_string("Driver", _ctx.driver);
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

    // Guard the entire open path against C++ exceptions from ADBC drivers.
    // Drivers loaded via dlopen (e.g. DuckDB, Go-based FlightSQL) may throw
    // std::out_of_range, std::runtime_error, or other C++ exceptions that
    // would otherwise propagate up and crash the BE process.
    try {
        auto start = MonotonicMillis();
        {
            SCOPED_TIMER(_profile.connect_timer);
            RETURN_IF_ERROR(_init_adbc());
        }
        _connect_time_ms = MonotonicMillis() - start;

        RETURN_IF_ERROR(_execute_query());

        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError(fmt::format("ADBC driver threw C++ exception during open: {}", e.what()));
    } catch (...) {
        return Status::InternalError("ADBC driver threw unknown C++ exception during open");
    }
}

Status ADBCScanner::_init_adbc() {
    AdbcError error = ADBC_ERROR_INIT;

    // Use the Driver Manager's upstream discovery contract.
    RETURN_ADBC_NOT_OK(AdbcDatabaseNew(&_database, &error), error);
    _database_initialized = true;
    error = ADBC_ERROR_INIT;
    RETURN_ADBC_NOT_OK(AdbcDriverManagerDatabaseSetLoadFlags(&_database, ADBC_LOAD_FLAG_DEFAULT, &error), error);

    // Set the logical driver name before forwarding connection and driver options.
    error = ADBC_ERROR_INIT;
    RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database, "driver", _ctx.driver.c_str(), &error), error);

    // Standard options: uri, username, password.
    if (!_ctx.uri.empty()) {
        error = ADBC_ERROR_INIT;
        RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database, "uri", _ctx.uri.c_str(), &error), error);
    }
    if (!_ctx.username.empty()) {
        error = ADBC_ERROR_INIT;
        RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database, "username", _ctx.username.c_str(), &error), error);
    }
    if (!_ctx.password.empty()) {
        error = ADBC_ERROR_INIT;
        RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database, "password", _ctx.password.c_str(), &error), error);
    }

    // Forward all driver-specific options before Init.
    for (const auto& [key, value] : _ctx.adbc_options) {
        // Skip uri/username/password — already set above
        if (key == "uri" || key == "username" || key == "password") continue;
        error = ADBC_ERROR_INIT;
        RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database, key.c_str(), value.c_str(), &error), error);
    }

    // Initialize the database after all options are set.
    error = ADBC_ERROR_INIT;
    RETURN_ADBC_NOT_OK(AdbcDatabaseInit(&_database, &error), error);

    // Create a per-fragment connection.
    error = ADBC_ERROR_INIT;
    RETURN_ADBC_NOT_OK(AdbcConnectionNew(&_connection, &error), error);
    _connection_initialized = true;
    error = ADBC_ERROR_INIT;
    RETURN_ADBC_NOT_OK(AdbcConnectionInit(&_connection, &_database, &error), error);

    // Create the statement.
    error = ADBC_ERROR_INIT;
    RETURN_ADBC_NOT_OK(AdbcStatementNew(&_connection, &_statement, &error), error);
    _statement_initialized = true;
    error = ADBC_ERROR_INIT;
    RETURN_ADBC_NOT_OK(AdbcStatementSetSqlQuery(&_statement, _ctx.sql.c_str(), &error), error);

    return Status::OK();
}

Status ADBCScanner::_execute_query() {
    AdbcError error = ADBC_ERROR_INIT;
    int64_t rows_affected = -1;

    RETURN_ADBC_NOT_OK(AdbcStatementExecuteQuery(&_statement, &_c_stream, &rows_affected, &error), error);

    // Get schema from the C stream
    struct ArrowSchema c_schema {};
    if (_c_stream.get_schema(&_c_stream, &c_schema) != 0) {
        const char* err = _c_stream.get_last_error(&_c_stream);
        return Status::InternalError(fmt::format("Failed to get schema from ADBC stream: {}", err ? err : "unknown"));
    }
    auto schema_result = arrow::ImportSchema(&c_schema);
    if (!schema_result.ok()) {
        return Status::InternalError("Failed to import Arrow schema: " + schema_result.status().ToString());
    }
    _arrow_schema = std::move(schema_result).ValueUnsafe();

    return Status::OK();
}

Status ADBCScanner::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    try {
        return _get_next_impl(state, chunk, eos);
    } catch (const std::exception& e) {
        return Status::InternalError(fmt::format("ADBC driver threw C++ exception during read: {}", e.what()));
    } catch (...) {
        return Status::InternalError("ADBC driver threw unknown C++ exception during read");
    }
}

Status ADBCScanner::_get_next_impl(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    *eos = false;
    if (state != nullptr && state->is_cancelled()) {
        return Status::Cancelled("ADBC scan cancelled");
    }

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
        SCOPED_TIMER(_profile.io_timer);
        COUNTER_UPDATE(_profile.io_counter, 1);
        struct ArrowArray c_array {};
        int rc = _c_stream.get_next(&_c_stream, &c_array);
        if (rc != 0) {
            const char* err = _c_stream.get_last_error(&_c_stream);
            return Status::InternalError(fmt::format("Arrow stream read error: {}", err ? err : "unknown"));
        }
        if (c_array.release == nullptr) {
            *eos = true;
            return Status::OK();
        }
        auto batch_result = arrow::ImportRecordBatch(&c_array, _arrow_schema);
        if (!batch_result.ok()) {
            return Status::InternalError("Failed to import record batch: " + batch_result.status().ToString());
        }
        batch = std::move(batch_result).ValueUnsafe();

        // Re-chunk if batch is larger than max_chunk_size
        if (batch->num_rows() > _max_chunk_size) {
            _pending_batch = batch;
            _pending_offset = _max_chunk_size;
            batch = _pending_batch->Slice(0, _max_chunk_size);
        }
    }

    {
        SCOPED_TIMER(_profile.fill_chunk_timer);
        RETURN_IF_ERROR(_convert_batch_to_chunk(batch, chunk));
    }

    _rows_read += (*chunk)->num_rows();
    _bytes_read += (*chunk)->bytes_usage();
    COUNTER_UPDATE(_profile.rows_read_counter, (*chunk)->num_rows());

    return Status::OK();
}

Status ADBCScanner::_convert_batch_to_chunk(const std::shared_ptr<arrow::RecordBatch>& batch, ChunkPtr* chunk) {
    size_t num_rows = batch->num_rows();
    const auto& slots = _tuple_desc->slots();
    auto result = std::make_shared<Chunk>();

    // Initialize chunk filter (1 = valid). The arrow converter accesses it unconditionally.
    Filter chunk_filter(num_rows, 1);
    size_t arrow_column_index = 0;

    for (SlotDescriptor* slot : slots) {
        if (!slot->is_materialized()) {
            continue;
        }

        if (arrow_column_index >= static_cast<size_t>(batch->num_columns())) {
            return Status::InternalError(fmt::format("ADBC: materialized column index {} >= batch columns {}",
                                                     arrow_column_index, batch->num_columns()));
        }

        auto arrow_column = batch->column(arrow_column_index);
        auto arrow_type = arrow_column->type();
        ArrowTypeId arrow_type_id = arrow_type->id();
        LogicalType sr_type = slot->type().type;
        bool is_nullable = slot->is_nullable();

        ConvertFunc converter = get_arrow_converter(arrow_type_id, sr_type, is_nullable, true);
        if (converter == nullptr) {
            return Status::InternalError(fmt::format("No Arrow converter for arrow type {} to StarRocks type {}",
                                                     arrow_type->ToString(), type_to_string(sr_type)));
        }

        // Create column with reserved capacity but size 0.
        // The converter internally calls resize(size + num_elements), so we must NOT pre-resize.
        auto column = ColumnHelper::create_column(slot->type(), slot->is_nullable());
        column->reserve(num_rows);

        if (num_rows > 0) {
            ConvertFuncTree converter_tree(converter);
            ArrowConvertContext context;
            context.set_current_column(slot->col_name(), slot->type());
            std::string conversion_error;
            context.report_error_message = [&](const std::string& reason, const std::string&, int64_t) {
                if (conversion_error.empty()) {
                    conversion_error = reason;
                }
            };
            RETURN_IF_ERROR(convert_arrow_array_to_column(&converter_tree, num_rows, arrow_column.get(), column.get(),
                                                          0, 0, &chunk_filter, &context));
            // Load converters mark rejected rows in the filter. A query must fail instead
            // of losing rows, and a later non-nullable column can overwrite this filter.
            if (std::find(chunk_filter.begin(), chunk_filter.end(), 0) != chunk_filter.end()) {
                return Status::InternalError(
                        fmt::format("ADBC: cannot convert column '{}': {}", slot->col_name(),
                                    conversion_error.empty() ? "value is out of range" : conversion_error));
            }
        }

        result->append_column(std::move(column), slot->id());
        ++arrow_column_index;
    }

    *chunk = std::move(result);
    return Status::OK();
}

void ADBCScanner::close(RuntimeState* state) {
    if (_closed) return;
    _closed = true;

    // Release Arrow C stream
    if (_c_stream.release) {
        _c_stream.release(&_c_stream);
        _c_stream.release = nullptr;
    }
    _arrow_schema.reset();
    _pending_batch.reset();

    // Release ADBC resources in reverse order: statement -> connection -> database
    AdbcError error = ADBC_ERROR_INIT;

    if (_statement_initialized) {
        AdbcStatusCode sc = AdbcStatementRelease(&_statement, &error);
        if (sc != ADBC_STATUS_OK) {
            LOG(WARNING) << "Failed to release ADBC statement: " << (error.message ? error.message : "Unknown error");
            if (error.release) error.release(&error);
        }
        _statement_initialized = false;
    }

    if (_connection_initialized) {
        error = ADBC_ERROR_INIT;
        AdbcStatusCode sc = AdbcConnectionRelease(&_connection, &error);
        if (sc != ADBC_STATUS_OK) {
            LOG(WARNING) << "Failed to release ADBC connection: " << (error.message ? error.message : "Unknown error");
            if (error.release) error.release(&error);
        }
        _connection_initialized = false;
    }

    if (_database_initialized) {
        error = ADBC_ERROR_INIT;
        AdbcStatusCode sc = AdbcDatabaseRelease(&_database, &error);
        if (sc != ADBC_STATUS_OK) {
            LOG(WARNING) << "Failed to release ADBC database: " << (error.message ? error.message : "Unknown error");
            if (error.release) error.release(&error);
        }
        _database_initialized = false;
    }
}

} // namespace starrocks
