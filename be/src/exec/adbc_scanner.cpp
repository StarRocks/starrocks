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

#include <sstream>

#include "base/time/time.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "exec/adbc_driver_registry.h"
#include "exec/adbc_parallel_reader.h"
#include "exec/arrow_to_starrocks_converter.h"
#include "exec/arrow_type_traits.h"
#include "runtime/descriptors.h"
#include "storage/chunk_helper.h"

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

ADBCScanner::ADBCScanner(const ADBCScanContext& ctx, const TupleDescriptor* tuple_desc)
        : _ctx(ctx), _tuple_desc(tuple_desc) {}

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

        RETURN_IF_ERROR(_init_adbc());

        _connect_time_ms = MonotonicMillis() - start;

        // Try parallel reading first; fall back to single stream if it fails
        auto parallel_status = _try_parallel_read();
        if (!parallel_status.ok()) {
            RETURN_IF_ERROR(_fallback_single_stream_read());
        }

        _opened = true;
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError(fmt::format("ADBC driver threw C++ exception during open: {}", e.what()));
    } catch (...) {
        return Status::InternalError("ADBC driver threw unknown C++ exception during open");
    }
}

Status ADBCScanner::_init_adbc() {
    AdbcError error = ADBC_ERROR_INIT;

    // 1. Ensure driver is loaded via the registry (load-once,
    //    never-dlclose). The registry calls AdbcLoadDriver() which
    //    performs dlopen(RTLD_NOW | RTLD_LOCAL) on first call, and returns
    //    the cached AdbcDriver on subsequent calls.
    auto driver_result = ADBCDriverRegistry::instance().get_or_load(_ctx.driver_url, _ctx.entrypoint);
    if (!driver_result.ok()) return driver_result.status();

    // 2. Init database
    RETURN_ADBC_NOT_OK(AdbcDatabaseNew(&_database, &error), error);

    // 3. Set driver path (MUST be first option per ADBC protocol)
    error = ADBC_ERROR_INIT;
    RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database, "driver", _ctx.driver_url.c_str(), &error), error);

    // 4. Optional entrypoint
    if (!_ctx.entrypoint.empty()) {
        error = ADBC_ERROR_INIT;
        RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database, "entrypoint", _ctx.entrypoint.c_str(), &error), error);
    }

    // 5. Standard options: uri, username, password
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

    // 6. Forward ALL adbc_options before Init
    for (const auto& [key, value] : _ctx.adbc_options) {
        // Skip uri/username/password — already set above
        if (key == "uri" || key == "username" || key == "password") continue;
        error = ADBC_ERROR_INIT;
        RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database, key.c_str(), value.c_str(), &error), error);
    }

    // 7. Initialize database (driver manager routes to the already-loaded driver)
    error = ADBC_ERROR_INIT;
    RETURN_ADBC_NOT_OK(AdbcDatabaseInit(&_database, &error), error);
    _database_initialized = true;

    // 8. Connection (per-fragment, never cached)
    error = ADBC_ERROR_INIT;
    RETURN_ADBC_NOT_OK(AdbcConnectionNew(&_connection, &error), error);
    error = ADBC_ERROR_INIT;
    RETURN_ADBC_NOT_OK(AdbcConnectionInit(&_connection, &_database, &error), error);
    _connection_initialized = true;

    // 9. Statement
    error = ADBC_ERROR_INIT;
    RETURN_ADBC_NOT_OK(AdbcStatementNew(&_connection, &_statement, &error), error);
    error = ADBC_ERROR_INIT;
    RETURN_ADBC_NOT_OK(AdbcStatementSetSqlQuery(&_statement, _ctx.sql.c_str(), &error), error);
    _statement_initialized = true;

    return Status::OK();
}

Status ADBCScanner::_try_parallel_read() {
    AdbcError error = ADBC_ERROR_INIT;
    ArrowSchema c_schema{};
    c_schema.release = nullptr;
    struct AdbcPartitions partitions {};
    int64_t rows_affected = -1;

    // Ensure partitions and schema are released on all exit paths
    auto cleanup_partitions = [&partitions]() {
        if (partitions.release) {
            partitions.release(&partitions);
            partitions.release = nullptr;
        }
    };
    auto cleanup_schema = [&c_schema]() {
        if (c_schema.release) {
            c_schema.release(&c_schema);
            c_schema.release = nullptr;
        }
    };

    AdbcStatusCode sc = AdbcStatementExecutePartitions(&_statement, &c_schema, &partitions, &rows_affected, &error);
    if (sc != ADBC_STATUS_OK) {
        if (error.release) error.release(&error);
        cleanup_schema();
        cleanup_partitions();
        return Status::NotSupported("ADBC partitions not supported by driver");
    }

    if (partitions.num_partitions == 0) {
        // Driver returned no partitions; treat as not-supported so caller falls back
        // to single-stream ExecuteQuery. (No prior GetFlightInfo state to deadlock on.)
        cleanup_schema();
        cleanup_partitions();
        return Status::NotSupported("ADBC ExecutePartitions returned 0 partitions");
    }

    if (partitions.num_partitions == 1) {
        // Single-partition path: read the partition we already have via
        // AdbcConnectionReadPartition (a DoGet on the existing ticket). Do NOT
        // fall back to AdbcStatementExecuteQuery — that would issue a second
        // GetFlightInfo on the same connection while the first is still being
        // held by the server's per-bearer-token query semaphore (Flight SQL
        // servers serialize GetFlightInfo on the same connection).
        AdbcError ep_error = ADBC_ERROR_INIT;
        AdbcStatusCode read_sc = AdbcConnectionReadPartition(&_connection, partitions.partitions[0],
                                                              partitions.partition_lengths[0], &_c_stream, &ep_error);
        cleanup_schema();
        cleanup_partitions();
        if (read_sc != ADBC_STATUS_OK) {
            std::string msg = ep_error.message ? ep_error.message : "Unknown ADBC error";
            if (ep_error.release) ep_error.release(&ep_error);
            return Status::InternalError("ADBC ReadPartition failed: " + msg);
        }
        _stream_initialized = true;

        // Pull schema off the stream so _convert_batch_to_chunk has it.
        struct ArrowSchema stream_schema {};
        if (_c_stream.get_schema(&_c_stream, &stream_schema) != 0) {
            const char* err = _c_stream.get_last_error(&_c_stream);
            return Status::InternalError(
                    fmt::format("Failed to get schema from ADBC partition stream: {}", err ? err : "unknown"));
        }
        auto schema_result = arrow::ImportSchema(&stream_schema);
        if (!schema_result.ok()) {
            return Status::InternalError("Failed to import Arrow schema: " + schema_result.status().ToString());
        }
        _arrow_schema = std::move(schema_result).ValueUnsafe();

        _use_parallel = false;
        return Status::OK();
    }

    // Multi-partition path: spin up the parallel reader.
    size_t num_threads = std::min(partitions.num_partitions, (size_t)4);
    _parallel_reader = std::make_unique<ADBCParallelReader>(&_database, partitions, num_threads);
    auto start_status = _parallel_reader->start();
    cleanup_schema();
    cleanup_partitions();
    if (!start_status.ok()) {
        _parallel_reader.reset();
        return start_status;
    }
    _use_parallel = true;

    return Status::OK();
}

Status ADBCScanner::_fallback_single_stream_read() {
    AdbcError error = ADBC_ERROR_INIT;
    int64_t rows_affected = -1;

    RETURN_ADBC_NOT_OK(AdbcStatementExecuteQuery(&_statement, &_c_stream, &rows_affected, &error), error);
    _stream_initialized = true;

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

    _use_parallel = false;

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
            // Read directly from the C stream
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

    // Initialize chunk filter (1 = valid). The arrow converter accesses it unconditionally.
    Filter chunk_filter(num_rows, 1);

    for (size_t i = 0; i < slots.size(); i++) {
        SlotDescriptor* slot = slots[i];
        if (!slot->is_materialized()) {
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

        ConvertFunc converter = get_arrow_converter(arrow_type_id, sr_type, is_nullable, true);
        if (converter == nullptr) {
            return Status::InternalError(fmt::format("No Arrow converter for arrow type {} to StarRocks type {}",
                                                     arrow_type->ToString(), type_to_string(sr_type)));
        }

        // Create column with reserved capacity but size 0.
        // The converter internally calls resize(size + num_elements), so we must NOT pre-resize.
        auto column = ColumnHelper::create_column(slot->type(), is_nullable);
        column->reserve(num_rows);

        if (is_nullable) {
            auto* nullable = down_cast<NullableColumn*>(column.get());
            auto* null_column = nullable->null_column_raw_ptr();
            size_t null_count = fill_null_column(arrow_column.get(), 0, num_rows, null_column, 0);
            nullable->set_has_null(null_count != 0);
            uint8_t* null_data = &null_column->get_data().front();

            Column* data_col = nullable->data_column_raw_ptr();
            RETURN_IF_ERROR(converter(arrow_column.get(), 0, num_rows, data_col, 0, null_data, &chunk_filter, nullptr,
                                      nullptr));
        } else {
            RETURN_IF_ERROR(converter(arrow_column.get(), 0, num_rows, column.get(), 0, nullptr, &chunk_filter, nullptr,
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

    // Close parallel reader first (Pitfall 6: must close before AdbcDatabaseRelease)
    if (_parallel_reader) {
        _parallel_reader->close();
        _parallel_reader.reset();
    }

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
