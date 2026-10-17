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

#include "connector/file/scanner/arrow_scanner.h"

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/message.h>
#include <arrow/ipc/reader.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <algorithm>
#include <utility>

#include "base/simd/simd.h"
#include "column/arrow/arrow_memory_pool.h"
#include "column/arrow/arrow_to_starrocks_converter.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/config_ingest_fwd.h"
#include "compute_env/load/stream_load_pipe.h"
#include "compute_env/load_path/load_path_state_helper.h"
#include "compute_env/load_path/rejected_record_writer.h"
#include "exprs/column_ref.h"
#include "formats/arrow/arrow_column_converter.h"
#include "gutil/strings/substitute.h"
#include "platform/fs_broker.h"
#include "runtime/runtime_state.h"

namespace starrocks {

static const int MAX_ERROR_MESSAGE_COUNTER = 100;

namespace {

class StarRocksArrowInputStream : public arrow::io::InputStream {
public:
    StarRocksArrowInputStream(RuntimeState* state, std::shared_ptr<SequentialFile> file)
            : _state(state), _file(std::move(file)), _pos(0), _closed(false) {}

    ~StarRocksArrowInputStream() override = default;

    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
        if (_closed) {
            return arrow::Status::Invalid("stream is closed");
        }
        auto res = _file->read(out, nbytes);
        if (!res.ok()) {
            return arrow::Status::IOError(res.status().to_string());
        }
        int64_t bytes_read = res.value();
        _pos += bytes_read;
        if (_state != nullptr) {
            _state->update_num_bytes_scan_from_source(bytes_read);
        }
        return bytes_read;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(auto buf, arrow::AllocateResizableBuffer(nbytes));
        ARROW_ASSIGN_OR_RAISE(auto bytes_read, Read(nbytes, buf->mutable_data()));
        if (bytes_read < nbytes) {
            ARROW_RETURN_NOT_OK(buf->Resize(bytes_read));
        }
        return std::shared_ptr<arrow::Buffer>(std::move(buf));
    }

    arrow::Result<int64_t> Tell() const override {
        if (_closed) {
            return arrow::Status::Invalid("stream is closed");
        }
        return _pos;
    }

    arrow::Status Close() override {
        _closed = true;
        return arrow::Status::OK();
    }

    bool closed() const override { return _closed; }

private:
    RuntimeState* _state;
    std::shared_ptr<SequentialFile> _file;
    int64_t _pos;
    bool _closed;
};

// Inspects all RECORD_BATCH message headers in an Arrow IPC stream buffer
// without decompressing or allocating body buffers so rejected messages
// report their true declared row count even when ReadNext() fails mid-stream.
int64_t count_ipc_stream_declared_rows(const std::shared_ptr<arrow::Buffer>& ipc_buffer) {
    if (ipc_buffer == nullptr || ipc_buffer->size() == 0) {
        return 0;
    }
    auto msg_reader = arrow::ipc::MessageReader::Open(std::make_shared<arrow::io::BufferReader>(ipc_buffer));
    if (msg_reader == nullptr) {
        return 1;
    }
    auto empty_schema = arrow::schema({});
    arrow::io::BufferReader empty_body(std::make_shared<arrow::Buffer>(nullptr, 0));
    arrow::ipc::IpcReadOptions header_opts = arrow::ipc::IpcReadOptions::Defaults();
    int64_t declared_rows = 0;
    while (true) {
        auto msg_res = msg_reader->ReadNextMessage();
        if (!msg_res.ok()) {
            declared_rows += 1;
            break;
        }
        auto msg = std::move(msg_res).MoveValueUnsafe();
        if (msg == nullptr) {
            break;
        }
        if (msg->type() == arrow::ipc::MessageType::RECORD_BATCH) {
            auto batch_res =
                    arrow::ipc::ReadRecordBatch(*msg->metadata(), empty_schema, nullptr, header_opts, &empty_body);
            if (batch_res.ok() && batch_res.ValueUnsafe() != nullptr) {
                declared_rows += batch_res.ValueUnsafe()->num_rows();
            } else {
                declared_rows += 1;
            }
        }
    }
    return declared_rows;
}

} // namespace

ArrowScanner::ArrowScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                           ScannerCounter* counter, bool schema_only)
        : FileScanner(state, profile, scan_range.params, counter, schema_only),
          _scan_range(scan_range),
          _curr_file_reader(nullptr),
          _max_chunk_size(state->chunk_size() ? state->chunk_size() : 4096) {
    _file_format_str = "arrow";
    _chunk_filter.reserve(_max_chunk_size);
    _conv_ctx.timezone = state->timezone();
    _conv_ctx.report_error_message = [state, ctx = &_conv_ctx](const std::string& reason, const std::string& raw_data,
                                                               int64_t row_offset_in_array) {
        if (ctx->error_message_counter > MAX_ERROR_MESSAGE_COUNTER) return;
        ctx->error_message_counter += 1;
        const std::string& col_name = ctx->current_column_name;
        std::string error_msg = strings::Substitute("file = $0, column = $1, raw data = $2", ctx->current_file,
                                                    col_name.empty() ? "null" : col_name, raw_data);
        if (ctx->consumer_partition != -1) {
            error_msg += strings::Substitute(", partition = $0", ctx->consumer_partition);
        }
        if (ctx->consumer_offset != -1) {
            error_msg += strings::Substitute(", offset = $0", ctx->consumer_offset);
        }
        if (!ctx->consumer_message_id.empty()) {
            error_msg += strings::Substitute(", message_id = $0", ctx->consumer_message_id);
        }
        LoadPathStateHelper::append_error_msg_to_file(state, error_msg, reason);

        auto* writer = LoadPathStateHelper::rejected_record_writer(state);
        if (writer != nullptr) {
            const std::string key = col_name.empty() ? std::string("_raw") : col_name;

            rapidjson::Document src_doc;
            src_doc.SetObject();
            auto& alloc = src_doc.GetAllocator();
            src_doc.AddMember("format", rapidjson::Value("arrow", 5, alloc), alloc);
            src_doc.AddMember("file",
                              rapidjson::Value(ctx->current_file.c_str(),
                                               static_cast<rapidjson::SizeType>(ctx->current_file.size()), alloc),
                              alloc);
            if (ctx->consumer_partition != -1) {
                src_doc.AddMember("partition", rapidjson::Value(ctx->consumer_partition), alloc);
            }
            if (ctx->consumer_offset != -1) {
                src_doc.AddMember("offset", rapidjson::Value(ctx->consumer_offset), alloc);
            }
            if (!ctx->consumer_message_id.empty()) {
                src_doc.AddMember(
                        "message_id",
                        rapidjson::Value(ctx->consumer_message_id.c_str(),
                                         static_cast<rapidjson::SizeType>(ctx->consumer_message_id.size()), alloc),
                        alloc);
            }
            if (ctx->current_batch_first_row_in_file >= 0 && row_offset_in_array >= 0) {
                int64_t row_in_file = ctx->current_batch_first_row_in_file + row_offset_in_array;
                src_doc.AddMember("row_in_file", rapidjson::Value(row_in_file), alloc);
            }
            if (ctx->file_size >= 0) {
                src_doc.AddMember("file_size", rapidjson::Value(ctx->file_size), alloc);
            }
            if (ctx->file_mtime_ms >= 0) {
                src_doc.AddMember("file_mtime_ms", rapidjson::Value(ctx->file_mtime_ms), alloc);
            }
            rapidjson::StringBuffer src_buf;
            rapidjson::Writer<rapidjson::StringBuffer> src_writer(src_buf);
            src_doc.Accept(src_writer);

            writer->append_from_slices({Slice{raw_data.data(), raw_data.size()}}, {key},
                                       /*error_code=*/"TYPE_MISMATCH", reason, col_name,
                                       std::string(src_buf.GetString(), src_buf.GetSize()));
        }
    };
}

ArrowScanner::~ArrowScanner() = default;

Status ArrowScanner::open() {
    RETURN_IF_ERROR(FileScanner::open());
    if (_scan_range.ranges.empty()) {
        return Status::OK();
    }
    auto range = _scan_range.ranges[0];
    _num_of_columns_from_file = range.__isset.num_of_columns_from_file
                                        ? implicit_cast<int>(range.num_of_columns_from_file)
                                        : implicit_cast<int>(_src_slot_descriptors.size());

    for (auto i = 0; i < _num_of_columns_from_file; ++i) {
        _conv_funcs.emplace_back(std::make_unique<ConvertFuncTree>());
    }
    _cast_exprs.resize(_num_of_columns_from_file, nullptr);

    if (range.__isset.num_of_columns_from_file) {
        int nums = range.columns_from_path.size();
        for (const auto& rng : _scan_range.ranges) {
            if (nums != rng.columns_from_path.size()) {
                return Status::InternalError("Different range different columns.");
            }
        }
    }
    return Status::OK();
}

Status ArrowScanner::open_next_reader() {
    if (_next_file >= _scan_range.ranges.size()) {
        _scanner_eof = true;
        return Status::EndOfFile("no more files to read");
    }
    const auto& range_desc = _scan_range.ranges[_next_file++];

    if (range_desc.start_offset == 0) {
        ++_counter->num_files_read;
    }

    TNetworkAddress address;
    if (!_scan_range.broker_addresses.empty()) {
        address = _scan_range.broker_addresses[0];
    }
    RETURN_IF_ERROR(create_sequential_file(range_desc, address, _scan_range.params, &_file));

    auto* stream_file = dynamic_cast<StreamLoadPipeInputStream*>(_file->stream().get());
    auto* pipe = stream_file ? stream_file->pipe().get() : nullptr;
    _is_discrete_pipe = pipe != nullptr && pipe->is_discrete_message_pipe();
    if (_is_discrete_pipe) {
        // Delay opening reader until next_batch() for discrete buffers (Kafka/Pulsar Routine Load)
        _curr_file_reader = nullptr;
    } else {
        // Continuous input stream for HTTP Stream Load and local/remote files
        auto arrow_stream = std::make_shared<StarRocksArrowInputStream>(_state, _file);
        auto reader_res = arrow::ipc::RecordBatchStreamReader::Open(arrow_stream);
        if (!reader_res.ok()) {
            return Status::InternalError("open RecordBatchStreamReader failed, reason: " +
                                         reader_res.status().ToString());
        }
        _curr_file_reader = std::move(reader_res).MoveValueUnsafe();
    }

    _conv_ctx.current_file = range_desc.path;
    _conv_ctx.current_batch_first_row_in_file = -1;
    _conv_ctx.file_mtime_ms = range_desc.__isset.modification_time ? range_desc.modification_time : -1;
    _conv_ctx.file_size = range_desc.size;
    _last_file_scan_rows = 0;
    return Status::OK();
}

Status ArrowScanner::next_batch() {
    SCOPED_RAW_TIMER(&_counter->read_batch_ns);
    _batch_start_idx = 0;

    if (_is_discrete_pipe && !_discrete_batches.empty()) {
        _batch = std::move(_discrete_batches.front());
        _discrete_batches.pop_front();
        _conv_ctx.current_batch_first_row_in_file = _last_file_scan_rows;
        _last_file_scan_rows += _batch->num_rows();
        return Status::OK();
    }

    auto format_source_info = [this]() {
        std::string info;
        if (_conv_ctx.consumer_partition != -1) {
            info += " at partition=" + std::to_string(_conv_ctx.consumer_partition);
        }
        if (_conv_ctx.consumer_offset != -1) {
            info += " offset=" + std::to_string(_conv_ctx.consumer_offset);
        }
        if (!_conv_ctx.consumer_message_id.empty()) {
            info += " message_id=" + _conv_ctx.consumer_message_id;
        }
        return info;
    };

    while (true) {
        if (_file == nullptr) {
            auto status = open_next_reader();
            if (!status.ok()) {
                if (status.is_end_of_file()) {
                    _scanner_eof = true;
                }
                return status;
            }
        }

        if (_is_discrete_pipe) {
            auto* stream_file = dynamic_cast<StreamLoadPipeInputStream*>(_file->stream().get());
            auto* pipe = stream_file ? stream_file->pipe().get() : nullptr;
            DCHECK(pipe != nullptr);

            auto res = pipe->read();
            if (!res.ok()) {
                _batch.reset();
                _discrete_batches.clear();
                _parser_buf.reset();
                if (res.status().is_end_of_file()) {
                    _scanner_eof = true;
                    return Status::EndOfFile("EOF");
                }
                return res.status();
            }
            _parser_buf = res.value();
            if (_parser_buf == nullptr || _parser_buf->remaining() == 0) {
                continue;
            }

            if (_state != nullptr) {
                _state->update_num_bytes_scan_from_source(_parser_buf->remaining());
            }

            _last_file_scan_rows = 0;
            _conv_ctx.current_batch_first_row_in_file = -1;
            _conv_ctx.consumer_partition = -1;
            _conv_ctx.consumer_offset = -1;
            _conv_ctx.consumer_message_id.clear();
            if (auto meta = _parser_buf->meta()) {
                if (meta->type() != ByteBufferMetaType::NONE) {
                    auto msg_meta = static_cast<const StreamMessageMeta*>(meta);
                    _conv_ctx.consumer_partition = msg_meta->partition();
                    _conv_ctx.consumer_offset = msg_meta->offset();
                    _conv_ctx.consumer_message_id = msg_meta->message_id();
                }
            }
            for (auto& conv : _conv_funcs) {
                conv = std::make_unique<ConvertFuncTree>();
            }

            int64_t filtered_rows = 0;
            Status decode_status = decode_discrete_message(&filtered_rows);
            if (!decode_status.ok()) {
                reject_discrete_message(std::string(decode_status.message()), filtered_rows);
                continue;
            }
            if (_discrete_batches.empty()) {
                _parser_buf.reset();
                continue;
            }
            _batch = std::move(_discrete_batches.front());
            _discrete_batches.pop_front();
            _batch_start_idx = 0;
            _conv_ctx.current_batch_first_row_in_file = _last_file_scan_rows;
            _last_file_scan_rows += _batch->num_rows();
            return Status::OK();
        }

        if (_curr_file_reader == nullptr) {
            _scanner_eof = true;
            return Status::EndOfFile("EOF");
        }

        arrow::Status status = _curr_file_reader->ReadNext(&_batch);
        if (!status.ok()) {
            std::string error_msg = "ReadNext batch failed, reason: " + status.ToString() + format_source_info();
            _conv_ctx.report_error_message(error_msg, "", -1);
            LOG(WARNING) << "Arrow routine load: " << error_msg;
            _counter->num_rows_filtered++;
            _curr_file_reader.reset();
            _parser_buf.reset();
            return Status::InternalError(error_msg);
        }

        if (_batch == nullptr) {
            _curr_file_reader.reset();
            _parser_buf.reset();
            for (auto& conv : _conv_funcs) {
                conv = std::make_unique<ConvertFuncTree>();
            }
            _file.reset();
            continue;
        }

        _conv_ctx.current_batch_first_row_in_file = _last_file_scan_rows;
        _last_file_scan_rows += _batch->num_rows();
        return Status::OK();
    }
}

Status ArrowScanner::validate_batch_schema(const std::shared_ptr<arrow::RecordBatch>& batch) {
    ObjectPool dummy_pool;
    for (auto i = 0; i < _num_of_columns_from_file; ++i) {
        SlotDescriptor* slot_desc = _src_slot_descriptors[i];
        if (slot_desc == nullptr) {
            continue;
        }
        auto array_ptr = batch->GetColumnByName(std::string(slot_desc->col_name()));
        if (array_ptr == nullptr) {
            if (!slot_desc->is_nullable()) {
                return Status::InvalidArgument(strings::Substitute(
                        "column $0 is non-nullable but missing in Arrow message", slot_desc->col_name()));
            }
        } else {
            RETURN_IF_ERROR(validate_arrow_column_type(array_ptr->type().get(), slot_desc));
            MutableColumnPtr dummy_column;
            ConvertFuncTree dummy_conv;
            Expr* dummy_expr = nullptr;
            RETURN_IF_ERROR(create_arrow_column(array_ptr->type().get(), slot_desc, &dummy_column, &dummy_conv,
                                                &dummy_expr, dummy_pool, _strict_mode));
        }
    }
    return Status::OK();
}

Status ArrowScanner::validate_ipc_schema(const std::shared_ptr<arrow::Schema>& schema) {
    if (schema == nullptr) {
        return Status::InvalidArgument("Arrow IPC stream schema is null");
    }
    ObjectPool dummy_pool;
    for (auto i = 0; i < _num_of_columns_from_file; ++i) {
        SlotDescriptor* slot_desc = _src_slot_descriptors[i];
        if (slot_desc == nullptr) {
            continue;
        }
        auto field = schema->GetFieldByName(std::string(slot_desc->col_name()));
        if (field == nullptr) {
            if (!slot_desc->is_nullable()) {
                return Status::InvalidArgument(strings::Substitute(
                        "column $0 is non-nullable but missing in Arrow message", slot_desc->col_name()));
            }
        } else {
            RETURN_IF_ERROR(validate_arrow_column_type(field->type().get(), slot_desc));
            MutableColumnPtr dummy_column;
            ConvertFuncTree dummy_conv;
            Expr* dummy_expr = nullptr;
            RETURN_IF_ERROR(create_arrow_column(field->type().get(), slot_desc, &dummy_column, &dummy_conv, &dummy_expr,
                                                dummy_pool, _strict_mode));
        }
    }
    return Status::OK();
}

Status ArrowScanner::decode_discrete_message(int64_t* filtered_rows) {
    DCHECK(_parser_buf != nullptr);
    SCOPED_RAW_TIMER(&_counter->arrow_decode_ns);
    *filtered_rows = 0;

    // Release any previous batches BEFORE resetting _discrete_memory_pool so
    // arrow::PoolBuffer::~PoolBuffer() frees into the pool that allocated it.
    _batch.reset();
    _discrete_batches.clear();
    const int64_t max_decoded_bytes = config::routine_load_arrow_max_decoded_bytes_per_message;
    _discrete_memory_pool = std::make_unique<ArrowBoundedMemoryPool>(max_decoded_bytes);

    auto owning_buffer = make_owning_arrow_buffer(_parser_buf);
    auto buffer_reader = std::make_shared<arrow::io::BufferReader>(owning_buffer);

    arrow::ipc::IpcReadOptions read_options = arrow::ipc::IpcReadOptions::Defaults();
    read_options.memory_pool = _discrete_memory_pool.get();

    auto reader_res = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader, read_options);
    if (!reader_res.ok()) {
        *filtered_rows = 1;
        _counter->arrow_messages_rejected++;
        return Status::InternalError("Arrow IPC parse error: " + reader_res.status().ToString());
    }
    auto reader = std::move(reader_res).MoveValueUnsafe();

    constexpr int64_t kBatchMetadataOverheadBytes = 1024;
    int64_t rows = 0;
    int64_t batch_count = 0;
    bool schema_checked = false;
    std::string schema_error;
    std::deque<std::shared_ptr<arrow::RecordBatch>> valid_batches;

    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        arrow::Status st = reader->ReadNext(&batch);
        if (!st.ok()) {
            valid_batches.clear();
            batch.reset();
            *filtered_rows = std::max<int64_t>({rows, count_ipc_stream_declared_rows(owning_buffer), int64_t{1}});
            _counter->arrow_messages_rejected++;
            std::string read_error = "Arrow IPC read batch error: " + st.ToString();
            if (!schema_error.empty()) {
                return Status::InternalError(schema_error + "; " + read_error);
            }
            return Status::InternalError(read_error);
        }
        if (batch == nullptr) {
            break;
        }

        rows += batch->num_rows();
        ++batch_count;
        if (max_decoded_bytes > 0 && (_discrete_memory_pool->bytes_allocated() > max_decoded_bytes ||
                                      batch_count * kBatchMetadataOverheadBytes > max_decoded_bytes)) {
            valid_batches.clear();
            batch.reset();
            *filtered_rows = std::max<int64_t>({rows, count_ipc_stream_declared_rows(owning_buffer), int64_t{1}});
            _counter->arrow_messages_rejected++;
            return Status::MemoryLimitExceeded(strings::Substitute(
                    "Arrow IPC message exceeds routine_load_arrow_max_decoded_bytes_per_message ($0 bytes)",
                    max_decoded_bytes));
        }

        if (!schema_checked) {
            Status schema_status = validate_batch_schema(batch);
            if (!schema_status.ok()) {
                schema_error = "schema mismatch: " + std::string(schema_status.message());
            }
            schema_checked = true;
        }
        if (schema_error.empty() && batch->num_rows() > 0) {
            valid_batches.push_back(std::move(batch));
        }
    }

    if (!schema_checked) {
        Status schema_status = validate_ipc_schema(reader->schema());
        if (!schema_status.ok()) {
            schema_error = "schema mismatch: " + std::string(schema_status.message());
        }
    }

    if (!schema_error.empty()) {
        valid_batches.clear();
        *filtered_rows = rows;
        _counter->arrow_messages_rejected++;
        return Status::InvalidArgument(schema_error);
    }

    _counter->arrow_messages_read++;
    _counter->arrow_decompressed_bytes += _discrete_memory_pool->total_bytes_allocated();
    _discrete_batches = std::move(valid_batches);
    return Status::OK();
}

void ArrowScanner::reject_discrete_message(const std::string& reason, int64_t filtered_rows) {
    std::string error_msg = "Arrow routine load: " + reason;
    if (_conv_ctx.consumer_partition != -1) {
        error_msg += " at partition=" + std::to_string(_conv_ctx.consumer_partition);
    }
    if (_conv_ctx.consumer_offset != -1) {
        error_msg += " offset=" + std::to_string(_conv_ctx.consumer_offset);
    }
    if (!_conv_ctx.consumer_message_id.empty()) {
        error_msg += " message_id=" + _conv_ctx.consumer_message_id;
    }
    _conv_ctx.report_error_message(error_msg, "", -1);
    LOG(WARNING) << error_msg;
    _counter->num_rows_filtered += std::max<int64_t>(filtered_rows, 1);

    _batch.reset();
    _discrete_batches.clear();
    _batch_start_idx = 0;
    _curr_file_reader.reset();
    _parser_buf.reset();
    for (auto& conv : _conv_funcs) {
        conv = std::make_unique<ConvertFuncTree>();
    }
    _conv_ctx.consumer_partition = -1;
    _conv_ctx.consumer_offset = -1;
    _conv_ctx.consumer_message_id.clear();
}

void ArrowScanner::filter_current_discrete_message(const std::string& reason, int64_t pending_rows) {
    // Include rows buffered in a discarded chunk and the unconsumed part of
    // this batch, plus any remaining decoded batches in this discrete message,
    // but never recount rows already returned in earlier chunks.
    int64_t filtered = pending_rows;
    if (_batch != nullptr) {
        filtered += _batch->num_rows() - static_cast<int64_t>(_batch_start_idx);
    }
    for (const auto& remaining_batch : _discrete_batches) {
        if (remaining_batch != nullptr) {
            filtered += remaining_batch->num_rows();
        }
    }
    _batch.reset();
    _discrete_batches.clear();
    _counter->arrow_messages_rejected++;
    reject_discrete_message(reason, filtered);
}

Status ArrowScanner::initialize_src_chunk(ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_counter->init_chunk_ns);
    // Record which file this chunk belongs to.  Used by finalize_src_chunk() to
    // attribute path/partition columns correctly even when get_next() breaks at
    // a file boundary after next_batch() has already advanced _next_file.
    _chunk_file_idx = _next_file - 1;
    _pool.clear();
    (*chunk) = std::make_shared<Chunk>();
    _chunk_filter.clear();
    for (auto i = 0; i < _num_of_columns_from_file; ++i) {
        SlotDescriptor* slot_desc = _src_slot_descriptors[i];
        if (slot_desc == nullptr) {
            continue;
        }
        MutableColumnPtr column;
        auto array_ptr = _batch->GetColumnByName(std::string(slot_desc->col_name()));
        if (array_ptr == nullptr) {
            _cast_exprs[i] = _pool.add(new ColumnRef(slot_desc));
            column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
        } else {
            RETURN_IF_ERROR(create_arrow_column(array_ptr->type().get(), slot_desc, &column, _conv_funcs[i].get(),
                                                &_cast_exprs[i], _pool, _strict_mode));
        }
        column->reserve(_max_chunk_size);
        (*chunk)->append_column(std::move(column), slot_desc->id());
    }
    return Status::OK();
}

Status ArrowScanner::append_batch_to_src_chunk(ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_counter->fill_ns);
    size_t num_elements =
            std::min<size_t>((_max_chunk_size - _chunk_start_idx), (_batch->num_rows() - _batch_start_idx));
    _chunk_filter.resize(_chunk_filter.size() + num_elements, 1);
    for (auto i = 0; i < _num_of_columns_from_file; ++i) {
        SlotDescriptor* slot_desc = _src_slot_descriptors[i];
        if (slot_desc == nullptr) {
            continue;
        }
        _conv_ctx.set_current_column(slot_desc->col_name(), slot_desc->type());
        auto* column = (*chunk)->get_column_raw_ptr_by_slot_id(slot_desc->id());
        auto array_ptr = _batch->GetColumnByName(std::string(slot_desc->col_name()));
        if (array_ptr == nullptr) {
            if (!column->append_nulls(num_elements)) {
                return Status::InvalidArgument(strings::Substitute("column $0 is non-nullable but missing in the file",
                                                                   slot_desc->col_name()));
            }
        } else {
            if (_conv_funcs[i]->func == nullptr) {
                auto& type_desc = slot_desc->type();
                TypeDescriptor raw_type_desc;
                bool need_cast = false;
                RETURN_IF_ERROR(build_arrow_column_convert_plan(array_ptr->type().get(), &type_desc,
                                                                slot_desc->is_nullable(), &raw_type_desc,
                                                                _conv_funcs[i].get(), need_cast, _strict_mode));
            }
            auto st = convert_arrow_array_to_column(_conv_funcs[i].get(), num_elements, array_ptr.get(), column,
                                                    _batch_start_idx, _chunk_start_idx, &_chunk_filter, &_conv_ctx);
            if (!st.ok()) {
                return st.clone_and_prepend(strings::Substitute("file=$0 column=$1 batch_row_range=[$2,$3)",
                                                                _conv_ctx.current_file, slot_desc->col_name(),
                                                                _batch_start_idx, _batch_start_idx + num_elements));
            }
        }
    }

    _chunk_start_idx += num_elements;
    _batch_start_idx += num_elements;
    return Status::OK();
}

Status ArrowScanner::finalize_src_chunk(ChunkPtr* chunk) {
    auto num_rows = (*chunk)->filter(_chunk_filter);
    _counter->num_rows_filtered += _chunk_start_idx - num_rows;
    ChunkPtr cast_chunk = std::make_shared<Chunk>();
    {
        SCOPED_RAW_TIMER(&_counter->cast_chunk_ns);
        for (auto i = 0; i < _num_of_columns_from_file; ++i) {
            SlotDescriptor* slot_desc = _src_slot_descriptors[i];
            if (slot_desc == nullptr) {
                continue;
            }

            ASSIGN_OR_RETURN(auto column, _cast_exprs[i]->evaluate_checked(nullptr, (*chunk).get()));
            column = ColumnHelper::unfold_const_column(slot_desc->type(), (*chunk)->num_rows(), column);
            cast_chunk->append_column(column, slot_desc->id());
        }
        auto range = _scan_range.ranges.at(_chunk_file_idx);
        if (range.__isset.num_of_columns_from_file) {
            fill_columns_from_path(cast_chunk, range.num_of_columns_from_file, range.columns_from_path,
                                   cast_chunk->num_rows());
        }
    }
    if (VLOG_ROW_IS_ON) {
        VLOG_ROW << "before finalize chunk: " << (*chunk)->debug_columns();
    }
    ASSIGN_OR_RETURN(auto dest_chunk, materialize(*chunk, cast_chunk));
    if (VLOG_ROW_IS_ON) {
        VLOG_ROW << "after finalize chunk: " << dest_chunk->debug_columns();
    }
    *chunk = dest_chunk;
    _chunk_start_idx = 0;
    return Status::OK();
}

StatusOr<ChunkPtr> ArrowScanner::get_next() {
    SCOPED_RAW_TIMER(&_counter->total_ns);
    while (true) {
        ChunkPtr chunk;
        _chunk_start_idx = 0;
        if (batch_is_exhausted()) {
            while (true) {
                Status status = next_batch();
                if (_scanner_eof) {
                    return status;
                }
                if (status.ok()) {
                    break;
                }
                if (status.is_end_of_file()) {
                    _curr_file_reader.reset();
                    _file.reset();
                    continue;
                }
                return status;
            }
        }
        auto init_st = initialize_src_chunk(&chunk);
        if (!init_st.ok()) {
            if (_is_discrete_pipe) {
                filter_current_discrete_message("initialization failed: " + std::string(init_st.message()));
                continue;
            }
            return init_st;
        }
        bool chunk_error = false;
        while (!_scanner_eof) {
            auto append_st = append_batch_to_src_chunk(&chunk);
            if (!append_st.ok()) {
                if (_is_discrete_pipe) {
                    filter_current_discrete_message("append batch failed: " + std::string(append_st.message()),
                                                    _chunk_start_idx);
                    chunk_error = true;
                    break;
                }
                return append_st;
            }
            if (chunk_is_full()) {
                break;
            }
            if (_is_discrete_pipe && batch_is_exhausted() && _discrete_batches.empty()) {
                _parser_buf.reset();
                break;
            }
            // Snapshot the current file index before fetching the next batch.  If
            // next_batch() opens a new file (i.e. _next_file advances), break here
            // so that finalize_src_chunk() can stamp all rows in this chunk with the
            // correct per-file path/partition values.  Mixing rows from two files in
            // one chunk would cause finalize_src_chunk() to apply the second file's
            // columns_from_path to every row, corrupting partition metadata for the
            // rows that came from the first file.
            const int file_before = _next_file;
            auto status = next_batch();
            if (status.ok()) {
                if (_next_file != file_before) {
                    // A new file was opened; finalize the current chunk
                    // before processing the new file in the next get_next() call.
                    break;
                }
                continue;
            }
            if (!status.is_end_of_file()) {
                return status;
            }

            _curr_file_reader.reset();
            _file.reset();
            if (chunk->num_rows() > 0) {
                break;
            }
            RETURN_IF_ERROR(next_batch());
            auto next_init_st = initialize_src_chunk(&chunk);
            if (!next_init_st.ok()) {
                if (_is_discrete_pipe) {
                    filter_current_discrete_message("initialization failed: " + std::string(next_init_st.message()));
                    chunk_error = true;
                    break;
                }
                return next_init_st;
            }
        }
        if (chunk_error) {
            continue;
        }
        RETURN_IF_ERROR(finalize_src_chunk(&chunk));
        if (chunk->is_empty()) {
            if (_scanner_eof) {
                return Status::EndOfFile("EOF");
            }
            continue;
        }
        return std::move(chunk);
    }
}

Status ArrowScanner::get_schema(std::vector<SlotDescriptor>* schema) {
    return Status::NotSupported("Arrow schema inference not supported yet");
}

void ArrowScanner::close() {
    _batch.reset();
    _discrete_batches.clear();
    _discrete_memory_pool.reset();
    _parser_buf.reset();
    _curr_file_reader.reset();
    _file.reset();
    _pool.clear();
    FileScanner::close();
}

bool ArrowScanner::chunk_is_full() {
    return _chunk_start_idx >= _max_chunk_size;
}

bool ArrowScanner::batch_is_exhausted() {
    return _scanner_eof || _batch == nullptr || _batch_start_idx >= _batch->num_rows();
}

} // namespace starrocks
