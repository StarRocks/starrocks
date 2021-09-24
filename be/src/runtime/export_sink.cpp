// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/export_sink.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/export_sink.h"

#include <memory>
#include <sstream>

#include "column/column.h"
#include "env/env_broker.h"
#include "exec/broker_writer.h"
#include "exprs/expr.h"
#include "formats/csv/converter.h"
#include "formats/csv/output_stream_file.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"
#include "util/time.h"
#include "util/types.h"
#include "util/uid_util.h"

namespace starrocks {

ExportSink::ExportSink(ObjectPool* pool, const RowDescriptor& row_desc, const std::vector<TExpr>& t_exprs)
        : _state(nullptr),
          _pool(pool),
          _row_desc(row_desc),
          _t_output_expr(t_exprs),
          _profile(nullptr),
          _bytes_written_counter(nullptr),
          _rows_written_counter(nullptr),
          _write_timer(nullptr) {}

Status ExportSink::init(const TDataSink& t_sink) {
    RETURN_IF_ERROR(DataSink::init(t_sink));
    _t_export_sink = t_sink.export_sink;

    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, _t_output_expr, &_output_expr_ctxs));
    return Status::OK();
}

Status ExportSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));

    _state = state;

    std::stringstream title;
    title << "ExportSink (frag_id=" << state->fragment_instance_id() << ")";
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(title.str()));
    SCOPED_TIMER(_profile->total_time_counter());

    _mem_tracker = std::make_unique<MemTracker>(-1, "ExportSink", state->instance_mem_tracker());

    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state, _row_desc, _mem_tracker.get()));

    // TODO(lingbin): add some Counter
    _bytes_written_counter = ADD_COUNTER(profile(), "BytesExported", TUnit::BYTES);
    _rows_written_counter = ADD_COUNTER(profile(), "RowsExported", TUnit::UNIT);
    _write_timer = ADD_TIMER(profile(), "WriteTime");

    return Status::OK();
}

Status ExportSink::open(RuntimeState* state) {
    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));
    // open broker
    int query_timeout = state->query_options().query_timeout;
    int timeout_ms = query_timeout > 3600 ? 3600000 : query_timeout * 1000;
    RETURN_IF_ERROR(open_file_writer(timeout_ms));
    return Status::OK();
}

Status ExportSink::send(RuntimeState* state, RowBatch* batch) {
    SCOPED_TIMER(_profile->total_time_counter());
    int num_rows = batch->num_rows();
    // we send at most 1024 rows at a time
    int batch_send_rows = num_rows > 1024 ? 1024 : num_rows;
    std::stringstream ss;
    for (int i = 0; i < num_rows;) {
        ss.str("");
        for (int j = 0; j < batch_send_rows && i < num_rows; ++j, ++i) {
            RETURN_IF_ERROR(gen_row_buffer(batch->get_row(i), &ss));
        }

        const std::string& buf = ss.str();

        SCOPED_TIMER(_write_timer);
        // TODO(lingbin): for broker writer, we should not send rpc each row.
        RETURN_IF_ERROR(_output_stream->write(Slice(buf)));
        COUNTER_UPDATE(_bytes_written_counter, buf.size());
    }
    COUNTER_UPDATE(_rows_written_counter, num_rows);
    return Status::OK();
}

Status ExportSink::gen_row_buffer(TupleRow* row, std::stringstream* ss) {
    int num_columns = _output_expr_ctxs.size();
    // const TupleDescriptor& desc = row_desc().TupleDescriptor;
    for (int i = 0; i < num_columns; ++i) {
        void* item = _output_expr_ctxs[i]->get_value(row);
        if (item == nullptr) {
            (*ss) << "\\N";
        } else {
            switch (_output_expr_ctxs[i]->root()->type().type) {
            case TYPE_BOOLEAN:
            case TYPE_TINYINT:
                (*ss) << (int)*static_cast<int8_t*>(item);
                break;
            case TYPE_SMALLINT:
                (*ss) << *static_cast<int16_t*>(item);
                break;
            case TYPE_INT:
                (*ss) << *static_cast<int32_t*>(item);
                break;
            case TYPE_BIGINT:
                (*ss) << *static_cast<int64_t*>(item);
                break;
            case TYPE_LARGEINT:
                (*ss) << reinterpret_cast<PackedInt128*>(item)->value;
                break;
            case TYPE_FLOAT: {
                char buffer[MAX_FLOAT_STR_LENGTH + 2];
                float float_value = *static_cast<float*>(item);
                buffer[0] = '\0';
                int length = FloatToBuffer(float_value, MAX_FLOAT_STR_LENGTH, buffer);
                DCHECK(length >= 0) << "gcvt float failed, float value=" << float_value;
                (*ss) << buffer;
                break;
            }
            case TYPE_DOUBLE: {
                // To prevent loss of precision on float and double types,
                // they are converted to strings before output.
                // For example: For a double value 27361919854.929001,
                // the direct output of using std::stringstream is 2.73619e+10,
                // and after conversion to a string, it outputs 27361919854.929001
                char buffer[MAX_DOUBLE_STR_LENGTH + 2];
                double double_value = *static_cast<double*>(item);
                buffer[0] = '\0';
                int length = DoubleToBuffer(double_value, MAX_DOUBLE_STR_LENGTH, buffer);
                DCHECK(length >= 0) << "gcvt double failed, double value=" << double_value;
                (*ss) << buffer;
                break;
            }
            case TYPE_DATE:
            case TYPE_DATETIME: {
                char buf[64];
                const DateTimeValue* time_val = (const DateTimeValue*)(item);
                time_val->to_string(buf);
                (*ss) << buf;
                break;
            }
            case TYPE_VARCHAR:
            case TYPE_CHAR: {
                const StringValue* string_val = (const StringValue*)(item);

                if (string_val->ptr == nullptr) {
                    if (string_val->len == 0) {
                    } else {
                        (*ss) << "\\N";
                    }
                } else {
                    (*ss) << std::string(string_val->ptr, string_val->len);
                }
                break;
            }
            case TYPE_DECIMAL: {
                const DecimalValue* decimal_val = reinterpret_cast<const DecimalValue*>(item);
                std::string decimal_str;
                int output_scale = _output_expr_ctxs[i]->root()->output_scale();

                if (output_scale > 0 && output_scale <= 30) {
                    decimal_str = decimal_val->to_string(output_scale);
                } else {
                    decimal_str = decimal_val->to_string();
                }
                (*ss) << decimal_str;
                break;
            }
            case TYPE_DECIMALV2: {
                const DecimalV2Value decimal_val(reinterpret_cast<const PackedInt128*>(item)->value);
                std::string decimal_str;
                int output_scale = _output_expr_ctxs[i]->root()->output_scale();

                if (output_scale > 0 && output_scale <= 30) {
                    decimal_str = decimal_val.to_string(output_scale);
                } else {
                    decimal_str = decimal_val.to_string();
                }
                (*ss) << decimal_str;
                break;
            }
            default: {
                std::stringstream err_ss;
                err_ss << "can't export this type. type = " << _output_expr_ctxs[i]->root()->type();
                return Status::InternalError(err_ss.str());
            }
            }
        }

        if (i < num_columns - 1) {
            (*ss) << _t_export_sink.column_separator;
        }
    }
    (*ss) << _t_export_sink.row_delimiter;

    return Status::OK();
}

Status ExportSink::close(RuntimeState* state, Status exec_status) {
    Expr::close(_output_expr_ctxs, state);
    if (_output_stream != nullptr) {
        Status st = _output_stream->finalize();
        _output_stream.reset();
        return st;
    }
    return Status::OK();
}

Status ExportSink::open_file_writer(int timeout_ms) {
    std::unique_ptr<WritableFile> output_file;
    std::string file_name;
    RETURN_IF_ERROR(gen_file_name(&file_name));
    std::string file_path = _t_export_sink.export_path + "/" + file_name;
    WritableFileOptions options{.sync_on_close = false, .mode = Env::MUST_CREATE};

    const auto& file_type = _t_export_sink.file_type;
    switch (file_type) {
    case TFileType::FILE_LOCAL:
        RETURN_IF_ERROR(Env::Default()->new_writable_file(options, file_path, &output_file));
        break;
    case TFileType::FILE_BROKER: {
        const TNetworkAddress& broker_addr = _t_export_sink.broker_addresses[0];
        EnvBroker env_broker(broker_addr, _t_export_sink.properties, timeout_ms);
        RETURN_IF_ERROR(env_broker.new_writable_file(options, file_path, &output_file));
        break;
    }
    case TFileType::FILE_STREAM:
        return Status::NotSupported(strings::Substitute("Unsupported file type $0", file_type));
    }

    using WriteBufferFile = vectorized::csv::OutputStreamFile;
    _output_stream = std::make_unique<WriteBufferFile>(std::move(output_file), 1024 * 1024);
    _converters.reserve(_output_expr_ctxs.size());
    for (auto* ctx : _output_expr_ctxs) {
        const auto& type = ctx->root()->type();
        auto conv = vectorized::csv::get_converter(type, ctx->root()->is_nullable());
        if (conv == nullptr) {
            return Status::InternalError("No CSV converter for type " + type.debug_string());
        }
        _converters.emplace_back(std::move(conv));
    }

    _state->add_export_output_file(file_path);
    return Status::OK();
}

// TODO(lingbin): add some other info to file name, like partition
Status ExportSink::gen_file_name(std::string* file_name) {
    if (!_t_export_sink.__isset.file_name_prefix) {
        return Status::InternalError("file name prefix is not set");
    }

    std::stringstream file_name_ss;
    // now file-number is 0.
    // <file-name-prefix>_<file-number>.csv.<timestamp>
    file_name_ss << _t_export_sink.file_name_prefix << "0.csv." << UnixMillis();
    *file_name = file_name_ss.str();
    return Status::OK();
}

Status ExportSink::send_chunk(RuntimeState* state, vectorized::Chunk* chunk) {
    const size_t num_rows = chunk->num_rows();
    const size_t num_cols = chunk->num_columns();
    if (num_cols != _converters.size()) {
        auto err = strings::Substitute("Unmatched number of columns expected=$0 real=$1", _converters.size(), num_cols);
        return Status::InternalError(err);
    }
    std::vector<const vectorized::Column*> columns_raw_ptr;
    columns_raw_ptr.reserve(num_cols);
    for (int i = 0; i < num_cols; i++) {
        auto root = _output_expr_ctxs[i]->root();
        if (!root->is_slotref()) {
            return Status::InternalError("Not slot ref column");
        }
        auto column_ref = ((vectorized::ColumnRef*)root);
        columns_raw_ptr.emplace_back(chunk->get_column_by_slot_id(column_ref->slot_id()).get());
    }

    const std::string& row_delimiter = _t_export_sink.row_delimiter;
    const std::string& column_delimiter = _t_export_sink.column_separator;

    vectorized::csv::Converter::Options opts;
    auto* os = _output_stream.get();
    for (size_t row = 0; row < num_rows; row++) {
        for (size_t col = 0; col < num_cols; col++) {
            auto col_ptr = columns_raw_ptr[col];
            RETURN_IF_ERROR(_converters[col]->write_string(os, *col_ptr, row, opts));
            RETURN_IF_ERROR(os->write((col == num_cols - 1) ? row_delimiter : column_delimiter));
        }
    }
    return Status::OK();
}

} // namespace starrocks
