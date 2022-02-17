// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/file_result_writer.cpp

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

#include "runtime/file_result_writer.h"

#include <memory>

#include "env/env_broker.h"
#include "exec/local_file_writer.h"
#include "exec/parquet_builder.h"
#include "exec/plain_text_builder.h"
#include "formats/csv/converter.h"
#include "formats/csv/output_stream.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/runtime_state.h"
#include "util/date_func.h"
#include "util/types.h"
#include "util/uid_util.h"

namespace starrocks {

FileResultWriter::FileResultWriter(const ResultFileOptions* file_opts,
                                   const std::vector<ExprContext*>& output_expr_ctxs, RuntimeProfile* parent_profile)
        : _file_opts(file_opts), _output_expr_ctxs(output_expr_ctxs), _parent_profile(parent_profile) {
    if (_file_opts->is_local_file) {
        _env = Env::Default();
    } else {
        // TODO(@c1oudman) Do you only need first element of broker addresses?
        _env = new EnvBroker(*_file_opts->broker_addresses.begin(), _file_opts->broker_properties,
                             config::broker_write_timeout_seconds * 1000);
        _owned_env.reset(_env);
    }
}

FileResultWriter::~FileResultWriter() {
    _close_file_writer(true);
}

Status FileResultWriter::init(RuntimeState* state) {
    _state = state;
    _init_profile();

    RETURN_IF_ERROR(_create_file_writer());
    return Status::OK();
}

void FileResultWriter::_init_profile() {
    RuntimeProfile* profile = _parent_profile->create_child("FileResultWriter", true, true);
    _append_row_batch_timer = ADD_TIMER(profile, "AppendBatchTime");
    _convert_tuple_timer = ADD_CHILD_TIMER(profile, "TupleConvertTime", "AppendBatchTime");
    _file_write_timer = ADD_CHILD_TIMER(profile, "FileWriteTime", "AppendBatchTime");
    _writer_close_timer = ADD_TIMER(profile, "FileWriterCloseTime");
    _written_rows_counter = ADD_COUNTER(profile, "NumWrittenRows", TUnit::UNIT);
    _written_data_bytes = ADD_COUNTER(profile, "WrittenDataBytes", TUnit::BYTES);
}

Status FileResultWriter::_create_file_writer() {
    std::string file_name = _get_next_file_name();
    std::unique_ptr<WritableFile> writable_file;
    RETURN_IF_ERROR(_env->new_writable_file(file_name, &writable_file));

    switch (_file_opts->file_format) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
        _file_builder = std::make_unique<PlainTextBuilder>(
                PlainTextBuilderOptions{_file_opts->column_separator, _file_opts->row_delimiter},
                std::move(writable_file), _output_expr_ctxs);
        break;
    case TFileFormatType::FORMAT_PARQUET:
        _file_builder = std::make_unique<ParquetBuilder>(std::move(writable_file), _output_expr_ctxs);
        break;
    default:
        return Status::InternalError(strings::Substitute("unsupported file format: $0", _file_opts->file_format));
    }
    LOG(INFO) << "create file for exporting query result. file name: " << file_name
              << ". query id: " << print_id(_state->query_id());
    return Status::OK();
}

// file name format as: my_prefix_0.csv
std::string FileResultWriter::_get_next_file_name() {
    std::stringstream ss;
    ss << _file_opts->file_path << (_file_idx++) << "." << _file_format_to_name();
    return ss.str();
}

std::string FileResultWriter::_file_format_to_name() {
    switch (_file_opts->file_format) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
        return "csv";
    case TFileFormatType::FORMAT_PARQUET:
        return "parquet";
    default:
        return "unknown";
    }
}

Status FileResultWriter::append_chunk(vectorized::Chunk* chunk) {
    assert(_file_builder != nullptr);
    RETURN_IF_ERROR(_file_builder->add_chunk(chunk));

    // split file if exceed limit
    RETURN_IF_ERROR(_create_new_file_if_exceed_size());

    return Status::OK();
}

Status FileResultWriter::_create_new_file_if_exceed_size() {
    if (_file_builder->file_size() < _file_opts->max_file_size_bytes) {
        return Status::OK();
    }
    // current file size exceed the max file size. close this file
    // and create new one
    {
        SCOPED_TIMER(_writer_close_timer);
        RETURN_IF_ERROR(_close_file_writer(false));
    }
    return Status::OK();
}

Status FileResultWriter::_close_file_writer(bool done) {
    if (_file_builder != nullptr) {
        RETURN_IF_ERROR(_file_builder->finish());
        _file_builder.reset();
    }

    if (!done) {
        // not finished, create new file writer for next file
        RETURN_IF_ERROR(_create_file_writer());
    }
    return Status::OK();
}

Status FileResultWriter::close() {
    // the following 2 profile "_written_rows_counter" and "_writer_close_timer"
    // must be outside the `_close_file_writer()`.
    // because `_close_file_writer()` may be called in deconstructor,
    // at that time, the RuntimeState may already been deconstructed,
    // so does the profile in RuntimeState.
    COUNTER_SET(_written_rows_counter, _written_rows);
    SCOPED_TIMER(_writer_close_timer);
    RETURN_IF_ERROR(_close_file_writer(true));
    return Status::OK();
}

} // namespace starrocks
