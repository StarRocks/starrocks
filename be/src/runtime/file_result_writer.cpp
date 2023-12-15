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

#include "column/chunk.h"
#include "exec/local_file_writer.h"
#include "exec/parquet_builder.h"
#include "exec/plain_text_builder.h"
#include "formats/csv/converter.h"
#include "formats/csv/output_stream.h"
#include "fs/fs_broker.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"
#include "util/date_func.h"
#include "util/uid_util.h"

namespace starrocks {

FileResultWriter::FileResultWriter(const ResultFileOptions* file_opts,
                                   const std::vector<ExprContext*>& output_expr_ctxs, RuntimeProfile* parent_profile)
        : _file_opts(file_opts), _output_expr_ctxs(output_expr_ctxs), _parent_profile(parent_profile) {}

FileResultWriter::~FileResultWriter() {
    (void)_close_file_writer(true);
}

Status FileResultWriter::init(RuntimeState* state) {
    _state = state;
    _init_profile();
    RETURN_IF_ERROR(_create_fs());

    return Status::OK();
}

void FileResultWriter::_init_profile() {
    RuntimeProfile* profile = _parent_profile->create_child("FileResultWriter", true, true);
    _append_chunk_timer = ADD_TIMER(profile, "AppendChunkTime");
    _convert_tuple_timer = ADD_CHILD_TIMER(profile, "TupleConvertTime", "AppendChunkTime");
    _file_write_timer = ADD_CHILD_TIMER(profile, "FileWriteTime", "AppendChunkTime");
    _writer_close_timer = ADD_TIMER(profile, "FileWriterCloseTime");
    _written_rows_counter = ADD_COUNTER(profile, "NumWrittenRows", TUnit::UNIT);
    _written_data_bytes = ADD_COUNTER(profile, "WrittenDataBytes", TUnit::BYTES);
}

Status FileResultWriter::_create_fs() {
    if (_fs == nullptr) {
        if (_file_opts->use_broker) {
            _fs = std::make_unique<BrokerFileSystem>(*_file_opts->broker_addresses.begin(),
                                                     _file_opts->broker_properties,
                                                     config::broker_write_timeout_seconds * 1000);
        } else {
            ASSIGN_OR_RETURN(_fs, FileSystem::CreateUniqueFromString(_file_opts->file_path, FSOptions(_file_opts)));
        }
    }
    if (_fs == nullptr) {
        return Status::InternalError(
                strings::Substitute("file system initialize failed for file $0", _file_opts->file_path));
    }
    return Status::OK();
}

Status FileResultWriter::_create_file_writer() {
    std::string file_name = _get_next_file_name();
    WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto writable_file, _fs->new_writable_file(opts, file_name));

    switch (_file_opts->file_format) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
        _file_builder = std::make_unique<PlainTextBuilder>(
                PlainTextBuilderOptions{_file_opts->column_separator, _file_opts->row_delimiter},
                std::move(writable_file), _output_expr_ctxs);
        break;
    case TFileFormatType::FORMAT_PARQUET: {
        ASSIGN_OR_RETURN(auto properties, parquet::ParquetBuildHelper::make_properties(_file_opts->parquet_options));
        auto result =
                parquet::ParquetBuildHelper::make_schema(_file_opts->file_column_names, _output_expr_ctxs,
                                                         std::vector<parquet::FileColumnId>(_output_expr_ctxs.size()));
        if (!result.ok()) {
            return Status::NotSupported(result.status().message());
        }
        auto schema = result.ValueOrDie();
        auto parquet_builder = std::make_unique<ParquetBuilder>(
                std::move(writable_file), std::move(properties), std::move(schema), _output_expr_ctxs,
                _file_opts->parquet_options.row_group_max_size, _file_opts->max_file_size_bytes);
        RETURN_IF_ERROR(parquet_builder->init());
        _file_builder = std::move(parquet_builder);
        break;
    }
    default:
        return Status::InternalError(strings::Substitute("unsupported file format: $0", _file_opts->file_format));
    }
    LOG(INFO) << "create file for exporting query result. file name: " << file_name
              << ". query id: " << print_id(_state->query_id());
    return Status::OK();
}

Status FileResultWriter::open(RuntimeState* state) {
    // Move the _create_file_writer from init to open, because libhdfs depends on JNI.
    // In init() function, we are in bthread environment, bthread and JNI have a conflict
    // In open() function, we are in pthread environemnt, pthread and JNI doesn't have a conflict
    RETURN_IF_ERROR(_create_file_writer());
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

Status FileResultWriter::append_chunk(Chunk* chunk) {
    assert(_file_builder != nullptr);
    {
        SCOPED_TIMER(_append_chunk_timer);
        RETURN_IF_ERROR(_file_builder->add_chunk(chunk));
    }
    _written_rows += chunk->num_rows();
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
        LOG(WARNING) << "row count is " << _file_builder->file_size();
        RETURN_IF_ERROR(_file_builder->finish());
        COUNTER_UPDATE(_written_data_bytes, _file_builder->file_size());
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
    // because `_close_file_writer()` may be called in deconstruct,
    // at that time, the RuntimeState may already been deconstructed,
    // so does the profile in RuntimeState.
    COUNTER_SET(_written_rows_counter, _written_rows);
    SCOPED_TIMER(_writer_close_timer);
    RETURN_IF_ERROR(_close_file_writer(true));
    return Status::OK();
}

} // namespace starrocks
