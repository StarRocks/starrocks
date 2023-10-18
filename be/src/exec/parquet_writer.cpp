
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

#include "exec/parquet_writer.h"

#include <fmt/format.h>

#include <utility>

#include "formats/parquet/file_writer.h"
#include "runtime/exec_env.h"
#include "util/uid_util.h"

namespace starrocks {

RollingAsyncParquetWriter::RollingAsyncParquetWriter(
        TableInfo tableInfo, const std::vector<ExprContext*>& output_expr_ctxs, RuntimeProfile* parent_profile,
        std::function<void(starrocks::parquet::AsyncFileWriter*, RuntimeState*)> commit_func, RuntimeState* state,
        int32_t driver_id)
        : _table_info(std::move(tableInfo)),
          _output_expr_ctxs(output_expr_ctxs),
          _parent_profile(parent_profile),
          _commit_func(std::move(commit_func)),
          _state(state),
          _driver_id(driver_id) {}

Status RollingAsyncParquetWriter::init() {
    ASSIGN_OR_RETURN(
            _fs, FileSystem::CreateUniqueFromString(_table_info.partition_location, FSOptions(&_table_info.cloud_conf)))
    _schema = _table_info.schema;
    _partition_location = _table_info.partition_location;

    ::parquet::WriterProperties::Builder builder;
    _table_info.enable_dictionary ? builder.enable_dictionary() : builder.disable_dictionary();
    ASSIGN_OR_RETURN(auto compression_codec,
                     parquet::ParquetBuildHelper::convert_compression_type(_table_info.compress_type));
    builder.compression(compression_codec);
    builder.version(::parquet::ParquetVersion::PARQUET_2_0);
    _properties = builder.build();

    return Status::OK();
}

// prepend fragment instance id to a file name so we can determine which files were written by which fragment instance or be.
// and we can also know how many files each instance and each driver has written according to file_counts and driver_id mark.
std::string RollingAsyncParquetWriter::_new_file_location() {
    _file_cnt += 1;
    _outfile_location = _partition_location + fmt::format("{}_{}_{}.parquet", print_id(_state->fragment_instance_id()),
                                                          _driver_id, _file_cnt);
    return _outfile_location;
}

Status RollingAsyncParquetWriter::_new_file_writer() {
    std::string new_file_location = _new_file_location();
    WritableFileOptions options{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto writable_file, _fs->new_writable_file(options, new_file_location))
    _writer = std::make_shared<starrocks::parquet::AsyncFileWriter>(
            std::move(writable_file), new_file_location, _partition_location, _properties, _schema, _output_expr_ctxs,
            ExecEnv::GetInstance()->pipeline_sink_io_pool(), _parent_profile, _max_file_size);
    auto st = _writer->init();
    return st;
}

Status RollingAsyncParquetWriter::append_chunk(Chunk* chunk, RuntimeState* state) {
    if (_writer == nullptr) {
        auto status = _new_file_writer();
        if (!status.ok()) {
            return status;
        }
    }
    // exceed file size
    if (_writer->file_size() > _max_file_size) {
        auto st = close_current_writer(state);
        if (st.ok()) {
            _new_file_writer();
        }
    }
    auto st = _writer->write(chunk);
    return st;
}

Status RollingAsyncParquetWriter::close_current_writer(RuntimeState* state) {
    Status st = _writer->close(state, _commit_func);
    if (st.ok()) {
        _pending_commits.emplace_back(_writer);
        return Status::OK();
    } else {
        LOG(WARNING) << "close file error: " << _outfile_location;
        return Status::IOError("close file error!");
    }
}

Status RollingAsyncParquetWriter::close(RuntimeState* state) {
    if (_writer != nullptr) {
        auto st = close_current_writer(state);
        if (!st.ok()) {
            return st;
        }
    }
    return Status::OK();
}

bool RollingAsyncParquetWriter::closed() {
    for (auto& writer : _pending_commits) {
        if (writer != nullptr && writer->closed()) {
            writer = nullptr;
        }
        if (writer != nullptr && (!writer->closed())) {
            return false;
        }
    }

    if (_writer != nullptr) {
        return _writer->closed();
    }

    return true;
}

} // namespace starrocks
