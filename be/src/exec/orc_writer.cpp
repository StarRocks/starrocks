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

#include "exec/orc_writer.h"

namespace starrocks {
RollingAsyncOrcWriter::RollingAsyncOrcWriter(const ORCInfo& orc_info, const std::vector<ExprContext*>& output_expr_ctxs,
                                             RuntimeProfile* parent_profile,
                                             std::function<void(AsyncOrcChunkWriter*, RuntimeState*)> commit_func,
                                             RuntimeState* state, int32_t driver_id)
        : _orc_info(orc_info),
          _output_expr_ctxs(output_expr_ctxs),
          _parent_profile(parent_profile),
          _commit_func(commit_func),
          _state(state),
          _driver_id(driver_id) {}

Status RollingAsyncOrcWriter::init() {
    ASSIGN_OR_RETURN(_fs,
                     FileSystem::CreateUniqueFromString(_orc_info.partition_location, FSOptions(&_orc_info.cloud_conf)))
    _schema = _orc_info.schema;
    _partition_location = _orc_info.partition_location;
    return Status::OK();
}

std::string RollingAsyncOrcWriter::_new_file_location() {
    _file_cnt += 1;
    _outfile_location = _partition_location +
                        fmt::format("{}_{}_{}.orc", print_id(_state->fragment_instance_id()), _driver_id, _file_cnt);
    return _outfile_location;
}

Status RollingAsyncOrcWriter::_new_chunk_writer() {
    std::string new_file_location = _new_file_location();
    WritableFileOptions options{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto writable_file, _fs->new_writable_file(options, new_file_location))

    _writer = std::make_shared<AsyncOrcChunkWriter>(std::move(writable_file), _options, _schema, _output_expr_ctxs,
                                                    ExecEnv::GetInstance()->pipeline_sink_io_pool(), _parent_profile);

    return Status::OK();
}

Status RollingAsyncOrcWriter::append_chunk(Chunk* chunk, RuntimeState* state) {
    if (_writer == nullptr) {
        RETURN_IF_ERROR(_new_chunk_writer());
    }
    // exceed file size
    if (_writer->file_size() > _max_file_size) {
        RETURN_IF_ERROR(close_current_writer(state));
        RETURN_IF_ERROR(_new_chunk_writer());
    }
    return _writer->write(chunk);
}

Status RollingAsyncOrcWriter::close_current_writer(RuntimeState* state) {
    Status st = _writer->close(state, _commit_func);
    if (st.ok()) {
        _pending_commits.emplace_back(_writer);
        return Status::OK();
    } else {
        LOG(WARNING) << "close file error: " << _outfile_location;
        return Status::IOError("close file error!");
    }
}

Status RollingAsyncOrcWriter::close(RuntimeState* state) {
    if (_writer != nullptr) {
        RETURN_IF_ERROR(close_current_writer(state));
    }
    return Status::OK();
}

bool RollingAsyncOrcWriter::closed() {
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