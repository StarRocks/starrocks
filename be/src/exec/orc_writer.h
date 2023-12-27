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

#pragma once

#include "common/logging.h"
#include "exec/pipeline/fragment_context.h"
#include "formats/orc/orc_chunk_writer.h"
#include "fs/fs.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"

namespace starrocks {

struct ORCInfo {
    TCompressionType::type compress_type = TCompressionType::SNAPPY;
    std::string partition_location = "";
    std::shared_ptr<orc::Type> schema;
    TCloudConfiguration cloud_conf;
};

class RollingAsyncOrcWriter {
public:
    RollingAsyncOrcWriter(const ORCInfo& orc_info, const std::vector<ExprContext*>& output_expr_ctxs,
                          RuntimeProfile* parent_profile,
                          std::function<void(AsyncOrcChunkWriter*, RuntimeState*)> commit_func, RuntimeState* state,
                          int32_t driver_id);

    ~RollingAsyncOrcWriter() = default;

    Status init();
    Status append_chunk(Chunk* chunk, RuntimeState* state);
    Status close(RuntimeState* state);
    bool writable() const { return _writer == nullptr || _writer->writable(); }
    bool closed();
    Status close_current_writer(RuntimeState* state);

private:
    Status _new_chunk_writer();
    std::string _new_file_location();

    std::unique_ptr<FileSystem> _fs;
    std::shared_ptr<AsyncOrcChunkWriter> _writer;
    std::shared_ptr<orc::WriterOptions> _options;
    std::shared_ptr<orc::Type> _schema;
    std::string _partition_location;
    ORCInfo _orc_info;
    int32_t _file_cnt = 0;

    std::string _outfile_location;
    std::vector<std::shared_ptr<AsyncOrcChunkWriter>> _pending_commits;
    int64_t _max_file_size = 1 * 1024 * 1024 * 1024; // 1GB

    std::vector<ExprContext*> _output_expr_ctxs;
    RuntimeProfile* _parent_profile;
    std::function<void(AsyncOrcChunkWriter*, RuntimeState*)> _commit_func;
    RuntimeState* _state;
    int32_t _driver_id;
};
} // namespace starrocks