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

#include <fmt/format.h>

#include <boost/thread/future.hpp>
#include <future>

#include "column/chunk.h"
#include "common/status.h"
#include "formats/file_writer.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::connector {

class ConnectorChunkSink {
public:
    struct Futures {
        std::vector<std::future<Status>> add_chunk_future;
        std::vector<std::future<formats::FileWriter::CommitResult>> commit_file_future;
    };

    virtual ~ConnectorChunkSink() = default;

    virtual Status init() = 0;

    virtual StatusOr<Futures> add(ChunkPtr chunk) = 0;

    virtual Futures finish() = 0;

    virtual std::function<void(const formats::FileWriter::CommitResult& result)> callback_on_success() = 0;
};

} // namespace starrocks::connector
