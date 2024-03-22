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
    // If any of the `add_chunk_futures` is not ready, the chunk sink cannot accept more chunks.
    // The chunk sink can still accept chunks if some `add_chunk_futures` is ready or not.
    struct Futures {
        std::vector<std::future<Status>> add_chunk_futures;
        std::vector<std::future<formats::FileWriter::CommitResult>> commit_file_futures;
    };

    virtual ~ConnectorChunkSink() = default;

    virtual Status init() = 0;

    virtual StatusOr<Futures> add(ChunkPtr chunk) = 0;

    virtual Futures finish() = 0;

    // callback function on commit file succeed.
    virtual std::function<void(const formats::FileWriter::CommitResult& result)> callback_on_success() = 0;
};

struct ConnectorChunkSinkContext {
public:
    virtual ~ConnectorChunkSinkContext() = default;
};

class ConnectorChunkSinkProvider {
public:
    virtual ~ConnectorChunkSinkProvider() = default;

    virtual StatusOr<std::unique_ptr<ConnectorChunkSink>> create_chunk_sink(
            std::shared_ptr<ConnectorChunkSinkContext> context, int32_t driver_id) = 0;
};

} // namespace starrocks::connector
