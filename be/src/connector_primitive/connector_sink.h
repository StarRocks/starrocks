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

#include <cstdint>
#include <memory>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "runtime/runtime_fwd.h"

namespace starrocks::formats {
class AsyncFlushStreamPoller;
} // namespace starrocks::formats

namespace starrocks::connector {

class SinkMemoryManager;
class SinkOperatorMemoryManager;

class ConnectorSink {
public:
    virtual ~ConnectorSink() = default;

    virtual Status init(formats::AsyncFlushStreamPoller* poller, RuntimeProfile* profile,
                        SinkMemoryManager* sink_mem_mgr) = 0;

    virtual Status add(const ChunkPtr& chunk) = 0;

    virtual Status finish() = 0;

    virtual void rollback() = 0;

    virtual bool is_finished() = 0;

    virtual Status status() = 0;

    virtual SinkOperatorMemoryManager* op_mem_mgr() const = 0;

    virtual void register_memory_candidates(SinkOperatorMemoryManager*) {}
};

struct ConnectorSinkContext {
    virtual ~ConnectorSinkContext() = default;
};

class ConnectorSinkProvider {
public:
    virtual ~ConnectorSinkProvider() = default;

    virtual StatusOr<std::unique_ptr<ConnectorSink>> create_sink(int32_t driver_id) = 0;
};

using ConnectorSinkProviderPtr = std::unique_ptr<ConnectorSinkProvider>;

} // namespace starrocks::connector
