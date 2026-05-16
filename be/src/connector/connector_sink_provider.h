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

#include <memory>

#include "common/statusor.h"

namespace starrocks::connector {

class ConnectorChunkSink;
class SinkMemoryManager;

struct ConnectorChunkSinkContext {
    virtual ~ConnectorChunkSinkContext() = default;

    // Called by ConnectorSinkOperatorFactory after SinkMemoryManager is created.
    // Composite sinks override this to receive the manager and create child managers.
    virtual void set_sink_mem_mgr(SinkMemoryManager* /*mgr*/) {}
};

class ConnectorChunkSinkProvider {
public:
    virtual ~ConnectorChunkSinkProvider() = default;

    virtual StatusOr<std::unique_ptr<ConnectorChunkSink>> create_chunk_sink(
            std::shared_ptr<ConnectorChunkSinkContext> context, int32_t driver_id) = 0;
};

} // namespace starrocks::connector
