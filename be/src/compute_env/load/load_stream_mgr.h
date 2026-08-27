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
#include <mutex>
#include <unordered_map>

#include "base/uid_util.h"                     // for std::hash for UniqueId
#include "compute_env/load/stream_load_pipe.h" // for StreamLoadPipe
#include "runtime/runtime_metrics.h"

namespace starrocks {

class MetricRegistry;

// Used to register all streams in process so that other modules can get them.
class LoadStreamMgr {
public:
    explicit LoadStreamMgr(MetricRegistry* metrics = nullptr) { install_metrics(metrics); }
    ~LoadStreamMgr() = default;

    void install_metrics(MetricRegistry* metrics) {
        if (metrics == nullptr || _metrics != nullptr) {
            return;
        }
        _metrics = metrics;
        // Each StreamLoadPipe has a limited buffer size (default 1M), it's not needed to count the
        // actual size of all StreamLoadPipe.
        REGISTER_GAUGE_RUNTIME_METRIC(metrics, stream_load_pipe_count, [this]() {
            std::lock_guard<std::mutex> l(_lock);
            return _stream_map.size();
        });
    }

    void close() {
        std::lock_guard<std::mutex> l(_lock);
        for (auto iter = _stream_map.begin(); iter != _stream_map.end();) {
            iter->second->close();
            iter = _stream_map.erase(iter);
        }
    }

    Status put(const UniqueId& id, const std::shared_ptr<StreamLoadPipe>& stream) {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _stream_map.find(id);
        if (it != std::end(_stream_map)) {
            return Status::InternalError("id already exist");
        }
        _stream_map.emplace(id, stream);
        return Status::OK();
    }

    std::shared_ptr<StreamLoadPipe> get(const UniqueId& id) {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _stream_map.find(id);
        if (it == std::end(_stream_map)) {
            return nullptr;
        }
        auto stream = it->second;
        return stream;
    }

    void remove(const UniqueId& id) {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _stream_map.find(id);
        if (it != std::end(_stream_map)) {
            _stream_map.erase(it);
        }
    }

private:
    MetricRegistry* _metrics = nullptr;
    std::mutex _lock;
    std::unordered_map<UniqueId, std::shared_ptr<StreamLoadPipe>> _stream_map;
};

} // namespace starrocks
