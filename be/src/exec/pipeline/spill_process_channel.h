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

#include <functional>
#include <memory>
#include <mutex>

#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/spill/executor.h"
#include "exec/spill/spiller.h"
#include "util/blocking_queue.hpp"
#include "util/runtime_profile.h"

namespace starrocks {
class SpillProcessChannel;
class Spiller;

class SpillProcessTask {
public:
    SpillProcessTask() = default;

    SpillProcessTask(std::function<StatusOr<ChunkPtr>()> task) : _task(std::move(task)) {}

    StatusOr<ChunkPtr> operator()() const { return _task(); }

    operator bool() const { return !!_task; }

    void reset();

private:
    std::function<StatusOr<ChunkPtr>()> _task;
};

using SpillProcessChannelPtr = std::shared_ptr<SpillProcessChannel>;

class SpillProcessChannelFactory {
public:
    //
    SpillProcessChannelFactory(size_t degree, std::shared_ptr<IOTaskExecutor> executor)
            : _channels(degree), _executor(std::move(executor)) {}

    SpillProcessChannelPtr get_or_create(int32_t sequence);

    std::shared_ptr<IOTaskExecutor>& executor() { return _executor; };

private:
    std::vector<SpillProcessChannelPtr> _channels;
    std::shared_ptr<IOTaskExecutor> _executor;
};
using SpillProcessChannelFactoryPtr = std::shared_ptr<SpillProcessChannelFactory>;

// SpillProcessOperator
class SpillProcessChannel {
public:
    SpillProcessChannel(SpillProcessChannelFactory* factory) : _parent(factory) {}

    bool add_spill_task(SpillProcessTask&& task) {
        _is_working = true;
        return _spill_tasks.put(std::move(task));
    }

    bool acquire_spill_task() { return _spill_tasks.blocking_get(&_current_task); }

    bool has_spill_task() { return !_spill_tasks.empty(); }

    void set_finishing() { _is_finishing = true; }
    bool is_finishing() { return _is_finishing; }
    bool is_working() { return _is_working; }

    auto& io_executor() { return _parent->executor(); }

    SpillProcessTask& current_task() { return _current_task; }

    bool has_output() { return (has_spill_task() || _current_task) && !_spiller->is_full(); }

    bool has_task() { return has_spill_task() || _current_task; }

    bool is_finished() { return is_finishing() && !has_spill_task() && !_current_task; }

    void set_spiller(std::shared_ptr<Spiller> spiller) { _spiller = std::move(spiller); }
    const std::shared_ptr<Spiller>& spiller() { return _spiller; }

private:
    bool _is_finishing = false;
    bool _is_working = false;
    std::shared_ptr<Spiller> _spiller;
    UnboundedBlockingQueue<SpillProcessTask> _spill_tasks;
    SpillProcessTask _current_task;
    SpillProcessChannelFactory* _parent;
    std::mutex _mutex;
};

} // namespace starrocks
