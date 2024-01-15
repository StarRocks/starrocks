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
#include <utility>

#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/spill/executor.h"
#include "exec/spill/spiller.h"
#include "runtime/runtime_state.h"
#include "util/blocking_queue.hpp"
#include "util/defer_op.h"
#include "util/runtime_profile.h"

namespace starrocks {
class SpillProcessChannel;
class SpillProcessTasksBuilder;
namespace spill {
class Spiller;
}

class SpillProcessTask {
public:
    SpillProcessTask() = default;

    SpillProcessTask(std::function<StatusOr<ChunkPtr>()> task) : _task(std::move(task)) {}

    StatusOr<ChunkPtr> operator()() const { return _task(); }

    operator bool() const { return !!_task; }

    SpillProcessTask& operator=(std::function<StatusOr<ChunkPtr>()> task) {
        _task = std::move(task);
        return *this;
    }

    void reset();

private:
    std::function<StatusOr<ChunkPtr>()> _task;
};

using SpillProcessChannelPtr = std::shared_ptr<SpillProcessChannel>;

class SpillProcessChannelFactory {
public:
    //
    SpillProcessChannelFactory(size_t degree, std::shared_ptr<spill::IOTaskExecutor> executor)
            : _channels(degree), _executor(std::move(executor)) {}

    SpillProcessChannelPtr get_or_create(int32_t sequence);

    std::shared_ptr<spill::IOTaskExecutor>& executor() { return _executor; };

private:
    std::vector<SpillProcessChannelPtr> _channels;
    std::shared_ptr<spill::IOTaskExecutor> _executor;
};
using SpillProcessChannelFactoryPtr = std::shared_ptr<SpillProcessChannelFactory>;

// SpillProcessOperator
class SpillProcessChannel {
public:
    SpillProcessChannel(SpillProcessChannelFactory* factory) : _parent(factory) {}

    bool add_spill_task(SpillProcessTask&& task) {
        DCHECK(!_is_finishing);
        _is_working = true;
        return _spill_tasks.put(std::move(task));
    }

    bool add_last_task(SpillProcessTask&& task) {
        DCHECK(!_is_finishing);
        _is_working = true;
        auto defer = DeferOp([this]() { set_finishing_if_not_reuseable(); });
        return _spill_tasks.put(std::move(task));
    }

    bool acquire_spill_task() { return _spill_tasks.blocking_get(&_current_task); }

    bool has_spill_task() { return !_spill_tasks.empty(); }

    void set_finishing() { _is_finishing = true; }
    bool is_finishing() { return _is_finishing; }
    bool is_working() { return _is_working; }

    void set_finishing_if_not_reuseable() {
        if (!_is_reuseable) {
            set_finishing();
        }
    }

    void set_reuseable(bool reuseable) { _is_reuseable = reuseable; }

    auto& io_executor() { return _parent->executor(); }

    SpillProcessTask& current_task() { return _current_task; }

    bool has_output() { return (has_spill_task() || _current_task) && !_spiller->is_full(); }

    bool has_task() { return has_spill_task() || _current_task; }

    bool is_finished() { return is_finishing() && !has_spill_task() && !_current_task; }

    void set_spiller(std::shared_ptr<spill::Spiller> spiller) { _spiller = std::move(spiller); }
    const std::shared_ptr<spill::Spiller>& spiller() { return _spiller; }

    Status execute(SpillProcessTasksBuilder& task_builder);

private:
    bool _is_reuseable = false;
    bool _is_finishing = false;
    bool _is_working = false;
    std::shared_ptr<spill::Spiller> _spiller;
    UnboundedBlockingQueue<SpillProcessTask> _spill_tasks;
    SpillProcessTask _current_task;
    SpillProcessChannelFactory* _parent;
};

class SpillProcessTasksBuilder {
public:
    SpillProcessTasksBuilder(RuntimeState* state, std::shared_ptr<spill::IOTaskExecutor> executor)
            : _runtime_state(state), _io_executor(std::move(executor)) {}

    template <class Task>
    SpillProcessTasksBuilder& then(Task task) {
        auto runtime_state = this->_runtime_state;
        auto& io_executor = this->_io_executor;
        std::function<StatusOr<ChunkPtr>()> spill_task = [runtime_state, io_executor, task]() -> StatusOr<ChunkPtr> {
            RETURN_IF_ERROR(task(runtime_state, io_executor));
            return Status::EndOfFile("eos");
        };
        _spill_tasks.emplace_back(std::move(spill_task));
        return *this;
    }

    template <class Task>
    void finally(Task task) {
        auto runtime_state = this->_runtime_state;
        auto& io_executor = this->_io_executor;
        _final_task = [runtime_state, io_executor, task]() -> StatusOr<ChunkPtr> {
            RETURN_IF_ERROR(task(runtime_state, io_executor));
            return Status::EndOfFile("eos");
        };
    }

    std::vector<SpillProcessTask>& tasks() { return _spill_tasks; }
    SpillProcessTask& final_task() { return _final_task; }

private:
    RuntimeState* _runtime_state;
    std::shared_ptr<spill::IOTaskExecutor> _io_executor;
    std::vector<SpillProcessTask> _spill_tasks;
    SpillProcessTask _final_task;
};

} // namespace starrocks
