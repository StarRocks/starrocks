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

#include <memory>
#include <shared_mutex>

#include "bthread/execution_queue.h"
#include "column/chunk.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::pipeline {

class SinkIOExecutor : public bthread::Executor {
public:
    static SinkIOExecutor* instance() {
        static SinkIOExecutor s_instance;
        return &s_instance;
    }

    int submit(void* (*fn)(void*), void* args) override {
        bool ret = ExecEnv::GetInstance()->pipeline_sink_io_pool()->try_offer([fn, args]() { fn(args); });
        return ret ? 0 : -1;
    }

private:
    SinkIOExecutor() = default;

    ~SinkIOExecutor() override = default;
};

// SinkIOBuffer accepts input from all sink operators, it uses an execution queue to asynchronously process chunks one by one.
// Because some interfaces of different writer include sync IO, we need to avoid calling the writer in pipeline execution thread.
// Many sinks have a similar working mode to the above (e.g. FileSink/ExportSink/MysqlTableSink), this abstraction was added to make them easier to use.
// @TODO: In fact, we need a MPSC queue, the producers in compute thread produce chunk, the consumer in io thread consume and process chunk.
// but the existing collaborative IO scheduling is diffcult to handle this scenario and can't be integrated into the workgroup mechanism.
// In order to achieve simplicity, the io task will be put into a dedicated thread pool, the sink io task of different queries is completely scheduled by os,
// which needs to be solved by a new adaptive io task scheduler.
class SinkIOBuffer {
public:
    SinkIOBuffer(int32_t num_sinkers) : _num_result_sinkers(num_sinkers) {}

    virtual ~SinkIOBuffer() = default;

    virtual Status prepare(RuntimeState* state, RuntimeProfile* parent_profile) = 0;

    virtual Status append_chunk(RuntimeState* state, const ChunkPtr& chunk) {
        if (Status status = get_io_status(); !status.ok()) {
            return status;
        }
        if (bthread::execution_queue_execute(*_exec_queue_id, chunk) != 0) {
            return Status::InternalError("submit io task failed");
        }
        ++_num_pending_chunks;
        return Status::OK();
    }

    virtual bool need_input() { return _num_pending_chunks < kExecutionQueueSizeLimit; }

    virtual Status set_finishing() {
        if (--_num_result_sinkers == 0) {
            // when all writes are over, we add a nullptr as a special mark to trigger close
            if (bthread::execution_queue_execute(*_exec_queue_id, nullptr) != 0) {
                return Status::InternalError("submit task failed");
            }
            ++_num_pending_chunks;
        }
        return Status::OK();
    }

    virtual bool is_finished() { return _is_finished && _num_pending_chunks == 0; }

    virtual void cancel_one_sinker() { _is_cancelled = true; }

    virtual void close(RuntimeState* state) {
        if (_exec_queue_id != nullptr) {
            bthread::execution_queue_stop(*_exec_queue_id);
        }
        _is_finished = true;
    }

    inline void set_io_status(const Status& status) {
        std::unique_lock l(_io_status_mutex);
        if (_io_status.ok()) {
            _io_status = status;
        }
    }

    inline Status get_io_status() const {
        std::shared_lock l(_io_status_mutex);
        return _io_status;
    }

    static int execute_io_task(void* meta, bthread::TaskIterator<ChunkPtr>& iter) {
        if (iter.is_queue_stopped()) {
            return 0;
        }
        auto* sink_io_buffer = static_cast<SinkIOBuffer*>(meta);
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(sink_io_buffer->_state->query_mem_tracker_ptr().get());
        for (; iter; ++iter) {
            sink_io_buffer->_process_chunk(iter);
            (*iter).reset();
        }
        return 0;
    }

protected:
    virtual void _process_chunk(bthread::TaskIterator<ChunkPtr>& iter) = 0;

    std::unique_ptr<bthread::ExecutionQueueId<ChunkPtr>> _exec_queue_id;

    std::atomic_int32_t _num_result_sinkers = 0;
    std::atomic_int64_t _num_pending_chunks = 0;
    std::atomic_bool _is_prepared = false;
    std::atomic_bool _is_cancelled = false;
    std::atomic_bool _is_finished = false;

    mutable std::shared_mutex _io_status_mutex;
    Status _io_status;

    RuntimeState* _state = nullptr;

    static const int32_t kExecutionQueueSizeLimit = 64;
};

} // namespace starrocks::pipeline
