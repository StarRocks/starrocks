// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <memory>
#include <shared_mutex>

DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include "bthread/execution_queue.h"
DIAGNOSTIC_POP
#include "column/chunk.h"
#include "runtime/runtime_state.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks {
namespace pipeline {

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

    ~SinkIOExecutor() = default;
};

class SinkIOBuffer {
public:
    SinkIOBuffer(int32_t num_sinkers) : _num_result_sinkers(num_sinkers) {}

    virtual ~SinkIOBuffer() = default;

    virtual Status prepare(RuntimeState* state, RuntimeProfile* parent_profile) = 0;

    virtual Status append_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
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

    virtual bool is_finished() { return _is_finished; }

    virtual void cancel_one_sinker() {
        _is_cancelled = true;
        if (_exec_queue_id != nullptr) {
            bthread::execution_queue_stop(*_exec_queue_id);
        }
    }

    virtual void close(RuntimeState* state) {
        _is_finished = true;
        if (_exec_queue_id != nullptr) {
            bthread::execution_queue_stop(*_exec_queue_id);
        }
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

    static int execute_io_task(void* meta, bthread::TaskIterator<const vectorized::ChunkPtr>& iter) {
        SinkIOBuffer* sink_io_buffer = static_cast<SinkIOBuffer*>(meta);
        for (; iter; ++iter) {
            sink_io_buffer->_process_chunk(iter);
        }
        return 0;
    }

protected:
    virtual void _process_chunk(bthread::TaskIterator<const vectorized::ChunkPtr>& iter) = 0;

    std::unique_ptr<bthread::ExecutionQueueId<const vectorized::ChunkPtr>> _exec_queue_id;

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

} // namespace pipeline
} // namespace starrocks