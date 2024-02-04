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

#include "sink_io_buffer.h"

namespace starrocks::pipeline {

Status SinkIOBuffer::append_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (Status status = get_io_status(); !status.ok()) {
        return status;
    }
    if (bthread::execution_queue_execute(*_exec_queue_id, std::make_shared<QueueItem>(chunk)) != 0) {
        return Status::InternalError("submit io task failed");
    }
    ++_num_pending_chunks;
    return Status::OK();
}

Status SinkIOBuffer::set_finishing() {
    if (--_num_result_sinkers == 0) {
        // when all writers are done, a nullptr is added as a special mark to trigger
        // the close action in io thread.
        if (bthread::execution_queue_execute(*_exec_queue_id, nullptr) != 0) {
            ++_num_result_sinkers;
            return Status::InternalError("submit task failed");
        }
        ++_num_pending_chunks;
    }
    return Status::OK();
}

Status SinkIOBuffer::prepare(RuntimeState* state, RuntimeProfile*) {
    bool expected = false;
    if (!_is_prepared.compare_exchange_strong(expected, true)) {
        return Status::OK();
    }

    bthread::ExecutionQueueOptions options;
    options.executor = SinkIOExecutor::instance();
    auto queue_id = std::make_unique<bthread::ExecutionQueueId<QueueItemPtr>>();
    int ret = bthread::execution_queue_start<QueueItemPtr>(queue_id.get(), &options, &SinkIOBuffer::execute_io_task,
                                                           this);
    if (ret != 0) {
        _is_prepared = false;
        return Status::InternalError("start execution queue error");
    }
    // make state change if all goes well
    _state = state;
    _exec_queue_id = std::move(queue_id);
    return Status::OK();
}

int SinkIOBuffer::_process_chunk_entrypoint(bthread::TaskIterator<QueueItemPtr>& iter) {
    // NOTE: don't have any expectation to the accuracy of `_num_pending_chunks` when executing in this io thread,
    // it is possible that the QueueItemPtr is pushed into the queue and get executed here, but the
    // _num_pending_chunks is not increased yet. This is a typical race condition in multi-thread environment.
    // refer to: https://github.com/StarRocks/starrocks/pull/38094

    // is it possible that the mem_tracker in _state is invalid?
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_state->query_mem_tracker_ptr().get());
    bool fast_skip = false;
    for (; iter; ++iter) {
        if (_is_finished) {
            fast_skip = true;
            break;
        }
        if (*iter == nullptr || _is_cancelled) {
            // The CANCEL state or the StopMarker triggers auto-finish workflow, enables item fast-skip processing.
            //
            // After invoking `close()`, is_finished() will return true and the caller will take this SinkIOBuffer
            // is completed and the resource is ready to release. There are two barriers to prevent invalid accessing
            // of `this` object:
            // 1. `_num_pending_chunks`, as long as there are on-the-fly pending chunks, but this is not always reliable.
            //   e.g. consider the following two threads execution sequences:
            //   [thread-A] append_chunk(chunk), done bthread::execution_queue_execute(), but not increasing the counter yet
            //   [thread-B] execute the io task and detect the cancel state, called close() and decreases `_num_pending_chunks`
            //              accordingly
            //   [thread-C] check is_finished(), returns true because of close() successful and _num_pending_chunks == 0
            //   [thread-A] execute ++_num_pending_chunks, is_finished() returns `false` again.
            // 2. bthread::execution_queue_join() in destructor, waiting for all items in queue are processed, either because of
            //   queue stopped state or because of fast skip.
            // Refer to: https://github.com/StarRocks/starrocks/pull/26028
            close(_state);
            fast_skip = true;
            break;
        }
        _process_chunk((*iter)->chunk_ptr);
        --_num_pending_chunks;
        // Do a favor to the query_mem_tracker:
        // decrease the chunk_ptr reference and possibly release the memory at the earliest
        // refer to: https://github.com/StarRocks/starrocks/pull/15915
        (*iter)->chunk_ptr.reset();
    }

    if (fast_skip) {
        // make sure the `_num_pending_chunks` still reflects the real pending chunks
        // and the chunk is released at the earliest.
        for (; iter; ++iter) {
            if (*iter != nullptr) {
                (*iter)->chunk_ptr.reset();
            }
            --_num_pending_chunks;
        }
    }
    return 0;
}
} // namespace starrocks::pipeline
