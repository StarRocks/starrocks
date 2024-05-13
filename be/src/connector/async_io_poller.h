#include <queue>
#include <future>
#include <arrow/status.h>
#include "common/status.h"
#include "io/async_flush_output_stream.h"
#include "formats/utils.h"

#pragma once

namespace starrocks::connector {

/// own a FIFO queue of `io::AsyncFlushOutputStream`
/// client periodically poll the state of async io operations via `poll()`
/// each `io::AsyncFlushOutputStream` will be destroyed once its async status is ready and fetched
class IOStatusPoller {
public:
    using Stream = io::AsyncFlushOutputStream;

    IOStatusPoller() = default;

    void enqueue(std::unique_ptr<Stream> stream) {
        auto async_status = stream->io_status();
        _queue.push_back({
            .stream = std::move(stream),
            .async_status = std::move(async_status),
        });
    }

    // return a pair of
    // 1. io status
    // 2. bool indicates if all io finished
    std::pair<Status, bool> poll() {
        Status status;
        while (!_queue.empty()) {
            auto& f = _queue.front();
            if (!is_ready(f.async_status)) {
                break;
            }
            status.update(f.async_status.get());
            _queue.pop_front();
        }

        return {status, _queue.empty()};
    }

    int64_t releasable_memory() {
        LOG_EVERY_SECOND(INFO) << "alive streams: " << _queue.size();
        int64_t releasable_memory = 0;
        for (auto& ss : _queue) {
            releasable_memory += ss.stream->releasable_memory();
        }
        return releasable_memory;
    }

private:
    struct StreamWithStatus {
        std::unique_ptr<Stream> stream;
        std::future<Status> async_status; // TODO: remove
    };

    std::deque<StreamWithStatus> _queue;
};

} // namespace starrocks::connector

