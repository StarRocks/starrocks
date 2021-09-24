// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <atomic>
#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>

#include "pipeline_driver.h"
#include "pipeline_driver_queue.h"
#include "util/thread.h"
namespace starrocks {
namespace pipeline {
class PipelineDriverPoller;
using PipelineDriverPollerPtr = std::unique_ptr<PipelineDriverPoller>;
class PipelineDriverPoller {
public:
    explicit PipelineDriverPoller(DriverQueue* dispatch_queue)
            : _dispatch_queue(dispatch_queue),
              _polling_thread(nullptr),
              _is_polling_thread_initialized(false),
              _is_shutdown(false) {}

    using DriverList = std::list<DriverPtr>;
    ~PipelineDriverPoller() = default;
    // start poller thread
    void start();
    // shutdown poller thread
    void shutdown();
    // add blocked driver to poller
    void add_blocked_driver(const DriverPtr& driver);

private:
    void run_internal();
    PipelineDriverPoller(const PipelineDriverPoller&) = delete;
    PipelineDriverPoller& operator=(const PipelineDriverPoller&) = delete;

private:
    std::mutex _mutex;
    std::condition_variable _cond;
    DriverList _blocked_drivers;
    DriverQueue* _dispatch_queue;
    Thread* _polling_thread;
    std::atomic<bool> _is_polling_thread_initialized;
    std::atomic<bool> _is_shutdown;
};
} // namespace pipeline
} // namespace starrocks