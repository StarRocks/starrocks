// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
    explicit PipelineDriverPoller(DriverQueue* driver_queue)
            : _driver_queue(driver_queue),
              _polling_thread(nullptr),
              _is_polling_thread_initialized(false),
              _is_shutdown(false) {}

    using DriverList = std::list<DriverRawPtr>;
    ~PipelineDriverPoller() { shutdown(); };
    // start poller thread
    void start();
    // shutdown poller thread
    void shutdown();
    // add blocked driver to poller
    void add_blocked_driver(const DriverRawPtr driver);
    // remove blocked driver from poller
    void remove_blocked_driver(DriverList& local_blocked_drivers, DriverList::iterator& driver_it);
    // only used for collect metrics
    size_t blocked_driver_queue_len() const {
        std::unique_lock<std::mutex> guard(_mutex);
        return _blocked_drivers.size();
    }

private:
    void run_internal();
    PipelineDriverPoller(const PipelineDriverPoller&) = delete;
    PipelineDriverPoller& operator=(const PipelineDriverPoller&) = delete;

private:
    mutable std::mutex _mutex;
    std::condition_variable _cond;
    DriverList _blocked_drivers;
    DriverQueue* _driver_queue;
    scoped_refptr<Thread> _polling_thread;
    std::atomic<bool> _is_polling_thread_initialized;
    std::atomic<bool> _is_shutdown;
};
} // namespace pipeline
} // namespace starrocks
