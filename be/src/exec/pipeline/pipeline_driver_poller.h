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
              _is_shutdown(false),
              _blocked_driver_queue_len(0) {}

    using DriverList = std::list<DriverRawPtr>;

    ~PipelineDriverPoller() { shutdown(); };
    void start();
    void shutdown();
    // add blocked driver to poller
    void add_blocked_driver(const DriverRawPtr driver);
    // remove blocked driver from poller
    void remove_blocked_driver(DriverList& local_blocked_drivers, DriverList::iterator& driver_it);
    // only used for collect metrics
    size_t blocked_driver_queue_len() const { return _blocked_driver_queue_len; }

    void iterate_immutable_driver(const IterateImmutableDriverFunc& call) const;

private:
    void run_internal();
    PipelineDriverPoller(const PipelineDriverPoller&) = delete;
    PipelineDriverPoller& operator=(const PipelineDriverPoller&) = delete;

    mutable std::mutex _global_mutex;
    std::condition_variable _cond;
    DriverList _blocked_drivers;

    mutable std::shared_mutex _local_mutex;
    DriverList _local_blocked_drivers;

    DriverQueue* _driver_queue;
    scoped_refptr<Thread> _polling_thread;
    std::atomic<bool> _is_polling_thread_initialized;
    std::atomic<bool> _is_shutdown;
<<<<<<< HEAD
=======

    // NOTE: The `driver` can be stored in the parked drivers when it will never not be called to run.
    // The parked driver needs to be actived when it needs to be triggered again.
    mutable std::mutex _global_parked_mutex;
    DriverList _parked_drivers;

    std::atomic<size_t> _blocked_driver_queue_len;
>>>>>>> 575e3dacd6 ([Enhancement] Avoid lock when get the length of pipeline block queue (#33721))
};
} // namespace pipeline
} // namespace starrocks
