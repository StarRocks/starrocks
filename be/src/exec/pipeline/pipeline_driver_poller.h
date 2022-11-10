// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
            : _driver_queue(driver_queue), _is_polling_thread_initialized(false), _is_shutdown(false) {}

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
        size_t size = 0;
        for (int poller_id = 0; poller_id < POLLER_NUM; ++poller_id) {
            std::unique_lock<std::mutex> guard(*_mutexs[poller_id]);
            size += _blocked_drivers[poller_id].size();
        }
        return size;
    }

private:
    void run_internal(int32_t poller_id);
    PipelineDriverPoller(const PipelineDriverPoller&) = delete;
    PipelineDriverPoller& operator=(const PipelineDriverPoller&) = delete;

private:
    mutable std::vector<std::mutex*> _mutexs;
    std::vector<std::condition_variable*> _conds;
    std::vector<DriverList> _blocked_drivers;
    DriverQueue* _driver_queue;
    std::vector<scoped_refptr<Thread>> _polling_threads;
    std::atomic<int32_t> _is_polling_thread_initialized;
    std::atomic<bool> _is_shutdown;

    std::atomic<int64_t> _add_cnt = 0;
    static const int32_t POLLER_NUM = 2;
};
} // namespace pipeline
} // namespace starrocks
