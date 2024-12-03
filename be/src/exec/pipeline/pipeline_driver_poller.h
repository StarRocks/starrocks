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

#include <atomic>
#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>

#include "pipeline_driver.h"
#include "pipeline_driver_queue.h"
#include "util/thread.h"

namespace starrocks::pipeline {

class PipelineDriverPoller;
using PipelineDriverPollerPtr = std::unique_ptr<PipelineDriverPoller>;
class PollerMetrics;

class PipelineDriverPoller {
public:
    explicit PipelineDriverPoller(std::string name, DriverQueue* driver_queue, CpuUtil::CpuIds cpuids,
                                  PollerMetrics* metrics)
            : _name(std::move(name)),
              _cpud_ids(std::move(cpuids)),
              _driver_queue(driver_queue),
              _polling_thread(nullptr),
              _is_polling_thread_initialized(false),
              _is_shutdown(false),
              _metrics(metrics) {}

    ~PipelineDriverPoller() { shutdown(); }

    DISALLOW_COPY(PipelineDriverPoller);

    using DriverList = std::list<DriverRawPtr>;

    void start();
    void shutdown();

    void add_blocked_driver(const DriverRawPtr driver);
    void remove_blocked_driver(DriverList& local_blocked_drivers, DriverList::iterator& driver_it);

    void park_driver(const DriverRawPtr driver);
    size_t activate_parked_driver(const ConstDriverPredicator& predicate_func);
    size_t calculate_parked_driver(const ConstDriverPredicator& predicate_func) const;

    void for_each_driver(const ConstDriverConsumer& call) const;

    void bind_cpus(const CpuUtil::CpuIds& cpuids);

private:
    void run_internal();
    void on_cancel(DriverRawPtr driver, std::vector<DriverRawPtr>& ready_drivers, DriverList& local_blocked_drivers,
                   DriverList::iterator& driver_it);

    const std::string _name;

    mutable std::mutex _global_mutex;
    std::condition_variable _cond;
    DriverList _blocked_drivers;
    CpuUtil::CpuIds _cpud_ids;

    mutable std::shared_mutex _local_mutex;
    DriverList _local_blocked_drivers;

    DriverQueue* _driver_queue;
    scoped_refptr<Thread> _polling_thread;
    std::atomic<bool> _is_polling_thread_initialized;
    std::atomic<bool> _is_shutdown;

    // NOTE: The `driver` can be stored in the parked drivers when it will never not be called to run.
    // The parked driver needs to be actived when it needs to be triggered again.
    mutable std::mutex _global_parked_mutex;
    DriverList _parked_drivers;

    PollerMetrics* _metrics;
};
} // namespace starrocks::pipeline
