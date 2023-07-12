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
#include <thread>
#include <unordered_map>

#include "http/rest_monitor_iface.h"
#include "util/thread.h"
#include "util/threadpool.h"

namespace starrocks {

/*
 * Base class providing interfaces of monitoring metrics.
   A thread automatically created when invoking the `start` method
   to execute the callback function, which is used to collect monitoring metrics

   It's usually necessary to specify a callback function when implementing a subclass
   The implementation of the class `ThreadPoolChecker` can be refered

   The class implement the interface of RestMonitorIface,
   so the metrics can be get by /api/monitor?module=module_name
 */
class BaseMonitor : public RestMonitorIface {
public:
    typedef void* (*CALLBACK_FUNCTION)(void*);
    BaseMonitor(std::string name);
    virtual ~BaseMonitor();

    std::string get_name();

    //get monitoring metrics in JSON format
    std::string get_info();

    virtual void start();
    // support exit gracefully
    virtual void stop();

protected:
    CALLBACK_FUNCTION _callback_function = nullptr;
    std::atomic<bool> _stop = false;
    std::atomic<bool> _start = false;
    std::thread _thread;
    std::string _monitor_name;
};

} // namespace starrocks