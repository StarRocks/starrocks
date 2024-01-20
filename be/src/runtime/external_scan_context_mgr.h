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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/external_scan_context_mgr.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <condition_variable>
#include <ctime>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "runtime/exec_env.h"

namespace starrocks {

struct ScanContext {
public:
    TUniqueId query_id;
    TUniqueId fragment_instance_id;
    int64_t offset = 0;
    // use this access_time to clean zombie context
    time_t last_access_time = 0;
    std::string context_id;
    short keep_alive_min = 0;
    ScanContext(std::string id) : context_id(std::move(id)) {}
    ScanContext(const TUniqueId& fragment_id, int64_t offset) : fragment_instance_id(fragment_id), offset(offset) {}
};

class ExternalScanContextMgr {
public:
    ExternalScanContextMgr(ExecEnv* exec_env);

    ~ExternalScanContextMgr();

    Status create_scan_context(std::shared_ptr<ScanContext>* p_context);

    Status get_scan_context(const std::string& context_id, std::shared_ptr<ScanContext>* p_context);

    Status clear_scan_context(const std::string& context_id);

    std::vector<std::map<std::string, std::string>> get_active_contexts();

    Status clear_inactive_scan_contexts(const int64_t inactive_time_s);

private:
    ExecEnv* _exec_env;
    std::map<std::string, std::shared_ptr<ScanContext>> _active_contexts;
    void gc_expired_context();
    std::unique_ptr<std::thread> _keep_alive_reaper;

    std::mutex _lock;
    std::condition_variable _cv;
    bool _closing = false;
};

} // namespace starrocks
