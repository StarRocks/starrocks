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

#include <bvar/bvar.h>

#include <unordered_map>

#include "base_monitor.h"
#include "common/config.h"
#include "util/thread.h"

namespace starrocks {

class BrpcDumper : public bvar::Dumper {
public:
    explicit BrpcDumper(std::unordered_map<std::string, std::string>* holder) : _bvars(holder) {}

    bool dump(const std::string& name, const butil::StringPiece& desc) override {
        (*_bvars)[name] = desc.as_string();
        return true;
    }

private:
    std::unordered_map<std::string, std::string>* _bvars;
};

class BrpcThreadChecker : public BaseMonitor {
public:
    BrpcThreadChecker();
    ~BrpcThreadChecker() = default;

    // monitor iface
    void debug(std::stringstream& ss) override;

private:
    static void* _brpc_thread_checker_callback(void* arg_this);
    std::mutex _brpc_thread_checker_mutex;

    std::unordered_map<std::string, std::string> _bvars_holder;
    BrpcDumper _brpc_dumper;
};

} // namespace starrocks
