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

#include <runtime/brpc_thread_checker.h>

namespace starrocks {

BrpcThreadChecker::BrpcThreadChecker() : BaseMonitor("brpc_thread_checker"), _brpc_dumper(BrpcDumper(_bvars_holder)) {
    _callback_function = _brpc_thread_checker_callback;
}

void* BrpcThreadChecker::_brpc_thread_checker_callback(void* arg_this) {
    LOG(INFO) << "BrpcThreadChecker start working.";

    auto* brpc_thread_checker_this = (BrpcThreadChecker*)arg_this;
    int32_t interval = config::brpc_check_interval;

    while (!brpc_thread_checker_this->_stop) {
        std::lock_guard lg(brpc_thread_checker_this->_brpc_thread_checker_mutex);
        bvar::DumpOptions dump_options;
        dump_options.white_wildcards = "bthread*";
        bvar::Variable::dump_exposed(&(brpc_thread_checker_this->_brpc_dumper), &dump_options);

        if (interval <= 0) {
            LOG(WARNING) << "brpc_check_interval config is illegal: " << interval << ", force set to 1";
            interval = 1;
        }
        int32_t left_seconds = interval;
        while (!brpc_thread_checker_this->_stop.load(std::memory_order_consume) && left_seconds > 0) {
            sleep(1);
            --left_seconds;
        }
    }
    LOG(INFO) << "BrpcThreadChecker going to exit.";
    return nullptr;
}


void BrpcThreadChecker::debug(std::stringstream& ss) {
    std::lock_guard lg(_brpc_thread_checker_mutex);
    for (const auto& iter : _bvars_holder) {
        ss << iter.first << ":" << iter.second;
        LOG(INFO) << ss.str();
    }
}

} // namespace starrocks
