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

namespace starrocks {
// wait-free.
// Multi-threaded calls ensure that each pending event is handled.
// Callbacks internally handle multiple events at once.
class AtomicRequestControler {
public:
    template <class CallBack>
    explicit AtomicRequestControler(std::atomic_int32_t& request, CallBack&& callback) : _request(request) {
        if (_request.fetch_add(1, std::memory_order_acq_rel) == 0) {
            int progress = _request.load(std::memory_order_acquire);
            do {
                callback();
            } while (_has_more(&progress));
        }
    }

private:
    bool _has_more(int* progress) {
        return !_request.compare_exchange_strong(*progress, 0, std::memory_order_release, std::memory_order_acquire);
    }

private:
    std::atomic_int32_t& _request;
};
} // namespace starrocks