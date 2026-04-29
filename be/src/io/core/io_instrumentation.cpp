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

#include "io/core/io_instrumentation.h"

namespace starrocks::io {
namespace {

void noop_read(int64_t /*bytes*/, int64_t /*latency_ns*/) noexcept {}

void noop_write(int64_t /*bytes*/, int64_t /*latency_ns*/) noexcept {}

void noop_sync(int64_t /*latency_ns*/) noexcept {}

} // namespace

const IOEventHooks IOInstrumentation::kNoopHooks{noop_read, noop_write, noop_sync};
std::atomic<const IOEventHooks*> IOInstrumentation::s_hooks{&IOInstrumentation::kNoopHooks};

const IOEventHooks* IOInstrumentation::set_hooks(const IOEventHooks* hooks) noexcept {
    if (hooks == nullptr) {
        hooks = &kNoopHooks;
    }
    return s_hooks.exchange(hooks, std::memory_order_relaxed);
}

} // namespace starrocks::io
