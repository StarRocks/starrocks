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
#include <cstdint>

namespace starrocks::io {

struct IOEventHooks {
    void (*on_read)(int64_t bytes, int64_t latency_ns) noexcept;
    void (*on_write)(int64_t bytes, int64_t latency_ns) noexcept;
    void (*on_sync)(int64_t latency_ns) noexcept;
};

class IOInstrumentation {
public:
    // Installed hook tables must stay immutable and remain alive for the rest
    // of the process. Production code is expected to publish the active table
    // once during startup before concurrent record_*() calls begin. Tests may
    // still swap hooks before exercising the API.
    static const IOEventHooks* set_hooks(const IOEventHooks* hooks) noexcept;

    static inline const IOEventHooks* get_hooks() noexcept { return s_hooks.load(std::memory_order_relaxed); }

    static inline void record_read(int64_t bytes, int64_t latency_ns) noexcept {
        const auto* hooks = get_hooks();
        hooks->on_read(bytes, latency_ns);
    }

    static inline void record_write(int64_t bytes, int64_t latency_ns) noexcept {
        const auto* hooks = get_hooks();
        hooks->on_write(bytes, latency_ns);
    }

    static inline void record_sync(int64_t latency_ns) noexcept {
        const auto* hooks = get_hooks();
        hooks->on_sync(latency_ns);
    }

private:
    static const IOEventHooks kNoopHooks;
    static std::atomic<const IOEventHooks*> s_hooks;
};

} // namespace starrocks::io
