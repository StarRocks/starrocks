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
#include <memory>
#include <utility>

#include "common/statusor.h"
#include "exec/runtime_filter/runtime_filter_probe.h"

namespace starrocks::pipeline {

// A ExecNode in non-pipeline engine can be decomposed into more than one OperatorFactories in pipeline engine.
// Pipeline framework do not care about that runtime filters take affects on which OperatorFactories, since
// it depends on Operators' implementation. so each OperatorFactory from the same ExecNode shared a
// RefCountedRuntimeFilterProbeCollector, in which refcount is introduced to guarantee that both prepare and
// close method of the RuntimeFilterProbeCollector inside this wrapper object is called only exactly-once.
class RefCountedRuntimeFilterProbeCollector;
using RefCountedRuntimeFilterProbeCollectorPtr = std::shared_ptr<RefCountedRuntimeFilterProbeCollector>;
class RefCountedRuntimeFilterProbeCollector {
public:
    RefCountedRuntimeFilterProbeCollector(size_t num_factories_generated,
                                          starrocks::RuntimeFilterProbeCollector&& rf_probe_collector)
            : _count((num_factories_generated << 32) | num_factories_generated),
              _num_operators_generated(num_factories_generated),
              _rf_probe_collector(std::move(rf_probe_collector)) {}

    template <class... Args>
    Status prepare(RuntimeState* state, Args&&... args) {
        // TODO: stdpain assign operator nums here
        if ((_count.fetch_sub(1) & PREPARE_COUNTER_MASK) == _num_operators_generated) {
            RETURN_IF_ERROR(_rf_probe_collector.prepare(state, std::forward<Args>(args)...));
            RETURN_IF_ERROR(_rf_probe_collector.open(state));
        }
        return Status::OK();
    }

    void close(RuntimeState* state) {
        static constexpr size_t k = 1ull << 32;
        if ((_count.fetch_sub(k) & CLOSE_COUNTER_MASK) == k) {
            _rf_probe_collector.close(state);
        }
    }

    starrocks::RuntimeFilterProbeCollector* get_rf_probe_collector() { return &_rf_probe_collector; }
    const starrocks::RuntimeFilterProbeCollector* get_rf_probe_collector() const { return &_rf_probe_collector; }

private:
    static constexpr size_t PREPARE_COUNTER_MASK = 0xffff'ffffull;
    static constexpr size_t CLOSE_COUNTER_MASK = 0xffff'ffff'0000'0000ull;

    // a refcount, low 32 bit used count the close invocation times, and the high 32 bit used to count the
    // prepare invocation times.
    std::atomic<size_t> _count;
    // how many OperatorFactories into whom a ExecNode is decomposed.
    const size_t _num_operators_generated;
    // a wrapped RuntimeFilterProbeCollector initialized by a ExecNode, which contains runtime bloom filters.
    starrocks::RuntimeFilterProbeCollector _rf_probe_collector;
};

} // namespace starrocks::pipeline
