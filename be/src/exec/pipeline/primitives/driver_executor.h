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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "common/thread/cpu_util.h"
#include "exec/pipeline/pipeline_fwd.h"

namespace starrocks::pipeline {

class DriverExecutor {
public:
    explicit DriverExecutor(std::string name) : _name(std::move(name)) {}
    virtual ~DriverExecutor() = default;
    virtual void initialize(int32_t num_threads) {}
    virtual void change_num_threads(int32_t num_threads) {}
    virtual void submit(DriverRawPtr driver) = 0;
    virtual void cancel(DriverRawPtr driver) = 0;
    virtual void close() = 0;

    // When all the root drivers (the drivers have no successors in the same fragment) have finished,
    // just notify FE timely the completeness of fragment via invocation of report_exec_state, but
    // the FragmentContext is not unregistered until all the drivers has finished, because some
    // non-root drivers maybe has pending io task executed in io threads asynchronously has reference
    // to objects owned by FragmentContext.
    virtual void report_exec_state(QueryContext* query_ctx, FragmentContext* fragment_ctx, const Status& status,
                                   bool done) = 0;

    virtual void report_audit_statistics(QueryContext* query_ctx, FragmentContext* fragment_ctx) = 0;
    virtual void report_audit_statistics_on_failure(QueryContext* query_ctx, FragmentContext* fragment_ctx) = 0;

    virtual void iterate_immutable_blocking_driver(const ConstDriverConsumer& call) const = 0;

    virtual void bind_cpus(const CpuUtil::CpuIds& cpuids, const std::vector<CpuUtil::CpuIds>& borrowed_cpuids) = 0;

    virtual Status update_exec_state_report_max_threads(int max_threads) { return Status::OK(); }
    virtual Status update_priority_exec_state_report_max_threads(int max_threads) { return Status::OK(); }

protected:
    std::string _name;
};

} // namespace starrocks::pipeline
