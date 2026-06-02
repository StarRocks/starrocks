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

#include <any>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>

#include "base/concurrency/race_detect.h"
#include "base/utility/defer_op.h"
#include "common/runtime_profile.h"
#include "exec/workgroup/work_group_fwd.h"
#include "gen_cpp/InternalService_types.h"

namespace starrocks::workgroup {

struct ScanTaskGroup {
    int64_t runtime_ns = 0;
    int sub_queue_level = 0;
};

#define TO_NEXT_STAGE(yield_point) yield_point++;

struct YieldContext {
    YieldContext() = default;
    YieldContext(size_t total_yield_point_cnt) : total_yield_point_cnt(total_yield_point_cnt) {}

    ~YieldContext() = default;

    DISALLOW_COPY(YieldContext);
    YieldContext(YieldContext&&) = default;
    YieldContext& operator=(YieldContext&&) = default;

    bool is_finished() const { return yield_point >= total_yield_point_cnt; }
    void set_finished() {
        yield_point = total_yield_point_cnt = 0;
        task_context_data.reset();
    }
    auto defer_finished() {
        return CancelableDefer([this]() { set_finished(); });
    }

    std::any task_context_data;
    size_t yield_point{};
    size_t total_yield_point_cnt{};
    const WorkGroup* wg = nullptr;
    // used to record the runtime information of a single call in order to decide whether to trigger yield.
    // It needs to be reset every time when the task is executed.
    int64_t time_spent_ns = 0;
    bool need_yield = false;
};

struct ScanTask {
public:
    using WorkFunction = std::function<void(YieldContext&)>;
    using YieldFunction = std::function<void(ScanTask&&)>;

    ScanTask() : ScanTask(nullptr, nullptr) {}
    explicit ScanTask(WorkFunction work_function) : ScanTask(nullptr, std::move(work_function)) {}
    ScanTask(WorkGroupPtr workgroup, WorkFunction work_function)
            : workgroup(std::move(workgroup)), work_function(std::move(work_function)) {}
    ScanTask(WorkGroupPtr workgroup, WorkFunction work_function, YieldFunction yield_function)
            : workgroup(std::move(workgroup)),
              work_function(std::move(work_function)),
              yield_function(std::move(yield_function)) {}
    ~ScanTask() = default;

    DISALLOW_COPY(ScanTask);
    // Enable move constructor and assignment.
    ScanTask(ScanTask&&) = default;
    ScanTask& operator=(ScanTask&&) = default;

    bool operator<(const ScanTask& rhs) const { return priority < rhs.priority; }
    ScanTask& operator++() {
        priority += 2;
        return *this;
    }

    void run() { work_function(work_context); }

    bool is_finished() const { return work_context.is_finished(); }

    bool has_yield_function() const { return yield_function != nullptr; }

    void execute_yield_function() {
        DCHECK(yield_function != nullptr) << "yield function must be set";
        yield_function(std::move(*this));
    }

    const YieldContext& get_work_context() const { return work_context; }

    void set_query_type(TQueryType::type type) { _query_type = type; }
    TQueryType::type query_type() const { return _query_type; }

public:
    WorkGroupPtr workgroup;
    YieldContext work_context;
    WorkFunction work_function;
    YieldFunction yield_function;
    int priority = 0;
    std::shared_ptr<ScanTaskGroup> task_group = nullptr;
    RuntimeProfile::HighWaterMarkCounter* peak_scan_task_queue_size_counter = nullptr;

private:
    TQueryType::type _query_type = TQueryType::EXTERNAL;
};

} // namespace starrocks::workgroup
