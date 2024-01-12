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

#include <memory>
#include <unordered_map>

#include "exec/pipeline/audit_statistics_reporter.h"
#include "exec/pipeline/exec_state_reporter.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/pipeline_driver_poller.h"
#include "exec/pipeline/pipeline_driver_queue.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/query_context.h"
#include "runtime/runtime_state.h"
#include "util/factory_method.h"
#include "util/limit_setter.h"
#include "util/threadpool.h"

namespace starrocks::pipeline {

class DriverExecutor;
using DriverExecutorPtr = std::shared_ptr<DriverExecutor>;

class DriverExecutor {
public:
    DriverExecutor(std::string name) : _name(std::move(name)) {}
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
                                   bool done, bool attach_profile) = 0;

    virtual void report_audit_statistics(QueryContext* query_ctx, FragmentContext* fragment_ctx, bool* done) = 0;

    virtual void iterate_immutable_blocking_driver(const IterateImmutableDriverFunc& call) const = 0;

    virtual size_t activate_parked_driver(const ImmutableDriverPredicateFunc& predicate_func) = 0;

    virtual void report_epoch(ExecEnv* exec_env, QueryContext* query_ctx,
                              std::vector<FragmentContext*> fragment_ctxs) = 0;

    virtual size_t calculate_parked_driver(const ImmutableDriverPredicateFunc& predicate_func) const = 0;

protected:
    std::string _name;
};

class GlobalDriverExecutor final : public FactoryMethod<DriverExecutor, GlobalDriverExecutor> {
public:
    GlobalDriverExecutor(const std::string& name, std::unique_ptr<ThreadPool> thread_pool, bool enable_resource_group);
    ~GlobalDriverExecutor() override = default;
    void initialize(int32_t num_threads) override;
    void change_num_threads(int32_t num_threads) override;
    void submit(DriverRawPtr driver) override;
    void cancel(DriverRawPtr driver) override;
    void close() override;
    void report_exec_state(QueryContext* query_ctx, FragmentContext* fragment_ctx, const Status& status, bool done,
                           bool attach_profile) override;
    void report_audit_statistics(QueryContext* query_ctx, FragmentContext* fragment_ctx, bool* done) override;

    void iterate_immutable_blocking_driver(const IterateImmutableDriverFunc& call) const override;

    size_t activate_parked_driver(const ImmutableDriverPredicateFunc& predicate_func) override;
    size_t calculate_parked_driver(const ImmutableDriverPredicateFunc& predicate_func) const override;

    void report_epoch(ExecEnv* exec_env, QueryContext* query_ctx, std::vector<FragmentContext*> fragment_ctxs) override;

private:
    using Base = FactoryMethod<DriverExecutor, GlobalDriverExecutor>;
    void _worker_thread();
    StatusOr<DriverRawPtr> _get_next_driver(std::queue<DriverRawPtr>& local_driver_queue);
    void _finalize_driver(DriverRawPtr driver, RuntimeState* runtime_state, DriverState state);
    RuntimeProfile* _build_merged_instance_profile(QueryContext* query_ctx, FragmentContext* fragment_ctx,
                                                   ObjectPool* obj_pool);

    void _finalize_epoch(DriverRawPtr driver, RuntimeState* runtime_state, DriverState state);

private:
    // The maximum duration that a driver could stay in local_driver_queue
    static constexpr int64_t LOCAL_MAX_WAIT_TIME_SPENT_NS = 1'000'000L;

    LimitSetter _num_threads_setter;
    std::unique_ptr<DriverQueue> _driver_queue;
    // _thread_pool must be placed after _driver_queue, because worker threads in _thread_pool use _driver_queue.
    std::unique_ptr<ThreadPool> _thread_pool;
    PipelineDriverPollerPtr _blocked_driver_poller;
    std::unique_ptr<ExecStateReporter> _exec_state_reporter;
    std::unique_ptr<AuditStatisticsReporter> _audit_statistics_reporter;

    std::atomic<int> _next_id = 0;
    std::atomic_int64_t _schedule_count = 0;
    std::atomic_int64_t _driver_execution_ns = 0;

    // metrics
    std::unique_ptr<UIntGauge> _driver_queue_len;
    std::unique_ptr<UIntGauge> _driver_poller_block_queue_len;
};

} // namespace starrocks::pipeline
