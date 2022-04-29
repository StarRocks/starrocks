// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>
#include <unordered_map>

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

namespace starrocks {
namespace pipeline {

class DriverExecutor;
using DriverExecutorPtr = std::shared_ptr<DriverExecutor>;

class DriverExecutor {
public:
    DriverExecutor() = default;
    virtual ~DriverExecutor() = default;
    virtual void initialize(int32_t num_threads) {}
    virtual void change_num_threads(int32_t num_threads) {}
    virtual void submit(DriverRawPtr driver){};
    virtual void cancel(DriverRawPtr driver){};

    // When all the root drivers (the drivers have no successors in the same fragment) have finished,
    // just notify FE timely the completeness of fragment via invocation of report_exec_state, but
    // the FragmentContext is not unregistered until all the drivers has finished, because some
    // non-root drivers maybe has pending io task executed in io threads asynchronously has reference
    // to objects owned by FragmentContext.
    virtual void report_exec_state(FragmentContext* fragment_ctx, const Status& status, bool done) = 0;
};

class GlobalDriverExecutor final : public FactoryMethod<DriverExecutor, GlobalDriverExecutor> {
public:
    GlobalDriverExecutor(std::unique_ptr<ThreadPool> thread_pool, bool enable_resource_group);
    ~GlobalDriverExecutor() override;
    void initialize(int32_t num_threads) override;
    void change_num_threads(int32_t num_threads) override;
    void submit(DriverRawPtr driver) override;
    void cancel(DriverRawPtr driver) override;
    void report_exec_state(FragmentContext* fragment_ctx, const Status& status, bool done) override;

private:
    void _worker_thread();
    void _finalize_driver(DriverRawPtr driver, RuntimeState* runtime_state, DriverState state);
    void _update_profile_by_level(FragmentContext* fragment_ctx, bool done);
    void _remove_non_core_metrics(FragmentContext* fragment_ctx, std::vector<RuntimeProfile*>& driver_profiles);
    void _simplify_common_metrics(RuntimeProfile* driver_profile);

private:
    LimitSetter _num_threads_setter;
    const bool _enable_resource_group;
    std::unique_ptr<DriverQueue> _driver_queue;
    // _thread_pool must be placed after _driver_queue, because worker threads in _thread_pool use _driver_queue.
    std::unique_ptr<ThreadPool> _thread_pool;
    PipelineDriverPollerPtr _blocked_driver_poller;
    std::unique_ptr<ExecStateReporter> _exec_state_reporter;

    std::atomic<int> _next_id = 0;
};

} // namespace pipeline
} // namespace starrocks
