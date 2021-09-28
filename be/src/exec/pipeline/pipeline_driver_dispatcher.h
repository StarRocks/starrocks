// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

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
class DriverDispatcher;
using DriverDispatcherPtr = std::shared_ptr<DriverDispatcher>;

class DriverDispatcher {
public:
    DriverDispatcher() = default;
    virtual ~DriverDispatcher() = default;
    virtual void initialize(int32_t num_threads) {}
    virtual void change_num_threads(int32_t num_threads) {}
    virtual void dispatch(DriverPtr driver){};

    // When all the root drivers (the drivers have no successors in the same fragment) have finished,
    // just notify FE timely the completeness of fragment via invocation of report_exec_state, but
    // the FragmentContext is not unregistered until all the drivers has finished, because some
    // non-root drivers maybe has pending io task executed in io threads asynchronously has reference
    // to objects owned by FragmentContext.
    virtual void report_exec_state(FragmentContext* fragment_ctx, const Status& status, bool done) = 0;
};

class GlobalDriverDispatcher final : public FactoryMethod<DriverDispatcher, GlobalDriverDispatcher> {
public:
    explicit GlobalDriverDispatcher(std::unique_ptr<ThreadPool> thread_pool);
    ~GlobalDriverDispatcher() override = default;
    void initialize(int32_t num_threads) override;
    void change_num_threads(int32_t num_threads) override;
    void dispatch(DriverPtr driver) override;
    void report_exec_state(FragmentContext* fragment_ctx, const Status& status, bool done) override;

private:
    void run();
    void finalize_driver(DriverPtr& driver, RuntimeState* runtime_state, DriverState state);

private:
    LimitSetter _num_threads_setter;
    std::unique_ptr<DriverQueue> _driver_queue;
    std::unique_ptr<ThreadPool> _thread_pool;
    PipelineDriverPollerPtr _blocked_driver_poller;
    std::unique_ptr<ExecStateReporter> _exec_state_reporter;
};

} // namespace pipeline
} // namespace starrocks