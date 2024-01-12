// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "util/threadpool.h"

namespace starrocks {
namespace pipeline {
class ExecStateReporter {
public:
    static TReportExecStatusParams create_report_exec_status_params(QueryContext* query_ctx,
                                                                    FragmentContext* fragment_ctx, const Status& status,
                                                                    bool done);
    static Status report_exec_status(const TReportExecStatusParams& params, ExecEnv* exec_env,
                                     const TNetworkAddress& fe_addr);

    ExecStateReporter();

    void submit(std::function<void()>&& report_task, bool priority = false);

private:
    std::unique_ptr<ThreadPool> _thread_pool;
    std::unique_ptr<ThreadPool> _priority_thread_pool;
};
} // namespace pipeline
} // namespace starrocks
