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

#include "exec/pipeline/audit_statistics_reporter.h"

#include <thrift/Thrift.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "agent/master_info.h"
#include "gen_cpp/FrontendService_types.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"

namespace starrocks::pipeline {

using apache::thrift::TException;
using apache::thrift::TProcessor;
using apache::thrift::transport::TTransportException;

// including the final status when execution finishes.
Status AuditStatisticsReporter::report_audit_statistics(const TReportAuditStatisticsParams& params, ExecEnv* exec_env,
                                                        const TNetworkAddress& fe_addr) {
    Status fe_status;
    FrontendServiceConnection coord(exec_env->frontend_client_cache(), fe_addr, &fe_status);
    if (!fe_status.ok()) {
        LOG(WARNING) << "Couldn't get a client for " << fe_addr;
        return fe_status;
    }

    TReportAuditStatisticsResult res;
    Status rpc_status;

    try {
        try {
            coord->reportAuditStatistics(res, params);
        } catch (TTransportException& e) {
            TTransportException::TTransportExceptionType type = e.getType();
            if (type != TTransportException::TTransportExceptionType::TIMED_OUT) {
                // if not TIMED_OUT, retry
                rpc_status = coord.reopen();

                if (!rpc_status.ok()) {
                    return rpc_status;
                }
                coord->reportAuditStatistics(res, params);
            } else {
                std::stringstream msg;
                msg << "ReportExecStatus() to " << fe_addr << " failed:\n" << e.what();
                LOG(WARNING) << msg.str();
                rpc_status = Status::InternalError(msg.str());
                return rpc_status;
            }
        }

        rpc_status = Status(res.status);
    } catch (TException& e) {
        std::stringstream msg;
        msg << "ReportExecStatus() to " << fe_addr << " failed:\n" << e.what();
        LOG(WARNING) << msg.str();
        rpc_status = Status::InternalError(msg.str());
        return rpc_status;
    }
    return rpc_status;
}
} // namespace starrocks::pipeline
