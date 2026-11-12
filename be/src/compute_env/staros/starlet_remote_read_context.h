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

#ifdef USE_STAROS

#include <fslib/remote_read_context.h>

#include "fs/remote_read_context.h"

namespace starrocks {

// Hands the BE's read attribution to the remote read scheduler of starlet. Without a started scheduler starlet
// ignores it, and reads are done exactly as before.
inline staros::starlet::fslib::RemoteReadContext to_fslib_remote_read(const RemoteReadContext& ctx) {
    namespace fslib = staros::starlet::fslib;
    fslib::RemoteReadContext out;
    out.is_set = ctx.is_set;
    switch (ctx.workload) {
    case RemoteReadContext::Workload::QUERY:
        out.workload = fslib::ReadWorkload::QUERY;
        break;
    case RemoteReadContext::Workload::COMPACTION:
        out.workload = fslib::ReadWorkload::COMPACTION;
        break;
    case RemoteReadContext::Workload::OTHER_BACKGROUND:
        out.workload = fslib::ReadWorkload::OTHER_BACKGROUND;
        break;
    case RemoteReadContext::Workload::LOAD:
        out.workload = fslib::ReadWorkload::LOAD;
        break;
    }
    out.group_id = ctx.group_id;
    out.group_weight = ctx.group_weight;
    out.group_config_version = ctx.group_config_version;
    out.flow_id = ctx.flow_id;
    return out;
}

} // namespace starrocks

#endif // USE_STAROS
