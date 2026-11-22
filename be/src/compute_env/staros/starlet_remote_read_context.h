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

// The BE mirrors starlet's workload enum value for value, so a context converts with a cast.
constexpr bool same_remote_read_workload(RemoteReadContext::Workload be, staros::starlet::fslib::ReadWorkload starlet) {
    return static_cast<int>(be) == static_cast<int>(starlet);
}
static_assert(same_remote_read_workload(RemoteReadContext::Workload::QUERY,
                                        staros::starlet::fslib::ReadWorkload::QUERY));
static_assert(same_remote_read_workload(RemoteReadContext::Workload::COMPACTION,
                                        staros::starlet::fslib::ReadWorkload::COMPACTION));
static_assert(same_remote_read_workload(RemoteReadContext::Workload::OTHER_BACKGROUND,
                                        staros::starlet::fslib::ReadWorkload::OTHER_BACKGROUND));
static_assert(same_remote_read_workload(RemoteReadContext::Workload::LOAD, staros::starlet::fslib::ReadWorkload::LOAD));

// Hands the BE's read attribution to the remote read scheduler of starlet. Without a started scheduler starlet
// ignores it, and reads are done exactly as before.
inline staros::starlet::fslib::RemoteReadContext to_fslib_remote_read(const RemoteReadContext& ctx) {
    staros::starlet::fslib::RemoteReadContext out;
    out.is_set = ctx.is_set;
    out.workload = static_cast<staros::starlet::fslib::ReadWorkload>(ctx.workload);
    out.group_id = ctx.group_id;
    out.group_weight = ctx.group_weight;
    out.group_config_version = ctx.group_config_version;
    out.flow_id = ctx.flow_id;
    return out;
}

} // namespace starrocks

#endif // USE_STAROS
