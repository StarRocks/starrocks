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

#include <butil/containers/doubly_buffered_data.h>

#include <optional>

#include "gen_cpp/HeartbeatService_types.h"

namespace starrocks {

using MasterInfoPtr = butil::DoublyBufferedData<TMasterInfo>::ScopedPtr;

// Put `TMasterInfo` instance into |ptr|. The instance will not be changed until |ptr| is destructed.
//
// Returns true on success, false otherwise.
[[nodiscard]] bool get_master_info(MasterInfoPtr* ptr);

// Returns a copy of the `TMasterInfo` instance.
//
// NOTE: `get_master_info()` relies on `get_master_info(MasterInfoPtr*)` and if
// `get_master_info(MasterInfoPtr*)` failed a default constructed `TMasterInfo`
// will be returned.
[[nodiscard]] TMasterInfo get_master_info();

// Returns true on success, false otherwise.
//
// NOTE: Do NOT call `update_master_info` while still holding a `MasterInfoPtr`
// filled by `get_master_info(MasterInfoPtr*)`, otherwise deadlock will happen.
[[nodiscard]] bool update_master_info(const TMasterInfo& master_info);

// Returns the token of TMasterInfo instance.
//
// NOTE: Relies on `get_master_info()` and if `get_master_info()` failed an empty string
// will be returned.
[[nodiscard]] std::string get_master_token();

// Returns the value of network_address field of TMasterInfo instance.
//
// NOTE: Relies on `get_master_info()` and if `get_master_info()` failed a default constructed
// TNetworkAddress will be returned.
[[nodiscard]] TNetworkAddress get_master_address();

[[nodiscard]] std::optional<int64_t> get_backend_id();

// Returns the value of current FE's run mode
[[nodiscard]] std::optional<TRunMode::type> get_master_run_mode();
} // namespace starrocks
