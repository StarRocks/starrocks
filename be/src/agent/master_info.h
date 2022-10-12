// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

} // namespace starrocks
