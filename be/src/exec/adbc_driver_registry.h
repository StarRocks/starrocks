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

#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_driver_manager.h>

#include <map>
#include <mutex>
#include <string>

#include "common/status.h"
#include "common/statusor.h"

namespace starrocks {

// Process-global singleton that loads ADBC driver .so files exactly once
// per resolved absolute path. Never calls dlclose — driver handles stay
// resident until process exit.
class ADBCDriverRegistry {
public:
    static ADBCDriverRegistry& instance();

    // Returns a fully-initialized AdbcDriver for the given driver_url.
    // On first call for a given resolved path: calls AdbcLoadDriver which
    // internally uses dlopen(RTLD_NOW | RTLD_LOCAL).
    // Subsequent calls return the cached driver.
    // entrypoint is optional — defaults to nullptr (driver manager default) if empty.
    StatusOr<const AdbcDriver*> get_or_load(const std::string& driver_url, const std::string& entrypoint = "");

    // For testing: number of loaded drivers
    size_t loaded_count() const;

private:
    ADBCDriverRegistry() = default;
    ~ADBCDriverRegistry() = default;
    ADBCDriverRegistry(const ADBCDriverRegistry&) = delete;
    ADBCDriverRegistry& operator=(const ADBCDriverRegistry&) = delete;

    struct DriverEntry {
        AdbcDriver driver{};
        bool loaded = false;
        Status load_status;
    };

    mutable std::mutex _mutex;
    std::map<std::string, DriverEntry> _drivers; // key = realpath(driver_url)
};

} // namespace starrocks
