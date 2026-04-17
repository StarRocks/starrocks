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

#include "exec/adbc_driver_registry.h"

#include <climits>
#include <cstdlib>

#include "common/logging.h"

namespace starrocks {

ADBCDriverRegistry& ADBCDriverRegistry::instance() {
    static ADBCDriverRegistry registry;
    return registry;
}

StatusOr<const AdbcDriver*> ADBCDriverRegistry::get_or_load(const std::string& driver_url,
                                                            const std::string& entrypoint) {
    // Canonicalize path via realpath to avoid loading the same .so twice
    // under different paths (symlinks, relative vs absolute, etc.)
    char resolved[PATH_MAX];
    if (realpath(driver_url.c_str(), resolved) == nullptr) {
        return Status::InvalidArgument(fmt::format("ADBC driver path not found or not readable: {}", driver_url));
    }
    std::string key(resolved);

    std::lock_guard<std::mutex> lock(_mutex);
    auto& entry = _drivers[key];

    if (entry.loaded) {
        if (!entry.load_status.ok()) return entry.load_status;
        return &entry.driver;
    }

    // Load via ADBC driver manager: AdbcLoadDriver performs dlopen(RTLD_NOW | RTLD_LOCAL)
    AdbcError error = ADBC_ERROR_INIT;
    const char* ep = entrypoint.empty() ? nullptr : entrypoint.c_str();
    AdbcStatusCode status = AdbcLoadDriver(key.c_str(), ep, ADBC_VERSION_1_1_0, &entry.driver, &error);

    if (status != ADBC_STATUS_OK) {
        std::string msg = error.message ? error.message : "Unknown ADBC driver load error";
        if (error.release) error.release(&error);
        entry.load_status = Status::InternalError(fmt::format("Failed to load ADBC driver '{}': {}", key, msg));
        entry.loaded = true;
        return entry.load_status;
    }

    LOG(INFO) << "ADBC driver loaded: " << key << (entrypoint.empty() ? "" : " (entrypoint=" + entrypoint + ")");
    entry.loaded = true;
    entry.load_status = Status::OK();
    return &entry.driver;
}

size_t ADBCDriverRegistry::loaded_count() const {
    std::lock_guard<std::mutex> lock(_mutex);
    size_t count = 0;
    for (const auto& [_, e] : _drivers) {
        if (e.loaded && e.load_status.ok()) ++count;
    }
    return count;
}

} // namespace starrocks
