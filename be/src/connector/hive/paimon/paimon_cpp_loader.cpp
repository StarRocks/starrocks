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

#include "connector/hive/paimon/paimon_cpp_loader.h"

#include <atomic>
#include <cstdlib>
#include <mutex>
#include <string>

#include "base/utility/dynamic_util.h"
#include "common/version.h"
#include "connector/hive/paimon/paimon_cpp_shim_api.h"
#include "fmt/format.h"

namespace starrocks {

namespace {

constexpr const char* kShimLibraryName = "libstarrocks_paimon.so";

std::string shim_library_path() {
    const char* home = std::getenv("STARROCKS_HOME");
    if (home == nullptr) {
        // UT / source-tree runs: let the dynamic loader search LD_LIBRARY_PATH.
        return kShimLibraryName;
    }
    // Absolute path on purpose: start_backend.sh puts $STARROCKS_HOME/lib on
    // LD_LIBRARY_PATH only when JAVA_HOME is unset, so the search path cannot
    // be relied on in production. The shim's own paimon dependencies resolve
    // next to it through its $ORIGIN rpath.
    return std::string(home) + "/lib/paimon-cpp-lib/" + kShimLibraryName;
}

std::string be_build_version() {
    return fmt::format("{}-{}", STARROCKS_COMMIT_HASH, STARROCKS_BUILD_TYPE);
}

// Set once under _load_mutex, then read lock-free. The shim handle is never
// dlclose()d: BE code holds vtables and code pointers into it for the process
// lifetime.
std::atomic<StarRocksPaimonCreateScannerFn> _create_scanner_fn{nullptr};
std::mutex _load_mutex;

Status load_shim_locked() {
    const std::string path = shim_library_path();

    void* handle = nullptr;
    if (const Status st = dynamic_open(path.c_str(), &handle); !st.ok()) {
        return Status::InternalError(fmt::format(
                "Paimon native reader is unavailable: {}. Either this BE was packaged without paimon-cpp or the "
                "paimon libraries were removed from be/lib/paimon-cpp-lib. Set session variable "
                "paimon_reader_mode=JNI to use the JNI reader instead.",
                st.message()));
    }

    void* build_version_sym = nullptr;
    RETURN_IF_ERROR(dynamic_lookup(handle, STARROCKS_PAIMON_BUILD_VERSION_SYMBOL, &build_version_sym));
    const char* shim_build_version = reinterpret_cast<StarRocksPaimonBuildVersionFn>(build_version_sym)();
    if (const std::string expected = be_build_version();
        shim_build_version == nullptr || expected != shim_build_version) {
        return Status::InternalError(
                fmt::format("Paimon native reader is unavailable: {} was built from '{}' but this BE is '{}'. The shim "
                            "compiles BE internals, so it must come from the same build as the BE binary.",
                            path, shim_build_version == nullptr ? "<null>" : shim_build_version, expected));
    }

    void* create_sym = nullptr;
    RETURN_IF_ERROR(dynamic_lookup(handle, STARROCKS_PAIMON_CREATE_SCANNER_SYMBOL, &create_sym));
    _create_scanner_fn.store(reinterpret_cast<StarRocksPaimonCreateScannerFn>(create_sym), std::memory_order_release);
    return Status::OK();
}

} // namespace

StatusOr<HdfsScanner*> create_paimon_cpp_scanner() {
    auto fn = _create_scanner_fn.load(std::memory_order_acquire);
    if (fn == nullptr) {
        std::lock_guard<std::mutex> guard(_load_mutex);
        fn = _create_scanner_fn.load(std::memory_order_acquire);
        if (fn == nullptr) {
            // Failures are not cached: an operator can drop the libraries into
            // be/lib and retry without restarting the BE.
            RETURN_IF_ERROR(load_shim_locked());
            fn = _create_scanner_fn.load(std::memory_order_acquire);
        }
    }
    return fn();
}

} // namespace starrocks
