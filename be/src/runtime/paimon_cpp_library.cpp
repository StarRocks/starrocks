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

#include "runtime/paimon_cpp_library.h"

#include <cstdlib>
#include <string>

#include "base/utility/dynamic_util.h"

namespace starrocks {

PaimonCppLibrary& PaimonCppLibrary::instance() {
    static PaimonCppLibrary library;
    return library;
}

StatusOr<void*> PaimonCppLibrary::get_symbol(Capability capability) {
    static constexpr std::array<const char*, 4> names = {
            "starrocks_paimon_create_scanner", "starrocks_paimon_create_sink_v1",
            "starrocks_paimon_bucket_calculator_v1", "starrocks_paimon_create_global_index_scanner"};
    auto& cached = _symbols[static_cast<size_t>(capability)];
    void* symbol = cached.load(std::memory_order_acquire);
    if (symbol != nullptr) return symbol;

    std::lock_guard<std::mutex> guard(_mutex);
    symbol = cached.load(std::memory_order_acquire);
    if (symbol != nullptr) return symbol;
    if (_handle == nullptr) {
        const char* home = std::getenv("STARROCKS_HOME");
        const std::string path =
                home ? std::string(home) + "/lib/paimon-cpp-lib/libstarrocks_paimon.so" : "libstarrocks_paimon.so";
        RETURN_IF_ERROR(dynamic_open(path.c_str(), &_handle));
    }
    auto status = dynamic_lookup(_handle, names[static_cast<size_t>(capability)], &symbol);
    if (!status.ok()) return Status::NotSupported(status.message());
    cached.store(symbol, std::memory_order_release);
    return symbol;
}

} // namespace starrocks
