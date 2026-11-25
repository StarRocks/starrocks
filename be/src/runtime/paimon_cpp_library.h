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

#include <array>
#include <atomic>
#include <mutex>

#include "common/statusor.h"

namespace starrocks {

// Process-wide loader for the optional Paimon shim. No SDK or connector types cross this boundary.
class PaimonCppLibrary {
public:
    enum class Capability { SCANNER, SINK, BUCKET_CALCULATOR, GLOBAL_INDEX_SCANNER };

    static PaimonCppLibrary& instance();
    StatusOr<void*> get_symbol(Capability capability);

private:
    PaimonCppLibrary() = default;

    std::mutex _mutex;
    // Never unload: live scanners, sinks and calculators retain vtables in this library.
    void* _handle = nullptr;
    std::array<std::atomic<void*>, 4> _symbols{};
};

} // namespace starrocks
