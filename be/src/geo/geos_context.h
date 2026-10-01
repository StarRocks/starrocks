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

#include <geos_c.h>

#include <memory>

#include "common/status.h"

namespace starrocks {

struct GeosGeometryDeleter {
    GEOSContextHandle_t context = nullptr;
    void operator()(GEOSGeometry* geometry) const noexcept;
};
using GeosGeometryPtr = std::unique_ptr<GEOSGeometry, GeosGeometryDeleter>;

// A GEOS context and its error callback belong to one execution worker.
// Destroy geometries, readers, and writers before destroying their context.
class GeosContext {
public:
    GeosContext();
    ~GeosContext();
    GeosContext(const GeosContext&) = delete;
    GeosContext& operator=(const GeosContext&) = delete;

    bool ready() const noexcept { return _context != nullptr; }
    GEOSContextHandle_t get() const noexcept { return _context; }
    void clear_error() noexcept { _last_error[0] = '\0'; }
    Status error(const char* fallback) const;

private:
    static void on_error(const char* message, void* user_data) noexcept;

    GEOSContextHandle_t _context = nullptr;
    char _last_error[512] = {};
};

} // namespace starrocks
