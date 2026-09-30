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

#include <cstddef>
#include <memory>
#include <string>

#include "base/string/slice.h"
#include "common/status.h"
#include "common/statusor.h"

namespace starrocks {

enum class GeosOverlayOperation { INTERSECTION, UNION, DIFFERENCE, SYMMETRIC_DIFFERENCE };

struct GeosGeometryDeleter {
    GEOSContextHandle_t context = nullptr;
    void operator()(GEOSGeometry* geometry) const noexcept;
};
using GeosGeometryPtr = std::unique_ptr<GEOSGeometry, GeosGeometryDeleter>;

// Owns a reentrant GEOS context for exactly one execution worker. GEOS geometry
// objects must be destroyed before this object; callers cannot share them across workers.
class GeosOverlay {
public:
    GeosOverlay();
    ~GeosOverlay();
    GeosOverlay(const GeosOverlay&) = delete;
    GeosOverlay& operator=(const GeosOverlay&) = delete;

    Status read_polygon(const Slice& wkb, GeosGeometryPtr* output);
    StatusOr<std::string> apply(const GEOSGeometry* left, const GEOSGeometry* right, GeosOverlayOperation operation);

private:
    static void on_error(const char* message, void* user_data) noexcept;
    Status error(const char* fallback) const;

    GEOSContextHandle_t _context = nullptr;
    GEOSWKBReader* _reader = nullptr;
    GEOSWKBWriter* _writer = nullptr;
    char _last_error[512] = {};
};

} // namespace starrocks
