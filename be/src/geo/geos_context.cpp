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

#include "geo/geos_context.h"

#include <cstdio>
#include <string>

namespace starrocks {

void GeosGeometryDeleter::operator()(GEOSGeometry* geometry) const noexcept {
    if (geometry != nullptr) GEOSGeom_destroy_r(context, geometry);
}

GeosContext::GeosContext() {
    _context = GEOS_init_r();
    if (_context != nullptr) GEOSContext_setErrorMessageHandler_r(_context, on_error, this);
}

GeosContext::~GeosContext() {
    if (_context != nullptr) GEOS_finish_r(_context);
}

void GeosContext::on_error(const char* message, void* user_data) noexcept {
    auto* self = static_cast<GeosContext*>(user_data);
    if (self != nullptr) std::snprintf(self->_last_error, sizeof(self->_last_error), "%s", message ? message : "");
}

Status GeosContext::error(const char* fallback) const {
    return Status::InvalidArgument(std::string(fallback) +
                                   (_last_error[0] == '\0' ? "" : std::string(": ") + _last_error));
}

} // namespace starrocks
