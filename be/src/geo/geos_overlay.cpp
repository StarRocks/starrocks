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

#include "geo/geos_overlay.h"

#include <exception>
#include <memory>

#include "geo/wkb.h"

namespace starrocks {
namespace {
constexpr size_t kMaxOverlayBytes = 64 * 1024 * 1024;
constexpr int kMaxOverlayCoordinates = 1'000'000;

struct GeosBytesDeleter {
    GEOSContextHandle_t context;
    void operator()(unsigned char* bytes) const noexcept {
        if (bytes != nullptr) GEOSFree_r(context, bytes);
    }
};
} // namespace

GeosOverlay::GeosOverlay() {
    if (!_geos.ready()) return;
    _reader = GEOSWKBReader_create_r(_geos.get());
    _writer = GEOSWKBWriter_create_r(_geos.get());
    if (_writer != nullptr) {
        GEOSWKBWriter_setOutputDimension_r(_geos.get(), _writer, 2);
        GEOSWKBWriter_setByteOrder_r(_geos.get(), _writer, 1);
    }
}

GeosOverlay::~GeosOverlay() {
    if (!_geos.ready()) return;
    if (_writer != nullptr) GEOSWKBWriter_destroy_r(_geos.get(), _writer);
    if (_reader != nullptr) GEOSWKBReader_destroy_r(_geos.get(), _reader);
}

Status GeosOverlay::read_polygon(const Slice& wkb, GeosGeometryPtr* output) {
    if (_reader == nullptr || _writer == nullptr) return Status::InternalError("GEOS context initialization failed");
    try {
        WkbGeometry geometry;
        RETURN_IF_ERROR(WkbCodec::parse_wkb(wkb, &geometry, WkbCoordinateSemantics::GEOMETRY_CARTESIAN));
        if (geometry.type != WkbGeometryType::POLYGON && geometry.type != WkbGeometryType::MULTIPOLYGON) {
            return Status::InvalidArgument("GEOMETRY overlay supports only POLYGON/MULTIPOLYGON inputs");
        }
        _geos.clear_error();
        GeosGeometryPtr parsed(
                GEOSWKBReader_read_r(_geos.get(), _reader, reinterpret_cast<const unsigned char*>(wkb.data), wkb.size),
                GeosGeometryDeleter{_geos.get()});
        if (!parsed) return _geos.error("GEOS could not read polygon WKB");
        const char valid = GEOSisValid_r(_geos.get(), parsed.get());
        if (valid != 1)
            return _geos.error(valid == 0 ? "GEOMETRY overlay requires valid polygon topology"
                                          : "GEOS polygon validity check failed");
        *output = std::move(parsed);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError(std::string("GEOS input preparation failed: ") + e.what());
    }
}

StatusOr<std::string> GeosOverlay::apply(const GEOSGeometry* left, const GEOSGeometry* right,
                                         GeosOverlayOperation operation) {
    if (_reader == nullptr || _writer == nullptr) return Status::InternalError("GEOS context initialization failed");
    if (left == nullptr || right == nullptr) return Status::InvalidArgument("GEOS overlay input is missing");
    try {
        _geos.clear_error();
        GEOSGeometry* raw = nullptr;
        switch (operation) {
        case GeosOverlayOperation::INTERSECTION:
            raw = GEOSIntersection_r(_geos.get(), left, right);
            break;
        case GeosOverlayOperation::UNION:
            raw = GEOSUnion_r(_geos.get(), left, right);
            break;
        case GeosOverlayOperation::DIFFERENCE:
            raw = GEOSDifference_r(_geos.get(), left, right);
            break;
        case GeosOverlayOperation::SYMMETRIC_DIFFERENCE:
            raw = GEOSSymDifference_r(_geos.get(), left, right);
            break;
        }
        GeosGeometryPtr result(raw, GeosGeometryDeleter{_geos.get()});
        if (!result) return _geos.error("GEOS overlay failed");

        const int coordinates = GEOSGetNumCoordinates_r(_geos.get(), result.get());
        if (coordinates < 0) return _geos.error("GEOS could not inspect overlay result");
        if (coordinates > kMaxOverlayCoordinates) {
            return Status::InvalidArgument("GEOS overlay result exceeds coordinate safety limit");
        }
        size_t size = 0;
        std::unique_ptr<unsigned char, GeosBytesDeleter> bytes(
                GEOSWKBWriter_write_r(_geos.get(), _writer, result.get(), &size), GeosBytesDeleter{_geos.get()});
        if (!bytes) return _geos.error("GEOS could not write overlay result");
        if (size > kMaxOverlayBytes) return Status::InvalidArgument("GEOS overlay result exceeds WKB safety limit");

        // The native codec enforces the supported XY/family/depth and allocation bounds on
        // the output too. Do not silently drop lower-dimensional or collection components.
        WkbGeometry verified;
        RETURN_IF_ERROR(WkbCodec::parse_wkb(Slice(reinterpret_cast<const char*>(bytes.get()), size), &verified,
                                            WkbCoordinateSemantics::GEOMETRY_CARTESIAN));
        return std::string(reinterpret_cast<const char*>(bytes.get()), size);
    } catch (const std::exception& e) {
        return Status::InternalError(std::string("GEOS overlay failed: ") + e.what());
    }
}

} // namespace starrocks
