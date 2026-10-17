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

#include "types/geo_wkb.h"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>

namespace starrocks {
namespace {

class WkbReader {
public:
    WkbReader(Slice bytes, const GeoWkbLimits& limits);

    StatusOr<GeoWkbInfo> read();

private:
    template <typename T>
    StatusOr<T> number(bool little);

    Status components(uint32_t count);
    Status coordinates(uint32_t count, uint32_t width);
    Status geometry(uint32_t depth, uint32_t expected_type);

    Slice _bytes;
    const GeoWkbLimits& _limits;
    size_t _offset = 0;
    GeoWkbInfo _info;
};

WkbReader::WkbReader(Slice bytes, const GeoWkbLimits& limits) : _bytes(bytes), _limits(limits) {}

StatusOr<GeoWkbInfo> WkbReader::read() {
    if (_bytes.size > _limits.max_bytes) return Status::InvalidArgument("WKB payload limit exceeded");
    RETURN_IF_ERROR(geometry(1, 0));
    if (_offset != _bytes.size) return Status::InvalidArgument("Trailing WKB bytes");
    return _info;
}

template <typename T>
StatusOr<T> WkbReader::number(bool little) {
    if (sizeof(T) > _bytes.size - _offset) return Status::InvalidArgument("Truncated WKB");
    T value;
    memcpy(&value, _bytes.data + _offset, sizeof(T));
    _offset += sizeof(T);
    if (little != (std::endian::native == std::endian::little)) value = std::byteswap(value);
    return value;
}

Status WkbReader::components(uint32_t count) {
    if (count > _limits.max_components - _info.components)
        return Status::InvalidArgument("WKB component limit exceeded");
    _info.components += count;
    return Status::OK();
}

Status WkbReader::coordinates(uint32_t count, uint32_t width) {
    if (count > _limits.max_coordinates - _info.coordinates)
        return Status::InvalidArgument("WKB coordinate limit exceeded");
    const size_t stride = width * sizeof(double);
    if (count > (_bytes.size - _offset) / stride) return Status::InvalidArgument("Truncated WKB coordinates");
    _info.coordinates += count;
    _offset += size_t(count) * stride;
    return Status::OK();
}

Status WkbReader::geometry(uint32_t depth, uint32_t expected_type) {
    if (depth > std::min(_limits.max_depth, uint32_t(64))) return Status::InvalidArgument("WKB nesting limit exceeded");
    RETURN_IF_ERROR(components(1));
    ASSIGN_OR_RETURN(auto order, number<uint8_t>(true));
    if (order > 1) return Status::InvalidArgument("Invalid WKB byte order");
    const bool little = order == 1;
    ASSIGN_OR_RETURN(auto code, number<uint32_t>(little));
    const uint32_t type = code % 1000;
    const uint32_t dimension = code / 1000;
    if (dimension > 3 || type < 1 || type > 7 || (expected_type != 0 && code != expected_type))
        return Status::InvalidArgument("Unsupported WKB type or invalid MULTI child");
    const GeoDimensionPB dimensions[] = {GEO_DIMENSION_XY, GEO_DIMENSION_XYZ, GEO_DIMENSION_XYM, GEO_DIMENSION_XYZM};
    if (depth == 1) {
        _info.geometry_type = type;
        _info.dimension = dimensions[dimension];
    } else if (_info.dimension != dimensions[dimension]) {
        _info.dimension = GEO_DIMENSION_MIXED;
    }
    const uint32_t width = 2 + (dimension == 3 ? 2 : dimension != 0);
    if (type == 1) {
        const size_t start = _offset;
        RETURN_IF_ERROR(coordinates(1, width));
        // An all-NaN XY tuple represents POINT EMPTY. Z/M do not change emptiness.
        _offset = start;
        ASSIGN_OR_RETURN(auto x, number<uint64_t>(little));
        ASSIGN_OR_RETURN(auto y, number<uint64_t>(little));
        _info.empty &= std::isnan(std::bit_cast<double>(x)) && std::isnan(std::bit_cast<double>(y));
        _offset = start + width * sizeof(double);
        return Status::OK();
    }
    ASSIGN_OR_RETURN(auto count, number<uint32_t>(little));
    if (type == 2) {
        RETURN_IF_ERROR(coordinates(count, width));
        _info.empty &= count == 0;
    } else if (type == 3) {
        RETURN_IF_ERROR(components(count));
        if (count > (_bytes.size - _offset) / sizeof(uint32_t)) return Status::InvalidArgument("Truncated WKB rings");
        for (uint32_t i = 0; i < count; ++i) {
            ASSIGN_OR_RETURN(auto points, number<uint32_t>(little));
            RETURN_IF_ERROR(coordinates(points, width));
            _info.empty &= points == 0;
        }
    } else {
        // Every child has at least byte-order and type headers. Check before iterating.
        if (count > _limits.max_components - _info.components)
            return Status::InvalidArgument("WKB component limit exceeded");
        if (count > (_bytes.size - _offset) / 5) return Status::InvalidArgument("Truncated WKB children");
        for (uint32_t i = 0; i < count; ++i) {
            RETURN_IF_ERROR(geometry(depth + 1, type == 7 ? 0 : dimension * 1000 + type - 3));
        }
    }
    return Status::OK();
}

} // namespace

StatusOr<GeoWkbInfo> inspect_geo_wkb(Slice wkb, const GeoWkbLimits& limits) {
    return WkbReader(wkb, limits).read();
}

} // namespace starrocks
