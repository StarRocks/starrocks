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

#include <h3/h3api.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include "base/coding.h"
#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/const_column.h"
#include "column/geo_column.h"
#include "column/nullable_column.h"
#include "exprs/geo_functions.h"
#include "geo/geo_measurements.h"
#include "geo/geo_types.h"
#include "geo/wkb.h"
#include "types/geo_wkb.h"

namespace starrocks {
namespace {

constexpr double kRadiansPerDegree = 0.017453292519943295769236907684886;
constexpr double kDegreesPerRadian = 1.0 / kRadiansPerDegree;

Status h3_error(const char* name, H3Error code) {
    return Status::InvalidArgument(std::string(name) + ": " + describeH3Error(code));
}

StatusOr<H3Index> h3_cell(int64_t value) {
    if (value <= 0 || !isValidCell(static_cast<H3Index>(value))) {
        return Status::InvalidArgument("H3 requires a valid positive cell index");
    }
    return static_cast<H3Index>(value);
}

// H3's cell-index layout stores resolution in bits 52..55 and 15 three-bit
// child digits below bit 45. These helpers are only used after h3_cell validates
// the complete index, including pentagon and unused-digit constraints.
constexpr int kH3ResolutionOffset = 52;
constexpr H3Index kH3ResolutionMask = H3Index{15} << kH3ResolutionOffset;

int h3_cell_resolution(H3Index cell) {
    return static_cast<int>((cell & kH3ResolutionMask) >> kH3ResolutionOffset);
}

// Parent of a validated cell at a coarser or equal resolution: rewrite the
// resolution field and set the digits it no longer uses to 7. This is the result of
// cellToParent without its per-digit loop.
struct H3ParentMask {
    explicit H3ParentMask(int resolution)
            : resolution_bits(static_cast<H3Index>(resolution) << kH3ResolutionOffset),
              unused_digits((H3Index{1} << (3 * (15 - resolution))) - 1) {}

    bool is_coarser_or_equal(H3Index cell) const { return resolution_bits <= (cell & kH3ResolutionMask); }
    H3Index parent(H3Index cell) const { return (cell & ~kH3ResolutionMask) | resolution_bits | unused_digits; }

    H3Index resolution_bits;
    H3Index unused_digits;
};

Status validate_h3_resolution(int value) {
    if (value < 0 || value > 15) {
        return Status::InvalidArgument("H3 resolution must be between 0 and 15");
    }
    return Status::OK();
}

Status h3_array_size_check(int64_t count) {
    if (count < 0 || count > static_cast<int64_t>(std::numeric_limits<uint32_t>::max())) {
        return Status::InvalidArgument("H3 array offset limit exceeded");
    }
    return Status::OK();
}

struct H3GeoInput {
    const GeoColumn* data;
    const NullableColumn* nullable;
    bool constant;
    size_t index(size_t row) const { return constant ? 0 : row; }
    bool is_null(size_t row) const { return nullable != nullptr && nullable->is_null(index(row)); }
    Slice wkb(size_t row) const { return data->get_wkb(index(row)); }
};

StatusOr<H3GeoInput> h3_geo_input(const ColumnPtr& column) {
    const bool constant = column->is_constant();
    const Column* source = constant ? down_cast<const ConstColumn*>(column.get())->data_column().get() : column.get();
    const auto* nullable = source->is_nullable() ? down_cast<const NullableColumn*>(source) : nullptr;
    const auto* geo = down_cast<const GeoColumn*>(nullable ? nullable->data_column().get() : source);
    const auto& descriptor = geo->descriptor();
    const auto& type = descriptor.type;
    if (type.logical_type != GEO_LOGICAL_TYPE_GEOGRAPHY || type.coordinate_system != GEO_COORDINATE_SYSTEM_SPHERICAL ||
        type.edge_algorithm != GEO_EDGE_ALGORITHM_SPHERICAL || type.crs != "OGC:CRS84" ||
        (type.srid.has_value() && type.srid.value() != 4326) || descriptor.storage.encoding != GEO_ENCODING_WKB ||
        descriptor.storage.dimension != GEO_DIMENSION_XY) {
        return Status::NotSupported("H3 requires XY CRS84 GEOGRAPHY with spherical edges");
    }
    return H3GeoInput{geo, nullable, constant};
}

StatusOr<WkbGeometry> h3_parse_geo(const H3GeoInput& input, size_t row, bool polygon) {
    const Slice wkb = input.wkb(row);
    ASSIGN_OR_RETURN(const auto info, inspect_geo_wkb(wkb));
    if (info.dimension != GEO_DIMENSION_XY ||
        (polygon ? (info.geometry_type != 3 && info.geometry_type != 6) : info.geometry_type != 1)) {
        return Status::InvalidArgument(polygon ? "H3_PolygonToCells requires XY POLYGON or MULTIPOLYGON"
                                               : "H3_FromGeo requires XY POINT");
    }
    WkbGeometry geometry;
    RETURN_IF_ERROR(WkbCodec::parse_wkb(wkb, &geometry, WkbCoordinateSemantics::GEOGRAPHY_CRS84));
    return geometry;
}

template <typename T>
T read_wkb_number(const char* data, bool little_endian) {
    T value;
    std::memcpy(&value, data, sizeof(value));
    if (little_endian != (std::endian::native == std::endian::little)) value = std::byteswap(value);
    return value;
}

// An H3_FromGeo argument. POINT EMPTY (an all-NaN XY tuple) maps to NULL.
struct H3Point {
    WkbCoordinate coordinate{};
    bool empty = false;
};

// Decodes an XY POINT without building a WkbGeometry. inspect_geo_wkb accepts an XY
// POINT only as exactly 21 bytes with byte order 0 or 1 and type code 1, so every
// other payload is rejected here; inspect_geo_wkb supplies the structural diagnostic.
// Coordinate checks and messages match WkbCodec::parse_wkb with CRS84 semantics.
// Producer validation metadata is never trusted.
StatusOr<H3Point> h3_decode_point(Slice wkb) {
    constexpr size_t kTypeOffset = 1;
    constexpr size_t kXOffset = kTypeOffset + sizeof(uint32_t);
    constexpr size_t kYOffset = kXOffset + sizeof(double);
    constexpr size_t kPointWkbSize = kYOffset + sizeof(double);
    const bool known_order = wkb.size == kPointWkbSize && (wkb.data[0] == 0 || wkb.data[0] == 1);
    const bool little_endian = known_order && wkb.data[0] == 1;
    if (!known_order || read_wkb_number<uint32_t>(wkb.data + kTypeOffset, little_endian) != 1) {
        RETURN_IF_ERROR(inspect_geo_wkb(wkb).status());
        return Status::InvalidArgument("H3_FromGeo requires XY POINT");
    }
    const double x = std::bit_cast<double>(read_wkb_number<uint64_t>(wkb.data + kXOffset, little_endian));
    const double y = std::bit_cast<double>(read_wkb_number<uint64_t>(wkb.data + kYOffset, little_endian));
    if (std::isnan(x) && std::isnan(y)) return H3Point{{x, y}, true};
    if (!std::isfinite(x) || !std::isfinite(y)) {
        return Status::InvalidArgument("Invalid WKB: POINT coordinates must be finite or both NaN for EMPTY");
    }
    if (!is_valid_crs84_coordinate(x, y)) {
        return Status::InvalidArgument("GEOGRAPHY coordinates must be finite CRS84 longitude/latitude");
    }
    return H3Point{{x, y}, false};
}

struct H3PreparedGeo {
    bool constant = false;
    Status parse_status = Status::OK();
    std::optional<WkbGeometry> geometry;
};

StatusOr<const WkbGeometry*> h3_geometry(const H3GeoInput& input, size_t row, bool polygon,
                                         const H3PreparedGeo* prepared, std::optional<WkbGeometry>* local,
                                         WkbGeometry* varying) {
    if (prepared != nullptr && prepared->constant) {
        RETURN_IF_ERROR(prepared->parse_status);
        return &prepared->geometry.value();
    }
    if (input.constant) {
        if (!local->has_value()) {
            ASSIGN_OR_RETURN(auto parsed, h3_parse_geo(input, row, polygon));
            local->emplace(std::move(parsed));
        }
        return &local->value();
    }
    ASSIGN_OR_RETURN(*varying, h3_parse_geo(input, row, polygon));
    return varying;
}

struct H3ArrayBuilder {
    decltype(Int64Column::create()) values = Int64Column::create();
    decltype(UInt32Column::create()) offsets = UInt32Column::create();
    decltype(NullColumn::create()) nulls = NullColumn::create();

    H3ArrayBuilder() { offsets->append(0); }

    void reserve_rows(size_t rows) {
        offsets->reserve(rows + 1);
        nulls->reserve(rows);
    }

    H3Index* begin_row(size_t capacity, bool zero_fill) {
        _row_begin = values->size();
        values->resize_uninitialized(_row_begin + capacity);
        // H3 can write through the corresponding unsigned type of the BIGINT storage.
        static_assert(std::is_same_v<H3Index, std::make_unsigned_t<int64_t> >);
        auto* cells = reinterpret_cast<H3Index*>(values->get_data().data() + _row_begin);
        if (zero_fill) std::memset(cells, 0, capacity * sizeof(H3Index));
        return cells;
    }

    Status finish_row(size_t count) {
        const size_t end = _row_begin + count;
        // Disk compaction may reduce its upper-bound allocation to fit the offset limit.
        if (end > std::numeric_limits<uint32_t>::max()) {
            return Status::InvalidArgument("H3 array offset limit exceeded");
        }
        values->resize(end);
        nulls->append(0);
        offsets->append(static_cast<uint32_t>(end));
        return Status::OK();
    }

    void append_null() {
        nulls->append(1);
        offsets->append(static_cast<uint32_t>(values->size()));
    }

    // Valid H3 cell indexes have the sign bit clear, so their BIGINT bits are unchanged.
    Status append(const std::vector<H3Index>& cells) {
        static_assert(sizeof(H3Index) == sizeof(int64_t));
        if (cells.size() > std::numeric_limits<uint32_t>::max() - values->size()) {
            return Status::InvalidArgument("H3 array offset limit exceeded");
        }
        values->reserve(values->size() + cells.size());
        if (!cells.empty()) {
            values->append_numbers(cells.data(), cells.size() * sizeof(H3Index));
        }
        nulls->append(0);
        offsets->append(static_cast<uint32_t>(values->size()));
        return Status::OK();
    }

    ColumnPtr build(size_t size, bool constant) {
        auto elements = NullableColumn::create(values, NullColumn::create(values->size(), 0));
        auto array = ArrayColumn::create(std::move(elements), offsets);
        ColumnPtr result = NullableColumn::create(std::move(array), nulls);
        if (constant) return ConstColumn::create(std::move(result), size);
        return result;
    }

private:
    size_t _row_begin = 0;
};

} // namespace

Status GeoFunctions::h3_polygon_to_cells_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL || context == nullptr || context->get_num_args() == 0 ||
        !context->is_constant_column(0)) {
        return Status::OK();
    }
    const auto* type = context->get_arg_type(0);
    if (type == nullptr || type->type != TYPE_GEOGRAPHY) {
        return Status::OK();
    }
    auto state = std::make_unique<H3PreparedGeo>();
    state->constant = true;
    const auto& column = context->get_constant_column(0);
    if (column != nullptr && !column->only_null()) {
        auto input = h3_geo_input(column);
        if (!input.ok()) {
            state->parse_status = input.status();
        } else {
            auto parsed = h3_parse_geo(input.value(), 0, true);
            if (parsed.ok()) {
                state->geometry.emplace(std::move(parsed.value()));
            } else {
                state->parse_status = parsed.status();
            }
        }
    }
    context->set_function_state(scope, state.release());
    return Status::OK();
}

Status GeoFunctions::h3_polygon_to_cells_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        delete reinterpret_cast<H3PreparedGeo*>(context->get_function_state(scope));
        context->set_function_state(scope, nullptr);
    }
    return Status::OK();
}

StatusOr<ColumnPtr> GeoFunctions::h3_from_geo(FunctionContext*, const Columns& columns) {
    const size_t size = columns[0]->size();
    if (columns[0]->only_null() || columns[1]->only_null()) return ColumnHelper::create_const_null_column(size);
    ASSIGN_OR_RETURN(const auto input, h3_geo_input(columns[0]));
    ColumnViewer<TYPE_INT> resolutions(columns[1]);
    const bool constant = ColumnHelper::is_all_const(columns);
    const size_t rows = constant ? 1 : size;
    ColumnBuilder<TYPE_BIGINT> result(rows);
    for (size_t row = 0; row < rows; ++row) {
        if (input.is_null(row) || resolutions.is_null(row)) {
            result.append_null();
            continue;
        }
        const int resolution = resolutions.value(row);
        RETURN_IF_ERROR(validate_h3_resolution(resolution));
        ASSIGN_OR_RETURN(const H3Point point, h3_decode_point(input.wkb(row)));
        if (point.empty) {
            result.append_null();
            continue;
        }
        const LatLng latlng{point.coordinate.y * kRadiansPerDegree, point.coordinate.x * kRadiansPerDegree};
        H3Index cell = 0;
        const H3Error error = latLngToCell(&latlng, resolution, &cell);
        if (error != E_SUCCESS) return h3_error("H3_FromGeo", error);
        if (!isValidCell(cell) || cell > static_cast<H3Index>(std::numeric_limits<int64_t>::max())) {
            return Status::InternalError("H3_FromGeo returned invalid cell");
        }
        result.append(static_cast<int64_t>(cell));
    }
    ColumnPtr output = result.build(false);
    if (constant) return ConstColumn::create(std::move(output), size);
    return output;
}

StatusOr<ColumnPtr> GeoFunctions::h3_resolution(FunctionContext*, const Columns& columns) {
    const size_t size = columns[0]->size();
    if (columns[0]->only_null()) return ColumnHelper::create_const_null_column(size);
    ColumnViewer<TYPE_BIGINT> input(columns[0]);
    const bool constant = columns[0]->is_constant();
    if (!columns[0]->has_null()) {
        auto result = Int32Column::create();
        auto& values = result->get_data();
        values.resize(constant ? 1 : size);
        for (size_t row = 0; row < values.size(); ++row) {
            ASSIGN_OR_RETURN(const H3Index cell, h3_cell(input.value(row)));
            const int resolution = h3_cell_resolution(cell);
            values[row] = resolution;
        }
        if (constant) return ConstColumn::create(std::move(result), size);
        return result;
    }
    auto values = Int32Column::create();
    auto& value_data = values->get_data();
    value_data.resize(constant ? 1 : size);
    auto nulls = NullColumn::create(value_data.size(), 0);
    bool any_null = false;
    for (size_t row = 0; row < value_data.size(); ++row) {
        if (input.is_null(row)) {
            value_data[row] = 0;
            nulls->get_data()[row] = 1;
            any_null = true;
            continue;
        }
        ASSIGN_OR_RETURN(const H3Index cell, h3_cell(input.value(row)));
        value_data[row] = h3_cell_resolution(cell);
    }
    ColumnPtr output = std::move(values);
    if (any_null) output = NullableColumn::create(std::move(output), std::move(nulls));
    if (constant) return ConstColumn::create(std::move(output), size);
    return output;
}

StatusOr<ColumnPtr> GeoFunctions::h3_to_parent(FunctionContext*, const Columns& columns) {
    const size_t size = columns[0]->size();
    if (columns[0]->only_null() || columns[1]->only_null()) return ColumnHelper::create_const_null_column(size);
    ColumnViewer<TYPE_BIGINT> input(columns[0]);
    ColumnViewer<TYPE_INT> resolutions(columns[1]);
    const bool constant = ColumnHelper::is_all_const(columns);
    if (!columns[0]->has_null() && !columns[1]->has_null()) {
        auto result = Int64Column::create();
        auto& values = result->get_data();
        values.resize(constant ? 1 : size);
        const int constant_resolution = columns[1]->is_constant() && !values.empty() ? resolutions.value(0) : -1;
        if (constant_resolution >= 0 && constant_resolution <= 15) {
            const H3ParentMask parent(constant_resolution);
            for (size_t row = 0; row < values.size(); ++row) {
                ASSIGN_OR_RETURN(const H3Index cell, h3_cell(input.value(row)));
                if (!parent.is_coarser_or_equal(cell)) {
                    return Status::InvalidArgument("H3_ToParent resolution exceeds cell resolution");
                }
                values[row] = static_cast<int64_t>(parent.parent(cell));
            }
        } else {
            // Invalid constant targets use the ordinary path: cell validation
            // must still precede resolution errors, and empty inputs stay empty.
            for (size_t row = 0; row < values.size(); ++row) {
                ASSIGN_OR_RETURN(const H3Index cell, h3_cell(input.value(row)));
                const int resolution = resolutions.value(row);
                RETURN_IF_ERROR(validate_h3_resolution(resolution));
                const H3ParentMask parent(resolution);
                if (!parent.is_coarser_or_equal(cell)) {
                    return Status::InvalidArgument("H3_ToParent resolution exceeds cell resolution");
                }
                values[row] = static_cast<int64_t>(parent.parent(cell));
            }
        }
        if (constant) return ConstColumn::create(std::move(result), size);
        return result;
    }
    auto values = Int64Column::create();
    auto& value_data = values->get_data();
    value_data.resize(constant ? 1 : size);
    auto nulls = NullColumn::create(value_data.size(), 0);
    bool any_null = false;
    for (size_t row = 0; row < value_data.size(); ++row) {
        if (input.is_null(row) || resolutions.is_null(row)) {
            value_data[row] = 0;
            nulls->get_data()[row] = 1;
            any_null = true;
            continue;
        }
        ASSIGN_OR_RETURN(const H3Index cell, h3_cell(input.value(row)));
        const int resolution = resolutions.value(row);
        RETURN_IF_ERROR(validate_h3_resolution(resolution));
        const H3ParentMask parent(resolution);
        if (!parent.is_coarser_or_equal(cell)) {
            return Status::InvalidArgument("H3_ToParent resolution exceeds cell resolution");
        }
        value_data[row] = static_cast<int64_t>(parent.parent(cell));
    }
    ColumnPtr output = std::move(values);
    if (any_null) output = NullableColumn::create(std::move(output), std::move(nulls));
    if (constant) return ConstColumn::create(std::move(output), size);
    return output;
}

StatusOr<ColumnPtr> GeoFunctions::h3_grid_disk(FunctionContext*, const Columns& columns) {
    const size_t size = columns[0]->size();
    if (columns[0]->only_null() || columns[1]->only_null()) return ColumnHelper::create_const_null_column(size);
    ColumnViewer<TYPE_BIGINT> input(columns[0]);
    ColumnViewer<TYPE_INT> ks(columns[1]);
    const bool constant = ColumnHelper::is_all_const(columns);
    H3ArrayBuilder result;
    result.reserve_rows(constant ? 1 : size);
    for (size_t row = 0; row < (constant ? 1 : size); ++row) {
        if (input.is_null(row) || ks.is_null(row)) {
            result.append_null();
            continue;
        }
        ASSIGN_OR_RETURN(const H3Index cell, h3_cell(input.value(row)));
        const int k = ks.value(row);
        if (k < 0) return Status::InvalidArgument("H3_GridDisk requires non-negative k");
        int64_t count = 0;
        const H3Error size_error = maxGridDiskSize(k, &count);
        if (size_error != E_SUCCESS) return h3_error("H3_GridDisk", size_error);
        RETURN_IF_ERROR(h3_array_size_check(count));
        // gridDisk requires a zero-initialized output buffer.
        H3Index* cells = result.begin_row(static_cast<size_t>(count), true);
        const H3Error error = gridDisk(cell, k, cells);
        if (error != E_SUCCESS) return h3_error("H3_GridDisk", error);
        size_t written = 0;
        for (size_t i = 0; i < static_cast<size_t>(count); ++i) {
            const H3Index value = cells[i];
            if (value == H3_NULL) continue;
            cells[written++] = value;
        }
        RETURN_IF_ERROR(result.finish_row(written));
    }
    return result.build(size, constant);
}

StatusOr<ColumnPtr> GeoFunctions::h3_to_children(FunctionContext*, const Columns& columns) {
    const size_t size = columns[0]->size();
    if (columns[0]->only_null() || columns[1]->only_null()) return ColumnHelper::create_const_null_column(size);
    ColumnViewer<TYPE_BIGINT> input(columns[0]);
    ColumnViewer<TYPE_INT> resolutions(columns[1]);
    const bool constant = ColumnHelper::is_all_const(columns);
    H3ArrayBuilder result;
    result.reserve_rows(constant ? 1 : size);
    for (size_t row = 0; row < (constant ? 1 : size); ++row) {
        if (input.is_null(row) || resolutions.is_null(row)) {
            result.append_null();
            continue;
        }
        ASSIGN_OR_RETURN(const H3Index cell, h3_cell(input.value(row)));
        const int resolution = resolutions.value(row);
        RETURN_IF_ERROR(validate_h3_resolution(resolution));
        if (resolution < getResolution(cell))
            return Status::InvalidArgument("H3_ToChildren resolution is below cell resolution");
        int64_t count = 0;
        const H3Error size_error = cellToChildrenSize(cell, resolution, &count);
        if (size_error != E_SUCCESS) return h3_error("H3_ToChildren", size_error);
        RETURN_IF_ERROR(h3_array_size_check(count));
        H3Index* cells = result.begin_row(static_cast<size_t>(count), false);
        const H3Error error = cellToChildren(cell, resolution, cells);
        if (error != E_SUCCESS) return h3_error("H3_ToChildren", error);
        RETURN_IF_ERROR(result.finish_row(static_cast<size_t>(count)));
    }
    return result.build(size, constant);
}

namespace {
// Encodes a cell boundary as canonical little-endian WKB: an XY POLYGON with one ring,
// closed by copying the bytes of the first vertex (so its exact bits, including signed zero, repeat).
class H3BoundaryWkb {
public:
    H3BoundaryWkb() {
        _bytes[0] = 1;
        encode_fixed32_le(_bytes.data() + 1, static_cast<uint32_t>(WkbGeometryType::POLYGON));
        encode_fixed32_le(_bytes.data() + 1 + sizeof(uint32_t), 1);
    }

    Status encode(const CellBoundary& boundary) {
        if (boundary.numVerts < 3 || boundary.numVerts > MAX_CELL_BNDRY_VERTS) {
            return Status::InternalError("H3_ToBoundary returned invalid vertex count");
        }
        encode_fixed32_le(_bytes.data() + 1 + 2 * sizeof(uint32_t), boundary.numVerts + 1);
        for (int i = 0; i < boundary.numVerts; ++i) {
            const double x = boundary.verts[i].lng * kDegreesPerRadian;
            const double y = boundary.verts[i].lat * kDegreesPerRadian;
            if (!is_valid_crs84_coordinate(x, y)) {
                return Status::InternalError("H3_ToBoundary returned coordinates outside CRS84");
            }
            uint8_t* coordinate = vertex(i);
            encode_fixed64_le(coordinate, std::bit_cast<uint64_t>(x));
            encode_fixed64_le(coordinate + sizeof(double), std::bit_cast<uint64_t>(y));
        }
        std::memcpy(vertex(boundary.numVerts), vertex(0), kCoordinateSize);
        _size = kHeaderSize + (boundary.numVerts + 1) * kCoordinateSize;
        return Status::OK();
    }

    Slice slice() const { return Slice(reinterpret_cast<const char*>(_bytes.data()), _size); }

private:
    static constexpr size_t kHeaderSize = sizeof(uint8_t) + 3 * sizeof(uint32_t);
    static constexpr size_t kCoordinateSize = 2 * sizeof(double);

    uint8_t* vertex(int i) { return _bytes.data() + kHeaderSize + i * kCoordinateSize; }

    std::array<uint8_t, kHeaderSize + (MAX_CELL_BNDRY_VERTS + 1) * kCoordinateSize> _bytes{};
    size_t _size = 0;
};
} // namespace

StatusOr<ColumnPtr> GeoFunctions::h3_to_boundary(FunctionContext* context, const Columns& columns) {
    const size_t size = columns[0]->size();
    if (columns[0]->only_null()) return ColumnHelper::create_const_null_column(size);
    if (context == nullptr || context->get_return_type().type != TYPE_GEOGRAPHY ||
        !context->get_return_type().geo_type.has_value()) {
        return Status::NotSupported("H3_ToBoundary requires GEOGRAPHY return descriptor");
    }
    GeoColumnDescriptor descriptor{context->get_return_type().geo_type.value(),
                                   {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED}};
    const auto& type = descriptor.type;
    if (type.logical_type != GEO_LOGICAL_TYPE_GEOGRAPHY || type.coordinate_system != GEO_COORDINATE_SYSTEM_SPHERICAL ||
        type.edge_algorithm != GEO_EDGE_ALGORITHM_SPHERICAL || type.crs != "OGC:CRS84") {
        return Status::NotSupported("H3_ToBoundary requires CRS84 spherical GEOGRAPHY result");
    }
    ColumnViewer<TYPE_BIGINT> input(columns[0]);
    const bool constant = columns[0]->is_constant();
    const size_t rows = constant ? 1 : size;
    auto data = GeoColumn::create(std::move(descriptor));
    auto nulls = NullColumn::create();
    data->reserve(rows);
    auto& null_data = nulls->get_data();
    null_data.resize(rows, 0);
    H3BoundaryWkb wkb;
    for (size_t row = 0; row < rows; ++row) {
        if (input.is_null(row)) {
            data->append_default();
            null_data[row] = 1;
            continue;
        }
        ASSIGN_OR_RETURN(const H3Index cell, h3_cell(input.value(row)));
        CellBoundary boundary{};
        const H3Error error = cellToBoundary(cell, &boundary);
        if (error != E_SUCCESS) return h3_error("H3_ToBoundary", error);
        RETURN_IF_ERROR(wkb.encode(boundary));
        data->append_wkb(wkb.slice());
    }
    auto result = NullableColumn::create(std::move(data), std::move(nulls));
    if (constant) return ConstColumn::create(std::move(result), size);
    return result;
}

namespace {
class H3PolygonComponent {
public:
    static StatusOr<H3PolygonComponent> create(const WkbGeometry& geometry) {
        H3PolygonComponent out;
        out._vertices.reserve(geometry.rings.size());
        for (const auto& ring : geometry.rings) {
            if (ring.size() < 4 || ring.front() != ring.back()) {
                return Status::InvalidArgument("H3_PolygonToCells requires closed rings");
            }
            auto& vertices = out._vertices.emplace_back();
            vertices.reserve(ring.size() - 1);
            for (size_t i = 0; i + 1 < ring.size(); ++i) {
                vertices.push_back({ring[i].y * kRadiansPerDegree, ring[i].x * kRadiansPerDegree});
            }
            out._vertex_count += ring.size();
        }
        if (out._vertices.empty()) return Status::InvalidArgument("H3_PolygonToCells requires an exterior ring");
        out._holes.reserve(out._vertices.size() - 1);
        for (size_t i = 1; i < out._vertices.size(); ++i) {
            out._holes.push_back({static_cast<int>(out._vertices[i].size()), out._vertices[i].data()});
        }
        return out;
    }

    H3PolygonComponent(H3PolygonComponent&&) noexcept = default;
    H3PolygonComponent& operator=(H3PolygonComponent&&) noexcept = default;
    H3PolygonComponent(const H3PolygonComponent&) = delete;
    H3PolygonComponent& operator=(const H3PolygonComponent&) = delete;

    ::GeoPolygon h3_polygon() {
        return {{static_cast<int>(_vertices[0].size()), _vertices[0].data()},
                static_cast<int>(_holes.size()),
                _holes.empty() ? nullptr : _holes.data()};
    }

    size_t vertex_count() const { return _vertex_count; }
    const std::vector<std::vector<LatLng> >& vertices() const { return _vertices; }
    int64_t slots = 0;

private:
    H3PolygonComponent() = default;
    std::vector<std::vector<LatLng> > _vertices;
    std::vector< ::GeoLoop> _holes;
    size_t _vertex_count = 0;
};

constexpr size_t kH3RecursiveFillMinVertices = 128;
constexpr double kH3RecursiveFillMaxLatitude = 70 * kRadiansPerDegree;
constexpr double kH3RecursiveFillMaxLongitudeSpan = 90 * kRadiansPerDegree;

// The pinned H3 4.5 center iterator avoids tracing every edge of complex
// polygons. Keep the standard fill for small, polar, or wide/seam-crossing
// components, where its behavior and startup cost are preferable.
bool h3_use_recursive_fill(const H3PolygonComponent& component) {
    if (component.vertex_count() < kH3RecursiveFillMinVertices) return false;
    double west = std::numeric_limits<double>::max();
    double east = std::numeric_limits<double>::lowest();
    for (const auto& ring : component.vertices()) {
        for (const auto& vertex : ring) {
            if (std::abs(vertex.lat) > kH3RecursiveFillMaxLatitude) return false;
            west = std::min(west, vertex.lng);
            east = std::max(east, vertex.lng);
        }
    }
    return east - west <= kH3RecursiveFillMaxLongitudeSpan;
}

StatusOr<bool> h3_polygon_is_valid(const WkbGeometry& geometry) {
    if (geometry.type != WkbGeometryType::POLYGON || geometry.rings.size() != 1) {
        return spherical_is_valid(geometry);
    }
    const auto& ring = geometry.rings.front();
    GeoCoordinateList coordinates;
    coordinates.list.reserve(ring.size());
    for (const auto& coordinate : ring) {
        coordinates.add({coordinate.x, coordinate.y});
    }
    const bool valid = starrocks::GeoPolygon::is_valid_ring(coordinates);
    return valid;
}

struct H3PolygonPlan {
    std::vector<H3PolygonComponent> components;
    uint64_t slots = 0;
};

StatusOr<H3PolygonPlan> h3_plan_polygon(const WkbGeometry& geometry, size_t count, int resolution) {
    H3PolygonPlan plan;
    plan.components.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        const auto& polygon = geometry.type == WkbGeometryType::POLYGON ? geometry : geometry.children[i];
        if (polygon.empty) continue;
        ASSIGN_OR_RETURN(auto component, H3PolygonComponent::create(polygon));
        const ::GeoPolygon polygon_view = component.h3_polygon();
        const H3Error error = maxPolygonToCellsSize(&polygon_view, resolution, 0, &component.slots);
        if (error != E_SUCCESS) return h3_error("H3_PolygonToCells size", error);
        if (component.slots < 0 ||
            static_cast<uint64_t>(component.slots) > std::numeric_limits<uint32_t>::max() - plan.slots) {
            return Status::InvalidArgument("H3 array offset limit exceeded");
        }
        plan.slots += static_cast<uint64_t>(component.slots);
        RETURN_IF_ERROR(h3_array_size_check(static_cast<int64_t>(plan.slots)));
        plan.components.push_back(std::move(component));
    }
    return plan;
}

Status h3_fill_polygon(H3PolygonPlan& plan, int resolution, std::vector<H3Index>& cells) {
    const bool single_component = plan.components.size() == 1;
    if (!single_component) {
        cells.reserve(static_cast<size_t>(plan.slots));
    }
    for (auto& component : plan.components) {
        std::vector<H3Index> scratch;
        scratch.assign(static_cast<size_t>(component.slots), 0);
        const bool recursive = h3_use_recursive_fill(component);
        const ::GeoPolygon polygon_view = component.h3_polygon();
        H3Error error = recursive ? polygonToCellsExperimental(&polygon_view, resolution, CONTAINMENT_CENTER,
                                                               component.slots, scratch.data())
                                  : polygonToCells(&polygon_view, resolution, 0, scratch.data());
        if (recursive) {
            // Keep the allocated standard-fill slab and fall back rather
            // than increasing it if the iterator needs more output slots.
            if (error == E_MEMORY_BOUNDS) {
                std::fill(scratch.begin(), scratch.end(), H3_NULL);
                error = polygonToCells(&polygon_view, resolution, 0, scratch.data());
            }
        }
        if (error != E_SUCCESS) return h3_error("H3_PolygonToCells", error);
        if (single_component) {
            cells = std::move(scratch);
            continue;
        }
        for (H3Index cell : scratch) {
            if (cell == H3_NULL) continue;
            cells.push_back(cell);
        }
    }
    size_t write = 0;
    if (single_component) {
        // Both pinned H3 fills already emit unique cells for one component.
        // Compact their allocated slab in place and retain its output order.
        for (size_t i = 0; i < cells.size(); ++i) {
            const H3Index cell = cells[i];
            if (cell == H3_NULL) continue;
            cells[write++] = cell;
        }
    } else {
        std::unordered_set<H3Index> seen;
        seen.reserve(cells.size());
        for (size_t i = 0; i < cells.size(); ++i) {
            if (seen.insert(cells[i]).second) cells[write++] = cells[i];
        }
    }
    cells.resize(write);
    return Status::OK();
}
} // namespace

StatusOr<ColumnPtr> GeoFunctions::h3_polygon_to_cells(FunctionContext* context, const Columns& columns) {
    const size_t size = columns[0]->size();
    if (columns[0]->only_null() || columns[1]->only_null()) return ColumnHelper::create_const_null_column(size);
    ASSIGN_OR_RETURN(const auto input, h3_geo_input(columns[0]));
    ColumnViewer<TYPE_INT> resolutions(columns[1]);
    const bool constant = ColumnHelper::is_all_const(columns);
    H3ArrayBuilder result;
    const auto* prepared = context == nullptr ? nullptr
                                              : reinterpret_cast<const H3PreparedGeo*>(
                                                        context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    std::optional<WkbGeometry> local;
    for (size_t row = 0; row < (constant ? 1 : size); ++row) {
        if (input.is_null(row) || resolutions.is_null(row)) {
            result.append_null();
            continue;
        }
        const int res = resolutions.value(row);
        RETURN_IF_ERROR(validate_h3_resolution(res));
        WkbGeometry varying;
        ASSIGN_OR_RETURN(const auto* geometry, h3_geometry(input, row, true, prepared, &local, &varying));
        const size_t count = geometry->type == WkbGeometryType::POLYGON ? 1 : geometry->children.size();
        if (geometry->empty) {
            RETURN_IF_ERROR(result.append({}));
            continue;
        }
        ASSIGN_OR_RETURN(const bool valid, h3_polygon_is_valid(*geometry));
        if (!valid) return Status::InvalidArgument("H3_PolygonToCells requires valid spherical GEOGRAPHY topology");
        ASSIGN_OR_RETURN(auto plan, h3_plan_polygon(*geometry, count, res));
        std::vector<H3Index> cells;
        RETURN_IF_ERROR(h3_fill_polygon(plan, res, cells));
        RETURN_IF_ERROR(result.append(cells));
    }
    return result.build(size, constant);
}

} // namespace starrocks
