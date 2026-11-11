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

#include "common/statusor.h"
#include "geo/wkb.h"

namespace starrocks {

enum class GeoMeasurementKind { AREA, LENGTH, PERIMETER };

struct GeoCentroidResult {
    bool empty = true;
    WkbCoordinate coordinate;
};

StatusOr<double> planar_measurement(const WkbGeometry& geometry, GeoMeasurementKind kind);
StatusOr<double> spherical_measurement(const WkbGeometry& geometry, GeoMeasurementKind kind);

StatusOr<GeoCentroidResult> planar_centroid(const WkbGeometry& geometry);
StatusOr<GeoCentroidResult> spherical_centroid(const WkbGeometry& geometry);

StatusOr<bool> planar_is_valid(const WkbGeometry& geometry);
StatusOr<bool> spherical_is_valid(const WkbGeometry& geometry);

} // namespace starrocks