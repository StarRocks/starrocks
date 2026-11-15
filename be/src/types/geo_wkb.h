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

#include <cstddef>
#include <cstdint>

#include "base/string/slice.h"
#include "common/statusor.h"
#include "gen_cpp/types.pb.h"

namespace starrocks {

// Limits apply to one structural scan, independently of producer-validation metadata.
struct GeoWkbLimits {
    size_t max_bytes = 64 * 1024 * 1024;
    uint32_t max_depth = 64;
    uint32_t max_components = 1'000'000;
    uint32_t max_coordinates = 1'000'000;
};

// No pointers into the input and no decoded coordinate arrays. Safe to retain by value.
struct GeoWkbInfo {
    uint32_t geometry_type = 0; // OGC 1..7, without ISO Z/M offsets.
    GeoDimensionPB dimension = GEO_DIMENSION_UNKNOWN;
    uint32_t components = 0;  // Geometries plus polygon rings.
    uint32_t coordinates = 0; // Includes the NaN tuple representing POINT EMPTY.
    bool empty = true;
};

// Read ISO WKB without normalizing it. Supports both byte orders and XY/Z/M/ZM.
// This checks structure, not topology, CRS, coordinate ranges or compute capability.
// EWKB flags/SRID are deliberately not accepted here: conversion belongs to SQL boundaries.
// Allocation-free; recursion is capped at 64 even if a caller supplies a larger limit.
StatusOr<GeoWkbInfo> inspect_geo_wkb(Slice wkb, const GeoWkbLimits& limits = {});

} // namespace starrocks
