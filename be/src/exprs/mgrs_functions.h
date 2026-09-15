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

#include "common/status.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"

namespace starrocks {

class MgrsFunctions {
public:
    /**
     * @param: [lng DOUBLE, lat DOUBLE] or [lng DOUBLE, lat DOUBLE, precision INT]
     * @return: VARCHAR
     * Encodes WGS84 (longitude, latitude) as an MGRS string.
     * precision controls digit pairs: 5=1m (default), 4=10m, 3=100m, 2=1km, 1=10km, 0=100km.
     * Returns NULL if lat ∉ [-80, 84], lon ∉ [-180, 180], or precision ∉ [0, 5].
     */
    DEFINE_VECTORIZED_FN(geo_to_mgrs); // handles both 2-arg and 3-arg forms

    /**
     * @param: [mgrs VARCHAR]
     * @return: DOUBLE
     * Decodes an MGRS string and returns the latitude of the grid-square centre.
     * Returns NULL for malformed input.
     */
    DEFINE_VECTORIZED_FN(mgrs_to_lat);

    /**
     * @param: [mgrs VARCHAR]
     * @return: DOUBLE
     * Decodes an MGRS string and returns the longitude of the grid-square centre.
     * Returns NULL for malformed input.
     */
    DEFINE_VECTORIZED_FN(mgrs_to_lng);
};

} // namespace starrocks
