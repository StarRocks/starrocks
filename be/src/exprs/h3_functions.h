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

class H3Functions {
public:
    /**
     * @param: [lng DOUBLE, lat DOUBLE, resolution INT]
     * @return: BIGINT
     * Returns the H3 cell index (resolution 0-15) containing the given
     * WGS84 longitude/latitude point. Returns NULL on invalid coordinates
     * or resolution out of range [0, 15].
     */
    DEFINE_VECTORIZED_FN(geo_to_h3);

    /**
     * @param: [h3index BIGINT]
     * @return: DOUBLE
     * Returns the latitude (degrees) of the center of the H3 cell.
     * Returns NULL if the index is invalid.
     */
    DEFINE_VECTORIZED_FN(h3_to_geo_lat);

    /**
     * @param: [h3index BIGINT]
     * @return: DOUBLE
     * Returns the longitude (degrees) of the center of the H3 cell.
     * Returns NULL if the index is invalid.
     */
    DEFINE_VECTORIZED_FN(h3_to_geo_lng);

    /**
     * @param: [h3index BIGINT]
     * @return: BOOLEAN
     * Returns true if the index is a valid H3 cell index, false otherwise.
     * Never returns NULL.
     */
    DEFINE_VECTORIZED_FN(h3_is_valid);

    /**
     * @param: [h3index BIGINT]
     * @return: INT
     * Returns the resolution (0-15) of the H3 cell index.
     * Returns NULL if the index is invalid.
     */
    DEFINE_VECTORIZED_FN(h3_get_resolution);
};

} // namespace starrocks
