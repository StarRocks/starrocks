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

#include "exprs/function_helper.h"

namespace starrocks {

// Scalar functions over serialized Apache DataSketches Theta sketches
// (VARBINARY, compact form). Sketches use the standard Apache DataSketches C++
// compact serialization with the default hash seed, so they interoperate with
// any other Apache DataSketches implementation built on the same defaults.
class DsThetaFunctions {
public:
    // Per-row distinct-count estimate from a compact theta sketch. NULL in -> NULL out.
    DEFINE_VECTORIZED_FN(ds_theta_estimate);

    // Pairwise union of two compact theta sketches. NULL if either input is NULL.
    DEFINE_VECTORIZED_FN(ds_theta_union);

    // Pairwise intersection of two compact theta sketches. NULL if either input is NULL.
    DEFINE_VECTORIZED_FN(ds_theta_intersect);

    // Pairwise A-not-B over two compact theta sketches. NULL if either input is NULL.
    DEFINE_VECTORIZED_FN(ds_theta_a_not_b);
};

} // namespace starrocks
