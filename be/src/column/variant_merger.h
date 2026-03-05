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

#include "base/status.h"
#include "base/statusor.h"
#include "column/column.h"

namespace starrocks {

class VariantColumn;

// VariantMerger centralizes VariantColumn merge orchestration.
//
// Why this class exists:
// - Keep merge policy in one place instead of scattering checks across callers.
// - Make staged rollout easier: stage-2 now supports a minimal numeric
//   widening policy, and later stages can plug in broader arbitration here.
//
// Current scope:
// 1) Merge multiple variant columns by row concatenation.
// 2) For shredded columns, run an explicit schema pre-alignment check before
//    append to avoid silent incompatibility.
// 3) For overlapping shredded paths with numeric type conflicts, widen to a
//    common type (for example BIGINT + DOUBLE => DOUBLE), then align+append.
// 4) For unsupported conflicts, return NotSupported.
//
// Input / output contract:
// - merge(inputs):
//   input: non-empty list of VariantColumn objects.
//   output: a new VariantColumn containing rows of all inputs in order.
// - merge_into(dst, src):
//   input: destination and source VariantColumn.
//   output: appends src rows into dst when schemas are compatible.
//
// Full type arbitration policy is intentionally incremental and will continue
// to expand in later stages.
class VariantMerger {
public:
    // Reconcile overlapping shredded path type conflicts between dst and src.
    // Numeric/decimalv3 paths are widened when possible; unsupported conflicts are hoisted to TYPE_VARIANT.
    static Status arbitrate_type_conflicts(VariantColumn* dst, VariantColumn* src);

    // Merge input variant columns into one output VariantColumn.
    // Returns InvalidArgument for empty/non-variant inputs.
    // Returns NotSupported when shredded schema/type conflicts cannot be
    // reconciled by current policy.
    static StatusOr<MutableColumnPtr> merge(const Columns& inputs);

    // Append `src` into `dst` with explicit pre-alignment check.
    // Returns NotSupported when schema/type conflicts cannot be reconciled
    // by current policy.
    static Status merge_into(VariantColumn* dst, const VariantColumn& src);
};

} // namespace starrocks
