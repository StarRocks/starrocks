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
#include "column/vectorized_fwd.h"
#include "compute_env/query/scan_conjuncts_manager.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/descriptors.h"

namespace starrocks {

class TKeyRange;
class TScanRangeParams;
class RuntimeState;

// Materialize partition column candidate values described by `column_range` into a Column.
// Supports either a list of literal `list_values` or an inclusive integer/date `[begin_key, end_key]` range.
// Used by the backend-side dynamic partition pruning path in both the shared-nothing OlapScanNode and
// the shared-data LakeDataSourceProvider.
StatusOr<ColumnPtr> build_partition_col_values(const SlotDescriptor* slot_desc, const TKeyRange& column_range,
                                               ObjectPool* obj_pool, RuntimeState* state);

// Drop scan ranges whose partition values cannot satisfy any of the single-column
// `partition_conjunct_ctxs`. The conjunct contexts must have been prepared and opened by the
// caller. The tuple descriptor is used to resolve partition column names to slots. On success
// `pruned_scan_ranges` is populated with the retained scan ranges.
Status prune_scan_ranges_by_partition_conjuncts(RuntimeState* state, const TupleDescriptor* tuple_desc,
                                                const std::vector<ExprContext*>& partition_conjunct_ctxs,
                                                const std::vector<TScanRangeParams>& scan_ranges,
                                                std::vector<TScanRangeParams>* pruned_scan_ranges);

} // namespace starrocks
