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

#include "runtime/runtime_filter/runtime_filter_descriptor.h"

#include "exprs/expr_factory.h"

namespace starrocks {

Status RuntimeFilterBuildDescriptor::init(ObjectPool* pool, const TRuntimeFilterDescription& desc,
                                          RuntimeState* state) {
    _filter_id = desc.filter_id;
    _build_expr_order = desc.expr_order;
    _has_remote_targets = desc.has_remote_targets;

    if (desc.__isset.runtime_filter_merge_nodes) {
        _merge_nodes = desc.runtime_filter_merge_nodes;
    }
    _has_consumer = false;
    _join_mode = desc.build_join_mode;
    if (desc.__isset.plan_node_id_to_target_expr && desc.plan_node_id_to_target_expr.size() != 0) {
        _has_consumer = true;
    }
    if (!desc.__isset.build_expr) {
        return Status::NotFound("build_expr not found");
    }
    if (desc.__isset.sender_finst_id) {
        _sender_finst_id = desc.sender_finst_id;
    }
    if (desc.__isset.broadcast_grf_senders) {
        _broadcast_grf_senders.insert(desc.broadcast_grf_senders.begin(), desc.broadcast_grf_senders.end());
    }
    if (desc.__isset.broadcast_grf_destinations) {
        _broadcast_grf_destinations = desc.broadcast_grf_destinations;
    }
    if (desc.__isset.is_broad_cast_join_in_skew) {
        _is_broad_cast_in_skew = desc.is_broad_cast_join_in_skew;
    }
    if (desc.__isset.skew_shuffle_filter_id) {
        _skew_shuffle_filter_id = desc.skew_shuffle_filter_id;
    }
    if (desc.__isset.is_asc) {
        _is_asc = desc.is_asc;
    }
    if (desc.__isset.is_nulls_first) {
        _is_nulls_first = desc.is_nulls_first;
    }
    if (desc.__isset.limit) {
        _limit = desc.limit;
    }
    if (desc.__isset.filter_type) {
        _runtime_filter_type = desc.filter_type;
    }

    WithLayoutMixin::init(desc);
    RETURN_IF_ERROR(ExprFactory::create_expr_tree(pool, desc.build_expr, &_build_expr_ctx, state));
    return Status::OK();
}

} // namespace starrocks
