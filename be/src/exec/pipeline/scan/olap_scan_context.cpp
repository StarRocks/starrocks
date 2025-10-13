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

#include "exec/pipeline/scan/olap_scan_context.h"

#include <memory>
#include <vector>

#include "exec/olap_scan_node.h"
#include "exec/pipeline/fragment_context.h"
#include "exprs/runtime_filter_bank.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet.h"

namespace starrocks::pipeline {

// more than one threads concurrently rewrite jit expression trees.
Status ConcurrentJitRewriter::rewrite(std::vector<ExprContext*>& expr_ctxs, ObjectPool* pool, bool enable_jit) {
    if (!enable_jit) {
        return Status::OK();
    }
    _barrier.arrive();
    for (int i = _id.fetch_add(1); i < expr_ctxs.size(); i = _id.fetch_add(1)) {
        auto st = expr_ctxs[i]->rewrite_jit_expr(pool);
        if (!st.ok()) {
            _errors++;
        }
    }
    _barrier.wait();
    // TODO(fzh): just generate 1 warnings
    if (_errors > 0) {
        return Status::JitCompileError("jit rewriter has errors");
    } else {
        return Status::OK();
    }
}

/// OlapScanContext.

const std::vector<ColumnAccessPathPtr>* OlapScanContext::column_access_paths() const {
    return &_scan_node->column_access_paths();
}

void OlapScanContext::attach_shared_input(int32_t operator_seq, int32_t source_index) {
    auto key = std::make_pair(operator_seq, source_index);
    VLOG_ROW << fmt::format("attach_shared_input ({}, {}), active {}", operator_seq, source_index,
                            _active_inputs.size());
    _num_active_inputs += _active_inputs.emplace(key).second;
}

void OlapScanContext::detach_shared_input(int32_t operator_seq, int32_t source_index) {
    auto key = std::make_pair(operator_seq, source_index);
    VLOG_ROW << fmt::format("detach_shared_input ({}, {}), remain {}", operator_seq, source_index,
                            _active_inputs.size());
    int erased = _active_inputs.erase(key);
    if (erased && _num_active_inputs.fetch_sub(1) == 1) {
        _active_inputs_empty = true;
    }
}

bool OlapScanContext::has_active_input() const {
    return !_active_inputs.empty();
}

BalancedChunkBuffer& OlapScanContext::get_shared_buffer() {
    return _chunk_buffer;
}

Status OlapScanContext::prepare(RuntimeState* state) {
    return Status::OK();
}

void OlapScanContext::close(RuntimeState* state) {
    _chunk_buffer.close();
    _rowset_release_guard.reset();
}

Status OlapScanContext::capture_tablet_rowsets(const std::vector<TInternalScanRange*>& olap_scan_ranges) {
    std::vector<std::vector<RowsetSharedPtr>> tablet_rowsets;
    tablet_rowsets.resize(olap_scan_ranges.size());
    _tablets.resize(olap_scan_ranges.size());
    for (int i = 0; i < olap_scan_ranges.size(); ++i) {
        auto* scan_range = olap_scan_ranges[i];

        ASSIGN_OR_RETURN(TabletSharedPtr tablet, OlapScanNode::get_tablet(scan_range));
        ASSIGN_OR_RETURN(tablet_rowsets[i], OlapScanNode::capture_tablet_rowsets(tablet, scan_range));

        VLOG(2) << "capture tablet rowsets: " << tablet->full_name() << ", rowsets: " << tablet_rowsets[i].size()
                << ", version: " << scan_range->version << ", gtid: " << scan_range->gtid;

        _tablets[i] = std::move(tablet);
    }
    _rowset_release_guard = MultiRowsetReleaseGuard(std::move(tablet_rowsets), adopt_acquire_t{});

    return Status::OK();
}
GlobalLateMaterilizationCtx::~GlobalLateMaterilizationCtx() {
    for (auto& guard : _rowset_release_guards) {
        guard->reset();
    }
}

void GlobalLateMaterilizationCtx::add_captured_tablet_rowsets(std::vector<std::vector<RowsetSharedPtr>> rowsets) {
    for (const auto& rowset_vec : rowsets) {
        Rowset::acquire_readers(rowset_vec);
    }
    std::unique_lock l(_mu);
    _rowset_release_guards.emplace_back(
            std::make_unique<MultiRowsetReleaseGuard>(std::move(rowsets), adopt_acquire_t{}));
}

Status OlapScanContext::parse_conjuncts(RuntimeState* state, const std::vector<ExprContext*>& runtime_in_filters,
                                        RuntimeFilterProbeCollector* runtime_bloom_filters, int32_t driver_sequence) {
    TEST_ERROR_POINT("OlapScanContext::parse_conjuncts");
    const TOlapScanNode& thrift_olap_scan_node = _scan_node->thrift_olap_scan_node();
    const TupleDescriptor* tuple_desc = state->desc_tbl().get_tuple_descriptor(thrift_olap_scan_node.tuple_id);

    // Get _conjunct_ctxs.
    _conjunct_ctxs = _scan_node->conjunct_ctxs();
    _conjunct_ctxs.insert(_conjunct_ctxs.end(), runtime_in_filters.begin(), runtime_in_filters.end());

    // eval_const_conjuncts.
    Status status;
    RETURN_IF_ERROR(ScanConjunctsManager::eval_const_conjuncts(_conjunct_ctxs, &status));
    if (!status.ok()) {
        return status;
    }

    // Init _conjuncts_manager.
    const TQueryOptions& query_options = state->query_options();
    int32_t max_scan_key_num;
    if (query_options.__isset.max_scan_key_num && query_options.max_scan_key_num > 0) {
        max_scan_key_num = query_options.max_scan_key_num;
    } else {
        max_scan_key_num = config::max_scan_key_num;
    }
    bool enable_column_expr_predicate = false;
    if (thrift_olap_scan_node.__isset.enable_column_expr_predicate) {
        enable_column_expr_predicate = thrift_olap_scan_node.enable_column_expr_predicate;
    }

    ScanConjunctsManagerOptions opts;
    opts.conjunct_ctxs_ptr = &_conjunct_ctxs;
    opts.tuple_desc = tuple_desc;
    opts.obj_pool = &_obj_pool;
    opts.key_column_names = &thrift_olap_scan_node.sort_key_column_names;
    opts.runtime_filters = runtime_bloom_filters;
    opts.runtime_state = state;
    opts.driver_sequence = driver_sequence;
    opts.scan_keys_unlimited = true;
    opts.max_scan_key_num = max_scan_key_num;
    opts.enable_column_expr_predicate = enable_column_expr_predicate;
    opts.pred_tree_params = state->fragment_ctx()->pred_tree_params();

    _conjuncts_manager = std::make_unique<ScanConjunctsManager>(std::move(opts));
    ScanConjunctsManager& cm = *_conjuncts_manager;

    // Parse conjuncts via _conjuncts_manager.
    RETURN_IF_ERROR(cm.parse_conjuncts());

    // Get key_ranges and not_push_down_conjuncts from _conjuncts_manager.
    RETURN_IF_ERROR(cm.get_key_ranges(&_key_ranges));
    cm.get_not_push_down_conjuncts(&_not_push_down_conjuncts);

    // rewrite after push down scan predicate, scan predicate should rewrite by local-dict
    RETURN_IF_ERROR(state->mutable_dict_optimize_parser()->rewrite_conjuncts(&_not_push_down_conjuncts));

    WARN_IF_ERROR(_jit_rewriter.rewrite(_not_push_down_conjuncts, &_obj_pool, state->is_jit_enabled()), "");

    return Status::OK();
}

/// OlapScanContextFactory.
OlapScanContextPtr OlapScanContextFactory::get_or_create(int32_t driver_sequence) {
    DCHECK_LT(driver_sequence, _dop);
    // ScanOperators sharing one morsel use the same context.
    int32_t idx = _shared_morsel_queue ? 0 : driver_sequence;
    DCHECK_LT(idx, _contexts.size());

    if (_contexts[idx] == nullptr) {
        _contexts[idx] = std::make_shared<OlapScanContext>(_scan_node, _scan_table_id, _dop, _shared_scan,
                                                           _chunk_buffer, _jit_rewriter);
    }
    return _contexts[idx];
}

} // namespace starrocks::pipeline
