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

#include "connector/lake_connector.h"

#include <algorithm>
#include <mutex>
#include <vector>

#include "base/string/string_parser.hpp"
#include "base/testutil/sync_point.h"
#include "base/time/time.h"
#include "column/column_access_path.h"
#include "common/config_lake_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "common/config_starlet_fwd.h"
#include "common/config_storage_fwd.h"
#include "exec/connector_scan_node.h"
#include "exec/olap_scan_prepare.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/scan/glm_manager.h"
#include "exprs/chunk_predicate_evaluator.h"
#include "exprs/expr_factory.h"
#include "exprs/jsonpath.h"
#include "fs/fs.h"
#include "fs/key_cache.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/global_dict/fragment_dict_state.h"
#include "runtime/global_dict/parser.h"
#include "runtime/starrocks_metrics.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate_rewriter.h"
#include "storage/lake/rowset.h"
#include "storage/lake/table_schema_service.h"
#include "storage/lake/tablet.h"
#include "storage/predicate_parser.h"
#include "storage/projection_iterator.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/short_key_range_option.h"
#include "storage/runtime_range_pruner.hpp"
#include "storage/virtual_column_utils.h"

namespace starrocks::connector {

namespace {

const SlotDescriptor* find_slot_descriptor(const std::vector<SlotDescriptor*>* slots, SlotId slot_id) {
    if (slots == nullptr) {
        return nullptr;
    }
    for (const auto* slot : *slots) {
        if (slot != nullptr && slot->id() == slot_id) {
            return slot;
        }
    }
    return nullptr;
}

bool contains_runtime_filter(const UnarrivedRuntimeFilterList& filters, int32_t filter_id) {
    for (const auto* desc : filters.unarrived_runtime_filters) {
        if (desc != nullptr && desc->filter_id() == filter_id) {
            return true;
        }
    }
    return false;
}

void disable_split_context_prepared_state(const pipeline::ScanSplitContext* split_context) {
    const auto* lake_split_context = dynamic_cast<const pipeline::LakeSplitContext*>(split_context);
    if (lake_split_context == nullptr || lake_split_context->prepared_read_state == nullptr) {
        return;
    }
    lake_split_context->prepared_read_state->disable_segment_prepared_state.store(true, std::memory_order_release);
}

bool can_use_split_context_prepared_state(const pipeline::LakeSplitContext* split_context) {
    if (split_context == nullptr || split_context->prepared_read_state == nullptr) {
        return false;
    }
    return !split_context->prepared_read_state->disable_segment_prepared_state.load(std::memory_order_acquire);
}

int64_t rowid_range_option_span_size(const RowidRangeOptionPtr& rowid_range) {
    if (rowid_range == nullptr) {
        return 0;
    }

    int64_t rows = 0;
    for (const auto& rowset_entry : rowid_range->rowid_range_per_segment_per_rowset) {
        for (const auto& segment_entry : rowset_entry.second) {
            const auto& segment_split = segment_entry.second;
            if (segment_split.row_id_range != nullptr) {
                rows += static_cast<int64_t>(segment_split.row_id_range->span_size());
            }
        }
    }
    return rows;
}

RowidRangeOptionPtr trim_pending_rowid_range_by_prepared_range(const pipeline::LakeSplitContext* split_context,
                                                               const RowidRangeOptionPtr& rowid_range) {
    if (split_context == nullptr ||
        split_context->adaptive_task_source != pipeline::LakeSplitContext::AdaptiveTaskSource::PENDING_COARSE ||
        rowid_range == nullptr || split_context->prepared_read_state == nullptr ||
        split_context->prepared_segment_state == nullptr ||
        split_context->prepared_segment_state->execution_pruned_range == nullptr ||
        split_context->prepared_segment_state->lifecycle.load(std::memory_order_acquire) !=
                static_cast<uint32_t>(lake::PreparedSegmentReadState::Lifecycle::PREPARED)) {
        return rowid_range;
    }

    const auto& prepared_state = split_context->prepared_read_state;
    if (split_context->rowset_index >= prepared_state->rowsets.size() ||
        split_context->rowset_index >= prepared_state->rowset_segments.size() ||
        split_context->segment_index >= prepared_state->rowset_segments[split_context->rowset_index].size()) {
        return rowid_range;
    }

    const auto& rowset = prepared_state->rowsets[split_context->rowset_index];
    const auto& segment = prepared_state->rowset_segments[split_context->rowset_index][split_context->segment_index];
    if (rowset == nullptr || segment == nullptr) {
        return rowid_range;
    }

    auto segment_split = rowid_range->get_segment_rowid_range(rowset.get(), segment.get());
    if (segment_split.row_id_range == nullptr) {
        return nullptr;
    }

    SparseRange<> trimmed_range = *segment_split.row_id_range;
    trimmed_range &= *split_context->prepared_segment_state->execution_pruned_range;
    if (trimmed_range.span_size() == 0) {
        return nullptr;
    }

    auto trimmed = std::make_shared<RowidRangeOption>();
    trimmed->add(rowset.get(), segment.get(), std::make_shared<SparseRange<>>(std::move(trimmed_range)),
                 segment_split.is_first_split_of_segment);
    return trimmed;
}

} // namespace

LakeDataSource::LakeDataSource(const LakeDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider), _scan_range(scan_range.internal_scan_range) {}

LakeDataSource::~LakeDataSource() {
    _reader.reset();
    _predicate_free_pool.clear();
}

std::string LakeDataSource::name() const {
    return "LakeDataSource";
}

Status LakeDataSource::open(RuntimeState* state) {
    _runtime_state = state;
    _prepare_only_mode = false;
    if (_reuse_pending) {
        _reuse_pending = false;
        auto late_rf_reinit = detect_late_runtime_filter_reinit();
        if (late_rf_reinit.triggered) {
            if (late_rf_reinit.action == LateRuntimeFilterReinitDecision::Action::FULL_REINIT) {
                disable_split_context_prepared_state(_split_context);
                record_late_runtime_filter_reinit(late_rf_reinit);
                return reinit_reader_for_current_morsel_with_runtime_filters();
            }
            if (late_rf_reinit.action == LateRuntimeFilterReinitDecision::Action::REFRESH_RUNTIME_RANGE_PRUNER) {
                RETURN_IF_ERROR(refresh_runtime_range_pruner_for_stream_build_filters());
            }
        }
        if (can_fast_reopen_current_morsel()) {
            return fast_reopen_reader_for_current_morsel();
        }
        return open_reader_for_current_morsel();
    }
    const TLakeScanNode& thrift_lake_scan_node = _provider->_t_lake_scan_node;
    TupleDescriptor* tuple_desc = state->desc_tbl().get_tuple_descriptor(thrift_lake_scan_node.tuple_id);
    _slots = &tuple_desc->slots();

    _runtime_profile->add_info_string("Table", tuple_desc->table_desc()->name());
    if (thrift_lake_scan_node.__isset.rollup_name) {
        _runtime_profile->add_info_string("Rollup", thrift_lake_scan_node.rollup_name);
    }
    if (thrift_lake_scan_node.__isset.sql_predicates) {
        _runtime_profile->add_info_string("Predicates", thrift_lake_scan_node.sql_predicates);
    }

    init_counter(state);

    // init column access paths
    if (thrift_lake_scan_node.__isset.column_access_paths) {
        for (int i = 0; i < thrift_lake_scan_node.column_access_paths.size(); ++i) {
            auto st = ColumnAccessPath::create(thrift_lake_scan_node.column_access_paths[i], state, state->obj_pool());
            if (LIKELY(st.ok())) {
                _column_access_paths.emplace_back(std::move(st.value()));
            } else {
                LOG(WARNING) << "Failed to create column access path: "
                             << thrift_lake_scan_node.column_access_paths[i].type << "index: " << i
                             << ", error: " << st.status();
            }
        }
    }

    // eval const conjuncts
    RETURN_IF_ERROR(ScanConjunctsManager::eval_const_conjuncts(_conjunct_ctxs, &_status));
    DictOptimizeParser::rewrite_descriptor(state, _conjunct_ctxs, thrift_lake_scan_node.dict_string_id_to_int_ids,
                                           &(tuple_desc->decoded_slots()));

    RETURN_IF_ERROR(rebuild_scan_conjuncts());

    RETURN_IF_ERROR(init_tablet_reader(_runtime_state));
    refresh_runtime_filter_versions();
    return Status::OK();
}

bool LakeDataSource::enable_global_late_materialization() const {
    const TLakeScanNode& thrift_lake_scan_node = _provider->_t_lake_scan_node;
    return thrift_lake_scan_node.__isset.enable_global_late_materialization &&
           thrift_lake_scan_node.enable_global_late_materialization;
}

void LakeDataSource::refresh_glm_context() const {
    if (!enable_global_late_materialization()) {
        return;
    }

    DCHECK(_runtime_state != nullptr);
    DCHECK(_morsel != nullptr);

    int32_t scan_node_id = _provider->_scan_node->id();
    auto* glm_mgr = _runtime_state->query_ctx()->global_late_materialization_ctx_mgr();
    auto* obj_pool = _runtime_state->query_ctx()->object_pool();
    auto creator = [&]() {
        auto* ctx = obj_pool->add(new LakeScanLazyMaterializationContext());
        return ctx;
    };
    auto* glm_ctx = down_cast<LakeScanLazyMaterializationContext*>(glm_mgr->get_or_create_ctx(scan_node_id, creator));
    glm_ctx->set_scan_node(_provider->_t_lake_scan_node);
    int64_t version = strtoul(_scan_range.version.c_str(), nullptr, 10);
    glm_ctx->capture_rowsets(_scan_range.tablet_id, version, _morsel->rowsets());
}

void LakeDataSource::close(RuntimeState* state) {
    _reuse_pending = false;
    _prepare_only_mode = false;
    release_reader(state);
    _prepared_read_state.reset();
    _predicate_free_pool.clear();
}

Status LakeDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    if (_prepare_only_mode) {
        return Status::EndOfFile("segment prepare only");
    }
    ASSIGN_OR_RETURN(auto chunk_ptr,
                     ChunkHelper::new_chunk_pooled_checked(_prj_iter->output_schema(), _runtime_state->chunk_size()));
    chunk->reset(chunk_ptr);

    do {
        RETURN_IF_ERROR(state->check_mem_limit("read chunk from storage"));
        Status status = _prj_iter->get_next(chunk_ptr);
        // update counter when eof or error
        if (UNLIKELY(!status.ok())) {
            update_realtime_counter(chunk_ptr);
            return status;
        }

        TRY_CATCH_ALLOC_SCOPE_START()

        for (auto slot : _query_slots) {
            size_t column_index = chunk_ptr->schema()->get_field_index_by_name(slot->col_name());
            chunk_ptr->set_slot_id_to_index(slot->id(), column_index);
        }

        if (!_non_pushdown_pred_tree.empty()) {
            SCOPED_TIMER(_expr_filter_timer);
            size_t nrows = chunk_ptr->num_rows();
            _selection.resize(nrows);
            RETURN_IF_ERROR(_non_pushdown_pred_tree.evaluate(chunk_ptr, _selection.data(), 0, nrows));
            size_t after_rows = chunk_ptr->filter(_selection);
            COUNTER_UPDATE(_expr_filter_counter, nrows - after_rows);
            DCHECK_CHUNK(chunk_ptr);
        }
        if (!_not_push_down_conjuncts.empty()) {
            SCOPED_TIMER(_expr_filter_timer);
            size_t before_rows = chunk_ptr->num_rows();
            RETURN_IF_ERROR(ChunkPredicateEvaluator::eval_conjuncts(_not_push_down_conjuncts, chunk_ptr));
            size_t after_rows = chunk_ptr->num_rows();
            DCHECK_CHUNK(chunk_ptr);
            COUNTER_UPDATE(_expr_filter_counter, before_rows - after_rows);
        }
        TRY_CATCH_ALLOC_SCOPE_END()
    } while (chunk_ptr->num_rows() == 0);
    update_realtime_counter(chunk_ptr);
    return Status::OK();
}

void LakeDataSource::get_split_tasks(std::vector<pipeline::ScanSplitContextPtr>* split_tasks) {
    if (_reader == nullptr) {
        split_tasks->clear();
        return;
    }
    _reader->get_split_tasks(split_tasks);
}

bool LakeDataSource::can_reuse_with(const pipeline::ScanMorsel& morsel) const {
    return can_reuse_current_morsel(morsel);
}

Status LakeDataSource::reuse(RuntimeState* state, pipeline::ScanMorsel* morsel) {
    if (morsel == nullptr) {
        return Status::NotSupported("lake child morsel reuse is not supported");
    }

    _runtime_state = state;
    _morsel = morsel;
    _split_context = morsel->get_split_context();
    _reuse_pending = true;
    return Status::OK();
}

void LakeDataSource::release_for_reuse(RuntimeState* state) {
    _reuse_pending = false;
    _prepare_only_mode = false;
    if (!enable_local_child_morsel_reuse()) {
        release_reader(state);
        _prepared_read_state.reset();
        return;
    }

    if (_reader == nullptr) {
        return;
    }

    const auto stats = _reader->stats();
    update_counter(state, stats);
    record_current_adaptive_task_read_stats(stats);
    _released_raw_rows_read += stats.raw_rows_read;
    _released_bytes_read += stats.bytes_read;
    _released_cpu_time_spent_ns += stats.decompress_ns + stats.vec_cond_ns + stats.del_filter_ns;

    // Prepared-state reuse keeps the reader alive across sibling morsels.
    // Flush the current snapshot now so the next open starts from a clean slate
    // and profile counters don't lose earlier morsel contributions.
    *_reader->mutable_stats() = OlapReaderStatistics{};
}

void LakeDataSource::refresh_reuse_signature() {
    _reuse_signature = {};
    if (!enable_local_child_morsel_reuse() || !_reader_schema_inited || _morsel == nullptr) {
        return;
    }
    if (_morsel->from_version() != 0 && !config::enable_lake_scan_child_morsel_delta_rowsets_reuse) {
        return;
    }

    const auto* split_context = dynamic_cast<const pipeline::LakeSplitContext*>(_split_context);
    if (split_context == nullptr || !_provider->could_split_physically() || split_context->rowid_range == nullptr ||
        split_context->short_key_range != nullptr) {
        return;
    }

    const auto* scan_range = _morsel->get_scan_range();
    if (scan_range == nullptr || !scan_range->__isset.internal_scan_range) {
        return;
    }

    const auto& internal_scan_range = scan_range->internal_scan_range;
    _reuse_signature.valid = true;
    _reuse_signature.tablet_id = internal_scan_range.tablet_id;
    _reuse_signature.version = internal_scan_range.version;
    _reuse_signature.from_version = _morsel->from_version();
    _reuse_signature.rowsets_identity = &(_morsel->rowsets());
}

bool LakeDataSource::can_reuse_with_signature(const pipeline::ScanMorsel& morsel) const {
    if (!_reuse_signature.valid) {
        return false;
    }
    if (morsel.from_version() != 0 && !config::enable_lake_scan_child_morsel_delta_rowsets_reuse) {
        return false;
    }

    const auto* split_context = dynamic_cast<const pipeline::LakeSplitContext*>(morsel.get_split_context());
    if (split_context == nullptr || split_context->rowid_range == nullptr ||
        split_context->short_key_range != nullptr) {
        return false;
    }

    const auto* scan_range = morsel.get_scan_range();
    if (scan_range == nullptr || !scan_range->__isset.internal_scan_range) {
        return false;
    }

    const auto& internal_scan_range = scan_range->internal_scan_range;
    return internal_scan_range.tablet_id == _reuse_signature.tablet_id &&
           morsel.from_version() == _reuse_signature.from_version &&
           internal_scan_range.version == _reuse_signature.version &&
           &(morsel.rowsets()) == _reuse_signature.rowsets_identity;
}

bool LakeDataSource::enable_local_child_morsel_reuse() const {
    return config::enable_lake_scan_child_morsel_reuse && !has_reuse_blocker();
}

bool LakeDataSource::enable_local_child_prepared_state_reuse() const {
    return config::enable_lake_scan_child_morsel_prepared_state_reuse && !has_reuse_blocker();
}

void LakeDataSource::refresh_runtime_filter_versions() {
    _runtime_filter_versions.clear();
    if (_runtime_filters == nullptr || _runtime_filters->size() == 0) {
        return;
    }

    for (const auto& [filter_id, desc] : _runtime_filters->descriptors()) {
        const RuntimeFilter* rf = desc->runtime_filter(runtime_membership_filter_eval_context.driver_sequence);
        _runtime_filter_versions.emplace(filter_id, rf != nullptr ? rf->rf_version() : 0);
    }
}

Status LakeDataSource::refresh_runtime_range_pruner_for_stream_build_filters() {
    if (_tablet_schema == nullptr || _runtime_filters == nullptr || _conjuncts_manager == nullptr) {
        return Status::OK();
    }

    auto* parser = _obj_pool.add(new OlapPredicateParser(_tablet_schema));
    UnarrivedRuntimeFilterList filters = _conjuncts_manager->unarrived_runtime_filters();
    for (const auto& [filter_id, desc] : _runtime_filters->descriptors()) {
        if (!desc->is_stream_build_filter() || contains_runtime_filter(filters, filter_id) ||
            !desc->can_push_down_runtime_filter()) {
            continue;
        }

        SlotId slot_id = 0;
        if (!desc->is_probe_slot_ref(&slot_id)) {
            continue;
        }
        const auto* slot_desc = find_slot_descriptor(_slots, slot_id);
        if (slot_desc == nullptr || !parser->can_pushdown(slot_desc)) {
            continue;
        }
        filters.add_unarrived_rf(desc, slot_desc, runtime_membership_filter_eval_context.driver_sequence);
    }

    _params.runtime_range_pruner = RuntimeScanRangePruner(parser, filters);
    return Status::OK();
}

LakeDataSource::LateRuntimeFilterReinitDecision LakeDataSource::detect_late_runtime_filter_reinit() const {
    LateRuntimeFilterReinitDecision decision;
    if (!config::enable_lake_scan_child_morsel_reinit_on_late_runtime_filter || _runtime_filters == nullptr ||
        _runtime_filters->size() == 0) {
        return decision;
    }

    auto choose_action = [](const RuntimeFilterProbeDescriptor* desc) {
        return desc->is_stream_build_filter() ? LateRuntimeFilterReinitDecision::Action::REFRESH_RUNTIME_RANGE_PRUNER
                                              : LateRuntimeFilterReinitDecision::Action::FULL_REINIT;
    };
    auto consider_change = [&](LateRuntimeFilterReinitDecision::Reason reason, int32_t filter_id,
                               const RuntimeFilterProbeDescriptor* desc) {
        const auto action = choose_action(desc);
        if (action == LateRuntimeFilterReinitDecision::Action::FULL_REINIT) {
            decision.triggered = true;
            decision.action = action;
            decision.reason = reason;
            decision.filter_id = filter_id;
            return true;
        }
        if (!decision.triggered) {
            decision.triggered = true;
            decision.action = action;
            decision.reason = reason;
            decision.filter_id = filter_id;
        }
        return false;
    };

    const auto& descriptors = _runtime_filters->descriptors();
    for (const auto& [filter_id, desc] : descriptors) {
        if (_runtime_filter_versions.contains(filter_id)) {
            continue;
        }
        if (consider_change(LateRuntimeFilterReinitDecision::Reason::FILTER_SET_CHANGED, filter_id, desc)) {
            return decision;
        }
    }
    if (_runtime_filter_versions.size() != descriptors.size()) {
        decision.triggered = true;
        decision.action = LateRuntimeFilterReinitDecision::Action::FULL_REINIT;
        decision.reason = LateRuntimeFilterReinitDecision::Reason::FILTER_SET_CHANGED;
        return decision;
    }

    for (const auto& [filter_id, desc] : descriptors) {
        const RuntimeFilter* rf = desc->runtime_filter(runtime_membership_filter_eval_context.driver_sequence);
        const size_t current_version = rf != nullptr ? rf->rf_version() : 0;
        auto it = _runtime_filter_versions.find(filter_id);
        if (it == _runtime_filter_versions.end()) {
            continue;
        }

        if (it->second == 0 && current_version > 0) {
            if (consider_change(LateRuntimeFilterReinitDecision::Reason::NEWLY_ARRIVED, filter_id, desc)) {
                return decision;
            }
            continue;
        }
        if (current_version > it->second) {
            if (consider_change(LateRuntimeFilterReinitDecision::Reason::VERSION_CHANGED, filter_id, desc)) {
                return decision;
            }
        }
    }
    return decision;
}

void LakeDataSource::record_late_runtime_filter_reinit(const LateRuntimeFilterReinitDecision& decision) {
    COUNTER_UPDATE(_late_rf_reinit_counter, 1);
    switch (decision.reason) {
    case LateRuntimeFilterReinitDecision::Reason::FILTER_SET_CHANGED:
        COUNTER_UPDATE(_late_rf_reinit_filter_set_changed_counter, 1);
        break;
    case LateRuntimeFilterReinitDecision::Reason::NEWLY_ARRIVED:
        COUNTER_UPDATE(_late_rf_reinit_new_arrival_counter, 1);
        break;
    case LateRuntimeFilterReinitDecision::Reason::VERSION_CHANGED:
        COUNTER_UPDATE(_late_rf_reinit_version_changed_counter, 1);
        break;
    case LateRuntimeFilterReinitDecision::Reason::NONE:
        break;
    }
}

Status LakeDataSource::reset_reader_state_for_reinit() {
    release_reader(_runtime_state);
    _prepared_read_state.reset();
    _reuse_signature = {};
    _runtime_filter_versions.clear();
    _reader_schema_inited = false;
    _use_projection_iterator = false;
    _tablet_schema.reset();
    _params = TabletReaderParams{};
    _key_ranges.clear();
    _scanner_ranges.clear();
    _query_slots.clear();
    _unused_output_column_ids.clear();
    _predicate_free_pool.clear();
    _not_push_down_conjuncts.clear();
    _non_pushdown_pred_tree = PredicateTree();
    _conjuncts_manager.reset();
    return Status::OK();
}

Status LakeDataSource::rebuild_scan_conjuncts() {
    const TLakeScanNode& thrift_lake_scan_node = _provider->_t_lake_scan_node;
    TupleDescriptor* tuple_desc = _runtime_state->desc_tbl().get_tuple_descriptor(thrift_lake_scan_node.tuple_id);
    if (tuple_desc == nullptr) {
        return Status::InternalError("failed to get tuple descriptor for LakeDataSource");
    }
    _slots = &tuple_desc->slots();

    const TQueryOptions& query_options = _runtime_state->query_options();
    int32_t max_scan_key_num;
    if (query_options.__isset.max_scan_key_num && query_options.max_scan_key_num > 0) {
        max_scan_key_num = query_options.max_scan_key_num;
    } else {
        max_scan_key_num = config::max_scan_key_num;
    }
    bool enable_column_expr_predicate = false;
    if (thrift_lake_scan_node.__isset.enable_column_expr_predicate) {
        enable_column_expr_predicate = thrift_lake_scan_node.enable_column_expr_predicate;
    }

    ScanConjunctsManagerOptions opts;
    opts.conjunct_ctxs_ptr = &_conjunct_ctxs;
    opts.tuple_desc = tuple_desc;
    opts.obj_pool = &_obj_pool;
    opts.key_column_names = &thrift_lake_scan_node.sort_key_column_names;
    opts.runtime_filters = _runtime_filters;
    opts.runtime_state = _runtime_state;
    opts.scan_keys_unlimited = true;
    opts.max_scan_key_num = max_scan_key_num;
    opts.enable_column_expr_predicate = enable_column_expr_predicate;
    opts.pred_tree_params = _runtime_state->fragment_ctx()->pred_tree_params();
    opts.driver_sequence = runtime_membership_filter_eval_context.driver_sequence;

    _conjuncts_manager = std::make_unique<ScanConjunctsManager>(opts);
    RETURN_IF_ERROR(_conjuncts_manager->parse_conjuncts());
    RETURN_IF_ERROR(build_scan_range(_runtime_state));
    return Status::OK();
}

Status LakeDataSource::reinit_reader_for_current_morsel_with_runtime_filters() {
    _ignore_split_context_prepared_state_once = true;
    auto reset_ignore_flag = DeferOp([this]() { _ignore_split_context_prepared_state_once = false; });
    RETURN_IF_ERROR(reset_reader_state_for_reinit());
    RETURN_IF_ERROR(rebuild_scan_conjuncts());
    RETURN_IF_ERROR(init_tablet_reader(_runtime_state));
    refresh_runtime_filter_versions();
    return Status::OK();
}

bool LakeDataSource::should_attach_prepared_read_state() const {
    if (_prepared_read_state != nullptr) {
        return true;
    }
    return _split_context == nullptr && _provider->could_split_physically() &&
           config::enable_lake_index_pruned_physical_split;
}

Status LakeDataSource::get_tablet(const TInternalScanRange& scan_range) {
    int64_t tablet_id = scan_range.tablet_id;
    int64_t version = strtoul(scan_range.version.c_str(), nullptr, 10);
    auto tablet_manager = ExecEnv::GetInstance()->lake_tablet_manager();
    ASSIGN_OR_RETURN(_tablet, tablet_manager->get_tablet(tablet_id, version));
    auto& lake_scan_node = _provider->_t_lake_scan_node;
    if (lake_scan_node.__isset.schema_key) {
        const auto& t_schema_key = lake_scan_node.schema_key;
        TableSchemaKeyPB schema_key_pb;
        schema_key_pb.set_schema_id(t_schema_key.schema_id);
        schema_key_pb.set_db_id(t_schema_key.db_id);
        schema_key_pb.set_table_id(t_schema_key.table_id);
        ASSIGN_OR_RETURN(_tablet_schema, tablet_manager->table_schema_service()->get_schema_for_scan(
                                                 schema_key_pb, tablet_id, _runtime_state->query_id(),
                                                 _runtime_state->fragment_ctx()->fe_addr(), _tablet.metadata()));
    } else {
        // no table schema meta indicates FE has not been upgraded to use fast schema evolution v2,
        // so fallback to the old way to get schema from tablet metadata
        _tablet_schema = _tablet.get_schema();
    }
    return Status::OK();
}

// mapping a slot-column-id to schema-columnid
Status LakeDataSource::init_global_dicts(TabletReaderParams* params) {
    const TLakeScanNode& thrift_lake_scan_node = _provider->_t_lake_scan_node;
    const auto* fragment_dict_state = _runtime_state->fragment_dict_state();
    DCHECK(fragment_dict_state != nullptr);
    const auto& global_dict_map = fragment_dict_state->query_global_dicts();
    auto global_dict = _obj_pool.add(new ColumnIdToGlobalDictMap());
    // mapping column id to storage column ids
    const TupleDescriptor* tuple_desc = _runtime_state->desc_tbl().get_tuple_descriptor(thrift_lake_scan_node.tuple_id);
    for (auto slot : tuple_desc->slots()) {
        if (!slot->is_materialized()) {
            continue;
        }
        auto iter = global_dict_map.find(slot->id());
        if (iter != global_dict_map.end()) {
            auto& dict_map = iter->second.first;
            int32_t index = _tablet_schema->field_index(slot->col_name());
            DCHECK(index >= 0);
            global_dict->emplace(index, const_cast<GlobalDictMap*>(&dict_map));
        }
    }
    params->global_dictmaps = global_dict;
    return Status::OK();
}

Status LakeDataSource::init_unused_output_columns(const std::vector<std::string>& unused_output_columns) {
    for (const auto& col_name : unused_output_columns) {
        int32_t index = _tablet_schema->field_index(col_name);
        if (index < 0 && !is_virtual_column(col_name)) {
            std::stringstream ss;
            ss << "invalid field name: " << col_name;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        _unused_output_column_ids.insert(index);
    }
    _params.unused_output_column_ids = &_unused_output_column_ids;
    return Status::OK();
}

Status LakeDataSource::init_scanner_columns(std::vector<uint32_t>& scanner_columns,
                                            std::vector<uint32_t>& reader_columns) {
    for (auto slot : *_slots) {
        DCHECK(slot->is_materialized());
        int32_t index = _tablet_schema->field_index(slot->col_name());
        if (index < 0) {
            std::stringstream ss;
            ss << "invalid field name: " << slot->col_name();
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        scanner_columns.push_back(index);
        if (!_unused_output_column_ids.count(index)) {
            _query_slots.push_back(slot);
        }
    }
    // Put key columns before non-key columns, as the `MergeIterator` and `AggregateIterator`
    // required.
    std::sort(scanner_columns.begin(), scanner_columns.end());
    if (scanner_columns.empty()) {
        return Status::InternalError("failed to build storage scanner, no materialized slot!");
    }

    // Return columns
    if (_params.skip_aggregation) {
        reader_columns = scanner_columns;
    } else {
        for (size_t i = 0; i < _tablet_schema->num_key_columns(); i++) {
            reader_columns.push_back(i);
        }
        for (auto index : scanner_columns) {
            if (!_tablet_schema->column(index).is_key()) {
                reader_columns.push_back(index);
            }
        }
    }
    // Actually only the key columns need to be sorted by id, here we check all
    // for simplicity.
    DCHECK(std::is_sorted(reader_columns.begin(), reader_columns.end()));
    return Status::OK();
}

void LakeDataSource::decide_chunk_size(bool has_predicate) {
    if (!has_predicate && _read_limit != -1 && _read_limit < _runtime_state->chunk_size()) {
        // Improve for select * from table limit x, x is small
        _params.chunk_size = _read_limit;
    } else {
        _params.chunk_size = _runtime_state->chunk_size();
    }
}

Status LakeDataSource::init_reader_params(const std::vector<OlapScanRange*>& key_ranges) {
    const TLakeScanNode& thrift_lake_scan_node = _provider->_t_lake_scan_node;
    bool skip_aggregation = thrift_lake_scan_node.is_preaggregation;
    auto parser = _obj_pool.add(new OlapPredicateParser(_tablet_schema));
    _params.is_pipeline = true;
    _params.reader_type = READER_QUERY;
    _params.skip_aggregation = skip_aggregation;
    _params.profile = _runtime_profile;
    _params.runtime_state = _runtime_state;
    _params.use_page_cache =
            !config::disable_storage_page_cache && _scan_range.fill_data_cache && !_scan_range.skip_page_cache;
    _params.lake_io_opts.fill_data_cache = _scan_range.fill_data_cache;
    _params.lake_io_opts.skip_disk_cache = _scan_range.skip_disk_cache;

    if (thrift_lake_scan_node.__isset.sorted_by_keys_per_tablet) {
        _params.sorted_by_keys_per_tablet = thrift_lake_scan_node.sorted_by_keys_per_tablet;
    }

    _params.runtime_range_pruner = RuntimeScanRangePruner(parser, _conjuncts_manager->unarrived_runtime_filters());
    _params.lake_io_opts.cache_file_only = _runtime_state->query_options().__isset.enable_cache_select &&
                                           _runtime_state->query_options().enable_cache_select &&
                                           config::lake_cache_select_in_physical_way;
    _params.splitted_scan_rows = _provider->get_splitted_scan_rows();
    _params.scan_dop = _provider->get_scan_dop();
    _params.enable_lake_adaptive_split_morsel_queue = _provider->use_lake_adaptive_split_morsel_queue();

    if (thrift_lake_scan_node.__isset.enable_prune_column_after_index_filter) {
        _params.prune_column_after_index_filter = thrift_lake_scan_node.enable_prune_column_after_index_filter;
    }
    if (thrift_lake_scan_node.__isset.enable_gin_filter) {
        _params.enable_gin_filter = thrift_lake_scan_node.enable_gin_filter;
    }

    ASSIGN_OR_RETURN(auto pred_tree, _conjuncts_manager->get_predicate_tree(parser, _predicate_free_pool));
    _params.enable_join_runtime_filter_pushdown = _runtime_state->enable_join_runtime_filter_pushdown();
    if (_params.enable_join_runtime_filter_pushdown) {
        ASSIGN_OR_RETURN(_params.runtime_filter_preds,
                         _conjuncts_manager->get_runtime_filter_predicates(&_obj_pool, parser));
    }
    decide_chunk_size(!pred_tree.empty());
    _has_any_predicate = !pred_tree.empty();

    PredicateAndNode pushdown_pred_root;
    PredicateAndNode non_pushdown_pred_root;
    pred_tree.root().partition_copy([parser](const auto& node) { return parser->can_pushdown(node); },
                                    &pushdown_pred_root, &non_pushdown_pred_root);
    _params.pred_tree = PredicateTree::create(std::move(pushdown_pred_root));
    _non_pushdown_pred_tree = PredicateTree::create(std::move(non_pushdown_pred_root));

    for (const auto& cid : _non_pushdown_pred_tree.column_ids()) {
        _unused_output_column_ids.erase(cid);
    }

    std::vector<ExprContext*> not_pushdown_conjuncts;
    _conjuncts_manager->get_not_push_down_conjuncts(&not_pushdown_conjuncts);
    std::unordered_set<SlotId> conjuncts_slot_ids;
    for (auto* expr : not_pushdown_conjuncts) {
        expr->root()->for_each_slot_id([&conjuncts_slot_ids](SlotId id) { conjuncts_slot_ids.insert(id); });
    }
    for (auto& slot : *_slots) {
        if (conjuncts_slot_ids.contains(slot->id())) {
            int32_t fid = _tablet_schema->field_index(slot->col_name());
            _unused_output_column_ids.erase(fid);
        }
    }

    {
        GlobalDictPredicatesRewriter not_pushdown_predicate_rewriter(*_params.global_dictmaps);
        RETURN_IF_ERROR(not_pushdown_predicate_rewriter.rewrite_predicate(&_obj_pool, _non_pushdown_pred_tree));
    }

    // Range
    for (const auto& key_range : key_ranges) {
        if (key_range->begin_scan_range.size() == 1 && key_range->begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
            continue;
        }

        _params.range = key_range->begin_include ? TabletReaderParams::RangeStartOperation::GE
                                                 : TabletReaderParams::RangeStartOperation::GT;
        _params.end_range = key_range->end_include ? TabletReaderParams::RangeEndOperation::LE
                                                   : TabletReaderParams::RangeEndOperation::LT;

        _params.start_key.push_back(key_range->begin_scan_range);
        _params.end_key.push_back(key_range->end_scan_range);
    }

    return Status::OK();
}

Status LakeDataSource::init_tablet_reader(RuntimeState* runtime_state) {
    const TLakeScanNode& thrift_lake_scan_node = _provider->_t_lake_scan_node;
    // output columns of `this` OlapScanner, i.e, the final output columns of `get_chunk`.
    std::vector<uint32_t> scanner_columns;
    // columns fetched from |_reader|.
    std::vector<uint32_t> reader_columns;

    RETURN_IF_ERROR(get_tablet(_scan_range));

    refresh_glm_context();

    RETURN_IF_ERROR(_extend_schema_by_access_paths());
    ASSIGN_OR_RETURN(_tablet_schema, extend_schema_by_virtual_columns(_tablet_schema, *_slots));
    RETURN_IF_ERROR(init_global_dicts(&_params));
    RETURN_IF_ERROR(init_unused_output_columns(thrift_lake_scan_node.unused_output_column_name));
    RETURN_IF_ERROR(init_reader_params(_scanner_ranges));

    // Setup SST warmup callback for CACHE SELECT on PK tables
    if (_params.lake_io_opts.cache_file_only && _slots != nullptr &&
        has_all_pk_columns_selected(_tablet_schema.get(), *_slots)) {
        auto metadata = _tablet.metadata();
        auto* tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
        _params.lake_io_opts.sst_warmup_done = std::make_shared<std::atomic<bool>>(false);
        _params.lake_io_opts.sst_warmup_fn = [metadata, tablet_mgr]() -> Status {
            return warmup_pk_index_sst_files(metadata.get(), tablet_mgr);
        };
    }

    RETURN_IF_ERROR(init_scanner_columns(scanner_columns, reader_columns));
    _reader_schema = ChunkHelper::convert_schema(_tablet_schema, reader_columns);
    RETURN_IF_ERROR(init_column_access_paths(&_reader_schema));
    // will modify schema field, need to copy schema
    RETURN_IF_ERROR(prune_schema_by_access_paths(&_reader_schema));
    _output_schema = ChunkHelper::convert_schema(_tablet_schema, scanner_columns);
    _use_projection_iterator = reader_columns.size() != scanner_columns.size();
    _reader_schema_inited = true;

    if (!_not_push_down_conjuncts.empty() || !_non_pushdown_pred_tree.empty()) {
        _expr_filter_timer = ADD_TIMER(_runtime_profile, "ExprFilterTime");
        _expr_filter_counter = ADD_COUNTER(_runtime_profile, "ExprFilterRows", TUnit::UNIT);

        _non_pushdown_predicates_counter = ADD_COUNTER_SKIP_MERGE(_runtime_profile, "NonPushdownPredicates",
                                                                  TUnit::UNIT, TCounterMergeType::SKIP_ALL);
        COUNTER_SET(_non_pushdown_predicates_counter,
                    static_cast<int64_t>(_not_push_down_conjuncts.size() + _non_pushdown_pred_tree.size()));
        if (runtime_state->fragment_ctx()->pred_tree_params().enable_show_in_profile) {
            _runtime_profile->add_info_string(
                    "NonPushdownPredicateTree",
                    _non_pushdown_pred_tree.visit([](const auto& node) { return node.debug_string(); }));
        }
    }

    return open_reader_for_current_morsel();
}

bool LakeDataSource::can_reuse_current_morsel(const pipeline::ScanMorsel& morsel) const {
    return enable_local_child_morsel_reuse() && can_reuse_with_signature(morsel);
}

bool LakeDataSource::has_reuse_blocker() const {
    const bool enable_cache_select =
            _runtime_state != nullptr && _runtime_state->query_options().__isset.enable_cache_select &&
            _runtime_state->query_options().enable_cache_select && config::lake_cache_select_in_physical_way;
    return enable_cache_select;
}

bool LakeDataSource::can_fast_reopen_current_morsel() const {
    if (!config::enable_lake_scan_child_morsel_fast_reopen || _reader == nullptr ||
        !enable_local_child_morsel_reuse()) {
        return false;
    }

    const auto* split_context = dynamic_cast<const pipeline::LakeSplitContext*>(_split_context);
    if (split_context == nullptr || split_context->task_type != pipeline::LakeSplitContext::TaskType::PHYSICAL_SPLIT) {
        return false;
    }
    if (!_provider->could_split_physically() || split_context->rowid_range == nullptr ||
        split_context->short_key_range != nullptr) {
        return false;
    }
    return true;
}

Status LakeDataSource::open_reader_for_current_morsel() {
    if (!_reader_schema_inited) {
        return Status::InternalError("lake reader schema is not initialized");
    }
    _prepare_only_mode = false;
    // Reused child morsels do not re-enter init_tablet_reader(), so refresh the
    // GLM rowset snapshot here to keep row-locator lookup aligned with the
    // current morsel's rowset view.
    refresh_glm_context();

    _params.rowid_range_option.reset();
    _params.short_key_ranges_option.reset();
    _params.prepared_target_rowset_index = -1;
    _params.prepared_target_segment_index = -1;
    const auto* split_context = dynamic_cast<const pipeline::LakeSplitContext*>(_split_context);
    const bool has_prepared_state_candidate = split_context != nullptr &&
                                              split_context->prepared_read_state != nullptr &&
                                              split_context->prepared_segment_state != nullptr;
    if (has_prepared_state_candidate) {
        COUNTER_UPDATE(_lake_prepared_state_candidate_counter, 1);
        if (!enable_local_child_prepared_state_reuse()) {
            COUNTER_UPDATE(_lake_prepared_state_reject_disabled_counter, 1);
        }
    }
    bool can_reuse_prepared_state = !_ignore_split_context_prepared_state_once && split_context != nullptr &&
                                    can_use_split_context_prepared_state(split_context) &&
                                    enable_local_child_prepared_state_reuse();
    if (can_reuse_prepared_state && split_context->prepared_read_state != nullptr) {
        COUNTER_UPDATE(_lake_prepared_state_reuse_hit_counter, 1);
        _prepared_read_state = split_context->prepared_read_state;
    } else if (split_context != nullptr) {
        _prepared_read_state.reset();
    }
    if (_split_context != nullptr) {
        if (_provider->could_split_physically()) {
            _params.rowid_range_option = split_context->rowid_range;
        } else {
            _params.short_key_ranges_option = split_context->short_key_range;
        }
        if (can_reuse_prepared_state &&
            split_context->task_type == pipeline::LakeSplitContext::TaskType::PHYSICAL_SPLIT &&
            split_context->prepared_read_state != nullptr && split_context->prepared_segment_state != nullptr) {
            _params.prepared_target_rowset_index = static_cast<int64_t>(split_context->rowset_index);
            _params.prepared_target_segment_index = static_cast<int64_t>(split_context->segment_index);
            COUNTER_UPDATE(_lake_prepared_state_targeted_read_counter, 1);
        }
    }

    bool need_split = _provider->could_split() && _split_context == nullptr;
    if (need_split) {
        _params.plan_node_id = _morsel->get_plan_node_id();
        _params.scan_range = _morsel->get_scan_range();
    } else {
        _params.plan_node_id = 0;
        _params.scan_range = nullptr;
    }

    refresh_reuse_signature();

    if (_reader == nullptr) {
        ASSIGN_OR_RETURN(_reader, _tablet.new_reader(_reader_schema, need_split, _provider->could_split_physically(),
                                                     _morsel->rowsets(), _tablet_schema));
        if (should_attach_prepared_read_state()) {
            if (_prepared_read_state == nullptr) {
                _prepared_read_state = std::make_shared<lake::TabletReader::PreparedReadState>();
            }
            _reader->set_prepared_read_state(_prepared_read_state);
        } else {
            _prepared_read_state.reset();
            _reader->set_prepared_read_state(nullptr);
        }
        if (_use_projection_iterator) {
            _prj_iter = new_projection_iterator(_output_schema, _reader);
        } else {
            _prj_iter = _reader;
        }

        DCHECK(_params.global_dictmaps != nullptr);
        RETURN_IF_ERROR(_prj_iter->init_encoded_schema(*_params.global_dictmaps));
        RETURN_IF_ERROR(_prj_iter->init_output_schema(*_params.unused_output_column_ids));
        _reader->set_is_asc_hint(_provider->is_asc_hint());
        RETURN_IF_ERROR(_reader->prepare());
    }
    _reader->set_prepared_read_state(should_attach_prepared_read_state() ? _prepared_read_state : nullptr);

    if (split_context != nullptr && split_context->task_type == pipeline::LakeSplitContext::TaskType::SEGMENT_PREPARE) {
        RowidRangeOptionPtr local_rowid_range;
        int64_t prepare_segment_split_ns = 0;
        const auto prepare_stats_before = _reader->stats();
        {
            SCOPED_RAW_TIMER(&prepare_segment_split_ns);
            RETURN_IF_ERROR(_reader->prepare_segment_split_task(_params, split_context, &local_rowid_range));
        }
        if (split_context->adaptive_task_source == pipeline::LakeSplitContext::AdaptiveTaskSource::SEED_LOCAL) {
            const auto prepare_stats_after = _reader->stats();
            COUNTER_UPDATE(_lake_adaptive_seed_prepare_timer, prepare_segment_split_ns);
            if (split_context->prepared_segment_state != nullptr) {
                COUNTER_UPDATE(_lake_adaptive_seed_prepare_build_state_timer,
                               split_context->prepared_segment_state->adaptive_prepare_build_state_ns);
                COUNTER_UPDATE(_lake_adaptive_seed_prepare_init_options_timer,
                               split_context->prepared_segment_state->adaptive_prepare_init_options_ns);
                COUNTER_UPDATE(_lake_adaptive_seed_prepare_prune_range_timer,
                               split_context->prepared_segment_state->adaptive_prepare_prune_range_ns);
                COUNTER_UPDATE(_lake_adaptive_seed_prepare_seek_bounds_timer,
                               split_context->prepared_segment_state->adaptive_prepare_seek_bounds_ns);
                COUNTER_UPDATE(_lake_adaptive_seed_prepare_tablet_bounds_timer,
                               split_context->prepared_segment_state->adaptive_prepare_tablet_bounds_ns);
                COUNTER_UPDATE(_lake_adaptive_seed_prepare_schema_timer,
                               split_context->prepared_segment_state->adaptive_prepare_schema_ns);
                COUNTER_UPDATE(_lake_adaptive_seed_prepare_iterator_prune_timer,
                               split_context->prepared_segment_state->adaptive_prepare_iterator_prune_ns);
                COUNTER_UPDATE(_lake_adaptive_seed_prepare_split_tasks_timer,
                               split_context->prepared_segment_state->adaptive_prepare_split_tasks_ns);
            }
            COUNTER_UPDATE(_lake_adaptive_seed_prepare_column_iterator_init_timer,
                           prepare_stats_after.column_iterator_init_ns - prepare_stats_before.column_iterator_init_ns);
            COUNTER_UPDATE(
                    _lake_adaptive_seed_prepare_key_range_filter_timer,
                    prepare_stats_after.rows_key_range_filter_ns - prepare_stats_before.rows_key_range_filter_ns);
            COUNTER_UPDATE(
                    _lake_adaptive_seed_prepare_bitmap_index_filter_timer,
                    prepare_stats_after.bitmap_index_filter_timer - prepare_stats_before.bitmap_index_filter_timer);
            COUNTER_UPDATE(_lake_adaptive_seed_prepare_zone_map_filter_timer,
                           prepare_stats_after.zone_map_filter_ns - prepare_stats_before.zone_map_filter_ns);
            COUNTER_UPDATE(_lake_adaptive_seed_prepare_bloom_filter_timer,
                           prepare_stats_after.bf_filter_ns - prepare_stats_before.bf_filter_ns);
            COUNTER_UPDATE(_lake_adaptive_seed_prepare_gin_filter_timer,
                           prepare_stats_after.gin_index_filter_ns - prepare_stats_before.gin_index_filter_ns);
            COUNTER_UPDATE(_lake_adaptive_seed_prepare_stats_filtered_rows_counter,
                           prepare_stats_after.rows_stats_filtered - prepare_stats_before.rows_stats_filtered);
            COUNTER_UPDATE(_lake_adaptive_seed_prepare_bloom_filtered_rows_counter,
                           prepare_stats_after.rows_bf_filtered - prepare_stats_before.rows_bf_filtered);
            COUNTER_UPDATE(
                    _lake_adaptive_seed_prepare_bitmap_filtered_rows_counter,
                    prepare_stats_after.rows_bitmap_index_filtered - prepare_stats_before.rows_bitmap_index_filtered);
            COUNTER_UPDATE(_lake_adaptive_seed_prepare_gin_filtered_rows_counter,
                           prepare_stats_after.rows_gin_filtered - prepare_stats_before.rows_gin_filtered);
        }
        observe_adaptive_split_task(split_context, local_rowid_range);
        if (local_rowid_range == nullptr) {
            _current_adaptive_task_source = pipeline::LakeSplitContext::AdaptiveTaskSource::NONE;
            _prepare_only_mode = true;
            return Status::OK();
        }
        _params.rowid_range_option = std::move(local_rowid_range);
        _params.short_key_ranges_option.reset();
    } else if (split_context != nullptr &&
               split_context->task_type == pipeline::LakeSplitContext::TaskType::PHYSICAL_SPLIT) {
        observe_adaptive_split_task(split_context, _params.rowid_range_option);
        if (split_context->adaptive_task_source == pipeline::LakeSplitContext::AdaptiveTaskSource::PENDING_COARSE) {
            _params.rowid_range_option =
                    trim_pending_rowid_range_by_prepared_range(split_context, _params.rowid_range_option);
            if (_params.rowid_range_option == nullptr) {
                _current_adaptive_task_source = pipeline::LakeSplitContext::AdaptiveTaskSource::NONE;
                _prepare_only_mode = true;
                return Status::OK();
            }
        }
    }

    RETURN_IF_ERROR(_reader->open(_params));
    refresh_runtime_filter_versions();
    return Status::OK();
}

Status LakeDataSource::fast_reopen_reader_for_current_morsel() {
    DCHECK(can_fast_reopen_current_morsel());
    if (!_reader_schema_inited) {
        return Status::InternalError("lake reader schema is not initialized");
    }
    refresh_glm_context();

    const auto* split_context = down_cast<const pipeline::LakeSplitContext*>(_split_context);
    const bool has_prepared_state_candidate =
            split_context->prepared_read_state != nullptr && split_context->prepared_segment_state != nullptr;
    if (has_prepared_state_candidate) {
        COUNTER_UPDATE(_lake_prepared_state_candidate_counter, 1);
        if (!enable_local_child_prepared_state_reuse()) {
            COUNTER_UPDATE(_lake_prepared_state_reject_disabled_counter, 1);
        }
    }
    bool can_reuse_prepared_state = !_ignore_split_context_prepared_state_once &&
                                    can_use_split_context_prepared_state(split_context) &&
                                    enable_local_child_prepared_state_reuse();
    if (can_reuse_prepared_state && split_context->prepared_read_state != nullptr) {
        COUNTER_UPDATE(_lake_prepared_state_reuse_hit_counter, 1);
        _prepared_read_state = split_context->prepared_read_state;
    } else {
        _prepared_read_state.reset();
    }

    _prepare_only_mode = false;
    _params.rowid_range_option = split_context->rowid_range;
    _params.short_key_ranges_option.reset();
    _params.plan_node_id = 0;
    _params.scan_range = nullptr;
    _params.prepared_target_rowset_index = -1;
    _params.prepared_target_segment_index = -1;
    if (can_reuse_prepared_state && split_context->prepared_read_state != nullptr &&
        split_context->prepared_segment_state != nullptr) {
        _params.prepared_target_rowset_index = static_cast<int64_t>(split_context->rowset_index);
        _params.prepared_target_segment_index = static_cast<int64_t>(split_context->segment_index);
        COUNTER_UPDATE(_lake_prepared_state_targeted_read_counter, 1);
    }
    observe_adaptive_split_task(split_context, _params.rowid_range_option);
    if (split_context->adaptive_task_source == pipeline::LakeSplitContext::AdaptiveTaskSource::PENDING_COARSE) {
        _params.rowid_range_option =
                trim_pending_rowid_range_by_prepared_range(split_context, _params.rowid_range_option);
        if (_params.rowid_range_option == nullptr) {
            _current_adaptive_task_source = pipeline::LakeSplitContext::AdaptiveTaskSource::NONE;
            _prepare_only_mode = true;
            return Status::OK();
        }
    }

    refresh_reuse_signature();
    _reader->set_prepared_read_state(_prepared_read_state);
    RETURN_IF_ERROR(_reader->open(_params));
    refresh_runtime_filter_versions();
    return Status::OK();
}

void LakeDataSource::release_reader(RuntimeState* state) {
    if (_reader == nullptr) {
        return;
    }

    if (_prj_iter != nullptr && _prj_iter.get() != _reader.get()) {
        _prj_iter->close();
    }
    _prj_iter.reset();
    _reader->close();
    // Some reader-owned iterators flush their final scan stats during close().
    // Snapshot after close so profile counters include that last batch of metrics.
    const auto stats = _reader->stats();
    update_counter(state, stats);
    record_current_adaptive_task_read_stats(stats);
    _released_raw_rows_read += stats.raw_rows_read;
    _released_bytes_read += stats.bytes_read;
    _released_cpu_time_spent_ns += stats.decompress_ns + stats.vec_cond_ns + stats.del_filter_ns;
    _reader.reset();
}

// Inherit default value from JSON parent column for extended subcolumn.
// This method extracts the default value of a JSON subfield based on the access path
// and sets it to the column if extraction succeeds.
void LakeDataSource::_inherit_default_value_from_json(TabletColumn* column, const TabletColumn& root_column,
                                                      const ColumnAccessPath* path) {
    if (!root_column.has_default_value() || root_column.type() != TYPE_JSON) {
        return;
    }

    const std::string& json_default = root_column.default_value();
    auto json_value_or = JsonValue::parse_json_or_string(Slice(json_default));
    if (!json_value_or.ok()) {
        LOG(WARNING) << "Failed to parse JSON default value: " << json_value_or.status();
        return;
    }

    // Extract the sub path from linear path, e.g. "profile.level" -> "$.level"
    const std::string& linear = path->linear_path();
    const std::string& parent = path->path();
    std::string json_path_str;
    if (linear.size() > parent.size() && linear.compare(0, parent.size(), parent) == 0) {
        // linear = "profile.level", parent = "profile" -> sub = ".level" -> "$level"
        json_path_str = "$" + linear.substr(parent.size());
    } else {
        json_path_str = "$";
    }

    auto json_path_or = JsonPath::parse(Slice(json_path_str));
    if (!json_path_or.ok()) {
        LOG(WARNING) << "Failed to parse JSON path: " << json_path_str;
        return;
    }

    vpack::Builder builder;
    vpack::Slice extracted = JsonPath::extract(&json_value_or.value(), json_path_or.value(), &builder);
    if (extracted.isNone() || extracted.isNull()) {
        return;
    }

    const LogicalType value_type = column->type();
    std::string default_value_str;

    if (value_type == TYPE_VARCHAR || value_type == TYPE_CHAR) {
        if (extracted.isString()) {
            default_value_str = extracted.copyString();
        } else {
            vpack::Options options = vpack::Options::Defaults;
            options.singleLinePrettyPrint = true;
            default_value_str = extracted.toJson(&options);
        }
        column->set_default_value(default_value_str);
        return;
    }

    if (value_type == TYPE_BOOLEAN) {
        if (extracted.isString()) {
            vpack::ValueLength len;
            const char* str = extracted.getStringUnchecked(len);
            StringParser::ParseResult parse_result;
            auto as_int = StringParser::string_to_int<int32_t>(str, len, &parse_result);
            if (parse_result == StringParser::PARSE_SUCCESS) {
                default_value_str = (as_int != 0) ? "1" : "0";
            } else {
                bool b = StringParser::string_to_bool(str, len, &parse_result);
                if (parse_result != StringParser::PARSE_SUCCESS) {
                    return;
                }
                default_value_str = b ? "1" : "0";
            }
        } else if (extracted.isBool()) {
            default_value_str = extracted.getBool() ? "1" : "0";
        } else if (extracted.isNumber()) {
            vpack::Options options = vpack::Options::Defaults;
            options.singleLinePrettyPrint = true;
            default_value_str = extracted.toJson(&options);
        } else {
            return;
        }
        column->set_default_value(default_value_str);
        return;
    }

    if (extracted.isString()) {
        default_value_str = extracted.copyString();
    } else if (extracted.isBool()) {
        default_value_str = extracted.getBool() ? "1" : "0";
    } else if (extracted.isNumber()) {
        vpack::Options options = vpack::Options::Defaults;
        options.singleLinePrettyPrint = true;
        default_value_str = extracted.toJson(&options);
    } else {
        return;
    }

    column->set_default_value(default_value_str);
}

// Extend the schema fields based on the column access paths.
// This ensures that only the necessary subfields required by the query are retained in the schema.
Status LakeDataSource::_extend_schema_by_access_paths() {
    auto& access_paths = _column_access_paths;
    bool need_extend =
            std::any_of(access_paths.begin(), access_paths.end(), [](auto& path) { return path->is_extended(); });
    if (!need_extend) {
        return {};
    }

    TabletSchemaSPtr tmp_schema = TabletSchema::copy(*_tablet_schema);
    int field_number = _provider->next_uniq_id();
    for (auto& path : access_paths) {
        if (!path->is_extended()) {
            continue;
        }
        int root_column_index = _tablet_schema->field_index(path->path());
        RETURN_IF(root_column_index < 0, Status::RuntimeError("unknown access path: " + path->path()));

        LogicalType value_type = path->value_type().type;
        TabletColumn column;
        column.set_name(path->linear_path());
        column.set_unique_id(++field_number);
        column.set_type(value_type);
        column.set_length(path->value_type().len);
        column.set_is_nullable(true);
        int32_t root_uid = _tablet_schema->column(static_cast<size_t>(root_column_index)).unique_id();
        column.set_extended_info(std::make_unique<ExtendedColumnInfo>(path.get(), root_uid));

        // Inherit default value from parent column if exists
        const auto& root_column = _tablet_schema->column(static_cast<size_t>(root_column_index));
        _inherit_default_value_from_json(&column, root_column, path.get());

        // For UNIQUE/AGG tables, extended flat JSON subcolumns behave like value columns
        // and must carry a valid aggregation for pre-aggregation. Use REPLACE.
        auto keys_type = _tablet_schema->keys_type();
        if (keys_type == KeysType::UNIQUE_KEYS || keys_type == KeysType::AGG_KEYS) {
            column.set_aggregation(StorageAggregateType::STORAGE_AGGREGATE_REPLACE);
        }

        tmp_schema->append_column(column);
        VLOG(2) << "extend the access path column: " << path->linear_path();
    }
    _tablet_schema = tmp_schema;
    return {};
}

Status LakeDataSource::init_column_access_paths(Schema* schema) {
    // column access paths
    int64_t leaf_size = 0;
    std::vector<ColumnAccessPathPtr> new_one;
    for (const auto& path : _column_access_paths) {
        auto& root = path->path();
        int32_t index = _tablet_schema->field_index(root);
        auto field = schema->get_field_by_name(root);
        if (index >= 0 && field != nullptr) {
            auto res = path->convert_by_index(field.get(), index);
            // read whole data, doesn't effect query
            if (LIKELY(res.ok())) {
                new_one.emplace_back(std::move(res.value()));
                leaf_size += path->leaf_size();
            } else {
                LOG(WARNING) << "failed to convert column access path: " << res.status();
            }
        } else {
            LOG(WARNING) << "failed to find column in schema: " << root;
        }
    }
    // Preserve access paths referenced by extended columns even if not selected by pushdown
    {
        std::unordered_set<const ColumnAccessPath*> kept;
        kept.reserve(new_one.size());
        for (const auto& p : new_one) kept.insert(p.get());

        for (size_t i = 0; i < _tablet_schema->num_columns(); ++i) {
            const auto& col = _tablet_schema->column(i);
            if (!col.is_extended() || col.extended_info() == nullptr || col.extended_info()->access_path == nullptr) {
                continue;
            }
            const ColumnAccessPath* needed = col.extended_info()->access_path;
            if (kept.find(needed) != kept.end()) continue;
            for (auto& owned : _column_access_paths) {
                if (owned.get() == needed) {
                    new_one.emplace_back(std::move(owned));
                    kept.insert(needed);
                    break;
                }
            }
        }
    }
    _column_access_paths = std::move(new_one);
    _params.column_access_paths = &_column_access_paths;

    // update counter
    COUNTER_SET(_pushdown_access_paths_counter, leaf_size);
    return Status::OK();
}

Status prune_field_by_access_paths(Field* field, ColumnAccessPath* path) {
    if (path->children().size() < 1) {
        return Status::OK();
    }
    if (field->type()->type() == LogicalType::TYPE_ARRAY) {
        DCHECK_EQ(path->children().size(), 1);
        DCHECK_EQ(field->sub_fields().size(), 1);
        RETURN_IF_ERROR(prune_field_by_access_paths(&field->sub_fields()[0], path->children()[0].get()));
    } else if (field->type()->type() == LogicalType::TYPE_MAP) {
        DCHECK_EQ(path->children().size(), 1);
        auto child_path = path->children()[0].get();
        if (child_path->is_index() || child_path->is_all()) {
            DCHECK_EQ(field->sub_fields().size(), 2);
            RETURN_IF_ERROR(prune_field_by_access_paths(&field->sub_fields()[1], child_path));
        } else {
            return Status::OK();
        }
    } else if (field->type()->type() == LogicalType::TYPE_STRUCT) {
        std::unordered_map<std::string_view, ColumnAccessPath*> path_index;
        for (auto& child_path : path->children()) {
            path_index[child_path->path()] = child_path.get();
        }

        std::vector<Field> new_fields;
        for (auto& child_fields : field->sub_fields()) {
            auto iter = path_index.find(child_fields.name());
            if (iter != path_index.end()) {
                auto child_path = iter->second;
                RETURN_IF_ERROR(prune_field_by_access_paths(&child_fields, child_path));
                new_fields.emplace_back(child_fields);
            }
        }

        field->set_sub_fields(new_fields);
    }
    return Status::OK();
}

Status LakeDataSource::prune_schema_by_access_paths(Schema* schema) {
    if (_column_access_paths.empty()) {
        return Status::OK();
    }

    // schema
    for (auto& path : _column_access_paths) {
        if (path->is_from_predicate()) {
            continue;
        }
        auto& root = path->path();
        auto field = schema->get_field_by_name(root);
        if (field == nullptr) {
            LOG(WARNING) << "failed to find column in schema: " << root;
            continue;
        }
        // field maybe modified, so we need to deep copy
        auto new_field = std::make_shared<Field>(*field);
        schema->set_field_by_name(new_field, root);
        RETURN_IF_ERROR(prune_field_by_access_paths(new_field.get(), path.get()));
    }

    return Status::OK();
}

bool has_all_pk_columns_selected(const TabletSchema* tablet_schema, const std::vector<SlotDescriptor*>& slots) {
    if (tablet_schema == nullptr) {
        return false;
    }
    if (tablet_schema->keys_type() != KeysType::PRIMARY_KEYS) {
        return false;
    }
    size_t num_key_columns = tablet_schema->num_key_columns();
    if (num_key_columns == 0) {
        return false;
    }
    std::unordered_set<std::string_view> slot_names;
    slot_names.reserve(slots.size());
    for (const auto* slot : slots) {
        slot_names.emplace(slot->col_name());
    }
    for (size_t i = 0; i < num_key_columns; i++) {
        if (!slot_names.contains(tablet_schema->column(i).name())) {
            return false;
        }
    }
    return true;
}

Status warmup_pk_index_sst_files(const TabletMetadataPB* metadata, lake::TabletManager* tablet_mgr) {
#ifndef USE_STAROS
    return Status::OK();
#else
    if (metadata == nullptr) {
        return Status::OK();
    }

    // Check if the table is a PK table with cloud-native persistent index
    if (!metadata->enable_persistent_index() ||
        metadata->persistent_index_type() != PersistentIndexTypePB::CLOUD_NATIVE) {
        return Status::OK();
    }

    if (!metadata->has_sstable_meta() || metadata->sstable_meta().sstables_size() == 0) {
        return Status::OK();
    }

    int64_t tablet_id = metadata->id();
    size_t buf_size = config::starlet_fs_stream_buffer_size_bytes;
    if (buf_size <= 0) {
        buf_size = 1048576; // 1MB
    }

    const auto& sstable_meta = metadata->sstable_meta();
    VLOG(2) << "Warmup PK index SST files: tablet_id=" << tablet_id << " sst_count=" << sstable_meta.sstables_size();
    for (const auto& sstable_pb : sstable_meta.sstables()) {
        std::string sst_path = tablet_mgr->sst_location(tablet_id, sstable_pb.filename());
        RandomAccessFileOptions opts;
        if (!sstable_pb.encryption_meta().empty()) {
            ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(sstable_pb.encryption_meta()));
            opts.encryption_info = std::move(info);
        }
        ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(opts, sst_path));
        int64_t file_size = sstable_pb.filesize();
        if (file_size <= 0) {
            ASSIGN_OR_RETURN(file_size, rf->get_size());
        }
        for (int64_t offset = 0; offset < file_size;) {
            int64_t cur_size = std::min(static_cast<int64_t>(buf_size), file_size - offset);
            RETURN_IF_ERROR(rf->touch_cache(offset, cur_size));
            offset += cur_size;
        }
    }

    return Status::OK();
#endif // USE_STAROS
}

Status LakeDataSource::build_scan_range(RuntimeState* state) {
    // Get key_ranges and not_push_down_conjuncts from _conjuncts_manager.
    RETURN_IF_ERROR(_conjuncts_manager->get_key_ranges(&_key_ranges));
    _conjuncts_manager->get_not_push_down_conjuncts(&_not_push_down_conjuncts);
    auto* fragment_dict_state = state->fragment_dict_state();
    DCHECK(fragment_dict_state != nullptr);
    RETURN_IF_ERROR(
            fragment_dict_state->mutable_dict_optimize_parser()->rewrite_conjuncts(state, &_not_push_down_conjuncts));

    int scanners_per_tablet = 64;
    int num_ranges = _key_ranges.size();
    int ranges_per_scanner = std::max(1, num_ranges / scanners_per_tablet);
    for (int i = 0; i < num_ranges;) {
        _scanner_ranges.push_back(_key_ranges[i].get());
        i++;
        for (int j = 1;
             i < num_ranges && j < ranges_per_scanner && _key_ranges[i]->end_include == _key_ranges[i - 1]->end_include;
             ++j, ++i) {
            _scanner_ranges.push_back(_key_ranges[i].get());
        }
    }
    return Status::OK();
}

void LakeDataSource::init_counter(RuntimeState* state) {
    _bytes_read_counter = ADD_COUNTER(_runtime_profile, "BytesRead", TUnit::BYTES);
    _rows_read_counter = ADD_COUNTER(_runtime_profile, "RowsRead", TUnit::UNIT);
    _late_rf_reinit_counter = ADD_COUNTER(_runtime_profile, "LakeScanLateRuntimeFilterReinit", TUnit::UNIT);
    _late_rf_reinit_new_arrival_counter =
            ADD_COUNTER(_runtime_profile, "LakeScanLateRuntimeFilterReinitNewArrival", TUnit::UNIT);
    _late_rf_reinit_version_changed_counter =
            ADD_COUNTER(_runtime_profile, "LakeScanLateRuntimeFilterReinitVersionChanged", TUnit::UNIT);
    _late_rf_reinit_filter_set_changed_counter =
            ADD_COUNTER(_runtime_profile, "LakeScanLateRuntimeFilterReinitFilterSetChanged", TUnit::UNIT);
    _lake_prepared_state_candidate_counter =
            ADD_COUNTER(_runtime_profile, "LakeScanPreparedStateReuseCandidates", TUnit::UNIT);
    _lake_prepared_state_reuse_hit_counter =
            ADD_COUNTER(_runtime_profile, "LakeScanPreparedStateReuseHits", TUnit::UNIT);
    _lake_prepared_state_reject_disabled_counter =
            ADD_COUNTER(_runtime_profile, "LakeScanPreparedStateReuseRejectDisabled", TUnit::UNIT);
    _lake_prepared_state_reject_adaptive_pending_counter =
            ADD_COUNTER(_runtime_profile, "LakeScanPreparedStateReuseRejectAdaptivePending", TUnit::UNIT);
    _lake_prepared_state_targeted_read_counter =
            ADD_COUNTER(_runtime_profile, "LakeScanPreparedStateTargetedReads", TUnit::UNIT);
    _lake_adaptive_seed_task_counter = ADD_COUNTER(_runtime_profile, "LakeAdaptiveSeedTasks", TUnit::UNIT);
    _lake_adaptive_seed_issued_rows_counter = ADD_COUNTER(_runtime_profile, "LakeAdaptiveSeedIssuedRows", TUnit::UNIT);
    _lake_adaptive_seed_final_rows_counter = ADD_COUNTER(_runtime_profile, "LakeAdaptiveSeedFinalRows", TUnit::UNIT);
    _lake_adaptive_seed_estimated_fanout_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptiveSeedEstimatedFanout", TUnit::UNIT);
    _lake_adaptive_seed_prepare_timer = ADD_TIMER(_runtime_profile, "LakeAdaptiveSeedPrepareTime");
    _lake_adaptive_seed_prepare_build_state_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptiveSeedPrepareBuildStateTime");
    _lake_adaptive_seed_prepare_init_options_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptiveSeedPrepareInitOptionsTime");
    _lake_adaptive_seed_prepare_prune_range_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptiveSeedPreparePruneRangeTime");
    _lake_adaptive_seed_prepare_seek_bounds_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptiveSeedPrepareSeekBoundsTime");
    _lake_adaptive_seed_prepare_tablet_bounds_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptiveSeedPrepareTabletBoundsTime");
    _lake_adaptive_seed_prepare_schema_timer = ADD_TIMER(_runtime_profile, "LakeAdaptiveSeedPrepareSchemaTime");
    _lake_adaptive_seed_prepare_iterator_prune_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptiveSeedPrepareIteratorPruneTime");
    _lake_adaptive_seed_prepare_split_tasks_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptiveSeedPrepareSplitTasksTime");
    _lake_adaptive_seed_prepare_column_iterator_init_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptiveSeedPrepareColumnIteratorInitTime");
    _lake_adaptive_seed_prepare_key_range_filter_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptiveSeedPrepareKeyRangeFilterTime");
    _lake_adaptive_seed_prepare_bitmap_index_filter_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptiveSeedPrepareBitmapIndexFilterTime");
    _lake_adaptive_seed_prepare_zone_map_filter_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptiveSeedPrepareZoneMapFilterTime");
    _lake_adaptive_seed_prepare_bloom_filter_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptiveSeedPrepareBloomFilterTime");
    _lake_adaptive_seed_prepare_gin_filter_timer = ADD_TIMER(_runtime_profile, "LakeAdaptiveSeedPrepareGinFilterTime");
    _lake_adaptive_seed_prepare_stats_filtered_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptiveSeedPrepareStatsFilteredRows", TUnit::UNIT);
    _lake_adaptive_seed_prepare_bloom_filtered_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptiveSeedPrepareBloomFilteredRows", TUnit::UNIT);
    _lake_adaptive_seed_prepare_bitmap_filtered_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptiveSeedPrepareBitmapFilteredRows", TUnit::UNIT);
    _lake_adaptive_seed_prepare_gin_filtered_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptiveSeedPrepareGinFilteredRows", TUnit::UNIT);
    _lake_adaptive_seed_reader_timer = ADD_TIMER(_runtime_profile, "LakeAdaptiveSeedReaderTime");
    _lake_adaptive_seed_raw_rows_counter = ADD_COUNTER(_runtime_profile, "LakeAdaptiveSeedRawRowsRead", TUnit::UNIT);
    _lake_adaptive_seed_output_rows_counter = ADD_COUNTER(_runtime_profile, "LakeAdaptiveSeedOutputRows", TUnit::UNIT);
    _lake_adaptive_refined_task_counter = ADD_COUNTER(_runtime_profile, "LakeAdaptiveRefinedTasks", TUnit::UNIT);
    _lake_adaptive_refined_create_segment_iter_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptiveRefinedCreateSegmentIterTime");
    _lake_adaptive_refined_segment_init_timer = ADD_TIMER(_runtime_profile, "LakeAdaptiveRefinedSegmentInitTime");
    _lake_adaptive_refined_block_load_timer = ADD_TIMER(_runtime_profile, "LakeAdaptiveRefinedBlockLoadTime");
    _lake_adaptive_refined_column_iterator_init_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptiveRefinedColumnIteratorInitTime");
    _lake_adaptive_refined_key_range_filter_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptiveRefinedKeyRangeFilterTime");
    _lake_adaptive_refined_seek_bounds_cache_hit_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptiveRefinedSeekBoundsCacheHits", TUnit::UNIT);
    _lake_adaptive_refined_seek_bounds_cache_miss_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptiveRefinedSeekBoundsCacheMisses", TUnit::UNIT);
    _lake_adaptive_refined_bitmap_index_filter_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptiveRefinedBitmapIndexFilterTime");
    _lake_adaptive_refined_zone_map_filter_timer = ADD_TIMER(_runtime_profile, "LakeAdaptiveRefinedZoneMapFilterTime");
    _lake_adaptive_refined_bloom_filter_timer = ADD_TIMER(_runtime_profile, "LakeAdaptiveRefinedBloomFilterTime");
    _lake_adaptive_refined_gin_filter_timer = ADD_TIMER(_runtime_profile, "LakeAdaptiveRefinedGinFilterTime");
    _lake_adaptive_refined_stats_filtered_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptiveRefinedStatsFilteredRows", TUnit::UNIT);
    _lake_adaptive_refined_bloom_filtered_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptiveRefinedBloomFilteredRows", TUnit::UNIT);
    _lake_adaptive_refined_bitmap_filtered_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptiveRefinedBitmapFilteredRows", TUnit::UNIT);
    _lake_adaptive_refined_gin_filtered_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptiveRefinedGinFilteredRows", TUnit::UNIT);
    _lake_adaptive_refined_reader_timer = ADD_TIMER(_runtime_profile, "LakeAdaptiveRefinedReaderTime");
    _lake_adaptive_refined_wall_timer = ADD_TIMER(_runtime_profile, "LakeAdaptiveRefinedWallTime");
    _lake_adaptive_refined_start_delay_after_ready_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptiveRefinedStartDelayAfterReadyTime");
    _lake_adaptive_refined_start_delay_after_ready_task_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptiveRefinedStartDelayAfterReadyTasks", TUnit::UNIT);
    _lake_adaptive_refined_first_output_delay_after_ready_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptiveRefinedFirstOutputDelayAfterReadyTime");
    _lake_adaptive_refined_first_output_after_ready_task_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptiveRefinedFirstOutputAfterReadyTasks", TUnit::UNIT);
    _lake_adaptive_refined_raw_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptiveRefinedRawRowsRead", TUnit::UNIT);
    _lake_adaptive_refined_output_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptiveRefinedOutputRows", TUnit::UNIT);
    _lake_adaptive_pending_task_counter = ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingTasks", TUnit::UNIT);
    _lake_adaptive_pending_empty_task_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingEmptyTasks", TUnit::UNIT);
    _lake_adaptive_pending_issued_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingIssuedRows", TUnit::UNIT);
    _lake_adaptive_pending_useful_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingUsefulRows", TUnit::UNIT);
    _lake_adaptive_pending_wasted_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingWastedRows", TUnit::UNIT);
    _lake_adaptive_pending_open_before_prepared_task_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingOpenBeforePreparedTasks", TUnit::UNIT);
    _lake_adaptive_pending_open_before_prepared_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingOpenBeforePreparedRows", TUnit::UNIT);
    _lake_adaptive_pending_open_before_prepared_reader_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptivePendingOpenBeforePreparedReaderTime");
    _lake_adaptive_pending_open_before_prepared_block_load_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptivePendingOpenBeforePreparedBlockLoadTime");
    _lake_adaptive_pending_open_after_prepared_task_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingOpenAfterPreparedTasks", TUnit::UNIT);
    _lake_adaptive_pending_open_after_prepared_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingOpenAfterPreparedRows", TUnit::UNIT);
    _lake_adaptive_pending_open_after_prepared_reader_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptivePendingOpenAfterPreparedReaderTime");
    _lake_adaptive_pending_open_after_prepared_block_load_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptivePendingOpenAfterPreparedBlockLoadTime");
    _lake_adaptive_pending_open_after_seek_bounds_ready_task_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingOpenAfterSeekBoundsReadyTasks", TUnit::UNIT);
    _lake_adaptive_pending_open_before_seek_bounds_ready_task_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingOpenBeforeSeekBoundsReadyTasks", TUnit::UNIT);
    _lake_adaptive_pending_create_segment_iter_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptivePendingCreateSegmentIterTime");
    _lake_adaptive_pending_segment_init_timer = ADD_TIMER(_runtime_profile, "LakeAdaptivePendingSegmentInitTime");
    _lake_adaptive_pending_block_load_timer = ADD_TIMER(_runtime_profile, "LakeAdaptivePendingBlockLoadTime");
    _lake_adaptive_pending_column_iterator_init_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptivePendingColumnIteratorInitTime");
    _lake_adaptive_pending_key_range_filter_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptivePendingKeyRangeFilterTime");
    _lake_adaptive_pending_seek_bounds_cache_hit_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingSeekBoundsCacheHits", TUnit::UNIT);
    _lake_adaptive_pending_seek_bounds_cache_miss_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingSeekBoundsCacheMisses", TUnit::UNIT);
    _lake_adaptive_pending_bitmap_index_filter_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptivePendingBitmapIndexFilterTime");
    _lake_adaptive_pending_zone_map_filter_timer = ADD_TIMER(_runtime_profile, "LakeAdaptivePendingZoneMapFilterTime");
    _lake_adaptive_pending_bloom_filter_timer = ADD_TIMER(_runtime_profile, "LakeAdaptivePendingBloomFilterTime");
    _lake_adaptive_pending_gin_filter_timer = ADD_TIMER(_runtime_profile, "LakeAdaptivePendingGinFilterTime");
    _lake_adaptive_pending_stats_filtered_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingStatsFilteredRows", TUnit::UNIT);
    _lake_adaptive_pending_bloom_filtered_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingBloomFilteredRows", TUnit::UNIT);
    _lake_adaptive_pending_bitmap_filtered_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingBitmapFilteredRows", TUnit::UNIT);
    _lake_adaptive_pending_gin_filtered_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingGinFilteredRows", TUnit::UNIT);
    _lake_adaptive_pending_reader_timer = ADD_TIMER(_runtime_profile, "LakeAdaptivePendingReaderTime");
    _lake_adaptive_pending_wall_timer = ADD_TIMER(_runtime_profile, "LakeAdaptivePendingWallTime");
    _lake_adaptive_pending_before_refined_ready_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptivePendingBeforeRefinedReadyTime");
    _lake_adaptive_pending_after_refined_ready_timer =
            ADD_TIMER(_runtime_profile, "LakeAdaptivePendingAfterRefinedReadyTime");
    _lake_adaptive_pending_after_refined_ready_task_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingAfterRefinedReadyTasks", TUnit::UNIT);
    _lake_adaptive_pending_output_rows_before_refined_ready_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingOutputRowsBeforeRefinedReady", TUnit::UNIT);
    _lake_adaptive_pending_output_rows_after_refined_ready_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingOutputRowsAfterRefinedReady", TUnit::UNIT);
    _lake_adaptive_pending_chunks_before_refined_ready_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingChunksBeforeRefinedReady", TUnit::UNIT);
    _lake_adaptive_pending_chunks_after_refined_ready_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingChunksAfterRefinedReady", TUnit::UNIT);
    _lake_adaptive_pending_first_output_before_refined_ready_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingFirstOutputBeforeRefinedReadyTasks", TUnit::UNIT);
    _lake_adaptive_pending_first_output_after_refined_ready_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingFirstOutputAfterRefinedReadyTasks", TUnit::UNIT);
    _lake_adaptive_pending_running_at_refined_ready_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingRunningAtRefinedReady", TUnit::UNIT);
    _lake_adaptive_pending_raw_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingRawRowsRead", TUnit::UNIT);
    _lake_adaptive_pending_output_rows_counter =
            ADD_COUNTER(_runtime_profile, "LakeAdaptivePendingOutputRows", TUnit::UNIT);
    _create_seg_iter_timer = ADD_TIMER(_runtime_profile, "CreateSegmentIter");

    _read_compressed_counter = ADD_COUNTER(_runtime_profile, "CompressedBytesRead", TUnit::BYTES);
    _read_uncompressed_counter = ADD_COUNTER(_runtime_profile, "UncompressedBytesRead", TUnit::BYTES);

    _raw_rows_counter = ADD_COUNTER(_runtime_profile, "RawRowsRead", TUnit::UNIT);
    _pushdown_predicates_counter =
            ADD_COUNTER_SKIP_MERGE(_runtime_profile, "PushdownPredicates", TUnit::UNIT, TCounterMergeType::SKIP_ALL);
    _pushdown_access_paths_counter =
            ADD_COUNTER_SKIP_MERGE(_runtime_profile, "PushdownAccessPaths", TUnit::UNIT, TCounterMergeType::SKIP_ALL);

    _get_delvec_timer = ADD_TIMER(_runtime_profile, "GetDelVec");
    _get_delta_column_group_timer = ADD_TIMER(_runtime_profile, "GetDeltaColumnGroup");
    _read_pk_index_timer = ADD_TIMER(_runtime_profile, "ReadPKIndex");

    // SegmentInit
    const std::string segment_init_name = "SegmentInit";
    _seg_init_timer = ADD_TIMER(_runtime_profile, segment_init_name);
    _bi_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "BitmapIndexFilter", segment_init_name);
    _bi_filtered_counter = ADD_CHILD_COUNTER(_runtime_profile, "BitmapIndexFilterRows", TUnit::UNIT, segment_init_name);
    _bf_filtered_counter = ADD_CHILD_COUNTER(_runtime_profile, "BloomFilterFilterRows", TUnit::UNIT, segment_init_name);
    _seg_zm_filtered_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "SegmentZoneMapFilterRows", TUnit::UNIT, segment_init_name);
    _seg_metadata_filtered_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "SegmentMetadataFilterRows", TUnit::UNIT, segment_init_name);
    _segs_metadata_filtered_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "SegmentsMetadataFiltered", TUnit::UNIT, segment_init_name);
    _seg_rt_filtered_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "SegmentRuntimeZoneMapFilterRows", TUnit::UNIT, segment_init_name);
    _zm_filtered_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "ZoneMapIndexFilterRows", TUnit::UNIT, segment_init_name);
    _sk_filtered_counter = ADD_CHILD_COUNTER(_runtime_profile, "ShortKeyFilterRows", TUnit::UNIT, segment_init_name);
    _rows_after_sk_filtered_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "RemainingRowsAfterShortKeyFilter", TUnit::UNIT, segment_init_name);
    _rows_key_range_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "ShortKeyRangeNumber", TUnit::UNIT, segment_init_name);
    _column_iterator_init_timer = ADD_CHILD_TIMER(_runtime_profile, "ColumnIteratorInit", segment_init_name);
    _bitmap_index_iterator_init_timer = ADD_CHILD_TIMER(_runtime_profile, "BitmapIndexIteratorInit", segment_init_name);
    _zone_map_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "ZoneMapIndexFilter", segment_init_name);
    _rows_key_range_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "ShortKeyFilter", segment_init_name);
    _bf_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "BloomFilterFilter", segment_init_name);

    const std::string gin_filter_name = "GinFilter";
    _gin_filtered_timer = ADD_CHILD_TIMER(_runtime_profile, gin_filter_name, segment_init_name);
    _gin_filtered_counter = ADD_CHILD_COUNTER(_runtime_profile, "GinFilterRows", TUnit::UNIT, gin_filter_name);
    _gin_prefix_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "GinPrefixFilter", gin_filter_name);
    _gin_ngram_dict_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "GinNgramDictFilter", gin_filter_name);
    _gin_predicate_dict_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "GinDictFilter", gin_filter_name);
    _gin_dict_counter = ADD_CHILD_COUNTER(_runtime_profile, "GinDictNum", TUnit::UNIT, gin_filter_name);
    _gin_ngram_dict_counter = ADD_CHILD_COUNTER(_runtime_profile, "GinNGramDictNum", TUnit::UNIT, gin_filter_name);
    _gin_ngram_dict_filtered_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "GinNGramFilteredDictNum", TUnit::UNIT, gin_filter_name);
    _gin_predicate_dict_filtered_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "GinPredicateFilteredDictNum", TUnit::UNIT, gin_filter_name);

    // SegmentRead
    const std::string segment_read_name = "SegmentRead";
    _block_load_timer = ADD_TIMER(_runtime_profile, segment_read_name);
    _block_fetch_timer = ADD_CHILD_TIMER(_runtime_profile, "BlockFetch", segment_read_name);
    _block_load_counter = ADD_CHILD_COUNTER(_runtime_profile, "BlockFetchCount", TUnit::UNIT, segment_read_name);
    _block_seek_timer = ADD_CHILD_TIMER(_runtime_profile, "BlockSeek", segment_read_name);
    _block_seek_counter = ADD_CHILD_COUNTER(_runtime_profile, "BlockSeekCount", TUnit::UNIT, segment_read_name);
    _pred_filter_timer = ADD_CHILD_TIMER(_runtime_profile, "PredFilter", segment_read_name);
    _pred_filter_counter = ADD_CHILD_COUNTER(_runtime_profile, "PredFilterRows", TUnit::UNIT, segment_read_name);
    _rf_pred_filter_timer = ADD_TIMER(_runtime_profile, "RuntimeFilterEvalTime");
    _rf_pred_input_rows = ADD_COUNTER(_runtime_profile, "RuntimeFilterInputRows", TUnit::UNIT);
    _rf_pred_output_rows = ADD_COUNTER(_runtime_profile, "RuntimeFilterOutputRows", TUnit::UNIT);
    _del_vec_filter_counter = ADD_CHILD_COUNTER(_runtime_profile, "DelVecFilterRows", TUnit::UNIT, segment_read_name);
    _chunk_copy_timer = ADD_CHILD_TIMER(_runtime_profile, "ChunkCopy", segment_read_name);
    _decompress_timer = ADD_CHILD_TIMER(_runtime_profile, "DecompressT", segment_read_name);
    _rowsets_read_count = ADD_CHILD_COUNTER(_runtime_profile, "RowsetsReadCount", TUnit::UNIT, segment_read_name);
    _segments_read_count = ADD_CHILD_COUNTER(_runtime_profile, "SegmentsReadCount", TUnit::UNIT, segment_read_name);
    _total_columns_data_page_count =
            ADD_CHILD_COUNTER(_runtime_profile, "TotalColumnsDataPageCount", TUnit::UNIT, segment_read_name);

    // IO statistics
    // IOTime
    _io_timer = ADD_TIMER(_runtime_profile, "IOTime");
    const std::string io_statistics_name = "IOStatistics";
    ADD_COUNTER(_runtime_profile, io_statistics_name, TUnit::NONE);
    // Page count
    _pages_count_memory_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "PagesCountMemory", TUnit::UNIT, io_statistics_name);
    _pages_count_local_disk_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "PagesCountLocalDisk", TUnit::UNIT, io_statistics_name);
    _pages_count_remote_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "PagesCountRemote", TUnit::UNIT, io_statistics_name);
    _pages_count_total_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "PagesCountTotal", TUnit::UNIT, io_statistics_name);
    // Compressed bytes read
    _compressed_bytes_read_local_disk_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "CompressedBytesReadLocalDisk", TUnit::BYTES, io_statistics_name);
    _compressed_bytes_read_remote_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "CompressedBytesReadRemote", TUnit::BYTES, io_statistics_name);
    _compressed_bytes_read_total_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "CompressedBytesReadTotal", TUnit::BYTES, io_statistics_name);
    _compressed_bytes_read_request_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "CompressedBytesReadRequest", TUnit::BYTES, io_statistics_name);
    // IO count
    _io_count_local_disk_counter =
            ADD_CHILD_COUNTER(_runtime_profile, "IOCountLocalDisk", TUnit::UNIT, io_statistics_name);
    _io_count_remote_counter = ADD_CHILD_COUNTER(_runtime_profile, "IOCountRemote", TUnit::UNIT, io_statistics_name);
    _io_count_total_counter = ADD_CHILD_COUNTER(_runtime_profile, "IOCountTotal", TUnit::UNIT, io_statistics_name);
    _io_count_request_counter = ADD_CHILD_COUNTER(_runtime_profile, "IOCountRequest", TUnit::UNIT, io_statistics_name);
    // IO time
    _io_ns_local_disk_timer = ADD_CHILD_TIMER(_runtime_profile, "IOTimeLocalDisk", io_statistics_name);
    _io_ns_remote_timer = ADD_CHILD_TIMER(_runtime_profile, "IOTimeRemote", io_statistics_name);
    _io_ns_total_timer = ADD_CHILD_TIMER(_runtime_profile, "IOTimeTotal", io_statistics_name);
    // Prefetch
    _prefetch_hit_counter = ADD_CHILD_COUNTER(_runtime_profile, "PrefetchHitCount", TUnit::UNIT, io_statistics_name);
    _prefetch_wait_finish_timer = ADD_CHILD_TIMER(_runtime_profile, "PrefetchWaitFinishTime", io_statistics_name);
    _prefetch_pending_timer = ADD_CHILD_TIMER(_runtime_profile, "PrefetchPendingTime", io_statistics_name);
}

void LakeDataSource::observe_adaptive_split_task(const pipeline::LakeSplitContext* split_context,
                                                 const RowidRangeOptionPtr& rowid_range) {
    _current_adaptive_task_source = pipeline::LakeSplitContext::AdaptiveTaskSource::NONE;
    _current_adaptive_task_start_num_rows_read = _num_rows_read;
    _current_adaptive_task_start_ns = 0;
    _current_adaptive_pending_has_output = false;
    _current_adaptive_refined_has_output = false;
    _current_adaptive_pending_running_counted = false;
    _current_adaptive_prepared_segment_state = nullptr;
    if (split_context == nullptr) {
        return;
    }

    switch (split_context->adaptive_task_source) {
    case pipeline::LakeSplitContext::AdaptiveTaskSource::SEED_LOCAL: {
        const int64_t seed_rows = rowid_range_option_span_size(rowid_range);
        _current_adaptive_task_source = pipeline::LakeSplitContext::AdaptiveTaskSource::SEED_LOCAL;
        COUNTER_UPDATE(_lake_adaptive_seed_task_counter, 1);
        COUNTER_UPDATE(_lake_adaptive_seed_issued_rows_counter, seed_rows);
        auto segment_state = split_context->prepared_segment_state;
        if (segment_state != nullptr) {
            COUNTER_UPDATE(_lake_adaptive_seed_final_rows_counter,
                           static_cast<int64_t>(segment_state->final_pruned_rows));
            COUNTER_UPDATE(_lake_adaptive_seed_estimated_fanout_counter,
                           static_cast<int64_t>(segment_state->estimated_fanout));
            SparseRange<> pending_issued_ranges;
            SparseRangePtr final_scan_range;
            {
                std::lock_guard<std::mutex> guard(segment_state->adaptive_issue_lock);
                pending_issued_ranges = segment_state->adaptive_pending_issued_ranges;
                final_scan_range = segment_state->execution_pruned_range;
            }
            if (final_scan_range != nullptr && pending_issued_ranges.span_size() > 0) {
                const int64_t pending_issued_rows = static_cast<int64_t>(pending_issued_ranges.span_size());
                pending_issued_ranges &= *final_scan_range;
                const int64_t useful_rows = static_cast<int64_t>(pending_issued_ranges.span_size());
                COUNTER_UPDATE(_lake_adaptive_pending_useful_rows_counter, useful_rows);
                COUNTER_UPDATE(_lake_adaptive_pending_wasted_rows_counter, pending_issued_rows - useful_rows);
            }
        }
        break;
    }
    case pipeline::LakeSplitContext::AdaptiveTaskSource::PENDING_COARSE: {
        const int64_t rowid_rows = rowid_range_option_span_size(rowid_range);
        _current_adaptive_task_source = pipeline::LakeSplitContext::AdaptiveTaskSource::PENDING_COARSE;
        _current_adaptive_task_start_ns = MonotonicNanos();
        _current_adaptive_prepared_segment_state = split_context->prepared_segment_state;
        if (_current_adaptive_prepared_segment_state != nullptr) {
            const bool open_before_prepared =
                    _current_adaptive_prepared_segment_state->lifecycle.load(std::memory_order_acquire) !=
                    static_cast<uint32_t>(lake::PreparedSegmentReadState::Lifecycle::PREPARED);
            const bool open_after_seek_bounds_ready =
                    _current_adaptive_prepared_segment_state->seek_range_rowid_bounds_ready.load(
                            std::memory_order_acquire);
            _current_adaptive_pending_open_before_prepared = open_before_prepared;
            if (open_after_seek_bounds_ready) {
                COUNTER_UPDATE(_lake_adaptive_pending_open_after_seek_bounds_ready_task_counter, 1);
            } else {
                COUNTER_UPDATE(_lake_adaptive_pending_open_before_seek_bounds_ready_task_counter, 1);
            }
            if (open_before_prepared) {
                COUNTER_UPDATE(_lake_adaptive_pending_open_before_prepared_task_counter, 1);
                COUNTER_UPDATE(_lake_adaptive_pending_open_before_prepared_rows_counter, rowid_rows);
            } else {
                COUNTER_UPDATE(_lake_adaptive_pending_open_after_prepared_task_counter, 1);
                COUNTER_UPDATE(_lake_adaptive_pending_open_after_prepared_rows_counter, rowid_rows);
            }
            _current_adaptive_prepared_segment_state->adaptive_pending_running_tasks.fetch_add(
                    1, std::memory_order_acq_rel);
            _current_adaptive_pending_running_counted = true;
        }
        COUNTER_UPDATE(_lake_adaptive_pending_task_counter, 1);
        if (rowid_rows == 0) {
            COUNTER_UPDATE(_lake_adaptive_pending_empty_task_counter, 1);
        } else {
            COUNTER_UPDATE(_lake_adaptive_pending_issued_rows_counter, rowid_rows);
        }
        break;
    }
    case pipeline::LakeSplitContext::AdaptiveTaskSource::REFINED_CHILD:
        _current_adaptive_task_source = pipeline::LakeSplitContext::AdaptiveTaskSource::REFINED_CHILD;
        _current_adaptive_task_start_ns = MonotonicNanos();
        _current_adaptive_prepared_segment_state = split_context->prepared_segment_state;
        if (_current_adaptive_prepared_segment_state != nullptr) {
            const int64_t refined_ready_ns =
                    _current_adaptive_prepared_segment_state->adaptive_refined_ready_ns.load(std::memory_order_acquire);
            if (refined_ready_ns > 0) {
                COUNTER_UPDATE(_lake_adaptive_refined_start_delay_after_ready_timer,
                               std::max<int64_t>(0, _current_adaptive_task_start_ns - refined_ready_ns));
                COUNTER_UPDATE(_lake_adaptive_refined_start_delay_after_ready_task_counter, 1);
                COUNTER_UPDATE(_lake_adaptive_pending_running_at_refined_ready_counter,
                               _current_adaptive_prepared_segment_state->adaptive_pending_running_at_refined_ready.load(
                                       std::memory_order_acquire));
            }
        }
        COUNTER_UPDATE(_lake_adaptive_refined_task_counter, 1);
        break;
    case pipeline::LakeSplitContext::AdaptiveTaskSource::NONE:
        break;
    }
}

void LakeDataSource::record_current_adaptive_task_read_stats(const OlapReaderStatistics& stats) {
    const int64_t reader_time_ns = stats.create_segment_iter_ns + stats.segment_init_ns + stats.block_load_ns;
    if (_current_adaptive_task_source == pipeline::LakeSplitContext::AdaptiveTaskSource::SEED_LOCAL) {
        COUNTER_UPDATE(_lake_adaptive_seed_reader_timer, reader_time_ns);
        COUNTER_UPDATE(_lake_adaptive_seed_raw_rows_counter, stats.raw_rows_read);
        COUNTER_UPDATE(_lake_adaptive_seed_output_rows_counter,
                       _num_rows_read - _current_adaptive_task_start_num_rows_read);
    } else if (_current_adaptive_task_source == pipeline::LakeSplitContext::AdaptiveTaskSource::REFINED_CHILD) {
        COUNTER_UPDATE(_lake_adaptive_refined_create_segment_iter_timer, stats.create_segment_iter_ns);
        COUNTER_UPDATE(_lake_adaptive_refined_segment_init_timer, stats.segment_init_ns);
        COUNTER_UPDATE(_lake_adaptive_refined_block_load_timer, stats.block_load_ns);
        COUNTER_UPDATE(_lake_adaptive_refined_column_iterator_init_timer, stats.column_iterator_init_ns);
        COUNTER_UPDATE(_lake_adaptive_refined_key_range_filter_timer, stats.rows_key_range_filter_ns);
        COUNTER_UPDATE(_lake_adaptive_refined_seek_bounds_cache_hit_counter, stats.seek_range_rowid_bounds_cache_hits);
        COUNTER_UPDATE(_lake_adaptive_refined_seek_bounds_cache_miss_counter,
                       stats.seek_range_rowid_bounds_cache_misses);
        COUNTER_UPDATE(_lake_adaptive_refined_bitmap_index_filter_timer, stats.bitmap_index_filter_timer);
        COUNTER_UPDATE(_lake_adaptive_refined_zone_map_filter_timer, stats.zone_map_filter_ns);
        COUNTER_UPDATE(_lake_adaptive_refined_bloom_filter_timer, stats.bf_filter_ns);
        COUNTER_UPDATE(_lake_adaptive_refined_gin_filter_timer, stats.gin_index_filter_ns);
        COUNTER_UPDATE(_lake_adaptive_refined_stats_filtered_rows_counter, stats.rows_stats_filtered);
        COUNTER_UPDATE(_lake_adaptive_refined_bloom_filtered_rows_counter, stats.rows_bf_filtered);
        COUNTER_UPDATE(_lake_adaptive_refined_bitmap_filtered_rows_counter, stats.rows_bitmap_index_filtered);
        COUNTER_UPDATE(_lake_adaptive_refined_gin_filtered_rows_counter, stats.rows_gin_filtered);
        COUNTER_UPDATE(_lake_adaptive_refined_reader_timer, reader_time_ns);
        if (_current_adaptive_task_start_ns > 0) {
            COUNTER_UPDATE(_lake_adaptive_refined_wall_timer,
                           std::max<int64_t>(0, MonotonicNanos() - _current_adaptive_task_start_ns));
        }
        COUNTER_UPDATE(_lake_adaptive_refined_raw_rows_counter, stats.raw_rows_read);
        COUNTER_UPDATE(_lake_adaptive_refined_output_rows_counter,
                       _num_rows_read - _current_adaptive_task_start_num_rows_read);
    } else if (_current_adaptive_task_source == pipeline::LakeSplitContext::AdaptiveTaskSource::PENDING_COARSE) {
        COUNTER_UPDATE(_lake_adaptive_pending_create_segment_iter_timer, stats.create_segment_iter_ns);
        COUNTER_UPDATE(_lake_adaptive_pending_segment_init_timer, stats.segment_init_ns);
        COUNTER_UPDATE(_lake_adaptive_pending_block_load_timer, stats.block_load_ns);
        COUNTER_UPDATE(_lake_adaptive_pending_column_iterator_init_timer, stats.column_iterator_init_ns);
        COUNTER_UPDATE(_lake_adaptive_pending_key_range_filter_timer, stats.rows_key_range_filter_ns);
        COUNTER_UPDATE(_lake_adaptive_pending_seek_bounds_cache_hit_counter, stats.seek_range_rowid_bounds_cache_hits);
        COUNTER_UPDATE(_lake_adaptive_pending_seek_bounds_cache_miss_counter,
                       stats.seek_range_rowid_bounds_cache_misses);
        COUNTER_UPDATE(_lake_adaptive_pending_bitmap_index_filter_timer, stats.bitmap_index_filter_timer);
        COUNTER_UPDATE(_lake_adaptive_pending_zone_map_filter_timer, stats.zone_map_filter_ns);
        COUNTER_UPDATE(_lake_adaptive_pending_bloom_filter_timer, stats.bf_filter_ns);
        COUNTER_UPDATE(_lake_adaptive_pending_gin_filter_timer, stats.gin_index_filter_ns);
        COUNTER_UPDATE(_lake_adaptive_pending_stats_filtered_rows_counter, stats.rows_stats_filtered);
        COUNTER_UPDATE(_lake_adaptive_pending_bloom_filtered_rows_counter, stats.rows_bf_filtered);
        COUNTER_UPDATE(_lake_adaptive_pending_bitmap_filtered_rows_counter, stats.rows_bitmap_index_filtered);
        COUNTER_UPDATE(_lake_adaptive_pending_gin_filtered_rows_counter, stats.rows_gin_filtered);
        COUNTER_UPDATE(_lake_adaptive_pending_reader_timer, reader_time_ns);
        if (_current_adaptive_pending_open_before_prepared) {
            COUNTER_UPDATE(_lake_adaptive_pending_open_before_prepared_reader_timer, reader_time_ns);
            COUNTER_UPDATE(_lake_adaptive_pending_open_before_prepared_block_load_timer, stats.block_load_ns);
        } else {
            COUNTER_UPDATE(_lake_adaptive_pending_open_after_prepared_reader_timer, reader_time_ns);
            COUNTER_UPDATE(_lake_adaptive_pending_open_after_prepared_block_load_timer, stats.block_load_ns);
        }
        const int64_t end_ns = MonotonicNanos();
        if (_current_adaptive_task_start_ns > 0) {
            const int64_t wall_ns = std::max<int64_t>(0, end_ns - _current_adaptive_task_start_ns);
            int64_t before_refined_ready_ns = wall_ns;
            int64_t after_refined_ready_ns = 0;
            const int64_t refined_ready_ns =
                    _current_adaptive_prepared_segment_state == nullptr
                            ? 0
                            : _current_adaptive_prepared_segment_state->adaptive_refined_ready_ns.load(
                                      std::memory_order_acquire);
            if (refined_ready_ns > 0) {
                if (refined_ready_ns <= _current_adaptive_task_start_ns) {
                    before_refined_ready_ns = 0;
                    after_refined_ready_ns = wall_ns;
                } else if (refined_ready_ns < end_ns) {
                    before_refined_ready_ns = refined_ready_ns - _current_adaptive_task_start_ns;
                    after_refined_ready_ns = end_ns - refined_ready_ns;
                }
            }
            COUNTER_UPDATE(_lake_adaptive_pending_wall_timer, wall_ns);
            COUNTER_UPDATE(_lake_adaptive_pending_before_refined_ready_timer, before_refined_ready_ns);
            COUNTER_UPDATE(_lake_adaptive_pending_after_refined_ready_timer, after_refined_ready_ns);
            if (after_refined_ready_ns > 0) {
                COUNTER_UPDATE(_lake_adaptive_pending_after_refined_ready_task_counter, 1);
            }
        }
        COUNTER_UPDATE(_lake_adaptive_pending_raw_rows_counter, stats.raw_rows_read);
        COUNTER_UPDATE(_lake_adaptive_pending_output_rows_counter,
                       _num_rows_read - _current_adaptive_task_start_num_rows_read);
    }
    if (_current_adaptive_pending_running_counted && _current_adaptive_prepared_segment_state != nullptr) {
        _current_adaptive_prepared_segment_state->adaptive_pending_running_tasks.fetch_sub(1,
                                                                                           std::memory_order_acq_rel);
    }
    _current_adaptive_task_source = pipeline::LakeSplitContext::AdaptiveTaskSource::NONE;
    _current_adaptive_task_start_ns = 0;
    _current_adaptive_pending_has_output = false;
    _current_adaptive_pending_open_before_prepared = false;
    _current_adaptive_refined_has_output = false;
    _current_adaptive_pending_running_counted = false;
    _current_adaptive_prepared_segment_state = nullptr;
}

void LakeDataSource::update_realtime_counter(Chunk* chunk) {
    _num_rows_read += chunk->num_rows();
    if (_current_adaptive_task_source == pipeline::LakeSplitContext::AdaptiveTaskSource::PENDING_COARSE &&
        chunk != nullptr && chunk->num_rows() > 0) {
        const int64_t refined_ready_ns =
                _current_adaptive_prepared_segment_state == nullptr
                        ? 0
                        : _current_adaptive_prepared_segment_state->adaptive_refined_ready_ns.load(
                                  std::memory_order_acquire);
        const bool after_refined_ready = refined_ready_ns > 0 && MonotonicNanos() >= refined_ready_ns;
        if (after_refined_ready) {
            COUNTER_UPDATE(_lake_adaptive_pending_output_rows_after_refined_ready_counter, chunk->num_rows());
            COUNTER_UPDATE(_lake_adaptive_pending_chunks_after_refined_ready_counter, 1);
            if (!_current_adaptive_pending_has_output) {
                COUNTER_UPDATE(_lake_adaptive_pending_first_output_after_refined_ready_counter, 1);
            }
        } else {
            COUNTER_UPDATE(_lake_adaptive_pending_output_rows_before_refined_ready_counter, chunk->num_rows());
            COUNTER_UPDATE(_lake_adaptive_pending_chunks_before_refined_ready_counter, 1);
            if (!_current_adaptive_pending_has_output) {
                COUNTER_UPDATE(_lake_adaptive_pending_first_output_before_refined_ready_counter, 1);
            }
        }
        _current_adaptive_pending_has_output = true;
    }
    if (_current_adaptive_task_source == pipeline::LakeSplitContext::AdaptiveTaskSource::REFINED_CHILD &&
        chunk != nullptr && chunk->num_rows() > 0 && !_current_adaptive_refined_has_output) {
        const int64_t refined_ready_ns =
                _current_adaptive_prepared_segment_state == nullptr
                        ? 0
                        : _current_adaptive_prepared_segment_state->adaptive_refined_ready_ns.load(
                                  std::memory_order_acquire);
        if (refined_ready_ns > 0) {
            COUNTER_UPDATE(_lake_adaptive_refined_first_output_delay_after_ready_timer,
                           std::max<int64_t>(0, MonotonicNanos() - refined_ready_ns));
            COUNTER_UPDATE(_lake_adaptive_refined_first_output_after_ready_task_counter, 1);
        }
        _current_adaptive_refined_has_output = true;
    }
    auto& stats = _reader->stats();
    _raw_rows_read = _released_raw_rows_read + stats.raw_rows_read;
    _bytes_read = _released_bytes_read + stats.bytes_read;
    _cpu_time_spent_ns = _released_cpu_time_spent_ns + stats.decompress_ns + stats.vec_cond_ns + stats.del_filter_ns;
}

void LakeDataSource::update_counter(RuntimeState* state) {
    DCHECK(_reader != nullptr);
    update_counter(state, _reader->stats());
}

void LakeDataSource::update_counter(RuntimeState* state, const OlapReaderStatistics& stats) {
    COUNTER_UPDATE(_create_seg_iter_timer, stats.create_segment_iter_ns);
    COUNTER_UPDATE(_rows_read_counter, _num_rows_read - _reported_num_rows_read);
    _reported_num_rows_read = _num_rows_read;

    COUNTER_UPDATE(_io_timer, stats.io_ns);
    COUNTER_UPDATE(_read_compressed_counter, stats.compressed_bytes_read);
    COUNTER_UPDATE(_decompress_timer, stats.decompress_ns);
    COUNTER_UPDATE(_read_uncompressed_counter, stats.uncompressed_bytes_read);
    COUNTER_UPDATE(_bytes_read_counter, stats.bytes_read);

    COUNTER_UPDATE(_block_load_timer, stats.block_load_ns);
    COUNTER_UPDATE(_block_load_counter, stats.blocks_load);
    COUNTER_UPDATE(_block_fetch_timer, stats.block_fetch_ns);
    COUNTER_UPDATE(_block_seek_timer, stats.block_seek_ns);

    COUNTER_UPDATE(_chunk_copy_timer, stats.vec_cond_chunk_copy_ns);
    COUNTER_UPDATE(_get_delvec_timer, stats.get_delvec_ns);
    COUNTER_UPDATE(_get_delta_column_group_timer, stats.get_delta_column_group_ns);
    COUNTER_UPDATE(_seg_init_timer, stats.segment_init_ns);
    COUNTER_UPDATE(_column_iterator_init_timer, stats.column_iterator_init_ns);
    COUNTER_UPDATE(_bitmap_index_iterator_init_timer, stats.bitmap_index_iterator_init_ns);
    COUNTER_UPDATE(_zone_map_filter_timer, stats.zone_map_filter_ns);
    COUNTER_UPDATE(_rows_key_range_filter_timer, stats.rows_key_range_filter_ns);
    COUNTER_UPDATE(_bf_filter_timer, stats.bf_filter_ns);
    COUNTER_UPDATE(_read_pk_index_timer, stats.read_pk_index_ns);

    COUNTER_UPDATE(_raw_rows_counter, stats.raw_rows_read);

    int64_t cond_evaluate_ns = 0;
    cond_evaluate_ns += stats.vec_cond_evaluate_ns;
    cond_evaluate_ns += stats.branchless_cond_evaluate_ns;
    cond_evaluate_ns += stats.expr_cond_evaluate_ns;

    // In order to avoid exposing too detailed metrics, we still record these infos on `_pred_filter_timer`
    // When we support metric classification, we can disassemble it again.
    COUNTER_UPDATE(_rf_pred_filter_timer, stats.rf_cond_evaluate_ns);
    COUNTER_UPDATE(_rf_pred_input_rows, stats.rf_cond_input_rows);
    COUNTER_UPDATE(_rf_pred_output_rows, stats.rf_cond_output_rows);

    COUNTER_UPDATE(_pred_filter_timer, cond_evaluate_ns);
    COUNTER_UPDATE(_pred_filter_counter, stats.rows_vec_cond_filtered);
    COUNTER_UPDATE(_del_vec_filter_counter, stats.rows_del_vec_filtered);

    COUNTER_UPDATE(_seg_zm_filtered_counter, stats.segment_stats_filtered);
    COUNTER_UPDATE(_seg_metadata_filtered_counter, stats.segment_metadata_filtered);
    COUNTER_UPDATE(_segs_metadata_filtered_counter, stats.segments_metadata_filtered);
    COUNTER_UPDATE(_seg_rt_filtered_counter, stats.runtime_stats_filtered);
    COUNTER_UPDATE(_zm_filtered_counter, stats.rows_stats_filtered);
    COUNTER_UPDATE(_bf_filtered_counter, stats.rows_bf_filtered);
    COUNTER_UPDATE(_sk_filtered_counter, stats.rows_key_range_filtered);
    COUNTER_UPDATE(_rows_after_sk_filtered_counter, stats.rows_after_key_range);
    COUNTER_UPDATE(_rows_key_range_counter, stats.rows_key_range_num);

    COUNTER_UPDATE(_bi_filtered_counter, stats.rows_bitmap_index_filtered);
    COUNTER_UPDATE(_bi_filter_timer, stats.bitmap_index_filter_timer);
    COUNTER_UPDATE(_block_seek_counter, stats.block_seek_num);

    COUNTER_UPDATE(_gin_filtered_timer, stats.gin_index_filter_ns);
    COUNTER_UPDATE(_gin_filtered_counter, stats.rows_gin_filtered);
    COUNTER_UPDATE(_gin_prefix_filter_timer, stats.gin_prefix_filter_ns);
    COUNTER_UPDATE(_gin_ngram_dict_filter_timer, stats.gin_ngram_filter_dict_ns);
    COUNTER_UPDATE(_gin_predicate_dict_filter_timer, stats.gin_predicate_filter_dict_ns);
    COUNTER_UPDATE(_gin_dict_counter, stats.gin_dict_count);
    COUNTER_UPDATE(_gin_ngram_dict_counter, stats.gin_ngram_dict_count);
    COUNTER_UPDATE(_gin_ngram_dict_filtered_counter, stats.gin_ngram_dict_filtered);
    COUNTER_UPDATE(_gin_predicate_dict_filtered_counter, stats.gin_predicate_dict_filtered);

    COUNTER_UPDATE(_rowsets_read_count, stats.rowsets_read_count);
    COUNTER_UPDATE(_segments_read_count, stats.segments_read_count);
    COUNTER_UPDATE(_total_columns_data_page_count, stats.total_columns_data_page_count);

    COUNTER_SET(_pushdown_predicates_counter, (int64_t)_params.pred_tree.size());

    if (_runtime_state->fragment_ctx()->pred_tree_params().enable_show_in_profile) {
        _runtime_profile->add_info_string(
                "PushdownPredicateTree", _params.pred_tree.visit([](const auto& node) { return node.debug_string(); }));
    }

    StarRocksMetrics::instance()->query_scan_bytes.increment(stats.bytes_read);
    StarRocksMetrics::instance()->query_scan_rows.increment(stats.raw_rows_read);

    if (stats.decode_dict_ns > 0) {
        RuntimeProfile::Counter* c = ADD_TIMER(_runtime_profile, "DictDecode");
        COUNTER_UPDATE(c, stats.decode_dict_ns);
        RuntimeProfile::Counter* count = ADD_COUNTER(_runtime_profile, "DictDecodeCount", TUnit::UNIT);
        COUNTER_UPDATE(count, stats.decode_dict_count);
    }
    if (stats.late_materialize_ns > 0) {
        RuntimeProfile::Counter* c = ADD_TIMER(_runtime_profile, "LateMaterialize");
        COUNTER_UPDATE(c, stats.late_materialize_ns);
        RuntimeProfile::Counter* rows = ADD_COUNTER(_runtime_profile, "LateMaterializeRows", TUnit::UNIT);
        COUNTER_UPDATE(rows, stats.late_materialize_rows);
    }
    if (stats.del_filter_ns > 0) {
        RuntimeProfile::Counter* c1 = ADD_TIMER(_runtime_profile, "DeleteFilter");
        RuntimeProfile::Counter* c2 = ADD_COUNTER(_runtime_profile, "DeleteFilterRows", TUnit::UNIT);
        COUNTER_UPDATE(c1, stats.del_filter_ns);
        COUNTER_UPDATE(c2, stats.rows_del_filtered);
    }

    int64_t pages_total = stats.total_pages_num;
    int64_t pages_from_memory = stats.cached_pages_num;
    int64_t pages_from_local_disk = stats.pages_from_local_disk;
    COUNTER_UPDATE(_pages_count_memory_counter, pages_from_memory);
    COUNTER_UPDATE(_pages_count_local_disk_counter, pages_from_local_disk);
    COUNTER_UPDATE(_pages_count_remote_counter, pages_total - pages_from_memory - pages_from_local_disk);
    COUNTER_UPDATE(_pages_count_total_counter, pages_total);

    COUNTER_UPDATE(_compressed_bytes_read_local_disk_counter, stats.compressed_bytes_read_local_disk);
    COUNTER_UPDATE(_compressed_bytes_read_remote_counter, stats.compressed_bytes_read_remote);
    COUNTER_UPDATE(_compressed_bytes_read_total_counter, stats.compressed_bytes_read);
    COUNTER_UPDATE(_compressed_bytes_read_request_counter, stats.compressed_bytes_read_request);

    COUNTER_UPDATE(_io_count_local_disk_counter, stats.io_count_local_disk);
    COUNTER_UPDATE(_io_count_remote_counter, stats.io_count_remote);
    COUNTER_UPDATE(_io_count_total_counter, stats.io_count);
    COUNTER_UPDATE(_io_count_request_counter, stats.io_count_request);

    COUNTER_UPDATE(_io_ns_local_disk_timer, stats.io_ns_read_local_disk);
    COUNTER_UPDATE(_io_ns_remote_timer, stats.io_ns_remote);
    COUNTER_UPDATE(_io_ns_total_timer, stats.io_ns);

    COUNTER_UPDATE(_prefetch_hit_counter, stats.prefetch_hit_count);
    COUNTER_UPDATE(_prefetch_wait_finish_timer, stats.prefetch_wait_finish_ns);
    COUNTER_UPDATE(_prefetch_pending_timer, stats.prefetch_pending_ns);

    // update cache related info for CACHE SELECT
    if (_runtime_state->query_options().__isset.query_type &&
        _runtime_state->query_options().query_type == TQueryType::LOAD) {
        _runtime_state->update_num_datacache_read_bytes(stats.compressed_bytes_read_local_disk);
        _runtime_state->update_num_datacache_read_time_ns(stats.io_ns_read_local_disk);
        _runtime_state->update_num_datacache_write_bytes(stats.compressed_bytes_write_local_disk);
        _runtime_state->update_num_datacache_write_time_ns(stats.io_ns_write_local_disk);
        _runtime_state->update_num_datacache_count(1);
    }

    if (stats.flat_json_hits.size() > 0 || stats.merge_json_hits.size() > 0) {
        RuntimeProfile::Counter* _access_path_hits_counter =
                ADD_COUNTER(_runtime_profile, "AccessPathHits", TUnit::UNIT);
        std::string access_path_hits = "AccessPathHits";
        int64_t total = 0;
        for (auto& [k, v] : stats.flat_json_hits) {
            std::string path = fmt::format("[Hit]{}", k);
            auto* path_counter = _runtime_profile->get_counter(path);
            if (path_counter == nullptr) {
                path_counter = ADD_CHILD_COUNTER(_runtime_profile, path, TUnit::UNIT, access_path_hits);
            }
            total += v;
            COUNTER_UPDATE(path_counter, v);
        }
        for (auto& [k, v] : stats.merge_json_hits) {
            std::string merge_path = fmt::format("[HitMerge]{}", k);
            auto* path_counter = _runtime_profile->get_counter(merge_path);
            if (path_counter == nullptr) {
                path_counter = ADD_CHILD_COUNTER(_runtime_profile, merge_path, TUnit::UNIT, access_path_hits);
            }
            total += v;
            COUNTER_UPDATE(path_counter, v);
        }
        COUNTER_UPDATE(_access_path_hits_counter, total);
    }
    if (stats.dynamic_json_hits.size() > 0) {
        RuntimeProfile::Counter* _access_path_unhits_counter =
                ADD_COUNTER(_runtime_profile, "AccessPathUnhits", TUnit::UNIT);
        std::string access_path_unhits = "AccessPathUnhits";
        int64_t total = 0;
        for (auto& [k, v] : stats.dynamic_json_hits) {
            std::string path = fmt::format("[Unhit]{}", k);
            auto* path_counter = _runtime_profile->get_counter(path);
            if (path_counter == nullptr) {
                path_counter = ADD_CHILD_COUNTER(_runtime_profile, path, TUnit::UNIT, access_path_unhits);
            }
            total += v;
            COUNTER_UPDATE(path_counter, v);
        }
        COUNTER_UPDATE(_access_path_unhits_counter, total);
    }
    if (stats.extract_json_hits.size() > 0) {
        const std::string counter_name = "AccessPathExtract";
        RuntimeProfile::Counter* counter = ADD_COUNTER(_runtime_profile, counter_name, TUnit::UNIT);
        int64_t total = 0;
        for (auto& [k, v] : stats.extract_json_hits) {
            std::string path = fmt::format("[Extract]{}", k);
            auto* path_counter = _runtime_profile->get_counter(path);
            if (path_counter == nullptr) {
                path_counter = ADD_CHILD_COUNTER(_runtime_profile, path, TUnit::UNIT, counter_name);
            }
            total += v;
            COUNTER_UPDATE(path_counter, v);
        }
        COUNTER_UPDATE(counter, total);
    }

    std::string parent_name = "SegmentRead";
    if (stats.json_init_ns > 0) {
        RuntimeProfile::Counter* c = ADD_CHILD_TIMER(_runtime_profile, "FlatJsonInit", parent_name);
        COUNTER_UPDATE(c, stats.json_init_ns);
    }
    if (stats.json_cast_ns > 0) {
        RuntimeProfile::Counter* c = ADD_CHILD_TIMER(_runtime_profile, "FlatJsonCast", parent_name);
        COUNTER_UPDATE(c, stats.json_cast_ns);
    }
    if (stats.json_merge_ns > 0) {
        RuntimeProfile::Counter* c = ADD_CHILD_TIMER(_runtime_profile, "FlatJsonMerge", parent_name);
        COUNTER_UPDATE(c, stats.json_merge_ns);
    }
    if (stats.json_flatten_ns > 0) {
        RuntimeProfile::Counter* c = ADD_CHILD_TIMER(_runtime_profile, "FlatJsonFlatten", parent_name);
        COUNTER_UPDATE(c, stats.json_flatten_ns);
    }
    if (state && state->query_ctx()) {
        state->query_ctx()->incr_read_stats(stats.io_count_local_disk, stats.io_count_remote);
    }
}

// ================================

LakeDataSourceProvider::LakeDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _t_lake_scan_node(plan_node.lake_scan_node) {}

DataSourcePtr LakeDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<LakeDataSource>(this, scan_range);
}

Status LakeDataSourceProvider::init(ObjectPool* pool, RuntimeState* state) {
    if (_t_lake_scan_node.__isset.bucket_exprs) {
        const auto& bucket_exprs = _t_lake_scan_node.bucket_exprs;
        _partition_exprs.resize(bucket_exprs.size());
        for (int i = 0; i < bucket_exprs.size(); ++i) {
            RETURN_IF_ERROR(ExprFactory::create_expr_tree(pool, bucket_exprs[i], &_partition_exprs[i], state));
        }
    }
    return Status::OK();
}

const TupleDescriptor* LakeDataSourceProvider::tuple_descriptor(RuntimeState* state) const {
    return state->desc_tbl().get_tuple_descriptor(_t_lake_scan_node.tuple_id);
}

// ================================

DataSourceProviderPtr LakeConnector::create_data_source_provider(ConnectorScanNode* scan_node,
                                                                 const TPlanNode& plan_node) const {
    return std::make_unique<LakeDataSourceProvider>(scan_node, plan_node);
}

StatusOr<pipeline::MorselQueuePtr> LakeDataSourceProvider::convert_scan_range_to_morsel_queue(
        const std::vector<TScanRangeParams>& scan_ranges, int node_id, int32_t pipeline_dop,
        bool enable_tablet_internal_parallel, TTabletInternalParallelMode::type tablet_internal_parallel_mode,
        size_t num_total_scan_ranges, size_t scan_parallelism) {
    int64_t lake_scan_parallelism = 0;
    if (!scan_ranges.empty() && enable_tablet_internal_parallel) {
        ASSIGN_OR_RETURN(_could_split, _could_tablet_internal_parallel(scan_ranges, pipeline_dop, num_total_scan_ranges,
                                                                       tablet_internal_parallel_mode,
                                                                       &lake_scan_parallelism, &splitted_scan_rows));
        if (_could_split) {
            ASSIGN_OR_RETURN(_could_split_physically, _could_split_tablet_physically(scan_ranges));
        }
    }

    _use_lake_adaptive_split_morsel_queue =
            _could_split && _could_split_physically && config::enable_lake_adaptive_split_morsel_queue;
    if (_use_lake_adaptive_split_morsel_queue) {
        pipeline::Morsels morsels;
        bool has_more_morsel = false;
        pipeline::ScanMorsel::build_scan_morsels(node_id, scan_ranges, accept_empty_scan_ranges(), &morsels,
                                                 &has_more_morsel);
        if (partition_order_hint().has_value()) {
            bool asc = partition_order_hint().value();
            std::stable_sort(morsels.begin(), morsels.end(), [asc](auto& l, auto& r) {
                auto l_partition_id = down_cast<pipeline::ScanMorsel*>(l.get())->partition_id();
                auto r_partition_id = down_cast<pipeline::ScanMorsel*>(r.get())->partition_id();
                return asc ? l_partition_id < r_partition_id : l_partition_id > r_partition_id;
            });
        }
        if (output_chunk_by_bucket()) {
            std::stable_sort(morsels.begin(), morsels.end(), [](auto& l, auto& r) {
                return down_cast<pipeline::ScanMorsel*>(l.get())->owner_id() <
                       down_cast<pipeline::ScanMorsel*>(r.get())->owner_id();
            });
        }
        auto morsel_queue = std::make_unique<pipeline::LakeAdaptiveSplitMorselQueue>(
                std::move(morsels), has_more_morsel, splitted_scan_rows);
        if (lake_scan_parallelism > 0) {
            morsel_queue->set_max_degree_of_parallelism(static_cast<size_t>(lake_scan_parallelism));
        }
        morsel_queue->set_has_more_from_split(true);
        return morsel_queue;
    }

    ASSIGN_OR_RETURN(auto morsel_queue,
                     DataSourceProvider::convert_scan_range_to_morsel_queue(
                             scan_ranges, node_id, pipeline_dop, enable_tablet_internal_parallel,
                             tablet_internal_parallel_mode, num_total_scan_ranges, (size_t)lake_scan_parallelism));
    if (_could_split) {
        morsel_queue->set_has_more_from_split(true);
    }
    return morsel_queue;
}

StatusOr<bool> LakeDataSourceProvider::_could_tablet_internal_parallel(
        const std::vector<TScanRangeParams>& scan_ranges, int32_t pipeline_dop, size_t num_total_scan_ranges,
        TTabletInternalParallelMode::type tablet_internal_parallel_mode, int64_t* scan_parallelism,
        int64_t* splitted_scan_rows) const {
    bool force_split = tablet_internal_parallel_mode == TTabletInternalParallelMode::type::FORCE_SPLIT;
    const int64_t enough_tablet_multiplier =
            std::max<int64_t>(1, config::lake_tablet_internal_parallel_enough_tablet_dop_multiplier);
    const size_t enough_tablet_parallelism_threshold =
            std::max<size_t>(1, static_cast<size_t>(pipeline_dop) * enough_tablet_multiplier);
    // The enough number of tablets shouldn't use tablet internal parallel.
    if (!force_split && num_total_scan_ranges >= enough_tablet_parallelism_threshold) {
        return false;
    }

    int64_t num_table_rows = 0;
    for (const auto& tablet_scan_range : scan_ranges) {
        int64_t version = std::stoll(tablet_scan_range.scan_range.internal_scan_range.version);
#ifdef BE_TEST
        ASSIGN_OR_RETURN(auto tablet_num_rows,
                         _tablet_manager->get_tablet_num_rows(
                                 tablet_scan_range.scan_range.internal_scan_range.tablet_id, version));
        num_table_rows += static_cast<int64_t>(tablet_num_rows);
#else
        ASSIGN_OR_RETURN(auto tablet_num_rows,
                         ExecEnv::GetInstance()->lake_tablet_manager()->get_tablet_num_rows(
                                 tablet_scan_range.scan_range.internal_scan_range.tablet_id, version));
        num_table_rows += static_cast<int64_t>(tablet_num_rows);
#endif
    }

    // splitted_scan_rows is restricted in the range [min_splitted_scan_rows, max_splitted_scan_rows].
    *splitted_scan_rows =
            config::tablet_internal_parallel_max_splitted_scan_bytes / _scan_node->estimated_scan_row_bytes();
    *splitted_scan_rows =
            std::max(config::tablet_internal_parallel_min_splitted_scan_rows,
                     std::min(*splitted_scan_rows, config::tablet_internal_parallel_max_splitted_scan_rows));
    *scan_parallelism = num_table_rows / *splitted_scan_rows;

    if (force_split) {
        return true;
    }

    bool could =
            *scan_parallelism >= pipeline_dop || *scan_parallelism >= config::tablet_internal_parallel_min_scan_dop;
    // if don't use tablet internal parallel scan, we choose the number of tablets as the dop of scan node
    // which is the same as olap scan
    if (!could) {
        *scan_parallelism = scan_ranges.size();
    }

    return could;
}

StatusOr<bool> LakeDataSourceProvider::_could_split_tablet_physically(
        const std::vector<TScanRangeParams>& scan_ranges) const {
    // Keys type needn't merge or aggregate.
    int64_t version = std::stoll(scan_ranges[0].scan_range.internal_scan_range.version);
    KeysType keys_type;
#ifdef BE_TEST
    ASSIGN_OR_RETURN(
            auto first_tablet_schema,
            _tablet_manager->get_tablet_schema(scan_ranges[0].scan_range.internal_scan_range.tablet_id, &version));
    keys_type = first_tablet_schema->keys_type();
#else
    ASSIGN_OR_RETURN(auto first_tablet_schema,
                     ExecEnv::GetInstance()->lake_tablet_manager()->get_tablet_schema(
                             scan_ranges[0].scan_range.internal_scan_range.tablet_id, &version));
    keys_type = first_tablet_schema->keys_type();
#endif

    const auto skip_aggr = _t_lake_scan_node.is_preaggregation;
    bool is_keys_type_matched = keys_type == PRIMARY_KEYS || keys_type == DUP_KEYS ||
                                ((keys_type == UNIQUE_KEYS || keys_type == AGG_KEYS) && skip_aggr);

    return is_keys_type_matched;
}

} // namespace starrocks::connector
