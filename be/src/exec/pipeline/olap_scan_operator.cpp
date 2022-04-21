// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/olap_scan_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/olap_chunk_source.h"
#include "exec/vectorized/olap_scan_node.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/rowset/rowset.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"

namespace starrocks::pipeline {

// ==================== OlapScanOperatorFactory ====================

OlapScanOperatorFactory::OlapScanOperatorFactory(int32_t id, ScanNode* scan_node)
        : ScanOperatorFactory(id, scan_node) {}

Status OlapScanOperatorFactory::do_prepare(RuntimeState* state) {
    const auto& conjunct_ctxs = _scan_node->conjunct_ctxs();
    vectorized::OlapScanNode* olap_scan_node = down_cast<vectorized::OlapScanNode*>(_scan_node);
    const auto& tolap_scan_node = olap_scan_node->thrift_olap_scan_node();
    auto tuple_desc = state->desc_tbl().get_tuple_descriptor(tolap_scan_node.tuple_id);
    vectorized::DictOptimizeParser::rewrite_descriptor(state, conjunct_ctxs, tolap_scan_node.dict_string_id_to_int_ids,
                                                       &(tuple_desc->decoded_slots()));
    return Status::OK();
}

void OlapScanOperatorFactory::do_close(RuntimeState* state) {
    _dict_optimize_parser.close(state);
}

OperatorPtr OlapScanOperatorFactory::do_create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<OlapScanOperator>(this, _id, _scan_node, _shared_phase);
}

Status OlapScanOperatorFactory::parse_conjuncts(RuntimeState* state) {
    auto* olap_scan_node = down_cast<vectorized::OlapScanNode*>(_scan_node);
    const TOlapScanNode& thrift_olap_scan_node = olap_scan_node->thrift_olap_scan_node();
    const TupleDescriptor* tuple_desc = state->desc_tbl().get_tuple_descriptor(thrift_olap_scan_node.tuple_id);

    // Get _conjunct_ctxs.
    _conjunct_ctxs = olap_scan_node->conjunct_ctxs();
    _conjunct_ctxs.insert(_conjunct_ctxs.end(), _runtime_in_filters.begin(), _runtime_in_filters.end());

    // eval_const_conjuncts.
    Status status;
    RETURN_IF_ERROR(vectorized::OlapScanConjunctsManager::eval_const_conjuncts(_conjunct_ctxs, &status));
    if (!status.ok()) {
        _shared_phase.store(ScanOperatorFactory::SharedPhase::EOS, std::memory_order_release);
        return Status::OK();
    }

    // Init _conjuncts_manager.
    vectorized::OlapScanConjunctsManager& cm = _conjuncts_manager;
    cm.conjunct_ctxs_ptr = &_conjunct_ctxs;
    cm.tuple_desc = tuple_desc;
    cm.obj_pool = &_obj_pool;
    cm.key_column_names = &thrift_olap_scan_node.key_column_name;
    cm.runtime_filters = this->get_runtime_bloom_filters();
    cm.runtime_state = state;

    const TQueryOptions& query_options = state->query_options();
    int32_t max_scan_key_num;
    if (query_options.__isset.max_scan_key_num && query_options.max_scan_key_num > 0) {
        max_scan_key_num = query_options.max_scan_key_num;
    } else {
        max_scan_key_num = config::doris_max_scan_key_num;
    }
    bool enable_column_expr_predicate = false;
    if (thrift_olap_scan_node.__isset.enable_column_expr_predicate) {
        enable_column_expr_predicate = thrift_olap_scan_node.enable_column_expr_predicate;
    }

    // Parse conjuncts via _conjuncts_manager.
    RETURN_IF_ERROR(cm.parse_conjuncts(true, max_scan_key_num, enable_column_expr_predicate));

    // Get key_ranges and not_push_down_conjuncts from _conjuncts_manager.
    RETURN_IF_ERROR(_conjuncts_manager.get_key_ranges(&_key_ranges));
    _conjuncts_manager.get_not_push_down_conjuncts(&_not_push_down_conjuncts);

    _dict_optimize_parser.set_mutable_dict_maps(state, state->mutable_query_global_dict_map());
    _dict_optimize_parser.rewrite_conjuncts<false>(&_not_push_down_conjuncts, state);

    return Status::OK();
}

// ==================== OlapScanOperator ====================

OlapScanOperator::OlapScanOperator(OperatorFactory* factory, int32_t id, ScanNode* scan_node,
                                   std::atomic<ScanOperatorFactory::SharedPhase>& shared_phase)
        : ScanOperator(factory, id, scan_node, shared_phase) {}

Status OlapScanOperator::do_prepare(RuntimeState*) {
    RETURN_IF_ERROR(_capture_tablet_rowsets());
    return Status::OK();
}

Status OlapScanOperator::do_open_shared(RuntimeState* state) {
    RETURN_IF_ERROR(_get_factory()->parse_conjuncts(state));
    return ScanOperator::do_open_shared(state);
}

void OlapScanOperator::do_close(RuntimeState*) {}

Status OlapScanOperator::_capture_tablet_rowsets() {
    const auto& morsels = this->morsel_queue()->morsels();
    _tablet_rowsets.resize(morsels.size());
    for (int i = 0; i < morsels.size(); ++i) {
        ScanMorsel* scan_morsel = (ScanMorsel*)morsels[i].get();
        auto* scan_range = scan_morsel->get_olap_scan_range();

        // Get version.
        int64_t version = strtoul(scan_range->version.c_str(), nullptr, 10);

        // Get tablet.
        TTabletId tablet_id = scan_range->tablet_id;
        std::string err;
        TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, true, &err);
        if (!tablet) {
            std::stringstream ss;
            SchemaHash schema_hash = strtoul(scan_range->schema_hash.c_str(), nullptr, 10);
            ss << "failed to get tablet. tablet_id=" << tablet_id << ", with schema_hash=" << schema_hash
               << ", reason=" << err;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        // Capture row sets of this version tablet.
        {
            std::shared_lock l(tablet->get_header_lock());
            RETURN_IF_ERROR(tablet->capture_consistent_rowsets(Version(0, version), &_tablet_rowsets[i]));
        }
    }

    return Status::OK();
}

ChunkSourcePtr OlapScanOperator::create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) {
    auto* olap_scan_node = down_cast<vectorized::OlapScanNode*>(_scan_node);
    return std::make_shared<OlapChunkSource>(_chunk_source_profiles[chunk_source_index].get(), std::move(morsel), this,
                                             olap_scan_node);
}

} // namespace starrocks::pipeline
