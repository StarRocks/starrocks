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

void OlapScanOperatorFactory::do_close(RuntimeState*) {}

OperatorPtr OlapScanOperatorFactory::do_create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<OlapScanOperator>(this, _id, _scan_node);
}

// ==================== OlapScanOperator ====================

OlapScanOperator::OlapScanOperator(OperatorFactory* factory, int32_t id, ScanNode* scan_node)
        : ScanOperator(factory, id, scan_node) {}

Status OlapScanOperator::do_prepare(RuntimeState*) {
    RETURN_IF_ERROR(_capture_tablet_rowsets());
    return Status::OK();
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
    vectorized::OlapScanNode* olap_scan_node = down_cast<vectorized::OlapScanNode*>(_scan_node);
    return std::make_shared<OlapChunkSource>(_chunk_source_profiles[chunk_source_index].get(), std::move(morsel), this,
                                             olap_scan_node);
}

} // namespace starrocks::pipeline
