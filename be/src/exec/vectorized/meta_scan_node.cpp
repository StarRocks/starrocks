// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/vectorized/meta_scan_node.h"


namespace starrocks {
namespace vectorized {

MetaScanNode::MetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _is_init(false),
          _desc_tbl(descs),
          _meta_scan_node(tnode.meta_scan_node),
          _tuple_id(tnode.olap_scan_node.tuple_id){}

MetaScanNode::~MetaScanNode() {
    if (runtime_state() != nullptr) {
        close(runtime_state());
    }
}

Status MetaScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    return Status::OK();
}

void MetaScanNode::_init_counter(RuntimeState* state) {
    _scan_timer = ADD_TIMER(_runtime_profile, "ScanTime");
    _meta_scan_profile = _runtime_profile->create_child("META_SCAN", true, false);

    _io_timer = ADD_TIMER(_meta_scan_profile, "IOTime");
}

Status MetaScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    for (auto& scan_range : scan_ranges) {
        _scan_ranges.emplace_back(std::make_unique<TInternalScanRange>(scan_range.scan_range.internal_scan_range));
        COUNTER_UPDATE(_tablet_counter, 1);
    }
    return Status::OK();
}

Status MetaScanNode::prepare(RuntimeState* state) {
    if (_is_init) {
        return Status::OK();
    }

    RETURN_IF_ERROR(ScanNode::prepare(state));
    _tablet_counter = ADD_COUNTER(runtime_profile(), "TabletCount ", TUnit::UNIT);

    // get desc tuple desc
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);

    if (nullptr == _tuple_desc) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    _is_init = true;
    return Status::OK();
}

Status MetaScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }

    SCOPED_TIMER(_runtime_profile->total_time_counter());
    return ScanNode::close(state);
}


} // namespace vectorized

} // namespace starrocks
