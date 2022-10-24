// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/vectorized/lake_meta_scan_node.h"

namespace starrocks {
namespace vectorized {

LakeMetaScanNode::LakeMetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _is_init(false),
          _tuple_id(tnode.olap_scan_node.tuple_id),
          _meta_scan_node(tnode.meta_scan_node),
          _desc_tbl(descs) {}

LakeMetaScanNode::~LakeMetaScanNode() {
    if (runtime_state() != nullptr) {
        close(runtime_state());
    }
}

Status LakeMetaScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    return Status::OK();
}

void LakeMetaScanNode::_init_counter(RuntimeState* state) {
    _scan_timer = ADD_TIMER(_runtime_profile, "ScanTime");
    _meta_scan_profile = _runtime_profile->create_child("LAEK_META_SCAN", true, false);

    _io_timer = ADD_TIMER(_meta_scan_profile, "IOTime");
}

Status LakeMetaScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    for (auto& scan_range : scan_ranges) {
        _scan_ranges.emplace_back(std::make_unique<TInternalScanRange>(scan_range.scan_range.internal_scan_range));
        COUNTER_UPDATE(_tablet_counter, 1);
    }
    return Status::OK();
}

Status LakeMetaScanNode::prepare(RuntimeState* state) {
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

Status LakeMetaScanNode::open(RuntimeState* state) {
    // for debug
    LOG(INFO) << "enter LakeMetaScanNode";
    if (!_is_init) {
        return Status::InternalError("Open before Init.");
    }
    // for debug
    LOG(INFO) << "_scan_ranges.size is " << _scan_ranges.size();
    for (auto& scan_range : _scan_ranges) {
        LakeMetaScannerParams scanner_params;
        scanner_params.scan_range = scan_range.get();
        LakeMetaScanner* scanner = _obj_pool.add(new LakeMetaScanner(this));
        RETURN_IF_ERROR(scanner->init(state, scanner_params));
        _scanners.push_back(scanner);
    }

    if (_scanners.size() <= 0) {
        return Status::InternalError("Invalid ScanRange.");
    }

    DCHECK_GT(_scanners.size(), 0);
    RETURN_IF_ERROR(_scanners[_cursor_idx]->open(state));

    _cursor_idx = 0;

    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(ExecNode::open(state));
    return Status::OK();
}

Status LakeMetaScanNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    DCHECK(state != nullptr && chunk != nullptr && eos != nullptr);
    RETURN_IF_CANCELLED(state);

    if (!_scanners[_cursor_idx]->has_more()) {
        _scanners[_cursor_idx]->close(state);
        _cursor_idx++;
    }
    if (_cursor_idx >= _scanners.size()) {
        *eos = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(_scanners[_cursor_idx]->open(state));
    RETURN_IF_ERROR(_scanners[_cursor_idx]->get_chunk(state, chunk));
    return Status::OK();
}

Status LakeMetaScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }

    SCOPED_TIMER(_runtime_profile->total_time_counter());
    return ScanNode::close(state);
}

} // namespace vectorized

} // namespace starrocks
