// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
#include "exec/vectorized/olap_meta_scan_node.h"

namespace starrocks {
namespace vectorized {

OlapMetaScanNode::OlapMetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs) 
        : ScanNode(pool, tnode, descs),
          _scanner_cursor(nullptr),
          _is_init(false),
          _tuple_id(tnode.olap_scan_node.tuple_id) {}

OlapMetaScanNode::~OlapMetaScanNode() {}

Status OlapMetaScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    return Status::OK();
}

void OlapMetaScanNode::_init_counter(RuntimeState* state) {
    return;
}

Status OlapMetaScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    for (auto& scan_range : scan_ranges) {
        _scan_ranges.emplace_back(new TInternalScanRange(scan_range.scan_range.internal_scan_range));
    }

    return Status::OK();
}

Status OlapMetaScanNode::prepare(RuntimeState* state) {
    if (_is_init) {
        return Status::OK();
    }

    RETURN_IF_ERROR(ScanNode::prepare(state));

    // get desc tuple desc
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    if (nullptr == _tuple_desc) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    for (auto& scan_range : _scan_ranges) {
        OlapMetaScannerParams scanner_params;
        scanner_params.scan_range = scan_range.get();
        auto* scanner = _obj_pool.add(new OlapMetaScanner(this));
        RETURN_IF_ERROR(scanner->init(state, scanner_params));
        _scanners.push_back(scanner);
    }

    if (_scanners.size() <= 0) {
        return Status::InternalError("Invalid ScanRange.");
    }

    _scanner_cursor = _scanners.front();
    _end_scanner = _scanners.back();
    _is_init = true;
    return Status::OK();
}

Status OlapMetaScanNode::open(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("Open before Init.");
    }
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(ExecNode::open(state));
    return Status::OK();
}

Status OlapMetaScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("get next for row_batch is not supported");
}

Status OlapMetaScanNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    DCHECK(state != nullptr && chunk != nullptr && eos != nullptr);
    RETURN_IF_CANCELLED(state);
    if (!_scanner_cursor->has_more()) {
        _next_cursor();  
    } 
    if (!_scanner_cursor) {
        *eos = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(_scanner_cursor->get_chunk(state, chunk)); 
    return Status::OK();
}

void OlapMetaScanNode::_next_cursor() {
    _scanner_cursor++;
    if (_scanner_cursor > _end_scanner) { _scanner_cursor = nullptr; }
}

Status OlapMetaScanNode::close(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    return ScanNode::close(state);
}

} // namespace vectorized

} // namespace starrocks
