// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/vectorized/lake_meta_scan_node.h"

namespace starrocks {
namespace vectorized {

LakeMetaScanNode::LakeMetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : MetaScanNode(pool, tnode, descs){}

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


} // namespace vectorized

} // namespace starrocks
