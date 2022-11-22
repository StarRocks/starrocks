// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include <gen_cpp/Descriptors_types.h>

#include "exec/scan_node.h"
#include "runtime/descriptors.h"

namespace starrocks {

class RuntimeState;

namespace vectorized {

class MetaScanNode : public starrocks::ScanNode {
public:
    MetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~MetaScanNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    ;
    Status close(RuntimeState* state) override;
    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

protected:
    bool _is_init;
    size_t _cursor_idx = 0;
    // params
    std::vector<std::unique_ptr<TInternalScanRange>> _scan_ranges;
    ObjectPool _obj_pool;
    DescriptorTbl _desc_tbl;
    TMetaScanNode _meta_scan_node;

private:
    void _init_counter(RuntimeState* state);
    friend class MetaScanner;

    TupleId _tuple_id;
    const TupleDescriptor* _tuple_desc = nullptr;

    // profile
    RuntimeProfile* _meta_scan_profile = nullptr;
    RuntimeProfile::Counter* _scan_timer = nullptr;
    RuntimeProfile::Counter* _io_timer = nullptr;
    RuntimeProfile::Counter* _tablet_counter = nullptr;
};

} // namespace vectorized
} // namespace starrocks
