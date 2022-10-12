// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include <exec/olap_common.h>
#include <gen_cpp/Descriptors_types.h>

#include "exec/scan_node.h"
#include "exec/vectorized/olap_meta_scanner.h"
#include "runtime/descriptors.h"

namespace starrocks {

class RuntimeState;

namespace vectorized {

class OlapMetaScanNode final : public starrocks::ScanNode {
public:
    OlapMetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~OlapMetaScanNode();

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    Status close(RuntimeState* state) override;
    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

    void debug_string(int indentation_level, std::stringstream* out) const override {
        *out << "vectorized:OlapMetaScanNode";
    }

private:
    void _init_counter(RuntimeState* state);
    friend class OlapMetaScanner;

    // params
    std::vector<std::unique_ptr<TInternalScanRange>> _scan_ranges;

    std::vector<OlapMetaScanner*> _scanners;
    size_t _cursor_idx = 0;

    bool _is_init;
    TupleId _tuple_id;
    TMetaScanNode _meta_scan_node;
    DescriptorTbl _desc_tbl;
    const TupleDescriptor* _tuple_desc = nullptr;
    ObjectPool _obj_pool;

    // profile
    RuntimeProfile* _meta_scan_profile = nullptr;
    RuntimeProfile::Counter* _scan_timer = nullptr;
    RuntimeProfile::Counter* _io_timer = nullptr;
    RuntimeProfile::Counter* _tablet_counter = nullptr;
};

} // namespace vectorized
} // namespace starrocks
