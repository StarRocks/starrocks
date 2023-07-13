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

#pragma once

#include <gen_cpp/Descriptors_types.h>

#include "exec/scan_node.h"
#include "runtime/descriptors.h"

namespace starrocks {

class RuntimeState;

class MetaScanNode final : public starrocks::ScanNode {
public:
    MetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~MetaScanNode() override;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;
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

} // namespace starrocks
