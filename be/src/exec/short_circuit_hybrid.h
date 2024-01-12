// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "common/object_pool.h"
#include "common/status.h"
#include "exec/data_sink.h"
#include "exec/exec_node.h"
#include "exec/scan_node.h"
#include "gen_cpp/ShortCircuit_types.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "service/brpc.h"
#include "storage/table_reader.h"
#include "util/stopwatch.hpp"

namespace starrocks {

using TableReaderPtr = std::shared_ptr<TableReader>;
class TabletManager;

// scan use current thread instead of io thread pool asynchronously
class ShortCircuitHybridScanNode : public ScanNode {
public:
    ShortCircuitHybridScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs,
                               const TScanRange& scan_range, RuntimeProfile* runtime_profile,
                               TExecShortCircuitParams& common_request)
            : ScanNode(pool, tnode, descs),
              _tnode(tnode),
              _runtime_profile(runtime_profile),
              _common_request(common_request),
              _tuple_id(tnode.olap_scan_node.tuple_id) {}

    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;
    Status open(RuntimeState* state);
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos);

    Status _process_key_chunk();
    Status _process_value_chunk(std::vector<bool>& found);

private:
    const TPlanNode& _tnode;
    TableReaderPtr _table_reader;
    RuntimeProfile* _runtime_profile;
    TExecShortCircuitParams& _common_request;
    TDescriptorTable* _t_desc_tbl;
    ChunkPtr _key_chunk;
    ChunkPtr _value_chunk;
    const std::vector<TKeyLiteralExpr>* _key_literal_exprs;
    TupleDescriptor* _tuple_desc;
    std::vector<TabletSharedPtr> _tablets;
    TabletSchemaCSPtr _tablet_schema;
    TupleId _tuple_id;
    std::vector<string> _versions;
    int64_t _num_rows;
};
} // namespace starrocks
