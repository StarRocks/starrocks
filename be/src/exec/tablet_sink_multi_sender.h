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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/tablet_sink.h

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

#include <memory>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "common/tracer.h"
#include "exec/data_sink.h"
#include "exec/tablet_info.h"
#include "exec/tablet_sink_index_channel.h"
#include "exec/tablet_sink_sender.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/doris_internal_service.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/mem_tracker.h"
#include "util/compression/block_compression.h"
#include "util/raw_container.h"
#include "util/ref_count_closure.h"
#include "util/reusable_closure.h"
#include "util/threadpool.h"

namespace starrocks {

class MemTracker;
class RuntimeProfile;
class RowDescriptor;
class TupleDescriptor;
class ExprContext;
class TExpr;

namespace stream_load {
// TabletSinkSender will control one index/table's send chunks.
class TabletSinkMultiSender : public TabletSinkSender {
public:
    TabletSinkMultiSender(PUniqueId load_id, int64_t txn_id, OlapTableLocationParam* location,
                          OlapTablePartitionParam* vectorized_partition, std::vector<IndexChannel*> channels,
                          std::unordered_map<int64_t, NodeChannel*> node_channels,
                          std::vector<ExprContext*> output_expr_ctxs, bool enable_replicated_storage,
                          TWriteQuorumType::type write_quorum_type, int num_repicas)
            : TabletSinkSender(load_id, txn_id, location, vectorized_partition, std::move(channels),
                               std::move(node_channels), std::move(output_expr_ctxs), enable_replicated_storage,
                               write_quorum_type, num_repicas) {}
    ~TabletSinkMultiSender() = default;

public:
    Status send_chunk(std::shared_ptr<OlapTableSchemaParam> schema, const std::vector<OlapTablePartition*>& partitions,
                      const std::vector<uint32_t>& tablet_indexes, const std::vector<uint16_t>& validate_select_idx,
                      std::unordered_map<int64_t, std::set<int64_t>>& index_id_partition_id, Chunk* chunk) override;
};

} // namespace stream_load
} // namespace starrocks
