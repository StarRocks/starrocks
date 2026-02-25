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

#include "exec/range_router.h"
#include "exec/tablet_sink_sender.h"

namespace starrocks {

// RangeTabletSinkSender specializes TabletSinkSender for range distribution
class RangeTabletSinkSender : public TabletSinkSender {
public:
    RangeTabletSinkSender(PUniqueId load_id, int64_t txn_id, IndexIdToTabletBEMap index_id_to_tablet_be_map,
                          OlapTablePartitionParam* partition_params, std::vector<IndexChannel*> channels,
                          std::unordered_map<int64_t, NodeChannel*> node_channels,
                          std::vector<ExprContext*> output_expr_ctxs, bool enable_replicated_storage,
                          TWriteQuorumType::type write_quorum_type, int num_repicas)
            : TabletSinkSender(std::move(load_id), txn_id, std::move(index_id_to_tablet_be_map), partition_params,
                               std::move(channels), std::move(node_channels), std::move(output_expr_ctxs),
                               enable_replicated_storage, write_quorum_type, num_repicas) {}

    ~RangeTabletSinkSender() override = default;

    // Override send_chunk to implement range-based routing
    Status send_chunk(const OlapTableSchemaParam* schema, const std::vector<OlapTablePartition*>& partitions,
                      const std::vector<uint32_t>& record_hashes, const std::vector<uint16_t>& validate_select_idx,
                      std::unordered_map<int64_t, std::set<int64_t>>& index_id_partition_id, Chunk* chunk) override;

private:
    // index_id -> partition_id -> range router
    std::unordered_map<int64_t, std::unordered_map<int64_t, std::unique_ptr<RangeRouter>>> _range_routers;
    std::vector<int64_t> _cur_result_tablet_ids;
};

} // namespace starrocks