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

#include "exec/tablet_sink_sender.h"

namespace starrocks::stream_load {
// TabletSinkSender will control one index/table's send chunks.
class TabletSinkMultiSender final : public TabletSinkSender {
public:
    TabletSinkMultiSender(PUniqueId load_id, int64_t txn_id, IndexIdToTabletBEMap index_id_to_tablet_be_map,
                          OlapTablePartitionParam* vectorized_partition, std::vector<IndexChannel*> channels,
                          std::unordered_map<int64_t, NodeChannel*> node_channels,
                          std::vector<ExprContext*> output_expr_ctxs, bool enable_replicated_storage,
                          TWriteQuorumType::type write_quorum_type, int num_repicas)
            : TabletSinkSender(load_id, txn_id, std::move(index_id_to_tablet_be_map), vectorized_partition,
                               std::move(channels), std::move(node_channels), std::move(output_expr_ctxs),
                               enable_replicated_storage, write_quorum_type, num_repicas) {}
    ~TabletSinkMultiSender() = default;

public:
    Status send_chunk(const OlapTableSchemaParam* schema, const std::vector<OlapTablePartition*>& partitions,
                      const std::vector<uint32_t>& tablet_indexes, const std::vector<uint16_t>& validate_select_idx,
                      std::unordered_map<int64_t, std::set<int64_t>>& index_id_partition_id, Chunk* chunk) override;
};

} // namespace starrocks::stream_load
