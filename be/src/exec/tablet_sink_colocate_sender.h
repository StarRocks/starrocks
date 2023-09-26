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

// TabletSinkColocateSender will control one index/table's send chunks.
class TabletSinkColocateSender final : public TabletSinkSender {
public:
    TabletSinkColocateSender(PUniqueId load_id, int64_t txn_id, IndexIdToTabletBEMap index_id_to_tablet_be_map,
                             OlapTablePartitionParam* vectorized_partition, std::vector<IndexChannel*> channels,
                             std::unordered_map<int64_t, NodeChannel*> node_channels,
                             std::vector<ExprContext*> output_expr_ctxs, bool enable_replicated_storage,
                             TWriteQuorumType::type write_quorum_type, int num_repicas);

    ~TabletSinkColocateSender() = default;

public:
    Status send_chunk(const OlapTableSchemaParam* schema, const std::vector<OlapTablePartition*>& partitions,
                      const std::vector<uint32_t>& tablet_indexes, const std::vector<uint16_t>& validate_select_idx,
                      std::unordered_map<int64_t, std::set<int64_t>>& index_id_partition_id, Chunk* chunkk) override;

    Status try_open(RuntimeState* state) override;
    Status open_wait() override;
    // async close interface: try_close() -> [is_close_done()] -> close_wait()
    // if is_close_done() return true, close_wait() will not block
    // otherwise close_wait() will block
    Status try_close(RuntimeState* state) override;
    Status close_wait(RuntimeState* state, Status close_status, TabletSinkProfile* ts_profile) override;

    bool is_open_done() override;
    bool is_full() override;
    bool is_close_done() override;

    bool get_immutable_partition_ids(std::set<int64_t>* partition_ids) override;

private:
    Status _send_chunks(const OlapTableSchemaParam* schema, Chunk* chunk,
                        const std::vector<std::vector<int64_t>>& index_tablet_ids,
                        const std::vector<uint16_t>& selection_idx);

    bool _colocate_mv_index{true};
    std::vector<std::vector<int64_t>> _index_tablet_ids;
};

} // namespace starrocks::stream_load
