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

#include "exec/tablet_sink_multi_sender.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/statusor.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"

namespace starrocks::stream_load {

Status TabletSinkMultiSender::send_chunk(const OlapTableSchemaParam* schema,
                                         const std::vector<OlapTablePartition*>& partitions,
                                         const std::vector<uint32_t>& tablet_indexes,
                                         const std::vector<uint16_t>& validate_select_idx,
                                         std::unordered_map<int64_t, std::set<int64_t>>& index_id_partition_id,
                                         Chunk* chunk) {
    size_t num_rows = chunk->num_rows();
    size_t selection_size = validate_select_idx.size();
    if (selection_size == 0) {
        return Status::OK();
    }
    _tablet_ids.resize(num_rows);
    if (num_rows > selection_size) {
        size_t index_size = partitions[validate_select_idx[0]]->indexes.size();
        for (size_t i = 0; i < index_size; ++i) {
            auto* index = schema->indexes()[i];
            for (size_t j = 0; j < selection_size; ++j) {
                uint16_t selection = validate_select_idx[j];
                index_id_partition_id[index->index_id].emplace(
                        partitions[selection]->associated_partition_ids[index->index_id]);
                _tablet_ids[selection] = partitions[selection]->indexes[i].tablets[tablet_indexes[selection]];
            }
            RETURN_IF_ERROR(_send_chunk_by_node(chunk, _channels[i], validate_select_idx));
        }
    } else { // Improve for all rows are selected
        size_t index_size = partitions[0]->indexes.size();
        for (size_t i = 0; i < index_size; ++i) {
            auto* index = schema->indexes()[i];
            for (size_t j = 0; j < num_rows; ++j) {
                index_id_partition_id[index->index_id].emplace(
                        partitions[j]->associated_partition_ids[index->index_id]);
                _tablet_ids[j] = partitions[j]->indexes[i].tablets[tablet_indexes[j]];
            }
            RETURN_IF_ERROR(_send_chunk_by_node(chunk, _channels[i], validate_select_idx));
        }
    }
    return Status::OK();
}

} // namespace starrocks::stream_load
