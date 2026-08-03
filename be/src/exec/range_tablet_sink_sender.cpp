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

#include "exec/range_tablet_sink_sender.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/statusor.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "fmt/format.h"
#include "runtime/descriptors.h"

namespace starrocks {

std::vector<TypeDescriptor> RangeTabletSinkSender::routing_key_types(const OlapTableIndexSchema* index_schema) const {
    std::vector<TypeDescriptor> key_types;
    if (index_schema->has_distributed_exprs) {
        // Per-index routing: key types come from the expr roots.
        key_types.reserve(index_schema->distributed_expr_ctxs.size());
        for (auto* ctx : index_schema->distributed_expr_ctxs) {
            key_types.push_back(ctx->root()->type());
        }
    } else {
        // Fallback: route by the shared partition distribution slots.
        const auto& slot_descs = _partition_params->distribution_slot_descs();
        key_types.reserve(slot_descs.size());
        for (auto* slot_desc : slot_descs) {
            key_types.push_back(slot_desc->type());
        }
    }
    return key_types;
}

Status RangeTabletSinkSender::send_chunk(const OlapTableSchemaParam* schema,
                                         const std::vector<OlapTablePartition*>& partitions,
                                         const std::vector<uint32_t>& record_hashes,
                                         const std::vector<uint16_t>& validate_select_idx,
                                         std::unordered_map<int64_t, std::set<int64_t>>& index_id_partition_id,
                                         Chunk* chunk) {
    if (validate_select_idx.empty()) {
        return Status::OK();
    }

    if (_tablet_ids.size() < chunk->num_rows()) {
        _tablet_ids.resize(chunk->num_rows());
    }

    std::unordered_map<int64_t, OlapTablePartition*> partition_map;
    std::unordered_map<int64_t, std::vector<uint16_t>> partition_row_indices_map;

    // 1. Shuffle rows into batches by partition
    for (uint16_t i : validate_select_idx) {
        int64_t partition_id = partitions[i]->id;

        partition_map[partition_id] = partitions[i];
        auto& row_indices = partition_row_indices_map[partition_id];
        if (row_indices.empty()) {
            row_indices.reserve(chunk->num_rows());
        }
        row_indices.push_back(i);
    }

    // 2. Process each index
    size_t index_size = schema->indexes().size();
    for (size_t i = 0; i < index_size; ++i) {
        auto* index_schema = schema->indexes()[i];
        DCHECK(_partition_params != nullptr && _partition_params->is_range_distribution());
        DCHECK(_partition_params->distribution_slot_descs().size() <= chunk->num_columns());

        // NOTE: Per-index range routing is sender-only. The proto POlapTableIndexSchema
        // intentionally has no `distributed_exprs` field, so remote add-chunks BEs (delta
        // writer) cannot route by per-index keys; routing must be resolved here.
        const bool has_distributed_exprs = index_schema->has_distributed_exprs;
        const auto& distributed_expr_ctxs = index_schema->distributed_expr_ctxs;

        // Evaluate this index's per-index distribution exprs ONCE per chunk (not per partition).
        // The result key columns are reused for every partition's route_chunk_rows call below.
        // Columns are owned here and kept alive past all routing for this index. Only the
        // non-empty distributed_exprs path uses these; K=1 (empty) and the fallback path do not.
        std::vector<ColumnPtr> key_columns;
        if (has_distributed_exprs && !distributed_expr_ctxs.empty()) {
            key_columns.reserve(distributed_expr_ctxs.size());
            for (auto* ctx : distributed_expr_ctxs) {
                ASSIGN_OR_RETURN(auto col, ctx->evaluate(chunk));
                key_columns.push_back(
                        ColumnHelper::unfold_const_column(ctx->root()->type(), chunk->num_rows(), std::move(col)));
            }
        }

        // Process each partition batch
        for (auto& [part_id, part] : partition_map) {
            const auto& row_indices = partition_row_indices_map[part_id];
            const auto& candidate_tablet_ids = part->indexes[i].tablet_ids;

            if (has_distributed_exprs && distributed_expr_ctxs.empty()) {
                // K=1: empty routing key => single tablet. The router would error on empty
                // boundaries, so fill the single candidate directly.
                if (candidate_tablet_ids.size() != 1) {
                    return Status::InternalError(
                            fmt::format("index {} partition {} declares empty distributed_exprs (K=1) but has {} "
                                        "candidate tablets",
                                        index_schema->index_id, part_id, candidate_tablet_ids.size()));
                }
                for (uint16_t row_idx : row_indices) {
                    _tablet_ids[row_idx] = candidate_tablet_ids[0];
                }
                index_id_partition_id[index_schema->index_id].emplace(part_id);
                continue;
            }

            // Get or create range router for this partition and index.
            // Use try_emplace to avoid double lookup
            auto [router_iter, inserted] = _range_routers[index_schema->index_id].try_emplace(part_id);

            RangeRouter* router_ptr = nullptr;
            if (inserted) {
                // New entry, need to initialize router
                const auto& tablets = part->indexes[i].tablets;
                std::vector<TTabletRange> tablet_ranges;
                tablet_ranges.reserve(tablets.size());
                for (const auto& tablet : tablets) {
                    DCHECK(tablet.__isset.range);
                    tablet_ranges.emplace_back(tablet.range);
                }
                auto router = std::make_unique<RangeRouter>();
                RETURN_IF_ERROR(router->init(tablet_ranges, routing_key_types(index_schema)));
                router_ptr = router.get();
                router_iter->second = std::move(router);
            } else {
                // Entry already exists
                router_ptr = router_iter->second.get();
            }

            DCHECK(router_ptr != nullptr);
            if (has_distributed_exprs) {
                // Route by the per-index key columns evaluated once above for this chunk.
                RETURN_IF_ERROR(router_ptr->route_chunk_rows(key_columns, row_indices, candidate_tablet_ids,
                                                             &_cur_result_tablet_ids));
            } else {
                const auto& slot_descs = _partition_params->distribution_slot_descs();
                RETURN_IF_ERROR(router_ptr->route_chunk_rows(chunk, slot_descs, row_indices, candidate_tablet_ids,
                                                             &_cur_result_tablet_ids));
            }
            DCHECK(_cur_result_tablet_ids.size() == row_indices.size());
            for (size_t j = 0; j < row_indices.size(); ++j) {
                _tablet_ids[row_indices[j]] = _cur_result_tablet_ids[j];
            }
            index_id_partition_id[index_schema->index_id].emplace(part_id);
        }

        // 3. Send chunk for this index
        // _send_chunk_by_node uses _tablet_ids and validate_select_idx
        RETURN_IF_ERROR(_send_chunk_by_node(chunk, _channels[i], validate_select_idx));
    }
    return Status::OK();
}

} // namespace starrocks