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

#include "exec/tablet_sink/tablet_sink_colocate_multi_sender.h"

#include "agent/master_info.h"
#include "agent/utils.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "common/statusor.h"
#include "config.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/stream_epoch_manager.h"
#include "exec/tablet_sink.h"
#include "exec/tablet_sink/tablet_sink_colocate_sender.h"
#include "exprs/expr.h"
#include "gutil/strings/fastmem.h"
#include "gutil/strings/join.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "serde/protobuf_serde.h"
#include "simd/simd.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "types/hll.h"
#include "util/brpc_stub_cache.h"
#include "util/compression/compression_utils.h"
#include "util/defer_op.h"
#include "util/thread.h"
#include "util/thrift_rpc_helper.h"
#include "util/uid_util.h"

namespace starrocks::stream_load {

Status TabletSinkColocateMultiSender::send_chunk(const OlapTableSchemaParam* schema,
                                                 const std::vector<OlapTablePartition*>& partitions,
                                                 const std::vector<uint32_t>& tablet_indexes,
                                                 const std::vector<uint16_t>& validate_select_idx,
                                                 std::unordered_map<int64_t, std::set<int64_t>>& index_id_partition_id,
                                                 Chunk* chunk) {
    if (UNLIKELY(!_colocate_mv_index)) {
        return TabletSinkSender::send_chunk(schema, partitions, tablet_indexes, validate_select_idx,
                                            index_id_partition_id, chunk);
    }

    Status err_st = Status::OK();
    size_t num_rows = chunk->num_rows();
    size_t selection_size = validate_select_idx.size();
    if (selection_size == 0) {
        return Status::OK();
    }

    if (num_rows > selection_size) {
        size_t index_size = partitions[validate_select_idx[0]]->indexes.size();
        _index_tablet_ids.resize(index_size);
        for (size_t i = 0; i < index_size; ++i) {
            _index_tablet_ids[i].resize(num_rows);
            auto* index = schema->indexes()[i];
            for (size_t j = 0; j < selection_size; ++j) {
                uint16_t selection = validate_select_idx[j];
                index_id_partition_id[index->index_id].emplace(
                        partitions[selection]->associated_partition_ids[index->index_id]);
                _tablet_ids[selection] = partitions[selection]->indexes[i].tablets[tablet_indexes[selection]];
                _index_tablet_ids[i][selection] = partitions[selection]->indexes[i].tablets[tablet_indexes[selection]];
            }
        }
        return _send_chunks(schema, chunk, _index_tablet_ids, validate_select_idx);
    } else { // Improve for all rows are selected
        size_t index_size = partitions[0]->indexes.size();
        _index_tablet_ids.resize(index_size);
        for (size_t i = 0; i < index_size; ++i) {
            auto* index = schema->indexes()[i];
            _index_tablet_ids[i].resize(num_rows);
            for (size_t j = 0; j < num_rows; ++j) {
                index_id_partition_id[index->index_id].emplace(
                        partitions[j]->associated_partition_ids[index->index_id]);
                _index_tablet_ids[i][j] = partitions[j]->indexes[i].tablets[tablet_indexes[j]];
            }
        }
        return _send_chunks(schema, chunk, _index_tablet_ids, validate_select_idx);
    }
}

} // namespace starrocks::stream_load
