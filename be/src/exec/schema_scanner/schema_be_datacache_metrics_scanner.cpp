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

#include "exec/schema_scanner/schema_be_datacache_metrics_scanner.h"

#include "agent/master_info.h"
#include "cache/block_cache/block_cache.h"
#include "column/datum.h"
#include "gutil/strings/substitute.h"
#include "runtime/string_value.h"

namespace starrocks {

TypeDescriptor SchemaBeDataCacheMetricsScanner::_dir_spaces_type = TypeDescriptor::create_array_type(
        TypeDescriptor::create_struct_type({"path", "quota_bytes"}, {TypeDescriptor::from_logical_type(TYPE_VARCHAR),
                                                                     TypeDescriptor::from_logical_type(TYPE_BIGINT)}));

TypeDescriptor SchemaBeDataCacheMetricsScanner::_used_bytes_detail_type = TypeDescriptor::create_map_type(
        TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_BIGINT));

SchemaScanner::ColumnDesc SchemaBeDataCacheMetricsScanner::_s_columns[] = {
        {"BE_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64), false},
        {"STATUS", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"DISK_QUOTA_BYTES", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64), true},
        {"DISK_USED_BYTES", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64), true},
        {"MEM_QUOTA_BYTES", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64), true},
        {"MEM_USED_BYTES", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64), true},
        {"META_USED_BYTES", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64), true},
        {"DIR_SPACES", _dir_spaces_type, 16, true},
        {"USED_BYTES_DETAIL", _used_bytes_detail_type, 16, true}};

SchemaBeDataCacheMetricsScanner::SchemaBeDataCacheMetricsScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(ColumnDesc)) {}

Status SchemaBeDataCacheMetricsScanner::start(RuntimeState* state) {
    RETURN_IF_ERROR(SchemaScanner::start(state));
    const auto& o_id = get_backend_id();
    _be_id = o_id.has_value() ? o_id.value() : -1;
    return Status::OK();
}

Status SchemaBeDataCacheMetricsScanner::get_next(ChunkPtr* chunk, bool* eos) {
#ifdef WITH_STARCACHE
    if (_is_fetched) {
        *eos = true;
        return Status::OK();
    }

    DatumArray row{};
    std::string status{};
    DataCacheMetrics metrics{};

    row.emplace_back(_be_id);

    if (config::datacache_enable) {
        const BlockCache* cache = BlockCache::instance();
        // retrive different priority's used bytes from level = 2 metrics
        metrics = cache->cache_metrics(2);

        switch (metrics.status) {
        case DataCacheStatus::NORMAL:
            status = "Normal";
            break;
        case DataCacheStatus::UPDATING:
            status = "Updating";
            break;
        default:
            status = "Abnormal";
        }

        row.emplace_back(Slice(status));
        row.emplace_back(metrics.disk_quota_bytes);
        row.emplace_back(metrics.disk_used_bytes);
        row.emplace_back(metrics.mem_quota_bytes);
        row.emplace_back(metrics.mem_used_bytes);
        row.emplace_back(metrics.meta_used_bytes);

        const auto& dir_spaces = metrics.disk_dir_spaces;
        DatumArray dir_spaces_array{};
        for (size_t i = 0; i < dir_spaces.size(); i++) {
            const auto& dir_space = dir_spaces[i];
            dir_spaces_array.emplace_back(DatumStruct{Slice(dir_space.path), dir_space.quota_bytes});
        }
        row.emplace_back(dir_spaces_array);

        DatumMap datum_map{};
        const auto& l2_metrics = metrics.detail_l2;
        if (l2_metrics != nullptr) {
            std::map<int32_t, int64_t> sorted_map{l2_metrics->prior_item_bytes.begin(),
                                                  l2_metrics->prior_item_bytes.end()};
            for (const auto& pair : sorted_map) {
                datum_map.emplace(pair.first, pair.second);
            }
        }
        row.emplace_back(datum_map);
    } else {
        status = "Disabled";
        row.emplace_back(Slice(status));
        row.emplace_back(kNullDatum);
        row.emplace_back(kNullDatum);
        row.emplace_back(kNullDatum);
        row.emplace_back(kNullDatum);
        row.emplace_back(kNullDatum);
        row.emplace_back(kNullDatum);
        row.emplace_back(kNullDatum);
    }

    for (const auto& [slot_id, index] : (*chunk)->get_slot_id_to_index_map()) {
        const ColumnPtr& column = (*chunk)->get_column_by_slot_id(slot_id);
        column->append_datum(row[slot_id - 1]);
    }

    *eos = false;
    _is_fetched = true;
    return Status::OK();
#else
    *eos = true;
    return Status::OK();
#endif
}

} // namespace starrocks
