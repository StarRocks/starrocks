//
// Created by Smith on 2023/11/30.
//

#include "exec/schema_scanner/schema_be_datacache_metrics_scanner.h"

#include <agent/master_info.h>
#include <block_cache/block_cache.h>
#include <gutil/strings/substitute.h>

namespace starrocks {

TypeDescriptor SchemaBeDataCacheMetricsScanner::_used_bytes_detail_type = TypeDescriptor::create_struct_type(
        {"priority_1", "priority_2"},
        {TypeDescriptor::from_logical_type(TYPE_BIGINT), TypeDescriptor::from_logical_type(TYPE_BIGINT)});
;

SchemaScanner::ColumnDesc SchemaBeDataCacheMetricsScanner::_s_columns[] = {
        {"BE_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64), false},
        {"STATUS", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"DISK_QUOTA_BYTES", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64), true},
        {"DISK_USED_BYTES", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64), true},
        {"MEM_QUOTA_BYTES", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64), true},
        {"MEM_USED_BYTES", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64), true},
        {"META_USED_BYTES", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64), true},
        {"DIR_SPACES", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
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
    if (_is_fetched) {
        *eos = true;
        return Status::OK();
    }

    DatumArray row{};
    std::string status{};
    std::string dir_spaces_str{};

    row.emplace_back(_be_id);

    if (config::datacache_enable) {
        const BlockCache* cache = BlockCache::instance();
        const DataCacheMetrics& metrics = cache->cache_metrics(2);

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
        dir_spaces_str.append("[");
        for (size_t i = 0; i < dir_spaces.size(); i++) {
            const auto& dir_space = dir_spaces[i];
            dir_spaces_str.append(
                    strings::Substitute(R"({"Path":"$0","QuotaBytes":$1})", dir_space.path, dir_space.quota_bytes));
            if (i < dir_spaces.size() - 1) {
                dir_spaces_str.append(",");
            }
        }
        dir_spaces_str.append("]");
        row.emplace_back(Slice(dir_spaces_str));

        // retrive different priority's used bytes from L2 metrics
        int64_t priority_0_used_bytes = 0;
        int64_t priority_1_used_bytes = 0;
        const auto& l2_metrics = metrics.detail_l2;
        if (l2_metrics != nullptr) {
            if (l2_metrics->prior_item_bytes.contains(0)) {
                priority_0_used_bytes = l2_metrics->prior_item_bytes[0];
            }
            if (l2_metrics->prior_item_bytes.contains(1)) {
                priority_1_used_bytes = l2_metrics->prior_item_bytes[1];
            }
        }
        row.emplace_back(DatumStruct{priority_0_used_bytes, priority_1_used_bytes});
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
}

} // namespace starrocks
