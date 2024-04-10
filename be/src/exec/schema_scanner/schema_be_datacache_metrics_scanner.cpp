//
// Created by Smith on 2023/11/30.
//

#include "exec/schema_scanner/schema_be_datacache_metrics_scanner.h"

#include <agent/master_info.h>
#include <block_cache/block_cache.h>
#include <gutil/strings/substitute.h>

namespace starrocks {

SchemaScanner::ColumnDesc SchemaBeDataCacheMetricsScanner::_s_columns[] = {
        {"BE_ID", TYPE_BIGINT, sizeof(int64), false},
        {"STATUS", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DISK_QUOTA_BYTES", TYPE_BIGINT, sizeof(int64), true},
        {"DISK_USED_BYTES", TYPE_BIGINT, sizeof(int64), true},
        {"MEM_QUOTA_BYTES", TYPE_BIGINT, sizeof(int64), true},
        {"MEM_USED_BYTES", TYPE_BIGINT, sizeof(int64), true},
        {"META_USED_BYTES", TYPE_BIGINT, sizeof(int64), true},
        {"DIR_SPACES", TYPE_VARCHAR, sizeof(StringValue), true}};

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
        const DataCacheMetrics& metrics = cache->cache_metrics();

        switch (metrics.status) {
        case starcache::CacheStatus::NORMAL:
            status = "Normal";
            break;
        case starcache::CacheStatus::UPDATING:
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
                    strings::Substitute("{\"Path\":\"$0\",\"QuotaBytes\":$1}", dir_space.path, dir_space.quota_bytes));
            if (i < dir_spaces.size() - 1) {
                dir_spaces_str.append(",");
            }
        }
        dir_spaces_str.append("]");
        row.emplace_back(Slice(dir_spaces_str));
    } else {
        status = "Disabled";
        row.emplace_back(Slice(status));
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
