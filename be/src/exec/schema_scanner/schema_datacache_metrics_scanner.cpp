//
// Created by Smith on 2023/11/30.
//

#include "exec/schema_scanner/schema_datacache_metrics_scanner.h"


namespace starrocks {

SchemaScanner::ColumnDesc SchemaDataCacheMetricsScanner::_s_columns[] = {
    {"DATABASE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
    {"PIPE_ID", TYPE_BIGINT, sizeof(int64_t), false},
    {"PIPE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
    {"STATE", TYPE_VARCHAR, sizeof(StringValue), false},
    {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
    {"LOAD_STATUS", TYPE_VARCHAR, sizeof(StringValue), false},
    {"LAST_ERROR", TYPE_VARCHAR, sizeof(StringValue), false},
    {"CREATED_TIME", TYPE_DATETIME, sizeof(DateTimeValue), false},
};


SchemaDataCacheMetricsScanner::SchemaDataCacheMetricsScanner() : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}


DatumArray SchemaDataCacheMetricsScanner::_build_row() {
    return {
        Slice("1")
    };
}



Status SchemaDataCacheMetricsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    auto& slot_id_map = (*chunk)->get_slot_id_to_index_map();
    auto datum_array = _build_row();
    for (const auto& [slot_id, index] : slot_id_map) {
        Column* column = (*chunk)->get_column_by_slot_id(slot_id).get();
        column->append_datum(datum_array[slot_id - 1]);
    }
    *eos = true;
    return Status::OK();
}




}

