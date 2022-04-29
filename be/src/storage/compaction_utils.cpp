// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/compaction_utils.h"

#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/vectorized/row_source_mask.h"

namespace starrocks {

const char* CompactionUtils::compaction_algorithm_to_string(CompactionAlgorithm v) {
    switch (v) {
    case HORIZONTAL_COMPACTION:
        return "HORIZONTAL_COMPACTION";
    case VERTICAL_COMPACTION:
        return "VERTICAL_COMPACTION";
    default:
        return "[Unknown CompactionAlgorithm]";
    }
}

int32_t CompactionUtils::get_read_chunk_size(int64_t mem_limit, int32_t config_chunk_size, int64_t total_num_rows,
                                             int64_t total_mem_footprint, size_t source_num) {
    uint64_t chunk_size = config_chunk_size;
    if (mem_limit > 0) {
        int64_t avg_row_size = (total_mem_footprint + 1) / (total_num_rows + 1);
        // The result of the division operation be zero, so added one
        chunk_size = 1 + mem_limit / (source_num * avg_row_size + 1);
    }

    if (chunk_size > config_chunk_size) {
        chunk_size = config_chunk_size;
    }
    return chunk_size;
}

Status CompactionUtils::construct_output_rowset_writer(Tablet* tablet, uint32_t max_rows_per_segment,
                                                       CompactionAlgorithm algorithm, Version version,
                                                       std::unique_ptr<RowsetWriter>* output_rowset_writer) {
    RowsetWriterContext context(kDataFormatV2, config::storage_format_version);
    context.rowset_id = StorageEngine::instance()->next_rowset_id();
    context.tablet_uid = tablet->tablet_uid();
    context.tablet_id = tablet->tablet_id();
    context.partition_id = tablet->partition_id();
    context.tablet_schema_hash = tablet->schema_hash();
    context.rowset_type = BETA_ROWSET;
    context.rowset_path_prefix = tablet->schema_hash_path();
    context.tablet_schema = &(tablet->tablet_schema());
    context.rowset_state = VISIBLE;
    context.version = version;
    context.segments_overlap = NONOVERLAPPING;
    context.max_rows_per_segment = max_rows_per_segment;
    context.writer_type =
            (algorithm == VERTICAL_COMPACTION ? RowsetWriterType::kVertical : RowsetWriterType::kHorizontal);
    Status st = RowsetFactory::create_rowset_writer(context, output_rowset_writer);
    if (!st.ok()) {
        std::stringstream ss;
        ss << "Fail to create rowset writer. tablet_id=" << context.tablet_id << " err=" << st;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

uint32_t CompactionUtils::get_segment_max_rows(int64_t max_segment_file_size, int64_t input_row_num,
                                               int64_t input_size) {
    // The range of config::max_segments_file_size is between [1, INT64_MAX]
    // If the configuration is set wrong, the config::max_segments_file_size will be a negtive value.
    // Using division instead multiplication can avoid the overflow
    int64_t max_segment_rows = max_segment_file_size / (input_size / (input_row_num + 1) + 1);
    if (max_segment_rows > INT32_MAX || max_segment_rows <= 0) {
        max_segment_rows = INT32_MAX;
    }
    return max_segment_rows;
}

void CompactionUtils::split_column_into_groups(size_t num_columns, size_t num_key_columns,
                                               int64_t max_columns_per_group,
                                               std::vector<std::vector<uint32_t>>* column_groups) {
    std::vector<uint32_t> key_columns;
    for (size_t i = 0; i < num_key_columns; ++i) {
        key_columns.emplace_back(i);
    }
    column_groups->emplace_back(std::move(key_columns));

    for (size_t i = num_key_columns; i < num_columns; ++i) {
        if ((i - num_key_columns) % max_columns_per_group == 0) {
            column_groups->emplace_back();
        }
        column_groups->back().emplace_back(i);
    }
}

CompactionAlgorithm CompactionUtils::choose_compaction_algorithm(size_t num_columns, int64_t max_columns_per_group,
                                                                 size_t source_num) {
    // if the number of columns in the schema is less than or equal to max_columns_per_group, use HORIZONTAL_COMPACTION.
    if (num_columns <= max_columns_per_group) {
        return HORIZONTAL_COMPACTION;
    }

    // if source_num is less than or equal to 1, heap merge iterator is not used in compaction,
    // and row source mask is not created.
    // if source_num is more than MAX_SOURCES, mask in RowSourceMask may overflow.
    if (source_num <= 1 || source_num > vectorized::RowSourceMask::MAX_SOURCES) {
        return HORIZONTAL_COMPACTION;
    }

    return VERTICAL_COMPACTION;
}

} // namespace starrocks
