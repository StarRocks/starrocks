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

#include "exec/hdfs_scanner_parquet.h"

#include "connector/deletion_vector/deletion_vector.h"
#include "exec/hdfs_scanner.h"
#include "exec/iceberg/iceberg_delete_builder.h"
#include "exec/paimon/paimon_delete_file_builder.h"
#include "formats/parquet/file_reader.h"
#include "pipeline/fragment_context.h"
#include "util/runtime_profile.h"

namespace starrocks {

static const std::string kParquetProfileSectionPrefix = "Parquet";

Status HdfsParquetScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    if (!scanner_params.deletes.empty()) {
        SCOPED_RAW_TIMER(&_app_stats.iceberg_delete_file_build_ns);
        auto iceberg_delete_builder =
                std::make_unique<IcebergDeleteBuilder>(_skip_rows_ctx, runtime_state, scanner_params);
        for (const auto& delete_file : scanner_params.deletes) {
            if (delete_file->file_content == TIcebergFileContent::POSITION_DELETES) {
                RETURN_IF_ERROR(iceberg_delete_builder->build_parquet(*delete_file));
            } else {
                const auto s = strings::Substitute("Unsupported iceberg file content: $0 in the scanner thread",
                                                   delete_file->file_content);
                LOG(WARNING) << s;
                return Status::InternalError(s);
            }
        }
        _app_stats.iceberg_delete_files_per_scan += scanner_params.deletes.size();
    } else if (scanner_params.paimon_deletion_file != nullptr) {
        std::unique_ptr<PaimonDeleteFileBuilder> paimon_delete_file_builder(
                new PaimonDeleteFileBuilder(scanner_params.fs, _skip_rows_ctx));
        RETURN_IF_ERROR(paimon_delete_file_builder->build(scanner_params.paimon_deletion_file.get()));
    } else if (scanner_params.deletion_vector_descriptor != nullptr) {
        if (scanner_params.split_context != nullptr) {
            auto split_ctx = down_cast<const parquet::SplitContext*>(scanner_params.split_context);
            _skip_rows_ctx = split_ctx->skip_rows_ctx;
            return Status::OK();
        }
        SCOPED_RAW_TIMER(&_app_stats.deletion_vector_build_ns);
        std::unique_ptr<DeletionVector> dv = std::make_unique<DeletionVector>(scanner_params);
        RETURN_IF_ERROR(dv->fill_row_indexes(_skip_rows_ctx));
        _app_stats.deletion_vector_build_count += 1;
    }
    return Status::OK();
}

void HdfsParquetScanner::do_update_counter(HdfsScanProfile* profile) {
    RuntimeProfile* root = profile->runtime_profile;
    // deletion vector build only in the first task which used for split sub-tasks,
    // and do not need to re-build in sub io tasks.
    do_update_deletion_vector_build_counter(root);
    // if we have split tasks, we don't need to update counter
    // and we will update those counters in sub io tasks.
    if (has_split_tasks()) {
        return;
    }

    RuntimeProfile::Counter* request_bytes_read = nullptr;
    RuntimeProfile::Counter* request_bytes_read_uncompressed = nullptr;
    RuntimeProfile::Counter* level_decode_timer = nullptr;
    RuntimeProfile::Counter* value_decode_timer = nullptr;
    RuntimeProfile::Counter* page_read_timer = nullptr;

    // reader init
    RuntimeProfile::Counter* footer_read_timer = nullptr;
    RuntimeProfile::Counter* footer_cache_write_counter = nullptr;
    RuntimeProfile::Counter* footer_cache_write_bytes = nullptr;
    RuntimeProfile::Counter* footer_cache_write_fail_counter = nullptr;
    RuntimeProfile::Counter* footer_cache_read_counter = nullptr;
    RuntimeProfile::Counter* footer_cache_read_timer = nullptr;
    RuntimeProfile::Counter* column_reader_init_timer = nullptr;

    // dict filter
    RuntimeProfile::Counter* group_chunk_read_timer = nullptr;
    RuntimeProfile::Counter* group_dict_filter_timer = nullptr;
    RuntimeProfile::Counter* group_dict_decode_timer = nullptr;

    // io coalesce
    RuntimeProfile::Counter* group_active_lazy_coalesce_together = nullptr;
    RuntimeProfile::Counter* group_active_lazy_coalesce_seperately = nullptr;

    // page statistics
    RuntimeProfile::Counter* has_page_statistics = nullptr;
    // page skip
    RuntimeProfile::Counter* page_skip = nullptr;
    // round-by-round
    RuntimeProfile::Counter* group_min_round_cost = nullptr;
    // page index
    RuntimeProfile::Counter* rows_before_page_index = nullptr;
    RuntimeProfile::Counter* page_index_timer = nullptr;
    // filter stats
    RuntimeProfile::Counter* total_row_groups = nullptr;
    RuntimeProfile::Counter* filtered_row_groups = nullptr;

    RuntimeProfile::Counter* statistics_tried_counter = nullptr;
    RuntimeProfile::Counter* statistics_success_counter = nullptr;
    RuntimeProfile::Counter* page_index_tried_counter = nullptr;
    RuntimeProfile::Counter* page_index_success_counter = nullptr;
    RuntimeProfile::Counter* page_index_filter_group_counter = nullptr;
    RuntimeProfile::Counter* bloom_filter_tried_counter = nullptr;
    RuntimeProfile::Counter* bloom_filter_success_counter = nullptr;

    ADD_COUNTER(root, kParquetProfileSectionPrefix, TUnit::NONE);
    request_bytes_read = ADD_CHILD_COUNTER(root, "RequestBytesRead", TUnit::BYTES, kParquetProfileSectionPrefix);
    request_bytes_read_uncompressed =
            ADD_CHILD_COUNTER(root, "RequestBytesReadUncompressed", TUnit::BYTES, kParquetProfileSectionPrefix);

    footer_cache_write_counter =
            ADD_CHILD_COUNTER(root, "FooterCacheWriteCount", TUnit::UNIT, kParquetProfileSectionPrefix);
    footer_cache_write_bytes =
            ADD_CHILD_COUNTER(root, "FooterCacheWriteBytes", TUnit::BYTES, kParquetProfileSectionPrefix);
    footer_cache_write_fail_counter =
            ADD_CHILD_COUNTER(root, "FooterCacheWriteFailCount", TUnit::UNIT, kParquetProfileSectionPrefix);
    footer_cache_read_counter =
            ADD_CHILD_COUNTER(root, "FooterCacheReadCount", TUnit::UNIT, kParquetProfileSectionPrefix);
    footer_cache_read_timer = ADD_CHILD_TIMER(root, "FooterCacheReadTimer", kParquetProfileSectionPrefix);

    level_decode_timer = ADD_CHILD_TIMER(root, "LevelDecodeTime", kParquetProfileSectionPrefix);
    value_decode_timer = ADD_CHILD_TIMER(root, "ValueDecodeTime", kParquetProfileSectionPrefix);

    page_read_timer = ADD_CHILD_TIMER(root, "PageReadTime", kParquetProfileSectionPrefix);
    footer_read_timer = ADD_CHILD_TIMER(root, "ReaderInitFooterRead", kParquetProfileSectionPrefix);
    column_reader_init_timer = ADD_CHILD_TIMER(root, "ReaderInitColumnReaderInit", kParquetProfileSectionPrefix);

    group_chunk_read_timer = ADD_CHILD_TIMER(root, "GroupChunkRead", kParquetProfileSectionPrefix);
    group_dict_filter_timer = ADD_CHILD_TIMER(root, "GroupDictFilter", kParquetProfileSectionPrefix);
    group_dict_decode_timer = ADD_CHILD_TIMER(root, "GroupDictDecode", kParquetProfileSectionPrefix);

    group_active_lazy_coalesce_together = ADD_CHILD_COUNTER(root, "GroupActiveLazyColumnIOCoalesceTogether",
                                                            TUnit::UNIT, kParquetProfileSectionPrefix);
    group_active_lazy_coalesce_seperately = ADD_CHILD_COUNTER(root, "GroupActiveLazyColumnIOCoalesceSeperately",
                                                              TUnit::UNIT, kParquetProfileSectionPrefix);

    has_page_statistics = ADD_CHILD_COUNTER(root, "HasPageStatistics", TUnit::UNIT, kParquetProfileSectionPrefix);
    page_skip = ADD_CHILD_COUNTER(root, "PageSkipCounter", TUnit::UNIT, kParquetProfileSectionPrefix);
    group_min_round_cost = root->AddLowWaterMarkCounter(
            "GroupMinRound", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TCounterAggregateType::AVG),
            kParquetProfileSectionPrefix);
    rows_before_page_index = ADD_CHILD_COUNTER(root, "RowsBeforePageIndex", TUnit::UNIT, kParquetProfileSectionPrefix);
    page_index_timer = ADD_CHILD_TIMER(root, "PageIndexTime", kParquetProfileSectionPrefix);
    total_row_groups = ADD_CHILD_COUNTER(root, "TotalRowGroups", TUnit::UNIT, kParquetProfileSectionPrefix);
    filtered_row_groups = ADD_CHILD_COUNTER(root, "FilteredRowGroups", TUnit::UNIT, kParquetProfileSectionPrefix);
    ADD_CHILD_COUNTER(root, "ReaderFilterCounter", TUnit::NONE, kParquetProfileSectionPrefix);
    statistics_tried_counter = ADD_CHILD_COUNTER(root, "StatisticsTriedCounter", TUnit::UNIT, "ReaderFilterCounter");
    statistics_success_counter =
            ADD_CHILD_COUNTER(root, "StatisticsSuccessCounter", TUnit::UNIT, "ReaderFilterCounter");
    page_index_tried_counter = ADD_CHILD_COUNTER(root, "PageIndexTriedCounter", TUnit::UNIT, "ReaderFilterCounter");
    page_index_success_counter = ADD_CHILD_COUNTER(root, "PageIndexSuccessCounter", TUnit::UNIT, "ReaderFilterCounter");
    page_index_filter_group_counter =
            ADD_CHILD_COUNTER(root, "PageIndexFilterGroupCounter", TUnit::UNIT, "ReaderFilterCounter");
    bloom_filter_tried_counter = ADD_CHILD_COUNTER(root, "BloomFilterTriedCounter", TUnit::UNIT, "ReaderFilterCounter");
    bloom_filter_success_counter =
            ADD_CHILD_COUNTER(root, "BloomFilterSuccessCounter", TUnit::UNIT, "ReaderFilterCounter");

    COUNTER_UPDATE(request_bytes_read, _app_stats.request_bytes_read);
    COUNTER_UPDATE(request_bytes_read_uncompressed, _app_stats.request_bytes_read_uncompressed);
    COUNTER_UPDATE(value_decode_timer, _app_stats.value_decode_ns);
    COUNTER_UPDATE(level_decode_timer, _app_stats.level_decode_ns);
    COUNTER_UPDATE(page_read_timer, _app_stats.page_read_ns);
    COUNTER_UPDATE(footer_read_timer, _app_stats.footer_read_ns);
    COUNTER_UPDATE(footer_cache_write_counter, _app_stats.footer_cache_write_count);
    COUNTER_UPDATE(footer_cache_write_bytes, _app_stats.footer_cache_write_bytes);
    COUNTER_UPDATE(footer_cache_write_fail_counter, _app_stats.footer_cache_write_fail_count);
    COUNTER_UPDATE(footer_cache_read_counter, _app_stats.footer_cache_read_count);
    COUNTER_UPDATE(footer_cache_read_timer, _app_stats.footer_cache_read_ns);
    COUNTER_UPDATE(column_reader_init_timer, _app_stats.column_reader_init_ns);
    COUNTER_UPDATE(group_chunk_read_timer, _app_stats.group_chunk_read_ns);
    COUNTER_UPDATE(group_dict_filter_timer, _app_stats.group_dict_filter_ns);
    COUNTER_UPDATE(group_dict_decode_timer, _app_stats.group_dict_decode_ns);
    COUNTER_UPDATE(group_active_lazy_coalesce_together, _app_stats.group_active_lazy_coalesce_together);
    COUNTER_UPDATE(group_active_lazy_coalesce_seperately, _app_stats.group_active_lazy_coalesce_seperately);
    int64_t page_stats = _app_stats.has_page_statistics ? 1 : 0;
    COUNTER_UPDATE(has_page_statistics, page_stats);
    COUNTER_UPDATE(page_skip, _app_stats.page_skip);
    group_min_round_cost->set(_app_stats.group_min_round_cost);
    do_update_iceberg_v2_counter(root, kParquetProfileSectionPrefix);
    do_update_deletion_vector_filter_counter(root);
    COUNTER_UPDATE(rows_before_page_index, _app_stats.rows_before_page_index);
    COUNTER_UPDATE(page_index_timer, _app_stats.page_index_ns);
    COUNTER_UPDATE(total_row_groups, _app_stats.parquet_total_row_groups);
    COUNTER_UPDATE(filtered_row_groups, _app_stats.parquet_filtered_row_groups);

    COUNTER_UPDATE(statistics_tried_counter, _app_stats._optimzation_counter.statistics_tried_counter);
    COUNTER_UPDATE(statistics_success_counter, _app_stats._optimzation_counter.statistics_success_counter);
    COUNTER_UPDATE(page_index_tried_counter, _app_stats._optimzation_counter.page_index_tried_counter);
    COUNTER_UPDATE(page_index_success_counter, _app_stats._optimzation_counter.page_index_success_counter);
    COUNTER_UPDATE(page_index_filter_group_counter, _app_stats._optimzation_counter.page_index_filter_group_counter);
    COUNTER_UPDATE(bloom_filter_tried_counter, _app_stats._optimzation_counter.bloom_filter_tried_counter);
    COUNTER_UPDATE(bloom_filter_success_counter, _app_stats._optimzation_counter.bloom_filter_success_counter);

    if (_scanner_ctx.conjuncts_manager != nullptr &&
        _runtime_state->fragment_ctx()->pred_tree_params().enable_show_in_profile) {
        root->add_info_string("ParquetPredicateTreeFilter", _scanner_ctx.predicate_tree.root().debug_string());
    }
}

Status HdfsParquetScanner::do_open(RuntimeState* runtime_state) {
    RETURN_IF_ERROR(open_random_access_file());
    // create file reader
    _reader = std::make_shared<parquet::FileReader>(runtime_state->chunk_size(), _file.get(), _file->get_size().value(),
                                                    _scanner_params.datacache_options,
                                                    _shared_buffered_input_stream.get(), _skip_rows_ctx);
    SCOPED_RAW_TIMER(&_app_stats.reader_init_ns);
    RETURN_IF_ERROR(_reader->init(&_scanner_ctx));
    return Status::OK();
}

Status HdfsParquetScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    Status status = _reader->get_next(chunk);
    return status;
}

void HdfsParquetScanner::do_close(RuntimeState* runtime_state) noexcept {
    _reader.reset();
}

} // namespace starrocks
