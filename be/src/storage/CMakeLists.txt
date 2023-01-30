# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/storage")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/storage")

add_subdirectory(rowset)

add_library(Storage STATIC
    aggregate_type.cpp
    base_tablet.cpp
    decimal12.cpp
    decimal_type_info.cpp
    delete_handler.cpp
    del_vector.cpp
    key_coder.cpp
    memtable_flush_executor.cpp
    segment_flush_executor.cpp
    segment_replicate_executor.cpp
    metadata_util.cpp
    kv_store.cpp
    olap_common.cpp
    olap_server.cpp
    options.cpp
    page_cache.cpp
    persistent_index.cpp
    primary_index.cpp
    primary_key_encoder.cpp
    protobuf_file.cpp
    rowset_update_state.cpp
    update_compaction_state.cpp
    version_graph.cpp
    storage_engine.cpp
    data_dir.cpp
    short_key_index.cpp
    snapshot_manager.cpp
    snapshot_meta.cpp
    tablet.cpp
    tablet_manager.cpp
    tablet_meta.cpp
    tablet_meta_manager.cpp
    tablet_schema.cpp
    tablet_schema_map.cpp
    tablet_updates.cpp
    txn_manager.cpp
    types.cpp
    update_manager.cpp
    utils.cpp
    compaction_utils.cpp
    rowset/array_column_iterator.cpp
    rowset/array_column_writer.cpp
    rowset/binary_plain_page.cpp
    rowset/bitmap_index_reader.cpp
    rowset/bitmap_index_writer.cpp
    rowset/bitshuffle_page.cpp
    rowset/bitshuffle_wrapper.cpp
    rowset/column_iterator.cpp
    rowset/column_reader.cpp
    rowset/column_writer.cpp
    rowset/column_decoder.cpp
    rowset/default_value_column_iterator.cpp
    rowset/dictcode_column_iterator.cpp
    rowset/encoding_info.cpp
    rowset/scalar_column_iterator.cpp
    rowset/index_page.cpp
    rowset/indexed_column_reader.cpp
    rowset/indexed_column_writer.cpp
    rowset/map_column_writer.cpp
    rowset/map_column_iterator.cpp
    rowset/struct_column_writer.cpp
    rowset/struct_column_iterator.cpp
    rowset/ordinal_page_index.cpp
    rowset/page_io.cpp
    rowset/binary_dict_page.cpp
    rowset/binary_prefix_page.cpp
    rowset/segment.cpp
    rowset/segment_writer.cpp
    rowset/segment_rewriter.cpp
    rowset/segment_group.cpp
    rowset/storage_page_decoder.cpp
    rowset/block_split_bloom_filter.cpp
    rowset/bloom_filter_index_reader.cpp
    rowset/bloom_filter_index_writer.cpp
    rowset/bloom_filter.cpp
    rowset/parsed_page.cpp
    rowset/zone_map_index.cpp
    rowset/segment_chunk_iterator_adapter.cpp
    rowset/segment_iterator.cpp
    rowset/segment_options.cpp
    rowset/rowid_range_option.cpp
    rowset/rowset_meta.cpp
    task/engine_batch_load_task.cpp
    task/engine_checksum_task.cpp
    task/engine_clone_task.cpp
    task/engine_storage_migration_task.cpp
    task/engine_alter_tablet_task.cpp
    aggregate_iterator.cpp
    async_delta_writer.cpp
    chunk_helper.cpp
    chunk_iterator.cpp
    column_predicate_cmp.cpp
    column_in_predicate.cpp
    column_not_in_predicate.cpp
    column_null_predicate.cpp
    column_or_predicate.cpp
    column_expr_predicate.cpp
    conjunctive_predicates.cpp
    convert_helper.cpp
    delete_predicates.cpp
    disjunctive_predicates.cpp
    empty_iterator.cpp
    merge_iterator.cpp
    predicate_parser.cpp
    projection_iterator.cpp
    push_handler.cpp
    row_source_mask.cpp
    schema_change.cpp
    schema_change_utils.cpp
    tablet_reader.cpp
    tablet_reader_params.cpp
    table_reader.cpp
    meta_reader.cpp
    lake_meta_reader.cpp
    olap_meta_reader.cpp
    seek_tuple.cpp
    union_iterator.cpp
    unique_iterator.cpp
    column_aggregate_func.cpp
    chunk_aggregator.cpp
    delta_writer.cpp
    memtable.cpp
    base_compaction.cpp
    cumulative_compaction.cpp
    compaction.cpp
    rowset_merger.cpp
    column_predicate_rewriter.cpp
    column_predicate_dict_conjuct.cpp
    compaction_task.cpp
    compaction_utils.cpp
    compaction_manager.cpp
    horizontal_compaction_task.cpp
    vertical_compaction_task.cpp
    compaction_task_factory.cpp
    default_compaction_policy.cpp
    size_tiered_compaction_policy.cpp
    cluster_id_mgr.cpp
    push_utils.cpp
    lake/async_delta_writer.cpp
    lake/compaction_policy.cpp
    lake/horizontal_compaction_task.cpp
    lake/delta_writer.cpp
    lake/gc.cpp
    lake/general_tablet_writer.cpp
    lake/pk_tablet_writer.cpp
    lake/rowset.cpp
    lake/schema_change.cpp
    lake/starlet_location_provider.cpp
    lake/tablet.cpp
    lake/tablet_manager.cpp
    lake/tablet_reader.cpp
    lake/metadata_iterator.cpp
    lake/spark_load.cpp
    lake/update_manager.cpp
    lake/rowset_update_state.cpp
    lake/meta_file.cpp
    lake/lake_primary_index.cpp
    binlog_util.cpp
    binlog_file_writer.cpp
    binlog_file_reader.cpp
    lake/update_compaction_state.cpp
    lake/txn_log_applier.cpp
)
