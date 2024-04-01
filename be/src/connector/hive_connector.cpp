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

#include "connector/hive_connector.h"

#include <filesystem>

#include "connector_sink/hive_chunk_sink.h"
#include "exec/exec_node.h"
#include "exec/hdfs_scanner_orc.h"
#include "exec/hdfs_scanner_parquet.h"
#include "exec/hdfs_scanner_partition.h"
#include "exec/hdfs_scanner_text.h"
#include "exec/jni_scanner.h"
#include "exprs/expr.h"
#include "storage/chunk_helper.h"

namespace starrocks::connector {

// ================================

DataSourceProviderPtr HiveConnector::create_data_source_provider(ConnectorScanNode* scan_node,
                                                                 const TPlanNode& plan_node) const {
    return std::make_unique<HiveDataSourceProvider>(scan_node, plan_node);
}

std::unique_ptr<ConnectorChunkSinkProvider> HiveConnector::create_data_sink_provider() const {
    return std::make_unique<HiveChunkSinkProvider>();
}

// ================================

HiveDataSourceProvider::HiveDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _hdfs_scan_node(plan_node.hdfs_scan_node) {}

DataSourcePtr HiveDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<HiveDataSource>(this, scan_range);
}

const TupleDescriptor* HiveDataSourceProvider::tuple_descriptor(RuntimeState* state) const {
    return state->desc_tbl().get_tuple_descriptor(_hdfs_scan_node.tuple_id);
}

// ================================

HiveDataSource::HiveDataSource(const HiveDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider), _scan_range(scan_range.hdfs_scan_range) {}

Status HiveDataSource::_check_all_slots_nullable() {
    for (const auto* slot : _tuple_desc->slots()) {
        if (!slot->is_nullable()) {
            return Status::RuntimeError(fmt::format(
                    "All columns must be nullable for external table. Column '{}' is not nullable, You can rebuild the"
                    "external table and We strongly recommend that you use catalog to access external data.",
                    slot->col_name()));
        }
    }
    return Status::OK();
}

std::string HiveDataSource::name() const {
    return "HiveDataSource";
}

Status HiveDataSource::open(RuntimeState* state) {
    const auto& hdfs_scan_node = _provider->_hdfs_scan_node;
    if (_split_context != nullptr) {
        auto split_context = down_cast<HdfsSplitContext*>(_split_context);
        _scan_range.offset = split_context->split_start;
        _scan_range.length = split_context->split_end - split_context->split_start;
    }

    if (_scan_range.file_length == 0) {
        _no_data = true;
        return Status::OK();
    }

    _runtime_state = state;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(hdfs_scan_node.tuple_id);
    _hive_table = dynamic_cast<const HiveTableDescriptor*>(_tuple_desc->table_desc());
    if (_hive_table == nullptr) {
        return Status::RuntimeError(
                "Invalid table type. Only hive/iceberg/hudi/delta lake/file/paimon table are supported");
    }
    RETURN_IF_ERROR(_check_all_slots_nullable());

    _use_datacache = config::datacache_enable;
    if (state->query_options().__isset.enable_scan_datacache) {
        _use_datacache &= state->query_options().enable_scan_datacache;
    }
    if (state->query_options().__isset.enable_populate_datacache) {
        _enable_populate_datacache = state->query_options().enable_populate_datacache;
    }
    if (state->query_options().__isset.enable_dynamic_prune_scan_range) {
        _enable_dynamic_prune_scan_range = state->query_options().enable_dynamic_prune_scan_range;
    }
    // Don't use datacache when priority = -1
    if (_scan_range.__isset.datacache_options && _scan_range.datacache_options.__isset.priority &&
        _scan_range.datacache_options.priority == -1) {
        _use_datacache = false;
    }
    if (state->query_options().__isset.enable_file_metacache) {
        _use_file_metacache = state->query_options().enable_file_metacache;
    }
    if (state->query_options().__isset.enable_connector_split_io_tasks) {
        _enable_split_tasks = state->query_options().enable_connector_split_io_tasks;
    }

    RETURN_IF_ERROR(_init_conjunct_ctxs(state));
    _init_tuples_and_slots(state);
    _init_counter(state);
    RETURN_IF_ERROR(_init_partition_values());
    if (_filter_by_eval_partition_conjuncts) {
        _no_data = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(_init_scanner(state));
    return Status::OK();
}

void HiveDataSource::_update_has_any_predicate() {
    auto f = [&]() {
        if (_runtime_filters != nullptr && _runtime_filters->size() > 0) return true;
        return false;
    };
    _has_any_predicate = f();
}

Status HiveDataSource::_init_conjunct_ctxs(RuntimeState* state) {
    const auto& hdfs_scan_node = _provider->_hdfs_scan_node;
    if (hdfs_scan_node.__isset.min_max_conjuncts) {
        RETURN_IF_ERROR(
                Expr::create_expr_trees(&_pool, hdfs_scan_node.min_max_conjuncts, &_min_max_conjunct_ctxs, state));
    }

    if (hdfs_scan_node.__isset.partition_conjuncts) {
        RETURN_IF_ERROR(
                Expr::create_expr_trees(&_pool, hdfs_scan_node.partition_conjuncts, &_partition_conjunct_ctxs, state));
        _has_partition_conjuncts = true;
    }

    if (hdfs_scan_node.__isset.case_sensitive) {
        _case_sensitive = hdfs_scan_node.case_sensitive;
    }

    RETURN_IF_ERROR(Expr::prepare(_min_max_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_partition_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_min_max_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_partition_conjunct_ctxs, state));
    _update_has_any_predicate();

    RETURN_IF_ERROR(_decompose_conjunct_ctxs(state));
    return Status::OK();
}

Status HiveDataSource::_init_partition_values() {
    if (!(_hive_table != nullptr && _has_partition_columns)) return Status::OK();

    auto* partition_desc = _hive_table->get_partition(_scan_range.partition_id);
    if (partition_desc == nullptr) {
        return Status::InternalError(
                fmt::format("Plan inconsistency. scan_range.partition_id = {} not found in partition description map",
                            _scan_range.partition_id));
    }

    const auto& partition_values = partition_desc->partition_key_value_evals();
    _partition_values = partition_desc->partition_key_value_evals();

    // init partition chunk
    auto partition_chunk = std::make_shared<Chunk>();
    for (int i = 0; i < _partition_slots.size(); i++) {
        SlotId slot_id = _partition_slots[i]->id();
        int partition_col_idx = _partition_index_in_hdfs_partition_columns[i];
        ASSIGN_OR_RETURN(auto partition_value_col, partition_values[partition_col_idx]->evaluate(nullptr));
        DCHECK(partition_value_col->is_constant());
        partition_chunk->append_column(partition_value_col, slot_id);
    }

    // eval conjuncts and skip if no rows.
    if (_has_scan_range_indicate_const_column) {
        std::vector<ExprContext*> ctxs;
        for (SlotId slotId : _scan_range.identity_partition_slot_ids) {
            if (_conjunct_ctxs_by_slot.find(slotId) != _conjunct_ctxs_by_slot.end()) {
                ctxs.insert(ctxs.end(), _conjunct_ctxs_by_slot.at(slotId).begin(),
                            _conjunct_ctxs_by_slot.at(slotId).end());
            }
        }
        RETURN_IF_ERROR(ExecNode::eval_conjuncts(ctxs, partition_chunk.get()));
    } else if (_has_partition_conjuncts) {
        RETURN_IF_ERROR(ExecNode::eval_conjuncts(_partition_conjunct_ctxs, partition_chunk.get()));
    }

    if (!partition_chunk->has_rows()) {
        _filter_by_eval_partition_conjuncts = true;
        return Status::OK();
    }

    if (_enable_dynamic_prune_scan_range && _runtime_filters) {
        _init_rf_counters();
        _runtime_filters->evaluate_partial_chunk(partition_chunk.get(), runtime_bloom_filter_eval_context);
        if (!partition_chunk->has_rows()) {
            _filter_by_eval_partition_conjuncts = true;
            return Status::OK();
        }
    }

    return Status::OK();
}

int32_t HiveDataSource::scan_range_indicate_const_column_index(SlotId id) const {
    if (!_scan_range.__isset.identity_partition_slot_ids) {
        return -1;
    }
    auto it = std::find(_scan_range.identity_partition_slot_ids.begin(), _scan_range.identity_partition_slot_ids.end(),
                        id);
    if (it == _scan_range.identity_partition_slot_ids.end()) {
        return -1;
    } else {
        return it - _scan_range.identity_partition_slot_ids.begin();
    }
}

void HiveDataSource::_init_tuples_and_slots(RuntimeState* state) {
    const auto& hdfs_scan_node = _provider->_hdfs_scan_node;
    if (hdfs_scan_node.__isset.min_max_tuple_id) {
        _min_max_tuple_id = hdfs_scan_node.min_max_tuple_id;
        _min_max_tuple_desc = state->desc_tbl().get_tuple_descriptor(_min_max_tuple_id);
    }

    const auto& slots = _tuple_desc->slots();
    for (int i = 0; i < slots.size(); i++) {
        if (_hive_table != nullptr && _hive_table->is_partition_col(slots[i])) {
            _partition_slots.push_back(slots[i]);
            _partition_index_in_chunk.push_back(i);
            _partition_index_in_hdfs_partition_columns.push_back(_hive_table->get_partition_col_index(slots[i]));
            _has_partition_columns = true;
        } else if (int32_t index = scan_range_indicate_const_column_index(slots[i]->id()); index >= 0) {
            _partition_slots.push_back(slots[i]);
            _partition_index_in_chunk.push_back(i);
            _partition_index_in_hdfs_partition_columns.push_back(index);
            _has_partition_columns = true;
            _has_scan_range_indicate_const_column = true;
        } else {
            _materialize_slots.push_back(slots[i]);
            _materialize_index_in_chunk.push_back(i);
        }
    }

    if (_scan_range.__isset.delete_column_slot_ids && !_scan_range.delete_column_slot_ids.empty()) {
        std::map<SlotId, SlotDescriptor*> id_to_slots;
        for (const auto& slot : _materialize_slots) {
            id_to_slots.emplace(slot->id(), slot);
        }

        int32_t delete_column_index = slots.size();
        _delete_column_tuple_desc = state->desc_tbl().get_tuple_descriptor(_provider->_hdfs_scan_node.mor_tuple_id);

        for (SlotDescriptor* d_slot_desc : _delete_column_tuple_desc->slots()) {
            _equality_delete_slots.emplace_back(d_slot_desc);
            if (!id_to_slots.contains(d_slot_desc->id())) {
                _materialize_slots.push_back(d_slot_desc);
                _materialize_index_in_chunk.push_back(delete_column_index++);
            }
        }
    }

    if (hdfs_scan_node.__isset.hive_column_names) {
        _hive_column_names = hdfs_scan_node.hive_column_names;
    }
    if (hdfs_scan_node.__isset.case_sensitive) {
        _case_sensitive = hdfs_scan_node.case_sensitive;
    }
    if (hdfs_scan_node.__isset.can_use_any_column) {
        _can_use_any_column = hdfs_scan_node.can_use_any_column;
    }
    if (hdfs_scan_node.__isset.can_use_min_max_count_opt) {
        _can_use_min_max_count_opt = hdfs_scan_node.can_use_min_max_count_opt;
    }
    if (hdfs_scan_node.__isset.use_partition_column_value_only) {
        _use_partition_column_value_only = hdfs_scan_node.use_partition_column_value_only;
    }

    // The reason why we need double check here is for iceberg table.
    // for some partitions, partition column maybe is not constant value.
    // If partition column is not constant value, we can not use this optimization,
    // And we can not use `can_use_any_column` either.
    // So checks are:
    // 1. can_use_any_column = true
    // 2. only one materialized slot
    // 3. besides that, all slots are partition slots.
    // 4. scan iceberg data file without equality delete files.
    auto check_opt_on_iceberg = [&]() {
        if (!_can_use_any_column) {
            return false;
        }
        if ((_partition_slots.size() + 1) != slots.size()) {
            return false;
        }
        if (_materialize_slots.size() != 1) {
            return false;
        }
        if (!_scan_range.delete_column_slot_ids.empty()) {
            return false;
        }
        return true;
    };
    if (!check_opt_on_iceberg()) {
        _use_partition_column_value_only = false;
        _can_use_any_column = false;
    }
}

Status HiveDataSource::_decompose_conjunct_ctxs(RuntimeState* state) {
    if (_conjunct_ctxs.empty()) {
        return Status::OK();
    }

    std::unordered_map<SlotId, SlotDescriptor*> slot_by_id;
    for (SlotDescriptor* slot : _tuple_desc->slots()) {
        slot_by_id[slot->id()] = slot;
    }

    std::vector<ExprContext*> cloned_conjunct_ctxs;
    RETURN_IF_ERROR(Expr::clone_if_not_exists(state, &_pool, _conjunct_ctxs, &cloned_conjunct_ctxs));

    for (ExprContext* ctx : cloned_conjunct_ctxs) {
        const Expr* root_expr = ctx->root();
        std::vector<SlotId> slot_ids;
        root_expr->get_slot_ids(&slot_ids);
        for (SlotId slot_id : slot_ids) {
            _slots_in_conjunct.insert(slot_id);
        }

        // For some conjunct like (a < 1) or (a > 7)
        // slot_ids = (a, a), but actually there is only one slot.
        bool single_slot = true;
        for (int i = 1; i < slot_ids.size(); i++) {
            if (slot_ids[i] != slot_ids[0]) {
                single_slot = false;
                break;
            }
        }
        if (!single_slot || slot_ids.empty()) {
            _scanner_conjunct_ctxs.emplace_back(ctx);
            for (SlotId slot_id : slot_ids) {
                _slots_of_mutli_slot_conjunct.insert(slot_id);
            }
            continue;
        }

        SlotId slot_id = slot_ids[0];
        if (slot_by_id.find(slot_id) != slot_by_id.end()) {
            if (_conjunct_ctxs_by_slot.find(slot_id) == _conjunct_ctxs_by_slot.end()) {
                _conjunct_ctxs_by_slot.insert({slot_id, std::vector<ExprContext*>()});
            }
            _conjunct_ctxs_by_slot[slot_id].emplace_back(ctx);
        }
    }
    return Status::OK();
}

void HiveDataSource::_init_counter(RuntimeState* state) {
    const auto& hdfs_scan_node = _provider->_hdfs_scan_node;

    _profile.runtime_profile = _runtime_profile;
    _profile.raw_rows_read_counter = ADD_COUNTER(_runtime_profile, "RawRowsRead", TUnit::UNIT);
    _profile.rows_read_counter = ADD_COUNTER(_runtime_profile, "RowsRead", TUnit::UNIT);
    _profile.late_materialize_skip_rows_counter = ADD_COUNTER(_runtime_profile, "LateMaterializeSkipRows", TUnit::UNIT);
    _profile.scan_ranges_counter = ADD_COUNTER(_runtime_profile, "ScanRanges", TUnit::UNIT);
    _profile.scan_ranges_size = ADD_COUNTER(_runtime_profile, "ScanRangesSize", TUnit::BYTES);

    _profile.reader_init_timer = ADD_TIMER(_runtime_profile, "ReaderInit");
    _profile.open_file_timer = ADD_TIMER(_runtime_profile, "OpenFile");
    _profile.expr_filter_timer = ADD_TIMER(_runtime_profile, "ExprFilterTime");

    _profile.column_read_timer = ADD_TIMER(_runtime_profile, "ColumnReadTime");
    _profile.column_convert_timer = ADD_TIMER(_runtime_profile, "ColumnConvertTime");

    {
        static const char* prefix = "SharedBuffered";
        ADD_COUNTER(_runtime_profile, prefix, TUnit::NONE);
        _profile.shared_buffered_shared_io_bytes =
                ADD_CHILD_COUNTER(_runtime_profile, "SharedIOBytes", TUnit::BYTES, prefix);
        _profile.shared_buffered_shared_align_io_bytes =
                ADD_CHILD_COUNTER(_runtime_profile, "SharedAlignIOBytes", TUnit::BYTES, prefix);
        _profile.shared_buffered_shared_io_count =
                ADD_CHILD_COUNTER(_runtime_profile, "SharedIOCount", TUnit::UNIT, prefix);
        _profile.shared_buffered_shared_io_timer = ADD_CHILD_TIMER(_runtime_profile, "SharedIOTime", prefix);
        _profile.shared_buffered_direct_io_bytes =
                ADD_CHILD_COUNTER(_runtime_profile, "DirectIOBytes", TUnit::BYTES, prefix);
        _profile.shared_buffered_direct_io_count =
                ADD_CHILD_COUNTER(_runtime_profile, "DirectIOCount", TUnit::UNIT, prefix);
        _profile.shared_buffered_direct_io_timer = ADD_CHILD_TIMER(_runtime_profile, "DirectIOTime", prefix);
    }

    if (_use_datacache) {
        static const char* prefix = "DataCache";
        ADD_COUNTER(_runtime_profile, prefix, TUnit::NONE);
        _profile.datacache_read_counter =
                ADD_CHILD_COUNTER(_runtime_profile, "DataCacheReadCounter", TUnit::UNIT, prefix);
        _profile.datacache_read_bytes = ADD_CHILD_COUNTER(_runtime_profile, "DataCacheReadBytes", TUnit::BYTES, prefix);
        _profile.datacache_read_mem_bytes =
                ADD_CHILD_COUNTER(_runtime_profile, "DataCacheReadMemBytes", TUnit::BYTES, "DataCacheReadBytes");
        _profile.datacache_read_disk_bytes =
                ADD_CHILD_COUNTER(_runtime_profile, "DataCacheReadDiskBytes", TUnit::BYTES, "DataCacheReadBytes");
        _profile.datacache_skip_read_counter =
                ADD_CHILD_COUNTER(_runtime_profile, "DataCacheSkipReadCounter", TUnit::UNIT, prefix);
        _profile.datacache_skip_read_bytes =
                ADD_CHILD_COUNTER(_runtime_profile, "DataCacheSkipReadBytes", TUnit::BYTES, prefix);
        _profile.datacache_read_timer = ADD_CHILD_TIMER(_runtime_profile, "DataCacheReadTimer", prefix);
        _profile.datacache_write_counter =
                ADD_CHILD_COUNTER(_runtime_profile, "DataCacheWriteCounter", TUnit::UNIT, prefix);
        _profile.datacache_write_bytes =
                ADD_CHILD_COUNTER(_runtime_profile, "DataCacheWriteBytes", TUnit::BYTES, prefix);
        _profile.datacache_write_timer = ADD_CHILD_TIMER(_runtime_profile, "DataCacheWriteTimer", prefix);
        _profile.datacache_write_fail_counter =
                ADD_CHILD_COUNTER(_runtime_profile, "DataCacheWriteFailCounter", TUnit::UNIT, prefix);
        _profile.datacache_write_fail_bytes =
                ADD_CHILD_COUNTER(_runtime_profile, "DataCacheWriteFailBytes", TUnit::BYTES, prefix);
        _profile.datacache_read_block_buffer_counter =
                ADD_CHILD_COUNTER(_runtime_profile, "DataCacheReadBlockBufferCounter", TUnit::UNIT, prefix);
        _profile.datacache_read_block_buffer_bytes =
                ADD_CHILD_COUNTER(_runtime_profile, "DataCacheReadBlockBufferBytes", TUnit::BYTES, prefix);
    }

    {
        static const char* prefix = "InputStream";
        ADD_COUNTER(_runtime_profile, prefix, TUnit::NONE);
        _profile.app_io_bytes_read_counter =
                ADD_CHILD_COUNTER(_runtime_profile, "AppIOBytesRead", TUnit::BYTES, prefix);
        _profile.app_io_timer = ADD_CHILD_TIMER(_runtime_profile, "AppIOTime", prefix);
        _profile.app_io_counter = ADD_CHILD_COUNTER(_runtime_profile, "AppIOCounter", TUnit::UNIT, prefix);
        _profile.fs_bytes_read_counter = ADD_CHILD_COUNTER(_runtime_profile, "FSIOBytesRead", TUnit::BYTES, prefix);
        _profile.fs_io_counter = ADD_CHILD_COUNTER(_runtime_profile, "FSIOCounter", TUnit::UNIT, prefix);
        _profile.fs_io_timer = ADD_CHILD_TIMER(_runtime_profile, "FSIOTime", prefix);
    }

    if (hdfs_scan_node.__isset.table_name) {
        _runtime_profile->add_info_string("Table", hdfs_scan_node.table_name);
    }
    if (hdfs_scan_node.__isset.sql_predicates) {
        _runtime_profile->add_info_string("Predicates", hdfs_scan_node.sql_predicates);
    }
    if (hdfs_scan_node.__isset.min_max_sql_predicates) {
        _runtime_profile->add_info_string("PredicatesMinMax", hdfs_scan_node.min_max_sql_predicates);
    }
    if (hdfs_scan_node.__isset.partition_sql_predicates) {
        _runtime_profile->add_info_string("PredicatesPartition", hdfs_scan_node.partition_sql_predicates);
    }
}

void HiveDataSource::_init_rf_counters() {
    auto* root = _runtime_profile;
    if (runtime_bloom_filter_eval_context.join_runtime_filter_timer == nullptr) {
        static const char* prefix = "DynamicPruneScanRange";
        ADD_COUNTER(root, prefix, TUnit::NONE);
        runtime_bloom_filter_eval_context.join_runtime_filter_timer =
                ADD_CHILD_TIMER(root, "JoinRuntimeFilterTime", prefix);
        runtime_bloom_filter_eval_context.join_runtime_filter_hash_timer =
                ADD_CHILD_TIMER(root, "JoinRuntimeFilterHashTime", prefix);
        runtime_bloom_filter_eval_context.join_runtime_filter_input_counter =
                ADD_CHILD_COUNTER(root, "JoinRuntimeFilterInputScanRanges", TUnit::UNIT, prefix);
        runtime_bloom_filter_eval_context.join_runtime_filter_output_counter =
                ADD_CHILD_COUNTER(root, "JoinRuntimeFilterOutputScanRanges", TUnit::UNIT, prefix);
        runtime_bloom_filter_eval_context.join_runtime_filter_eval_counter =
                ADD_CHILD_COUNTER(root, "JoinRuntimeFilterEvaluate", TUnit::UNIT, prefix);
    }
}

static void build_nested_fields(const TypeDescriptor& type, const std::string& parent, std::string* sb) {
    for (int i = 0; i < type.children.size(); i++) {
        const auto& t = type.children[i];
        if (t.is_unknown_type()) continue;
        std::string p = parent + "." + (type.is_struct_type() ? type.field_names[i] : fmt::format("${}", i));
        if (t.is_complex_type()) {
            build_nested_fields(t, p, sb);
        } else {
            sb->append(p);
            sb->append(",");
        }
    }
}

static std::string build_fs_options_properties(const FSOptions& options) {
    const TCloudConfiguration* cloud_configuration = options.cloud_configuration;
    static constexpr char KV_SEPARATOR = 0x1;
    static constexpr char PROP_SEPARATOR = 0x2;
    std::string data;

    if (cloud_configuration != nullptr) {
        if (cloud_configuration->__isset.cloud_properties) {
            for (const auto& cloud_property : cloud_configuration->cloud_properties) {
                data += cloud_property.key;
                data += KV_SEPARATOR;
                data += cloud_property.value;
                data += PROP_SEPARATOR;
            }
        } else {
            for (const auto& [key, value] : cloud_configuration->cloud_properties_v2) {
                data += key;
                data += KV_SEPARATOR;
                data += value;
                data += PROP_SEPARATOR;
            }
        }
    }

    if (data.size() > 0 && data.back() == PROP_SEPARATOR) {
        data.pop_back();
    }
    return data;
}

HdfsScanner* HiveDataSource::_create_hudi_jni_scanner(const FSOptions& options) {
    const auto& scan_range = _scan_range;
    const auto* hudi_table = dynamic_cast<const HudiTableDescriptor*>(_hive_table);
    auto* partition_desc = hudi_table->get_partition(scan_range.partition_id);
    std::string partition_full_path = partition_desc->location();

    std::string required_fields;
    for (auto slot : _tuple_desc->slots()) {
        required_fields.append(slot->col_name());
        required_fields.append(",");
    }
    required_fields = required_fields.substr(0, required_fields.size() - 1);

    std::string nested_fields;
    for (auto slot : _tuple_desc->slots()) {
        const TypeDescriptor& type = slot->type();
        if (type.is_complex_type()) {
            build_nested_fields(type, slot->col_name(), &nested_fields);
        }
    }
    if (!nested_fields.empty()) {
        nested_fields = nested_fields.substr(0, nested_fields.size() - 1);
    }

    std::string delta_file_paths;
    if (!scan_range.hudi_logs.empty()) {
        for (const std::string& log : scan_range.hudi_logs) {
            delta_file_paths.append(fmt::format("{}/{}", partition_full_path, log));
            delta_file_paths.append(",");
        }
        delta_file_paths = delta_file_paths.substr(0, delta_file_paths.size() - 1);
    }

    std::string data_file_path;
    if (scan_range.relative_path.empty()) {
        data_file_path = "";
    } else {
        data_file_path = fmt::format("{}/{}", partition_full_path, scan_range.relative_path);
    }

    std::map<std::string, std::string> jni_scanner_params;
    jni_scanner_params["base_path"] = hudi_table->get_base_path();
    jni_scanner_params["hive_column_names"] = hudi_table->get_hive_column_names();
    jni_scanner_params["hive_column_types"] = hudi_table->get_hive_column_types();
    jni_scanner_params["required_fields"] = required_fields;
    jni_scanner_params["nested_fields"] = nested_fields;
    jni_scanner_params["instant_time"] = hudi_table->get_instant_time();
    jni_scanner_params["delta_file_paths"] = delta_file_paths;
    jni_scanner_params["data_file_path"] = data_file_path;
    jni_scanner_params["data_file_length"] = std::to_string(scan_range.file_length);
    jni_scanner_params["serde"] = hudi_table->get_serde_lib();
    jni_scanner_params["input_format"] = hudi_table->get_input_format();
    jni_scanner_params["fs_options_props"] = build_fs_options_properties(options);
    jni_scanner_params["time_zone"] = hudi_table->get_time_zone();

    std::string scanner_factory_class = "com/starrocks/hudi/reader/HudiSliceScannerFactory";
    HdfsScanner* scanner = _pool.add(new JniScanner(scanner_factory_class, jni_scanner_params));
    return scanner;
}

HdfsScanner* HiveDataSource::_create_paimon_jni_scanner(const FSOptions& options) {
    const auto* paimon_table = dynamic_cast<const PaimonTableDescriptor*>(_hive_table);

    std::string required_fields;
    for (auto slot : _tuple_desc->slots()) {
        required_fields.append(slot->col_name());
        required_fields.append(",");
    }
    required_fields = required_fields.substr(0, required_fields.size() - 1);
    std::string nested_fields;
    for (auto slot : _tuple_desc->slots()) {
        const TypeDescriptor& type = slot->type();
        if (type.is_complex_type()) {
            build_nested_fields(type, slot->col_name(), &nested_fields);
        }
    }
    if (!nested_fields.empty()) {
        nested_fields = nested_fields.substr(0, nested_fields.size() - 1);
    }
    std::map<std::string, std::string> jni_scanner_params;
    jni_scanner_params["required_fields"] = required_fields;
    jni_scanner_params["split_info"] = _scan_range.paimon_split_info;
    jni_scanner_params["predicate_info"] = _scan_range.paimon_predicate_info;
    jni_scanner_params["nested_fields"] = nested_fields;
    jni_scanner_params["native_table"] = paimon_table->get_paimon_native_table();
    jni_scanner_params["time_zone"] = paimon_table->get_time_zone();

    std::string scanner_factory_class = "com/starrocks/paimon/reader/PaimonSplitScannerFactory";
    HdfsScanner* scanner = _pool.add(new JniScanner(scanner_factory_class, jni_scanner_params));
    return scanner;
}

HdfsScanner* HiveDataSource::_create_hive_jni_scanner(const FSOptions& options) {
    const auto& scan_range = _scan_range;
    static const char* serde_property_prefix = "SerDe.";

    std::string required_fields;
    for (auto const& slot : _materialize_slots) {
        required_fields.append(slot->col_name());
        required_fields.append(",");
    }
    required_fields = required_fields.substr(0, required_fields.size() - 1);

    std::string nested_fields;
    for (auto slot : _materialize_slots) {
        const TypeDescriptor& type = slot->type();
        if (type.is_complex_type()) {
            build_nested_fields(type, slot->col_name(), &nested_fields);
        }
    }
    if (!nested_fields.empty()) {
        nested_fields = nested_fields.substr(0, nested_fields.size() - 1);
    }

    std::string data_file_path;
    std::string hive_column_names;
    std::string hive_column_types;
    std::string serde;
    std::string input_format;
    std::map<std::string, std::string> serde_properties;
    std::string time_zone;

    if (dynamic_cast<const FileTableDescriptor*>(_hive_table)) {
        const auto* file_table = dynamic_cast<const FileTableDescriptor*>(_hive_table);

        data_file_path = scan_range.full_path;

        hive_column_names = file_table->get_hive_column_names();
        hive_column_types = file_table->get_hive_column_types();
        serde = file_table->get_serde_lib();
        input_format = file_table->get_input_format();
        time_zone = file_table->get_time_zone();
    } else {
        const auto* hdfs_table = dynamic_cast<const HdfsTableDescriptor*>(_hive_table);

        auto* partition_desc = hdfs_table->get_partition(scan_range.partition_id);
        std::string partition_full_path = partition_desc->location();
        data_file_path = fmt::format("{}/{}", partition_full_path, scan_range.relative_path);

        hive_column_names = hdfs_table->get_hive_column_names();
        hive_column_types = hdfs_table->get_hive_column_types();
        serde = hdfs_table->get_serde_lib();
        input_format = hdfs_table->get_input_format();
        serde_properties = hdfs_table->get_serde_properties();
        time_zone = hdfs_table->get_time_zone();
    }

    std::map<std::string, std::string> jni_scanner_params;

    jni_scanner_params["hive_column_names"] = hive_column_names;
    jni_scanner_params["hive_column_types"] = hive_column_types;
    jni_scanner_params["required_fields"] = required_fields;
    jni_scanner_params["nested_fields"] = nested_fields;
    jni_scanner_params["data_file_path"] = data_file_path;
    jni_scanner_params["block_offset"] = std::to_string(scan_range.offset);
    jni_scanner_params["block_length"] = std::to_string(scan_range.length);
    jni_scanner_params["serde"] = serde;
    jni_scanner_params["input_format"] = input_format;
    jni_scanner_params["fs_options_props"] = build_fs_options_properties(options);
    jni_scanner_params["time_zone"] = time_zone;

    for (const auto& pair : serde_properties) {
        jni_scanner_params[serde_property_prefix + pair.first] = pair.second;
    }

    std::string scanner_factory_class = "com/starrocks/hive/reader/HiveScannerFactory";

    HdfsScanner* scanner = _pool.add(new HiveJniScanner(scanner_factory_class, jni_scanner_params));
    return scanner;
}

HdfsScanner* HiveDataSource::_create_odps_jni_scanner(const FSOptions& options) {
    const auto* odps_table = dynamic_cast<const OdpsTableDescriptor*>(_hive_table);
    std::string required_fields;
    for (auto slot : _tuple_desc->slots()) {
        required_fields.append(slot->col_name());
        required_fields.append(",");
    }
    required_fields = required_fields.substr(0, required_fields.size() - 1);
    std::string nested_fields;
    for (auto slot : _tuple_desc->slots()) {
        const TypeDescriptor& type = slot->type();
        if (type.is_complex_type()) {
            build_nested_fields(type, slot->col_name(), &nested_fields);
        }
    }
    if (!nested_fields.empty()) {
        nested_fields = nested_fields.substr(0, nested_fields.size() - 1);
    }
    std::map<std::string, std::string> jni_scanner_params;
    jni_scanner_params["project_name"] = odps_table->get_database_name();
    jni_scanner_params["table_name"] = odps_table->get_table_name();
    jni_scanner_params["required_fields"] = required_fields;
    jni_scanner_params.insert(_scan_range.odps_split_infos.begin(), _scan_range.odps_split_infos.end());
    jni_scanner_params["nested_fields"] = nested_fields;
    jni_scanner_params["time_zone"] = odps_table->get_time_zone();

    const AliyunCloudConfiguration aliyun_cloud_configuration =
            CloudConfigurationFactory::create_aliyun(*options.cloud_configuration);
    AliyunCloudCredential aliyun_cloud_credential = aliyun_cloud_configuration.aliyun_cloud_credential;
    jni_scanner_params["endpoint"] = aliyun_cloud_credential.endpoint;
    jni_scanner_params["access_id"] = aliyun_cloud_credential.access_key;
    jni_scanner_params["access_key"] = aliyun_cloud_credential.secret_key;

    std::string scanner_factory_class = "com/starrocks/odps/reader/OdpsSplitScannerFactory";
    HdfsScanner* scanner = _pool.add(new JniScanner(scanner_factory_class, jni_scanner_params));
    return scanner;
}

Status HiveDataSource::_init_scanner(RuntimeState* state) {
    SCOPED_TIMER(_profile.open_file_timer);

    const auto& scan_range = _scan_range;
    std::string native_file_path = scan_range.full_path;
    if (_hive_table != nullptr && _hive_table->has_partition() && !_hive_table->has_base_path()) {
        auto* partition_desc = _hive_table->get_partition(scan_range.partition_id);
        if (partition_desc == nullptr) {
            return Status::InternalError(fmt::format(
                    "Plan inconsistency. scan_range.partition_id = {} not found in partition description map",
                    scan_range.partition_id));
        }
        std::filesystem::path file_path(partition_desc->location());
        file_path /= scan_range.relative_path;
        native_file_path = file_path.native();
    }
    if (native_file_path.empty()) {
        native_file_path = _hive_table->get_base_path() + scan_range.relative_path;
    }

    const auto& hdfs_scan_node = _provider->_hdfs_scan_node;
    auto fsOptions =
            FSOptions(hdfs_scan_node.__isset.cloud_configuration ? &hdfs_scan_node.cloud_configuration : nullptr);

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateUniqueFromString(native_file_path, fsOptions));

    HdfsScannerParams scanner_params;
    scanner_params.runtime_filter_collector = _runtime_filters;
    scanner_params.scan_range = &scan_range;
    scanner_params.fs = _pool.add(fs.release());
    scanner_params.path = native_file_path;
    scanner_params.file_size = _scan_range.file_length;
    scanner_params.modification_time = _scan_range.modification_time;
    scanner_params.tuple_desc = _tuple_desc;
    scanner_params.materialize_slots = _materialize_slots;
    scanner_params.materialize_index_in_chunk = _materialize_index_in_chunk;
    scanner_params.partition_slots = _partition_slots;
    scanner_params.partition_index_in_chunk = _partition_index_in_chunk;
    scanner_params._partition_index_in_hdfs_partition_columns = _partition_index_in_hdfs_partition_columns;
    scanner_params.partition_values = _partition_values;
    scanner_params.conjunct_ctxs = _scanner_conjunct_ctxs;
    scanner_params.conjunct_ctxs_by_slot = _conjunct_ctxs_by_slot;
    scanner_params.slots_in_conjunct = _slots_in_conjunct;
    scanner_params.slots_of_mutli_slot_conjunct = _slots_of_mutli_slot_conjunct;
    scanner_params.min_max_conjunct_ctxs = _min_max_conjunct_ctxs;
    scanner_params.min_max_tuple_desc = _min_max_tuple_desc;
    scanner_params.hive_column_names = &_hive_column_names;
    scanner_params.case_sensitive = _case_sensitive;
    scanner_params.profile = &_profile;
    scanner_params.lazy_column_coalesce_counter = get_lazy_column_coalesce_counter();
    scanner_params.split_context = down_cast<HdfsSplitContext*>(_split_context);
    scanner_params.enable_split_tasks = _enable_split_tasks;
    if (state->query_options().__isset.connector_max_split_size) {
        scanner_params.connector_max_split_size = state->query_options().connector_max_split_size;
    }

    if (!_equality_delete_slots.empty()) {
        MORParams& mor_params = scanner_params.mor_params;
        mor_params.tuple_desc = _tuple_desc;
        mor_params.equality_slots = _equality_delete_slots;
        mor_params.delete_column_tuple_desc = _delete_column_tuple_desc;
        mor_params.mor_tuple_id = _provider->_hdfs_scan_node.mor_tuple_id;
        mor_params.runtime_profile = _runtime_profile;
    }

    for (const auto& delete_file : scan_range.delete_files) {
        scanner_params.deletes.emplace_back(&delete_file);
    }

    if (dynamic_cast<const IcebergTableDescriptor*>(_hive_table)) {
        auto tbl = dynamic_cast<const IcebergTableDescriptor*>(_hive_table);
        scanner_params.iceberg_schema = tbl->get_iceberg_schema();
        scanner_params.iceberg_equal_delete_schema = tbl->get_iceberg_equal_delete_schema();
    }
    scanner_params.use_datacache = _use_datacache;
    scanner_params.enable_populate_datacache = _enable_populate_datacache;
    scanner_params.can_use_any_column = _can_use_any_column;
    scanner_params.can_use_min_max_count_opt = _can_use_min_max_count_opt;
    scanner_params.use_file_metacache = _use_file_metacache;

    HdfsScanner* scanner = nullptr;
    auto format = scan_range.file_format;

    bool use_hudi_jni_reader = false;
    if (scan_range.__isset.use_hudi_jni_reader) {
        use_hudi_jni_reader = scan_range.use_hudi_jni_reader;
    }
    bool use_paimon_jni_reader = false;
    if (scan_range.__isset.use_paimon_jni_reader) {
        use_paimon_jni_reader = scan_range.use_paimon_jni_reader;
    }
    bool use_odps_jni_reader = false;
    if (scan_range.__isset.use_odps_jni_reader) {
        use_odps_jni_reader = scan_range.use_odps_jni_reader;
    }

    if (_use_partition_column_value_only) {
        DCHECK(_can_use_any_column);
        scanner = _pool.add(new HdfsPartitionScanner());
    } else if (use_paimon_jni_reader) {
        scanner = _create_paimon_jni_scanner(fsOptions);
    } else if (use_hudi_jni_reader) {
        scanner = _create_hudi_jni_scanner(fsOptions);
    } else if (use_odps_jni_reader) {
        scanner = _create_odps_jni_scanner(fsOptions);
    } else if (format == THdfsFileFormat::PARQUET) {
        scanner = _pool.add(new HdfsParquetScanner());
    } else if (format == THdfsFileFormat::ORC) {
        scanner_params.orc_use_column_names = state->query_options().orc_use_column_names;
        scanner = _pool.add(new HdfsOrcScanner());
    } else if (format == THdfsFileFormat::TEXT) {
        scanner = _pool.add(new HdfsTextScanner());
    } else if ((format == THdfsFileFormat::AVRO || format == THdfsFileFormat::RC_BINARY ||
                format == THdfsFileFormat::RC_TEXT || format == THdfsFileFormat::SEQUENCE_FILE) &&
               (dynamic_cast<const HdfsTableDescriptor*>(_hive_table) != nullptr ||
                dynamic_cast<const FileTableDescriptor*>(_hive_table) != nullptr)) {
        scanner = _create_hive_jni_scanner(fsOptions);
    } else {
        std::string msg = fmt::format("unsupported hdfs file format: {}", format);
        LOG(WARNING) << msg;
        return Status::NotSupported(msg);
    }
    RETURN_IF_ERROR(scanner->init(state, scanner_params));
    Status st = scanner->open(state);
    if (!st.ok()) {
        if (scanner->is_jni_scanner()) {
            return st;
        }

        auto msg = fmt::format("file = {}", native_file_path);

        // After catching the AWS 404 file not found error and returning it to the FE,
        // the FE will refresh the file information of table and re-execute the SQL operation.
        if (st.is_io_error() && st.message().find("404") != std::string_view::npos) {
            st = Status::RemoteFileNotFound(st.message());
        }
        return st.clone_and_append(msg);
    }
    _scanner = scanner;
    return Status::OK();
}

void HiveDataSource::close(RuntimeState* state) {
    if (_scanner != nullptr) {
        if (!_scanner->has_split_tasks()) {
            COUNTER_UPDATE(_profile.scan_ranges_counter, 1);
            COUNTER_UPDATE(_profile.scan_ranges_size, _scan_range.length);
        }
        _scanner->close();
    }
    Expr::close(_min_max_conjunct_ctxs, state);
    Expr::close(_partition_conjunct_ctxs, state);
    Expr::close(_scanner_conjunct_ctxs, state);
    for (auto& it : _conjunct_ctxs_by_slot) {
        Expr::close(it.second, state);
    }
}

Status HiveDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    if (_no_data) {
        return Status::EndOfFile("no data");
    }
    _init_chunk(chunk, _runtime_state->chunk_size());
    do {
        RETURN_IF_ERROR(_scanner->get_next(state, chunk));
    } while ((*chunk)->num_rows() == 0);

    // The column order of chunk is required to be invariable. In order to simplify the logic of each scanner,
    // we force to reorder the columns of chunk, so scanner doesn't have to care about the column order anymore.
    // The overhead of reorder is negligible because we only swap columns.
    ChunkHelper::reorder_chunk(*_tuple_desc, chunk->get());

    return Status::OK();
}

void HiveDataSource::_init_chunk(ChunkPtr* chunk, size_t n) {
    *chunk = ChunkHelper::new_chunk(*_tuple_desc, n);

    if (!_equality_delete_slots.empty()) {
        std::map<SlotId, SlotDescriptor*> id_to_slots;
        for (const auto& slot : _tuple_desc->slots()) {
            id_to_slots.emplace(slot->id(), slot);
        }

        for (const auto& slot : _equality_delete_slots) {
            if (!id_to_slots.contains(slot->id())) {
                const auto column = ColumnHelper::create_column(slot->type(), slot->is_nullable());
                column->reserve(n);
                (*chunk)->append_column(column, slot->id());
            }
        }
    }
}

const std::string HiveDataSource::get_custom_coredump_msg() const {
    const std::string path = !_scan_range.relative_path.empty() ? _scan_range.relative_path : _scan_range.full_path;
    return strings::Substitute("Hive file path: $0, partition id: $1, length: $2, offset: $3", path,
                               _scan_range.partition_id, _scan_range.length, _scan_range.offset);
}

int64_t HiveDataSource::raw_rows_read() const {
    if (_scanner == nullptr) return 0;
    return _scanner->raw_rows_read();
}
int64_t HiveDataSource::num_rows_read() const {
    if (_scanner == nullptr) return 0;
    return _scanner->num_rows_read();
}
int64_t HiveDataSource::num_bytes_read() const {
    if (_scanner == nullptr) return 0;
    return _scanner->num_bytes_read();
}
int64_t HiveDataSource::cpu_time_spent() const {
    if (_scanner == nullptr) return 0;
    return _scanner->cpu_time_spent();
}

int64_t HiveDataSource::io_time_spent() const {
    if (_scanner == nullptr) return 0;
    return _scanner->io_time_spent();
}

int64_t HiveDataSource::estimated_mem_usage() const {
    if (_scanner == nullptr) return 0;
    return _scanner->estimated_mem_usage();
}

void HiveDataSourceProvider::peek_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    for (const auto& sc : scan_ranges) {
        const TScanRange& x = sc.scan_range;
        if (!x.__isset.hdfs_scan_range) continue;
        const THdfsScanRange& y = x.hdfs_scan_range;
        _max_file_length = std::max(_max_file_length, y.file_length);
    }
}

void HiveDataSourceProvider::default_data_source_mem_bytes(int64_t* min_value, int64_t* max_value) {
    DataSourceProvider::default_data_source_mem_bytes(min_value, max_value);
    // here we compute as default mem bytes = max(MIN_SIZE, min(max_file_length, MAX_SIZE))
    int64_t size = std::max(*min_value, std::min(_max_file_length * 3 / 2, *max_value));
    *min_value = *max_value = size;
}

void HiveDataSource::get_split_tasks(std::vector<pipeline::ScanSplitContextPtr>* split_tasks) {
    if (_scanner == nullptr) return;
    _scanner->move_split_tasks(split_tasks);
}

} // namespace starrocks::connector
