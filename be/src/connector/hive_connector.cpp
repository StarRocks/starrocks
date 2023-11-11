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

#include "exec/exec_node.h"
#include "exec/hdfs_scanner_orc.h"
#include "exec/hdfs_scanner_parquet.h"
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

Status HiveDataSource::open(RuntimeState* state) {
    // right now we don't force user to set JAVA_HOME.
    // but when we access hdfs via JNI, we have to make sure JAVA_HOME is set,
    // otherwise be will crash because of failure to create JVM.
    const char* p = std::getenv("JAVA_HOME");
    if (p == nullptr) {
        return Status::RuntimeError("env 'JAVA_HOME' is not set");
    }
    const auto& hdfs_scan_node = _provider->_hdfs_scan_node;
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

    _use_block_cache = config::block_cache_enable;
    if (state->query_options().__isset.use_scan_block_cache) {
        _use_block_cache &= state->query_options().use_scan_block_cache;
    }
    if (state->query_options().__isset.enable_populate_block_cache) {
        _enable_populate_block_cache = state->query_options().enable_populate_block_cache;
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
    _partition_values = partition_values;

    if (_has_partition_conjuncts) {
        ChunkPtr partition_chunk = ChunkHelper::new_chunk(_partition_slots, 1);
        // append partition data
        for (int i = 0; i < _partition_slots.size(); i++) {
            SlotId slot_id = _partition_slots[i]->id();
            int partition_col_idx = _partition_index_in_hdfs_partition_columns[i];
            ASSIGN_OR_RETURN(auto partition_value_col, partition_values[partition_col_idx]->evaluate(nullptr));
            assert(partition_value_col->is_constant());
            auto* const_column = ColumnHelper::as_raw_column<ConstColumn>(partition_value_col);
            const ColumnPtr& data_column = const_column->data_column();
            ColumnPtr& chunk_part_column = partition_chunk->get_column_by_slot_id(slot_id);
            if (data_column->is_nullable()) {
                chunk_part_column->append_default();
            } else {
                chunk_part_column->append(*data_column, 0, 1);
            }
        }

        // eval conjuncts and skip if no rows.
        RETURN_IF_ERROR(ExecNode::eval_conjuncts(_partition_conjunct_ctxs, partition_chunk.get()));
        if (!partition_chunk->has_rows()) {
            _filter_by_eval_partition_conjuncts = true;
        }
    }
    return Status::OK();
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
        } else {
            _materialize_slots.push_back(slots[i]);
            _materialize_index_in_chunk.push_back(i);
        }
    }

    if (hdfs_scan_node.__isset.hive_column_names) {
        _hive_column_names = hdfs_scan_node.hive_column_names;
    }
    if (hdfs_scan_node.__isset.case_sensitive) {
        _case_sensitive = hdfs_scan_node.case_sensitive;
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
            _conjunct_slots.insert(slot_id);
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
        if (!single_slot) {
            _scanner_conjunct_ctxs.emplace_back(ctx);
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
    _profile.rows_read_counter = ADD_COUNTER(_runtime_profile, "RowsRead", TUnit::UNIT);
    _profile.scan_ranges_counter = ADD_COUNTER(_runtime_profile, "ScanRanges", TUnit::UNIT);

    _profile.reader_init_timer = ADD_TIMER(_runtime_profile, "ReaderInit");
    _profile.open_file_timer = ADD_TIMER(_runtime_profile, "OpenFile");
    _profile.expr_filter_timer = ADD_TIMER(_runtime_profile, "ExprFilterTime");

    _profile.column_read_timer = ADD_TIMER(_runtime_profile, "ColumnReadTime");
    _profile.column_convert_timer = ADD_TIMER(_runtime_profile, "ColumnConvertTime");

    {
        static const char* prefix = "SharedBuffered";
        ADD_COUNTER(_runtime_profile, prefix, TUnit::UNIT);
        _profile.shared_buffered_shared_io_bytes =
                ADD_CHILD_COUNTER(_runtime_profile, "SharedIOBytes", TUnit::BYTES, prefix);
        _profile.shared_buffered_shared_io_count =
                ADD_CHILD_COUNTER(_runtime_profile, "SharedIOCount", TUnit::UNIT, prefix);
        _profile.shared_buffered_shared_io_timer = ADD_CHILD_TIMER(_runtime_profile, "SharedIOTime", prefix);
        _profile.shared_buffered_direct_io_bytes =
                ADD_CHILD_COUNTER(_runtime_profile, "DirectIOBytes", TUnit::BYTES, prefix);
        _profile.shared_buffered_direct_io_count =
                ADD_CHILD_COUNTER(_runtime_profile, "DirectIOCount", TUnit::UNIT, prefix);
        _profile.shared_buffered_direct_io_timer = ADD_CHILD_TIMER(_runtime_profile, "DirectIOTime", prefix);
    }

    if (_use_block_cache) {
        static const char* prefix = "BlockCache";
        ADD_COUNTER(_runtime_profile, prefix, TUnit::UNIT);
        _profile.block_cache_read_counter =
                ADD_CHILD_COUNTER(_runtime_profile, "BlockCacheReadCounter", TUnit::UNIT, prefix);
        _profile.block_cache_read_bytes =
                ADD_CHILD_COUNTER(_runtime_profile, "BlockCacheReadBytes", TUnit::BYTES, prefix);
        _profile.block_cache_read_timer = ADD_CHILD_TIMER(_runtime_profile, "BlockCacheReadTimer", prefix);
        _profile.block_cache_write_counter =
                ADD_CHILD_COUNTER(_runtime_profile, "BlockCacheWriteCounter", TUnit::UNIT, prefix);
        _profile.block_cache_write_bytes =
                ADD_CHILD_COUNTER(_runtime_profile, "BlockCacheWriteBytes", TUnit::BYTES, prefix);
        _profile.block_cache_write_timer = ADD_CHILD_TIMER(_runtime_profile, "BlockCacheWriteTimer", prefix);
        _profile.block_cache_write_fail_counter =
                ADD_CHILD_COUNTER(_runtime_profile, "BlockCacheWriteFailCounter", TUnit::UNIT, prefix);
        _profile.block_cache_write_fail_bytes =
                ADD_CHILD_COUNTER(_runtime_profile, "BlockCacheWriteFailBytes", TUnit::BYTES, prefix);
    }

    {
        static const char* prefix = "InputStream";
        ADD_COUNTER(_runtime_profile, prefix, TUnit::UNIT);
        _profile.app_io_bytes_read_counter =
                ADD_CHILD_COUNTER(_runtime_profile, "AppIOBytesRead", TUnit::BYTES, prefix);
        _profile.app_io_timer = ADD_CHILD_TIMER(_runtime_profile, "AppIOTime", prefix);
        _profile.app_io_counter = ADD_CHILD_COUNTER(_runtime_profile, "AppIOCounter", TUnit::UNIT, prefix);
        _profile.fs_bytes_read_counter = ADD_CHILD_COUNTER(_runtime_profile, "FSBytesRead", TUnit::BYTES, prefix);
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

HdfsScanner* HiveDataSource::_create_hudi_jni_scanner() {
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

    std::string scanner_factory_class = "com/starrocks/hudi/reader/HudiSliceScannerFactory";
    HdfsScanner* scanner = _pool.add(new JniScanner(scanner_factory_class, jni_scanner_params));
    return scanner;
}

HdfsScanner* HiveDataSource::_create_paimon_jni_scanner() {
    const auto* paimon_table = dynamic_cast<const PaimonTableDescriptor*>(_hive_table);

    std::string required_fields;
    for (auto slot : _tuple_desc->slots()) {
        required_fields.append(slot->col_name());
        required_fields.append(",");
    }
    required_fields = required_fields.substr(0, required_fields.size() - 1);

    std::map<std::string, std::string> jni_scanner_params;
    jni_scanner_params["catalog_type"] = paimon_table->get_catalog_type();
    jni_scanner_params["metastore_uri"] = paimon_table->get_metastore_uri();
    jni_scanner_params["warehouse_path"] = paimon_table->get_warehouse_path();
    jni_scanner_params["database_name"] = paimon_table->get_database_name();
    jni_scanner_params["table_name"] = paimon_table->get_table_name();
    jni_scanner_params["required_fields"] = required_fields;
    jni_scanner_params["split_info"] = _scan_range.paimon_split_info;
    jni_scanner_params["predicate_info"] = _scan_range.paimon_predicate_info;

    std::string scanner_factory_class = "com/starrocks/paimon/reader/PaimonSplitScannerFactory";
    HdfsScanner* scanner = _pool.add(new JniScanner(scanner_factory_class, jni_scanner_params));
    return scanner;
}

Status HiveDataSource::_init_scanner(RuntimeState* state) {
    const auto& scan_range = _scan_range;
    std::string native_file_path = scan_range.full_path;
    if (_hive_table != nullptr && _hive_table->has_partition()) {
        auto* partition_desc = _hive_table->get_partition(scan_range.partition_id);
        if (partition_desc == nullptr) {
            return Status::InternalError(fmt::format(
                    "Plan inconsistency. scan_range.partition_id = {} not found in partition description map",
                    scan_range.partition_id));
        }

        SCOPED_TIMER(_profile.open_file_timer);

        std::filesystem::path file_path(partition_desc->location());
        file_path /= scan_range.relative_path;
        native_file_path = file_path.native();
    }

    const auto& hdfs_scan_node = _provider->_hdfs_scan_node;
    auto fsOptions =
            FSOptions(hdfs_scan_node.__isset.cloud_configuration ? &hdfs_scan_node.cloud_configuration : nullptr);

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateUniqueFromString(native_file_path, fsOptions));

    COUNTER_UPDATE(_profile.scan_ranges_counter, 1);
    HdfsScannerParams scanner_params;
    scanner_params.runtime_filter_collector = _runtime_filters;
    scanner_params.scan_ranges = {&scan_range};
    scanner_params.fs = _pool.add(fs.release());
    scanner_params.path = native_file_path;
    scanner_params.file_size = _scan_range.file_length;
    scanner_params.tuple_desc = _tuple_desc;
    scanner_params.materialize_slots = _materialize_slots;
    scanner_params.materialize_index_in_chunk = _materialize_index_in_chunk;
    scanner_params.partition_slots = _partition_slots;
    scanner_params.partition_index_in_chunk = _partition_index_in_chunk;
    scanner_params._partition_index_in_hdfs_partition_columns = _partition_index_in_hdfs_partition_columns;
    scanner_params.partition_values = _partition_values;
    scanner_params.conjunct_ctxs = _scanner_conjunct_ctxs;
    scanner_params.conjunct_ctxs_by_slot = _conjunct_ctxs_by_slot;
    scanner_params.conjunct_slots = _conjunct_slots;
    scanner_params.min_max_conjunct_ctxs = _min_max_conjunct_ctxs;
    scanner_params.min_max_tuple_desc = _min_max_tuple_desc;
    scanner_params.hive_column_names = &_hive_column_names;
    scanner_params.case_sensitive = _case_sensitive;
    scanner_params.profile = &_profile;
    scanner_params.open_limit = nullptr;
    scanner_params.lazy_column_coalesce_counter = get_lazy_column_coalesce_counter();
    for (const auto& delete_file : scan_range.delete_files) {
        scanner_params.deletes.emplace_back(&delete_file);
    }
    if (dynamic_cast<const IcebergTableDescriptor*>(_hive_table)) {
        auto tbl = dynamic_cast<const IcebergTableDescriptor*>(_hive_table);
        scanner_params.iceberg_schema = tbl->get_iceberg_schema();
    }
    scanner_params.use_block_cache = _use_block_cache;
    scanner_params.enable_populate_block_cache = _enable_populate_block_cache;

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

    if (use_paimon_jni_reader) {
        scanner = _create_paimon_jni_scanner();
    } else if (use_hudi_jni_reader) {
        scanner = _create_hudi_jni_scanner();
    } else if (format == THdfsFileFormat::PARQUET) {
        scanner = _pool.add(new HdfsParquetScanner());
    } else if (format == THdfsFileFormat::ORC) {
        scanner = _pool.add(new HdfsOrcScanner());
    } else if (format == THdfsFileFormat::TEXT) {
        scanner = _pool.add(new HdfsTextScanner());
    } else {
        std::string msg = fmt::format("unsupported hdfs file format: {}", format);
        LOG(WARNING) << msg;
        return Status::NotSupported(msg);
    }
    RETURN_IF_ERROR(scanner->init(state, scanner_params));
    Status st = scanner->open(state);
    if (!st.ok()) {
        auto msg = fmt::format("file = {}", native_file_path);

        // After catching the AWS 404 file not found error and returning it to the FE,
        // the FE will refresh the file information of table and re-execute the SQL operation.
        if (st.is_io_error() && st.message().starts_with("code=404")) {
            st = Status::RemoteFileNotFound(st.message());
        }
        return st.clone_and_append(msg);
    }
    _scanner = scanner;
    return Status::OK();
}

void HiveDataSource::close(RuntimeState* state) {
    if (_scanner != nullptr) {
        _scanner->close(state);
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

} // namespace starrocks::connector
