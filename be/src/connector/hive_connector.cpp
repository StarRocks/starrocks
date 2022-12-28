// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "connector/hive_connector.h"

#include "exec/exec_node.h"
#include "exec/vectorized/hdfs_scanner_orc.h"
#include "exec/vectorized/hdfs_scanner_parquet.h"
#include "exec/vectorized/hdfs_scanner_text.h"
#include "exprs/expr.h"
#include "storage/chunk_helper.h"

namespace starrocks {
namespace connector {
using namespace vectorized;

// ================================

DataSourceProviderPtr HiveConnector::create_data_source_provider(vectorized::ConnectorScanNode* scan_node,
                                                                 const TPlanNode& plan_node) const {
    return std::make_unique<HiveDataSourceProvider>(scan_node, plan_node);
}

// ================================

HiveDataSourceProvider::HiveDataSourceProvider(vectorized::ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _hdfs_scan_node(plan_node.hdfs_scan_node) {}

DataSourcePtr HiveDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<HiveDataSource>(this, scan_range);
}

// ================================

HiveDataSource::HiveDataSource(const HiveDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider), _scan_range(scan_range.hdfs_scan_range) {}

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
    _lake_table = dynamic_cast<const LakeTableDescriptor*>(_tuple_desc->table_desc());
    if (_lake_table == nullptr) {
        return Status::RuntimeError("Invalid table type. Only hive/iceberg/hudi table are supported");
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

Status HiveDataSource::_init_conjunct_ctxs(RuntimeState* state) {
    const auto& hdfs_scan_node = _provider->_hdfs_scan_node;
    if (hdfs_scan_node.__isset.min_max_conjuncts) {
        RETURN_IF_ERROR(Expr::create_expr_trees(&_pool, hdfs_scan_node.min_max_conjuncts, &_min_max_conjunct_ctxs));
    }

    if (hdfs_scan_node.__isset.partition_conjuncts) {
        RETURN_IF_ERROR(Expr::create_expr_trees(&_pool, hdfs_scan_node.partition_conjuncts, &_partition_conjunct_ctxs));
        _has_partition_conjuncts = true;
    }

    RETURN_IF_ERROR(Expr::prepare(_min_max_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_partition_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_min_max_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_partition_conjunct_ctxs, state));

    _decompose_conjunct_ctxs();
    return Status::OK();
}

Status HiveDataSource::_init_partition_values() {
    if (!(_lake_table != nullptr && _has_partition_columns)) return Status::OK();

    auto* partition_desc = _lake_table->get_partition(_scan_range.partition_id);
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
            ColumnPtr data_column = const_column->data_column();
            ColumnPtr chunk_part_column = partition_chunk->get_column_by_slot_id(slot_id);
            if (data_column->is_nullable()) {
                chunk_part_column->append_nulls(1);
            } else {
                chunk_part_column->append(*data_column, 0, 1);
            }
        }

        // eval conjuncts and skip if no rows.
        ExecNode::eval_conjuncts(_partition_conjunct_ctxs, partition_chunk.get());
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
        if (_lake_table != nullptr && _lake_table->is_partition_col(slots[i])) {
            _partition_slots.push_back(slots[i]);
            _partition_index_in_chunk.push_back(i);
            _partition_index_in_hdfs_partition_columns.push_back(_lake_table->get_partition_col_index(slots[i]));
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

void HiveDataSource::_decompose_conjunct_ctxs() {
    if (_conjunct_ctxs.empty()) {
        return;
    }

    std::unordered_map<SlotId, SlotDescriptor*> slot_by_id;
    for (SlotDescriptor* slot : _tuple_desc->slots()) {
        slot_by_id[slot->id()] = slot;
    }

    for (ExprContext* ctx : _conjunct_ctxs) {
        const Expr* root_expr = ctx->root();
        std::vector<SlotId> slot_ids;
        if (root_expr->get_slot_ids(&slot_ids) != 1) {
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
}

void HiveDataSource::_init_counter(RuntimeState* state) {
    const auto& hdfs_scan_node = _provider->_hdfs_scan_node;

    _profile.runtime_profile = _runtime_profile;
    _profile.rows_read_counter = ADD_COUNTER(_runtime_profile, "RowsRead", TUnit::UNIT);
    _profile.bytes_read_counter = ADD_COUNTER(_runtime_profile, "BytesRead", TUnit::BYTES);

    _profile.scan_timer = ADD_TIMER(_runtime_profile, "ScanTime");
    _profile.scan_ranges_counter = ADD_COUNTER(_runtime_profile, "ScanRanges", TUnit::UNIT);
    _profile.scan_files_counter = ADD_COUNTER(_runtime_profile, "ScanFiles", TUnit::UNIT);

    _profile.reader_init_timer = ADD_TIMER(_runtime_profile, "ReaderInit");
    _profile.open_file_timer = ADD_TIMER(_runtime_profile, "OpenFile");
    _profile.expr_filter_timer = ADD_TIMER(_runtime_profile, "ExprFilterTime");

    _profile.io_timer = ADD_TIMER(_runtime_profile, "IOTime");
    _profile.io_counter = ADD_COUNTER(_runtime_profile, "IOCounter", TUnit::UNIT);
    _profile.column_read_timer = ADD_TIMER(_runtime_profile, "ColumnReadTime");
    _profile.column_convert_timer = ADD_TIMER(_runtime_profile, "ColumnConvertTime");

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

Status HiveDataSource::_init_scanner(RuntimeState* state) {
    const auto& scan_range = _scan_range;
    COUNTER_UPDATE(_profile.scan_files_counter, 1);
    std::string native_file_path = scan_range.full_path;
    if (_lake_table != nullptr && _lake_table->has_partition()) {
        auto* partition_desc = _lake_table->get_partition(scan_range.partition_id);
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

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateUniqueFromString(native_file_path));

    COUNTER_UPDATE(_profile.scan_ranges_counter, 1);
    HdfsScannerParams scanner_params;
    scanner_params.runtime_filter_collector = _runtime_filters;
    scanner_params.scan_ranges = {&scan_range};
    scanner_params.fs = _pool.add(fs.release());
    scanner_params.path = native_file_path;
    scanner_params.tuple_desc = _tuple_desc;
    scanner_params.materialize_slots = _materialize_slots;
    scanner_params.materialize_index_in_chunk = _materialize_index_in_chunk;
    scanner_params.partition_slots = _partition_slots;
    scanner_params.partition_index_in_chunk = _partition_index_in_chunk;
    scanner_params._partition_index_in_hdfs_partition_columns = _partition_index_in_hdfs_partition_columns;
    scanner_params.partition_values = _partition_values;
    scanner_params.conjunct_ctxs = _scanner_conjunct_ctxs;
    scanner_params.conjunct_ctxs_by_slot = _conjunct_ctxs_by_slot;
    scanner_params.min_max_conjunct_ctxs = _min_max_conjunct_ctxs;
    scanner_params.min_max_tuple_desc = _min_max_tuple_desc;
    scanner_params.hive_column_names = &_hive_column_names;
    scanner_params.case_sensitive = _case_sensitive;
    scanner_params.profile = &_profile;
    scanner_params.open_limit = nullptr;

    HdfsScanner* scanner = nullptr;
    auto format = scan_range.file_format;
    if (format == THdfsFileFormat::PARQUET) {
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
}

Status HiveDataSource::get_next(RuntimeState* state, vectorized::ChunkPtr* chunk) {
    if (_no_data) {
        return Status::EndOfFile("no data");
    }
    _init_chunk(chunk, _runtime_state->chunk_size());
    SCOPED_TIMER(_profile.scan_timer);
    do {
        RETURN_IF_ERROR(_scanner->get_next(state, chunk));
    } while ((*chunk)->num_rows() == 0);
    return Status::OK();
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

} // namespace connector
} // namespace starrocks
