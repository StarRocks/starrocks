// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/hdfs_chunk_source.h"

#include "env/env.h"
#include "exec/pipeline/scan_operator.h"
#include "exec/vectorized/hdfs_scan_node.h"
#include "exec/vectorized/hdfs_scanner_orc.h"
#include "exec/vectorized/hdfs_scanner_parquet.h"
#include "exec/vectorized/hdfs_scanner_text.h"
#include "exec/workgroup/work_group.h"
#include "exprs/vectorized/runtime_filter.h"
#include "gutil/map_util.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "storage/vectorized/chunk_helper.h"
#include "util/hdfs_util.h"

namespace starrocks::pipeline {
using namespace vectorized;

HdfsChunkSource::HdfsChunkSource(RuntimeProfile* runtime_profile, MorselPtr&& morsel, ScanOperator* op,
                                 vectorized::HdfsScanNode* scan_node)
        : ChunkSource(runtime_profile, std::move(morsel)),
          _scan_node(scan_node),
          _limit(scan_node->limit()),
          _runtime_in_filters(op->runtime_in_filters()),
          _runtime_bloom_filters(op->runtime_bloom_filters()) {
    _conjunct_ctxs = scan_node->conjunct_ctxs();
    _conjunct_ctxs.insert(_conjunct_ctxs.end(), _runtime_in_filters.begin(), _runtime_in_filters.end());
    ScanMorsel* scan_morsel = (ScanMorsel*)_morsel.get();
    _scan_range = scan_morsel->get_hdfs_scan_range();
}

HdfsChunkSource::~HdfsChunkSource() {
    if (_runtime_state != nullptr) {
        close(_runtime_state);
    }
}

Status HdfsChunkSource::prepare(RuntimeState* state) {
    // right now we don't force user to set JAVA_HOME.
    // but when we access hdfs via JNI, we have to make sure JAVA_HOME is set,
    // otherwise be will crash because of failure to create JVM.
    const char* p = std::getenv("JAVA_HOME");
    if (p == nullptr) {
        return Status::RuntimeError("env 'JAVA_HOME' is not set");
    }

    if (_scan_range->file_length == 0) {
        _no_data = true;
        return Status::OK();
    }

    const auto& hdfs_scan_node = _scan_node->thrift_hdfs_scan_node();
    _runtime_state = state;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(hdfs_scan_node.tuple_id);
    _lake_table = dynamic_cast<const LakeTableDescriptor*>(_tuple_desc->table_desc());
    if (_lake_table == nullptr) {
        return Status::RuntimeError("Invalid table type. Only hive/iceberg/hudi table are supported");
    }

    RETURN_IF_ERROR(_init_conjunct_ctxs(state));
    _init_tuples_and_slots(state);
    _init_counter(state);
    _init_partition_values();
    if (_filter_by_eval_partition_conjuncts) {
        _no_data = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(_init_scanner(state));
    return Status::OK();
}

Status HdfsChunkSource::_init_conjunct_ctxs(RuntimeState* state) {
    const auto& hdfs_scan_node = _scan_node->thrift_hdfs_scan_node();
    if (hdfs_scan_node.__isset.min_max_conjuncts) {
        RETURN_IF_ERROR(Expr::create_expr_trees(_pool, hdfs_scan_node.min_max_conjuncts, &_min_max_conjunct_ctxs));
    }

    if (hdfs_scan_node.__isset.partition_conjuncts) {
        RETURN_IF_ERROR(Expr::create_expr_trees(_pool, hdfs_scan_node.partition_conjuncts, &_partition_conjunct_ctxs));
        _has_partition_conjuncts = true;
    }
    RETURN_IF_ERROR(Expr::prepare(_min_max_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_partition_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_min_max_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_partition_conjunct_ctxs, state));

    _decompose_conjunct_ctxs();
    return Status::OK();
}

void HdfsChunkSource::_init_partition_values() {
    if (!(_lake_table != nullptr && _has_partition_columns)) return;

    auto* partition_desc = _lake_table->get_partition(_scan_range->partition_id);
    const auto& partition_values = partition_desc->partition_key_value_evals();
    _partition_values = partition_values;

    if (_has_partition_conjuncts) {
        ChunkPtr partition_chunk = ChunkHelper::new_chunk(_partition_slots, 1);
        // append partition data
        for (size_t i = 0; i < _partition_slots.size(); i++) {
            SlotId slot_id = _partition_slots[i]->id();
            int partition_col_idx = _partition_index_in_hdfs_partition_columns[i];
            auto partition_value_col = partition_values[partition_col_idx]->evaluate(nullptr);
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
}

void HdfsChunkSource::_init_tuples_and_slots(RuntimeState* state) {
    const auto& hdfs_scan_node = _scan_node->thrift_hdfs_scan_node();
    if (hdfs_scan_node.__isset.min_max_tuple_id) {
        _min_max_tuple_id = hdfs_scan_node.min_max_tuple_id;
        _min_max_tuple_desc = state->desc_tbl().get_tuple_descriptor(_min_max_tuple_id);
    }

    const auto& slots = _tuple_desc->slots();
    for (size_t i = 0; i < slots.size(); i++) {
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
}

void HdfsChunkSource::_decompose_conjunct_ctxs() {
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

void HdfsChunkSource::_init_counter(RuntimeState* state) {
    const auto& hdfs_scan_node = _scan_node->thrift_hdfs_scan_node();

    _profile.runtime_profile = _runtime_profile;
    _profile.pool = _pool;
    _profile.rows_read_counter = ADD_COUNTER(_runtime_profile, "RowsRead", TUnit::UNIT);
    _profile.bytes_read_counter = ADD_COUNTER(_runtime_profile, "BytesRead", TUnit::BYTES);

    _profile.scan_timer = ADD_TIMER(_runtime_profile, "ScanTime");
    _profile.scanner_queue_timer = ADD_TIMER(_runtime_profile, "ScannerQueueTime");
    _profile.scanner_queue_counter = ADD_COUNTER(_runtime_profile, "ScannerQueueCounter", TUnit::UNIT);

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

Status HdfsChunkSource::_init_scanner(RuntimeState* state) {
    const auto& scan_range = *_scan_range;
    COUNTER_UPDATE(_profile.scan_files_counter, 1);
    std::string native_file_path = scan_range.full_path;
    if (_lake_table != nullptr && _lake_table->has_partition()) {
        auto* partition_desc = _lake_table->get_partition(scan_range.partition_id);

        SCOPED_TIMER(_profile.open_file_timer);

        std::filesystem::path file_path(partition_desc->location());
        file_path /= scan_range.relative_path;
        native_file_path = file_path.native();
    }

    ASSIGN_OR_RETURN(auto env, Env::CreateUniqueFromString(native_file_path));

    COUNTER_UPDATE(_profile.scan_ranges_counter, 1);
    HdfsScannerParams scanner_params;
    scanner_params.runtime_filter_collector = _runtime_bloom_filters;
    scanner_params.scan_ranges = {&scan_range};
    scanner_params.env = _pool->add(env.release());
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
    scanner_params.profile = &_profile;
    scanner_params.open_limit = nullptr;

    HdfsScanner* scanner = nullptr;
    auto format = scan_range.file_format;
    if (format == THdfsFileFormat::PARQUET) {
        scanner = _pool->add(new HdfsParquetScanner());
    } else if (format == THdfsFileFormat::ORC) {
        scanner = _pool->add(new HdfsOrcScanner());
    } else if (format == THdfsFileFormat::TEXT) {
        scanner = _pool->add(new HdfsTextScanner());
    } else {
        std::string msg = fmt::format("unsupported hdfs file format: {}", format);
        LOG(WARNING) << msg;
        return Status::NotSupported(msg);
    }
    RETURN_IF_ERROR(scanner->init(state, scanner_params));
    RETURN_IF_ERROR(scanner->open(state));
    _scanner = scanner;
    return Status::OK();
}

void HdfsChunkSource::_init_chunk(ChunkPtr* chunk) {
    *chunk = ChunkHelper::new_chunk(*_tuple_desc, _runtime_state->chunk_size());
}

void HdfsChunkSource::close(RuntimeState* state) {
    if (_closed) return;
    if (_scanner != nullptr) {
        _scanner->close(state);
    }
    Expr::close(_min_max_conjunct_ctxs, state);
    Expr::close(_partition_conjunct_ctxs, state);
    _closed = true;
}

// ===================================================================

bool HdfsChunkSource::has_next_chunk() const {
    // If we need and could get next chunk from storage engine,
    // the _status must be ok.
    return _status.ok();
}

bool HdfsChunkSource::has_output() const {
    return !_chunk_buffer.empty();
}

size_t HdfsChunkSource::get_buffer_size() const {
    return _chunk_buffer.get_size();
}

StatusOr<vectorized::ChunkPtr> HdfsChunkSource::get_next_chunk_from_buffer() {
    vectorized::ChunkPtr chunk = nullptr;
    _chunk_buffer.try_get(&chunk);
    return chunk;
}

Status HdfsChunkSource::buffer_next_batch_chunks_blocking(size_t batch_size, bool& can_finish) {
    if (!_status.ok()) {
        return _status;
    }
    using namespace vectorized;

    for (size_t i = 0; i < batch_size && !can_finish; ++i) {
        ChunkPtr chunk;
        _status = _read_chunk_from_storage(_runtime_state, &chunk);
        if (!_status.ok()) {
            // end of file is normal case, need process chunk
            if (_status.is_end_of_file()) {
                _chunk_buffer.put(std::move(chunk));
            }
            break;
        }
        _chunk_buffer.put(std::move(chunk));
    }
    return _status;
}

Status HdfsChunkSource::buffer_next_batch_chunks_blocking_for_workgroup(size_t batch_size, bool& can_finish,
                                                                        size_t* num_read_chunks, int worker_id,
                                                                        workgroup::WorkGroupPtr running_wg) {
    if (!_status.ok()) {
        return _status;
    }

    using namespace vectorized;
    int64_t time_spent = 0;
    for (size_t i = 0; i < batch_size && !can_finish; ++i) {
        {
            SCOPED_RAW_TIMER(&time_spent);

            ChunkPtr chunk;
            _status = _read_chunk_from_storage(_runtime_state, &chunk);
            if (!_status.ok()) {
                // end of file is normal case, need process chunk
                if (_status.is_end_of_file()) {
                    ++(*num_read_chunks);
                    _chunk_buffer.put(std::move(chunk));
                }
                break;
            }

            ++(*num_read_chunks);
            _chunk_buffer.put(std::move(chunk));
        }

        if (time_spent >= YIELD_MAX_TIME_SPENT) {
            break;
        }

        if (time_spent >= YIELD_PREEMPT_MAX_TIME_SPENT &&
            workgroup::WorkGroupManager::instance()->get_owners_of_scan_worker(worker_id, running_wg)) {
            break;
        }
    }

    return _status;
}

Status HdfsChunkSource::_read_chunk_from_storage(RuntimeState* state, vectorized::ChunkPtr* chunk) {
    if (_no_data) {
        return Status::EndOfFile("no data");
    }
    if (state->is_cancelled()) {
        return Status::Cancelled("canceled state");
    }
    _init_chunk(chunk);
    SCOPED_TIMER(_profile.scan_timer);
    do {
        RETURN_IF_ERROR(_scanner->get_next(state, chunk));
    } while ((*chunk)->num_rows() == 0);
    // Improve for select * from table limit x, x is small
    if (_limit != -1 && _scanner->num_rows_read() >= _limit) {
        return Status::EndOfFile("limit reach");
    }
    return Status::OK();
}

} // namespace starrocks::pipeline
