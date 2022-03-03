// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/hdfs_chunk_source.h"

#include "env/env_hdfs.h"
#include "env/env_s3.h"
#include "exec/pipeline/scan_operator.h"
#include "exec/vectorized/hdfs_scan_node.h"
#include "exec/workgroup/work_group.h"
#include "exprs/vectorized/runtime_filter.h"
#include "gutil/map_util.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "storage/vectorized/chunk_helper.h"
#include "util/hdfs_util.h"

namespace starrocks::pipeline {
using namespace vectorized;

class OpenLimitAllocator {
public:
    OpenLimitAllocator() = default;
    ~OpenLimitAllocator() = default;

    OpenLimitAllocator(const OpenLimitAllocator&) = delete;
    void operator=(const OpenLimitAllocator&) = delete;

    static OpenLimitAllocator& instance() {
        static OpenLimitAllocator obj;
        return obj;
    }

    std::atomic<int32_t>* allocate(const std::string& key);

private:
    std::mutex _lock;
    std::unordered_map<std::string, std::atomic<int32_t>*> _data;
};

std::atomic<int32_t>* OpenLimitAllocator::allocate(const std::string& key) {
    std::lock_guard l(_lock);
    return LookupOrInsertNew(&_data, key, 0);
}

HdfsChunkSource::HdfsChunkSource(MorselPtr&& morsel, ScanOperator* op, vectorized::HdfsScanNode* scan_node)
        : ChunkSource(std::move(morsel)),
          _scan_node(scan_node),
          _limit(scan_node->limit()),
          _runtime_in_filters(op->runtime_in_filters()),
          _runtime_bloom_filters(op->runtime_bloom_filters()),
          _runtime_profile(op->unique_metrics()) {
    _conjunct_ctxs = scan_node->conjunct_ctxs();
    _conjunct_ctxs.insert(_conjunct_ctxs.end(), _runtime_in_filters.begin(), _runtime_in_filters.end());
    ScanMorsel* scan_morsel = (ScanMorsel*)_morsel.get();
    _scan_range = scan_morsel->get_hdfs_scan_range();
}

Status HdfsChunkSource::prepare(RuntimeState* state) {
    // right now we don't force user to set JAVA_HOME.
    // but when we access hdfs via JNI, we have to make sure JAVA_HOME is set,
    // otherwise be will crash because of failure to create JVM.
    const char* p = std::getenv("JAVA_HOME");
    if (p == nullptr) {
        return Status::RuntimeError("env 'JAVA_HOME' is not set");
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
    if (_filter_by_eval_partition_conjuncts) return Status::OK();

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
    if (!(_lake_table != nullptr && _has_partition_columns && _has_partition_conjuncts)) return;

    auto* partition_desc = _lake_table->get_partition(_scan_range->partition_id);
    const auto& partition_values = partition_desc->partition_key_value_evals();
    ChunkPtr partition_chunk = ChunkHelper::new_chunk(_partition_slots, 1);

    // append partition data
    for (size_t i = 0; i < _partition_slots.size(); i++) {
        SlotId slot_id = _partition_slots[i]->id();
        int partition_col_idx = _partition_index_in_hdfs_partition_columns[i];
        auto partition_value_col = partition_values[partition_col_idx]->evaluate(nullptr);
        assert(partition_value_col->is_constant());
        auto* const_column = ColumnHelper::as_raw_column<ConstColumn>(partition_value_col);
        ColumnPtr data_column = const_column->data_column();
        ColumnPtr chunk_part_column = _partition_chunk->get_column_by_slot_id(slot_id);
        if (data_column->is_nullable()) {
            chunk_part_column->append_nulls(1);
        } else {
            chunk_part_column->append(*data_column, 0, 1);
        }
    }

    // eval conjuncts and skip if no rows.
    ExecNode::eval_conjuncts(_partition_conjunct_ctxs, partition_chunk.get());
    if (partition_chunk->has_rows()) {
        _partition_values = partition_values;
    } else {
        _filter_by_eval_partition_conjuncts = true;
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

    _scan_timer = ADD_TIMER(_runtime_profile, "ScanTime");
    _scanner_queue_timer = ADD_TIMER(_runtime_profile, "ScannerQueueTime");
    _reader_init_timer = ADD_TIMER(_runtime_profile, "ReaderInit");
    _open_file_timer = ADD_TIMER(_runtime_profile, "OpenFile");
    _expr_filter_timer = ADD_TIMER(_runtime_profile, "ExprFilterTime");

    _io_timer = ADD_TIMER(_runtime_profile, "IoTime");
    _io_counter = ADD_COUNTER(_runtime_profile, "IoCounter", TUnit::UNIT);
    _column_read_timer = ADD_TIMER(_runtime_profile, "ColumnReadTime");
    _column_convert_timer = ADD_TIMER(_runtime_profile, "ColumnConvertTime");

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
    _scan_ranges_counter = ADD_COUNTER(_runtime_profile, "ScanRanges", TUnit::UNIT);
    _scan_files_counter = ADD_COUNTER(_runtime_profile, "ScanFiles", TUnit::UNIT);
}

Status HdfsChunkSource::_init_scanner(RuntimeState* state) {
    const auto& scan_range = *_scan_range;
    std::string scan_range_path = scan_range.full_path;
    if (_lake_table != nullptr && _lake_table->has_partition()) {
        scan_range_path = scan_range.relative_path;
    }

    COUNTER_UPDATE(_scan_files_counter, 1);
    std::string native_file_path = scan_range.full_path;
    if (_lake_table != nullptr) {
        auto* partition_desc = _lake_table->get_partition(scan_range.partition_id);

        SCOPED_TIMER(_open_file_timer);

        std::filesystem::path file_path(partition_desc->location());
        file_path /= scan_range.relative_path;
        native_file_path = file_path.native();
    }

    Env* env = nullptr;
    if (is_hdfs_path(native_file_path.c_str())) {
        env = _pool->add(new EnvHdfs());
    } else if (is_object_storage_path(native_file_path.c_str())) {
#ifdef STARROCKS_WITH_AWS
        env = _pool->add(new EnvS3());
#else
        return Status::NotSupported("Does not support read S3 file");
#endif
    } else {
        env = Env::Default();
    }

    std::string name_node;
    RETURN_IF_ERROR(get_namenode_from_path(native_file_path, &name_node));

    auto* hdfs_file_desc = _pool->add(new HdfsFileDesc());
    hdfs_file_desc->env = env;
    hdfs_file_desc->path = native_file_path;
    hdfs_file_desc->partition_id = scan_range.partition_id;
    hdfs_file_desc->scan_range_path = scan_range_path;
    hdfs_file_desc->file_length = scan_range.file_length;
    hdfs_file_desc->splits.emplace_back(&scan_range);
    hdfs_file_desc->hdfs_file_format = scan_range.file_format;
    hdfs_file_desc->open_limit = OpenLimitAllocator::instance().allocate(name_node);

    HdfsScannerParams scanner_params;
    scanner_params.runtime_filter_collector = _runtime_bloom_filters;
    scanner_params.scan_ranges = hdfs_file_desc.splits;
    scanner_params.env = hdfs_file_desc.env;
    scanner_params.path = hdfs_file_desc.path;
    scanner_params.tuple_desc = _tuple_desc;
    scanner_params.materialize_slots = _materialize_slots;
    scanner_params.materialize_index_in_chunk = _materialize_index_in_chunk;
    scanner_params.partition_slots = _partition_slots;
    scanner_params.partition_index_in_chunk = _partition_index_in_chunk;
    scanner_params._partition_index_in_hdfs_partition_columns = _partition_index_in_hdfs_partition_columns;
    scanner_params.partition_values = _partition_values_map[hdfs_file_desc.partition_id];
    scanner_params.conjunct_ctxs = _scanner_conjunct_ctxs;
    scanner_params.conjunct_ctxs_by_slot = _conjunct_ctxs_by_slot;
    scanner_params.min_max_conjunct_ctxs = _min_max_conjunct_ctxs;
    scanner_params.min_max_tuple_desc = _min_max_tuple_desc;
    scanner_params.hive_column_names = &_hive_column_names;
    scanner_params.parent = this;
    scanner_params.open_limit = hdfs_file_desc.open_limit;

    HdfsScanner* scanner = nullptr;
    if (hdfs_file_desc.hdfs_file_format == THdfsFileFormat::PARQUET) {
        _parquet_profile.init(_runtime_profile);
        scanner = _pool->add(new HdfsParquetScanner());
    } else if (hdfs_file_desc.hdfs_file_format == THdfsFileFormat::ORC) {
        scanner = _pool->add(new HdfsOrcScanner());
    } else if (hdfs_file_desc.hdfs_file_format == THdfsFileFormat::TEXT) {
        scanner = _pool->add(new HdfsTextScanner());
    } else {
        std::string msg = fmt::format("unsupported hdfs file format: {}", hdfs_file_desc.hdfs_file_format);
        LOG(WARNING) << msg;
        return Status::NotSupported(msg);
    }
    RETURN_IF_ERROR(scanner->init(state, scanner_params));

    return Status::OK();
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
        ChunkUniquePtr chunk(
                ChunkHelper::new_chunk_pooled(_prj_iter->encoded_schema(), _runtime_state->chunk_size(), true));
        _status = _read_chunk_from_storage(_runtime_state, chunk.get());
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

            ChunkUniquePtr chunk(
                    ChunkHelper::new_chunk_pooled(_prj_iter->encoded_schema(), _runtime_state->chunk_size(), true));
            _status = _read_chunk_from_storage(_runtime_state, chunk.get());
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

Status HdfsChunkSource::_read_chunk_from_storage(RuntimeState* state, vectorized::Chunk* chunk) {
    if (state->is_cancelled()) {
        return Status::Cancelled("canceled state");
    }
    SCOPED_TIMER(_scan_timer);
    do {
        // TODO(yan):
    } while (chunk->num_rows() == 0);
    _update_realtime_counter(chunk);
    // Improve for select * from table limit x, x is small
    if (_limit != -1 && _num_rows_read >= _limit) {
        return Status::EndOfFile("limit reach");
    }
    return Status::OK();
}

Status HdfsChunkSource::close(RuntimeState* state) {
    _update_counter();
    return Status::OK();
}

void HdfsChunkSource::_update_realtime_counter(vectorized::Chunk* chunk) {
    COUNTER_UPDATE(_read_compressed_counter, _reader->stats().compressed_bytes_read);
    _compressed_bytes_read += _reader->stats().compressed_bytes_read;
    _reader->mutable_stats()->compressed_bytes_read = 0;

    COUNTER_UPDATE(_raw_rows_counter, _reader->stats().raw_rows_read);
    _raw_rows_read += _reader->stats().raw_rows_read;
    _reader->mutable_stats()->raw_rows_read = 0;
    _num_rows_read += chunk->num_rows();
}

void HdfsChunkSource::_update_counter() {
    COUNTER_UPDATE(_create_seg_iter_timer, _reader->stats().create_segment_iter_ns);
    COUNTER_UPDATE(_rows_read_counter, _num_rows_read);

    COUNTER_UPDATE(_io_timer, _reader->stats().io_ns);
    COUNTER_UPDATE(_read_compressed_counter, _reader->stats().compressed_bytes_read);
    _compressed_bytes_read += _reader->stats().compressed_bytes_read;
    COUNTER_UPDATE(_decompress_timer, _reader->stats().decompress_ns);
    COUNTER_UPDATE(_read_uncompressed_counter, _reader->stats().uncompressed_bytes_read);
    COUNTER_UPDATE(_bytes_read_counter, _reader->stats().bytes_read);

    COUNTER_UPDATE(_block_load_timer, _reader->stats().block_load_ns);
    COUNTER_UPDATE(_block_load_counter, _reader->stats().blocks_load);
    COUNTER_UPDATE(_block_fetch_timer, _reader->stats().block_fetch_ns);
    COUNTER_UPDATE(_block_seek_timer, _reader->stats().block_seek_ns);

    COUNTER_UPDATE(_raw_rows_counter, _reader->stats().raw_rows_read);
    _raw_rows_read += _reader->mutable_stats()->raw_rows_read;
    COUNTER_UPDATE(_chunk_copy_timer, _reader->stats().vec_cond_chunk_copy_ns);

    COUNTER_UPDATE(_seg_init_timer, _reader->stats().segment_init_ns);

    COUNTER_UPDATE(_pred_filter_timer, _reader->stats().vec_cond_evaluate_ns);
    COUNTER_UPDATE(_pred_filter_counter, _reader->stats().rows_vec_cond_filtered);
    COUNTER_UPDATE(_del_vec_filter_counter, _reader->stats().rows_del_vec_filtered);

    COUNTER_UPDATE(_zm_filtered_counter, _reader->stats().rows_stats_filtered);
    COUNTER_UPDATE(_bf_filtered_counter, _reader->stats().rows_bf_filtered);
    COUNTER_UPDATE(_sk_filtered_counter, _reader->stats().rows_key_range_filtered);
    COUNTER_UPDATE(_index_load_timer, _reader->stats().index_load_ns);

    COUNTER_UPDATE(_read_pages_num_counter, _reader->stats().total_pages_num);
    COUNTER_UPDATE(_cached_pages_num_counter, _reader->stats().cached_pages_num);

    COUNTER_UPDATE(_bi_filtered_counter, _reader->stats().rows_bitmap_index_filtered);
    COUNTER_UPDATE(_bi_filter_timer, _reader->stats().bitmap_index_filter_timer);
    COUNTER_UPDATE(_block_seek_counter, _reader->stats().block_seek_num);

    COUNTER_UPDATE(_rowsets_read_count, _reader->stats().rowsets_read_count);
    COUNTER_UPDATE(_segments_read_count, _reader->stats().segments_read_count);
    COUNTER_UPDATE(_total_columns_data_page_count, _reader->stats().total_columns_data_page_count);

    COUNTER_SET(_pushdown_predicates_counter, (int64_t)_params.predicates.size());

    StarRocksMetrics::instance()->query_scan_bytes.increment(_compressed_bytes_read);
    StarRocksMetrics::instance()->query_scan_rows.increment(_raw_rows_read);

    if (_reader->stats().decode_dict_ns > 0) {
        RuntimeProfile::Counter* c = ADD_TIMER(_scan_profile, "DictDecode");
        COUNTER_UPDATE(c, _reader->stats().decode_dict_ns);
    }
    if (_reader->stats().late_materialize_ns > 0) {
        RuntimeProfile::Counter* c = ADD_TIMER(_scan_profile, "LateMaterialize");
        COUNTER_UPDATE(c, _reader->stats().late_materialize_ns);
    }
    if (_reader->stats().del_filter_ns > 0) {
        RuntimeProfile::Counter* c1 = ADD_TIMER(_scan_profile, "DeleteFilter");
        RuntimeProfile::Counter* c2 = ADD_COUNTER(_scan_profile, "DeleteFilterRows", TUnit::UNIT);
        COUNTER_UPDATE(c1, _reader->stats().del_filter_ns);
        COUNTER_UPDATE(c2, _reader->stats().rows_del_filtered);
    }
}

} // namespace starrocks::pipeline
