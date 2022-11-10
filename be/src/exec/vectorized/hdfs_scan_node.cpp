// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/hdfs_scan_node.h"

#include <atomic>
#include <memory>

#include "env/env_hdfs.h"
#include "env/env_s3.h"
#include "exec/vectorized/hdfs_scanner.h"
#include "exec/vectorized/hdfs_scanner_orc.h"
#include "exec/vectorized/hdfs_scanner_parquet.h"
#include "exec/vectorized/hdfs_scanner_text.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/runtime_filter.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "gutil/map_util.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/hdfs/hdfs_fs_cache.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "storage/vectorized/chunk_helper.h"
#include "util/defer_op.h"
#include "util/hdfs_util.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::vectorized {

// NOTE: this class is append-only
class OpenLimitAllocator {
public:
    OpenLimitAllocator() = default;
    ~OpenLimitAllocator() {
        for (auto& it : _data) {
            delete it.second;
        }
    }

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

HdfsScanNode::HdfsScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs) {}

HdfsScanNode::~HdfsScanNode() {
    if (runtime_state() != nullptr) {
        close(runtime_state());
    }
}

Status HdfsScanNode::_init_table() {
    _lake_table = dynamic_cast<const LakeTableDescriptor*>(_tuple_desc->table_desc());
    if (_lake_table == nullptr) {
        return Status::RuntimeError("Invalid table type. Only hive/iceberg/hudi table are supported");
    }
    return Status::OK();
}

Status HdfsScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    const auto& hdfs_scan_node = tnode.hdfs_scan_node;

    if (hdfs_scan_node.__isset.min_max_conjuncts) {
        RETURN_IF_ERROR(Expr::create_expr_trees(_pool, hdfs_scan_node.min_max_conjuncts, &_min_max_conjunct_ctxs));
    }

    if (hdfs_scan_node.__isset.partition_conjuncts) {
        RETURN_IF_ERROR(Expr::create_expr_trees(_pool, hdfs_scan_node.partition_conjuncts, &_partition_conjunct_ctxs));
        _has_partition_conjuncts = true;
    }

    _tuple_desc = state->desc_tbl().get_tuple_descriptor(hdfs_scan_node.tuple_id);
    RETURN_IF_ERROR(_init_table());

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

    if (_has_partition_columns && _has_partition_conjuncts) {
        _partition_chunk = ChunkHelper::new_chunk(_partition_slots, 1);
    }

    if (hdfs_scan_node.__isset.hive_column_names) {
        _hive_column_names = hdfs_scan_node.hive_column_names;
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

    return Status::OK();
}

Status HdfsScanNode::prepare(RuntimeState* state) {
    // right now we don't force user to set JAVA_HOME.
    // but when we access hdfs via JNI, we have to make sure JAVA_HOME is set,
    // otherwise be will crash because of failure to create JVM.
    const char* p = std::getenv("JAVA_HOME");
    if (p == nullptr) {
        return Status::RuntimeError("env 'JAVA_HOME' is not set");
    }

    RETURN_IF_ERROR(ScanNode::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_min_max_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_partition_conjunct_ctxs, state));
    _init_counter();
    _runtime_state = state;
    return Status::OK();
}

Status HdfsScanNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    RETURN_IF_ERROR(ScanNode::open(state));

    RETURN_IF_ERROR(Expr::open(_min_max_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_partition_conjunct_ctxs, state));

    _decompose_conjunct_ctxs();
    if (_lake_table != nullptr) {
        RETURN_IF_ERROR(_init_partition_values_map());
    }
    for (auto& scan_range : _scan_ranges) {
        RETURN_IF_ERROR(_find_and_insert_hdfs_file(scan_range));
    }

    return Status::OK();
}

void HdfsScanNode::_decompose_conjunct_ctxs() {
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

Status HdfsScanNode::_start_scan_thread(RuntimeState* state) {
    if (_hdfs_files.empty()) {
        _update_status(Status::EndOfFile("empty hdfs files"));
        _result_chunks.shutdown();
        return Status::OK();
    }

    // create and init scanner
    for (auto& hdfs_file : _hdfs_files) {
        // skip empty file
        if (hdfs_file->file_length == 0) {
            continue;
        }
        RETURN_IF_ERROR(_create_and_init_scanner(state, *hdfs_file));
    }

    // init chunk pool
    _pending_scanners.reverse();
    _num_scanners = _pending_scanners.size();
    _chunks_per_scanner = config::doris_scanner_row_num / runtime_state()->chunk_size();
    _chunks_per_scanner += static_cast<int>(config::doris_scanner_row_num % runtime_state()->chunk_size() != 0);
    int concurrency = std::min<int>(kMaxConcurrency, _num_scanners);
    int chunks = _chunks_per_scanner * concurrency;
    _chunk_pool.reserve(chunks);
    TRY_CATCH_BAD_ALLOC(_fill_chunk_pool(chunks));

    // start scanner
    std::lock_guard<std::mutex> l(_mtx);
    for (int i = 0; i < concurrency; i++) {
        CHECK(_submit_scanner(_pop_pending_scanner(), true));
    }

    return Status::OK();
}

Status HdfsScanNode::_create_and_init_scanner(RuntimeState* state, const HdfsFileDesc& hdfs_file_desc) {
    HdfsScannerParams scanner_params;
    scanner_params.runtime_filter_collector = &_runtime_filter_collector;
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
    scanner_params.open_limit = hdfs_file_desc.open_limit;
    scanner_params.profile = &_profile;

    HdfsScanner* scanner = nullptr;
    if (hdfs_file_desc.hdfs_file_format == THdfsFileFormat::PARQUET) {
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
    _push_pending_scanner(scanner);

    return Status::OK();
}

bool HdfsScanNode::_submit_scanner(HdfsScanner* scanner, bool blockable) {
    auto* thread_pool = _runtime_state->exec_env()->thread_pool();
    int delta = static_cast<int>(!scanner->keep_priority());
    int32_t num_submit = _scanner_submit_count.fetch_add(delta, std::memory_order_relaxed);

    PriorityThreadPool::Task task;
    task.work_function = [this, scanner] { _scanner_thread(scanner); };
    task.priority = _compute_priority(num_submit);
    _running_threads.fetch_add(1, std::memory_order_release);

    if (thread_pool->try_offer(task)) {
        return true;
    }
    if (blockable) {
        thread_pool->offer(task);
        return true;
    }

    LOG(WARNING) << "thread pool busy";
    _running_threads.fetch_sub(1, std::memory_order_release);
    _scanner_submit_count.fetch_sub(delta, std::memory_order_relaxed);
    return false;
}

// The more tasks you submit, the less priority you get.
int HdfsScanNode::_compute_priority(int32_t num_submitted_tasks) {
    // int nice = 20;
    // while (nice > 0 && num_submitted_tasks > (22 - nice) * (20 - nice) * 6) {
    //     --nice;
    // }
    // return nice;
    if (num_submitted_tasks < 5) return 20;
    if (num_submitted_tasks < 19) return 19;
    if (num_submitted_tasks < 49) return 18;
    if (num_submitted_tasks < 91) return 17;
    if (num_submitted_tasks < 145) return 16;
    if (num_submitted_tasks < 211) return 15;
    if (num_submitted_tasks < 289) return 14;
    if (num_submitted_tasks < 379) return 13;
    if (num_submitted_tasks < 481) return 12;
    if (num_submitted_tasks < 595) return 11;
    if (num_submitted_tasks < 721) return 10;
    if (num_submitted_tasks < 859) return 9;
    if (num_submitted_tasks < 1009) return 8;
    if (num_submitted_tasks < 1171) return 7;
    if (num_submitted_tasks < 1345) return 6;
    if (num_submitted_tasks < 1531) return 5;
    if (num_submitted_tasks < 1729) return 4;
    if (num_submitted_tasks < 1939) return 3;
    if (num_submitted_tasks < 2161) return 2;
    if (num_submitted_tasks < 2395) return 1;
    return 0;
}

void HdfsScanNode::_scanner_thread(HdfsScanner* scanner) {
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(scanner->runtime_state()->instance_mem_tracker());
    DeferOp op([&] {
        tls_thread_status.set_mem_tracker(prev_tracker);
        _running_threads.fetch_sub(1, std::memory_order_release);

        if (_closed_scanners.load(std::memory_order_acquire) == _num_scanners) {
            _result_chunks.shutdown();
        }
    });

    // if global status was not ok
    // we need fast failure
    if (!_get_status().ok()) {
        scanner->release_pending_token(&_pending_token);
        scanner->close(_runtime_state);
        _closed_scanners.fetch_add(1, std::memory_order_release);
        _close_pending_scanners();
        return;
    }

    int concurrency_limit = config::max_hdfs_file_handle;

    // There is a situation where once a resource overrun has occurred,
    // the scanners that were previously overrun are basically in a pending state,
    // so even if there are enough resources to follow, they cannot be fully utilized,
    // and we need to schedule the scanners that are in a pending state as well.
    if (scanner->has_pending_token()) {
        int concurrency = std::min<int>(kMaxConcurrency, _num_scanners);
        int need_put = concurrency - _running_threads;
        int left_resource = concurrency_limit - scanner->open_limit();
        if (left_resource > 0) {
            need_put = std::min(left_resource, need_put);
            std::lock_guard<std::mutex> l(_mtx);
            while (need_put-- > 0 && !_pending_scanners.empty()) {
                if (!_submit_scanner(_pop_pending_scanner(), false)) {
                    break;
                }
            }
        }
    }

    if (!scanner->has_pending_token()) {
        scanner->acquire_pending_token(&_pending_token);
    }

    // if opened file greater than this. scanner will push back to pending list.
    // We can't have all scanners in the pending state, we need to
    // make sure there is at least one thread on each SCAN NODE that can be running
    if (!scanner->is_open() && scanner->open_limit() > concurrency_limit) {
        if (!scanner->has_pending_token()) {
            std::lock_guard<std::mutex> l(_mtx);
            _push_pending_scanner(scanner);
            return;
        }
    }

    Status status = scanner->open(_runtime_state);
    scanner->set_keep_priority(false);

    bool resubmit = false;
    int64_t raw_rows_threshold = scanner->raw_rows_read() + config::doris_scanner_row_num;

    ChunkPtr chunk = nullptr;

    while (status.ok()) {
        {
            std::lock_guard<std::mutex> l(_mtx);
            if (_chunk_pool.empty()) {
                scanner->set_keep_priority(true);
                scanner->release_pending_token(&_pending_token);
                _push_pending_scanner(scanner);
                scanner = nullptr;
                break;
            }
            chunk = _chunk_pool.pop();
        }

        status = scanner->get_next(_runtime_state, &chunk);
        if (!status.ok()) {
            std::lock_guard<std::mutex> l(_mtx);
            _chunk_pool.push(chunk);
            break;
        }

        if (!_result_chunks.put(chunk)) {
            status = Status::Aborted("result chunks has been shutdown");
            break;
        }
        if (scanner->raw_rows_read() >= raw_rows_threshold) {
            resubmit = true;
            break;
        }
    }

    Status global_status = _get_status();
    if (global_status.ok()) {
        if (status.ok() && resubmit) {
            if (!_submit_scanner(scanner, false)) {
                std::lock_guard<std::mutex> l(_mtx);
                scanner->release_pending_token(&_pending_token);
                _push_pending_scanner(scanner);
            }
        } else if (status.ok()) {
            DCHECK(scanner == nullptr);
        } else if (status.is_end_of_file()) {
            scanner->release_pending_token(&_pending_token);
            scanner->close(_runtime_state);
            _closed_scanners.fetch_add(1, std::memory_order_release);
            std::lock_guard<std::mutex> l(_mtx);
            auto nscanner = _pending_scanners.empty() ? nullptr : _pop_pending_scanner();
            if (nscanner != nullptr && !_submit_scanner(nscanner, false)) {
                _push_pending_scanner(nscanner);
            }
        } else {
            _update_status(status);
            scanner->release_pending_token(&_pending_token);
            scanner->close(_runtime_state);
            _closed_scanners.fetch_add(1, std::memory_order_release);
            _close_pending_scanners();
        }
    } else {
        // sometimes state == ok but global_status was not ok
        if (scanner != nullptr) {
            scanner->release_pending_token(&_pending_token);
            scanner->close(_runtime_state);
            _closed_scanners.fetch_add(1, std::memory_order_release);
            _close_pending_scanners();
        }
    }
}

void HdfsScanNode::_close_pending_scanners() {
    std::lock_guard<std::mutex> l(_mtx);
    while (!_pending_scanners.empty()) {
        auto* scanner = _pop_pending_scanner();
        scanner->close(_runtime_state);
        _closed_scanners.fetch_add(1, std::memory_order_release);
    }
}

void HdfsScanNode::_push_pending_scanner(HdfsScanner* scanner) {
    scanner->enter_pending_queue();
    _pending_scanners.push(scanner);
}

HdfsScanner* HdfsScanNode::_pop_pending_scanner() {
    HdfsScanner* scanner = _pending_scanners.pop();
    uint64_t time = scanner->exit_pending_queue();
    COUNTER_UPDATE(_profile.scanner_queue_timer, time);
    return scanner;
}

Status HdfsScanNode::_get_status() {
    std::lock_guard<SpinLock> lck(_status_mutex);
    return _status;
}

void HdfsScanNode::_fill_chunk_pool(int count) {
    std::lock_guard<std::mutex> l(_mtx);

    for (int i = 0; i < count; i++) {
        auto chunk = ChunkHelper::new_chunk(*_tuple_desc, runtime_state()->chunk_size());
        _chunk_pool.push(std::move(chunk));
    }
}

Status HdfsScanNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    if (!_start && _status.ok()) {
        Status status = _start_scan_thread(state);
        _update_status(status);
        LOG_IF(ERROR, !status.ok()) << "Failed to start scan node: " << status.to_string();
        _start = true;
        RETURN_IF_ERROR(status);
    } else if (!_start) {
        _result_chunks.shutdown();
        _start = true;
    }

    (*chunk).reset();

    Status status = _get_status();
    if (!status.ok()) {
        *eos = true;
        return status.is_end_of_file() ? Status::OK() : status;
    }

    {
        std::lock_guard<std::mutex> l(_mtx);
        const int32_t num_closed = _closed_scanners.load(std::memory_order_acquire);
        const int32_t num_pending = _pending_scanners.size();
        const int32_t num_running = _num_scanners - num_pending - num_closed;
        if ((num_pending > 0) && (num_running < kMaxConcurrency)) {
            if (_chunk_pool.size() >= (num_running + 1) * _chunks_per_scanner) {
                (void)_submit_scanner(_pop_pending_scanner(), true);
            }
        }
    }

    if (_result_chunks.blocking_get(chunk)) {
        TRY_CATCH_BAD_ALLOC(_fill_chunk_pool(1));

        eval_join_runtime_filters(chunk);

        _num_rows_returned += (*chunk)->num_rows();
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
        if (reached_limit()) {
            int64_t num_rows_over = _num_rows_returned - _limit;
            (*chunk)->set_num_rows((*chunk)->num_rows() - num_rows_over);
            COUNTER_SET(_rows_returned_counter, _limit);
            _update_status(Status::EndOfFile("HdfsScanNode has reach limit"));
            _result_chunks.shutdown();
        }
        *eos = false;
        DCHECK_CHUNK(*chunk);
        return Status::OK();
    }

    _update_status(Status::EndOfFile("EOF of HdfsScanNode"));
    *eos = true;
    status = _get_status();
    return status.is_end_of_file() ? Status::OK() : status;
}

Status HdfsScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    _update_status(Status::Cancelled("closed"));
    _result_chunks.shutdown();
    while (_running_threads.load(std::memory_order_acquire) > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    _close_pending_scanners();

    Expr::close(_min_max_conjunct_ctxs, state);
    Expr::close(_partition_conjunct_ctxs, state);

    RETURN_IF_ERROR(ScanNode::close(state));
    return Status::OK();
}

Status HdfsScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    for (const auto& scan_range : scan_ranges) {
        auto& range = scan_range.scan_range.hdfs_scan_range;
        if (range.file_length == 0) continue;
        _scan_ranges.emplace_back(range);
        COUNTER_UPDATE(_profile.scan_ranges_counter, 1);
    }

    return Status::OK();
}

Status HdfsScanNode::_init_partition_values_map() {
    if (_scan_ranges.empty() || !_lake_table->has_partition()) {
        return Status::OK();
    }

    for (auto& scan_range : _scan_ranges) {
        auto* partition_desc = _lake_table->get_partition(scan_range.partition_id);
        auto partition_id = scan_range.partition_id;
        if (_partition_values_map.find(partition_id) == _partition_values_map.end()) {
            _partition_values_map[partition_id] = partition_desc->partition_key_value_evals();
        }
    }

    if (_has_partition_columns && _has_partition_conjuncts) {
        auto it = _partition_values_map.begin();
        while (it != _partition_values_map.end()) {
            ASSIGN_OR_RETURN(bool res, _filter_partition(it->second));
            if (res) {
                _partition_values_map.erase(it++);
            } else {
                it++;
            }
        }
    }
    return Status::OK();
}

StatusOr<bool> HdfsScanNode::_filter_partition(const std::vector<ExprContext*>& partition_values) {
    _partition_chunk->reset();

    // append partition data
    for (size_t i = 0; i < _partition_slots.size(); i++) {
        SlotId slot_id = _partition_slots[i]->id();
        int partition_col_idx = _partition_index_in_hdfs_partition_columns[i];
        ASSIGN_OR_RETURN(auto partition_value_col, partition_values[partition_col_idx]->evaluate(nullptr));
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
    RETURN_IF_ERROR(ExecNode::eval_conjuncts(_partition_conjunct_ctxs, _partition_chunk.get()));
    if (_partition_chunk->has_rows()) {
        return false;
    }
    return true;
}

Status HdfsScanNode::_find_and_insert_hdfs_file(const THdfsScanRange& scan_range) {
    std::string scan_range_path = scan_range.full_path;
    if (_lake_table != nullptr && _lake_table->has_partition()) {
        if (_partition_values_map.find(scan_range.partition_id) == _partition_values_map.end()) {
            // partition has been filtered
            return Status::OK();
        }
        scan_range_path = scan_range.relative_path;
    }

    // search file in hdfs file array
    // if found, add file splits to hdfs file desc
    // if not found, create
    for (auto& item : _hdfs_files) {
        if (item->partition_id == scan_range.partition_id && item->scan_range_path == scan_range_path) {
            item->splits.emplace_back(&scan_range);
            return Status::OK();
        }
    }

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

    Env* env = nullptr;
    if (is_hdfs_path(native_file_path.c_str())) {
        env = _pool->add(new EnvHdfs());
    } else if (is_object_storage_path(native_file_path.c_str())) {
        env = _pool->add(new EnvS3());
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
    _hdfs_files.emplace_back(hdfs_file_desc);
    return Status::OK();
}

void HdfsScanNode::_update_status(const Status& status) {
    std::lock_guard<SpinLock> lck(_status_mutex);
    if (_status.ok()) {
        _status = status;
    }
}

void HdfsScanNode::_init_counter() {
    _profile.runtime_profile = _runtime_profile.get();

    // inherited from scan node.
    _profile.rows_read_counter = _rows_read_counter;
    _profile.bytes_read_counter = _bytes_read_counter;

    _profile.scan_timer = ADD_TIMER(_runtime_profile, "ScanTime");
    _profile.scanner_queue_timer = ADD_TIMER(_runtime_profile, "ScannerQueueTime");
    _profile.scan_ranges_counter = ADD_COUNTER(_runtime_profile, "ScanRanges", TUnit::UNIT);
    _profile.scan_files_counter = ADD_COUNTER(_runtime_profile, "ScanFiles", TUnit::UNIT);

    _profile.reader_init_timer = ADD_TIMER(_runtime_profile, "ReaderInit");
    _profile.open_file_timer = ADD_TIMER(_runtime_profile, "OpenFile");
    _profile.expr_filter_timer = ADD_TIMER(_runtime_profile, "ExprFilterTime");

    _profile.io_timer = ADD_TIMER(_runtime_profile, "IoTime");
    _profile.io_counter = ADD_COUNTER(_runtime_profile, "IoCounter", TUnit::UNIT);
    _profile.column_read_timer = ADD_TIMER(_runtime_profile, "ColumnReadTime");
    _profile.column_convert_timer = ADD_TIMER(_runtime_profile, "ColumnConvertTime");
}

} // namespace starrocks::vectorized
