// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/hdfs_scan_node.h"

#include <memory>

#include "env/env_hdfs.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/runtime_filter.h"
#include "runtime/exec_env.h"
#include "runtime/hdfs/hdfs_fs_cache.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "storage/vectorized/chunk_helper.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::vectorized {
HdfsScanNode::HdfsScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs) {}

Status HdfsScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));

    if (tnode.hdfs_scan_node.__isset.min_max_conjuncts) {
        RETURN_IF_ERROR(
                Expr::create_expr_trees(_pool, tnode.hdfs_scan_node.min_max_conjuncts, &_min_max_conjunct_ctxs));
    }

    if (tnode.hdfs_scan_node.__isset.partition_conjuncts) {
        RETURN_IF_ERROR(
                Expr::create_expr_trees(_pool, tnode.hdfs_scan_node.partition_conjuncts, &_partition_conjunct_ctxs));
        _has_partition_conjuncts = true;
    }

    _tuple_id = tnode.hdfs_scan_node.tuple_id;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    _hdfs_table = dynamic_cast<const HdfsTableDescriptor*>(_tuple_desc->table_desc());

    if (tnode.hdfs_scan_node.__isset.min_max_tuple_id) {
        _min_max_tuple_id = tnode.hdfs_scan_node.min_max_tuple_id;
        _min_max_tuple_desc = state->desc_tbl().get_tuple_descriptor(_min_max_tuple_id);
        _min_max_row_desc = _pool->add(new RowDescriptor(state->desc_tbl(), std::vector<TTupleId>{_min_max_tuple_id},
                                                         std::vector<bool>{true}));
    }

    const auto& slots = _tuple_desc->slots();
    for (size_t i = 0; i < slots.size(); i++) {
        if (_hdfs_table->is_partition_col(slots[i])) {
            _partition_slots.push_back(slots[i]);
            _partition_index_in_chunk.push_back(i);
            _partition_index_in_hdfs_partition_columns.push_back(_hdfs_table->get_partition_col_index(slots[i]));
            _has_partition_columns = true;
        } else {
            _materialize_slots.push_back(slots[i]);
            _materialize_index_in_chunk.push_back(i);
        }
    }

    if (_has_partition_columns && _has_partition_conjuncts) {
        _partition_chunk = ChunkHelper::new_chunk(_partition_slots, 1);
    }

    if (tnode.hdfs_scan_node.__isset.hive_column_names) {
        _hive_column_names = tnode.hdfs_scan_node.hive_column_names;
    }

    _mem_pool = std::make_unique<MemPool>(state->instance_mem_tracker());

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
    RETURN_IF_ERROR(Expr::prepare(_min_max_conjunct_ctxs, state, *_min_max_row_desc, expr_mem_tracker()));
    RETURN_IF_ERROR(Expr::prepare(_partition_conjunct_ctxs, state, row_desc(), expr_mem_tracker()));
    _init_counter(state);

    _runtime_state = state;
    return Status::OK();
}

Status HdfsScanNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    RETURN_IF_ERROR(ScanNode::open(state));

    RETURN_IF_ERROR(Expr::open(_min_max_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_partition_conjunct_ctxs, state));

    _pre_process_conjunct_ctxs();
    _init_partition_expr_map();

    for (auto& scan_range : _scan_ranges) {
        RETURN_IF_ERROR(_find_and_insert_hdfs_file(scan_range));
    }

    return Status::OK();
}

void HdfsScanNode::_pre_process_conjunct_ctxs() {
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
    _chunks_per_scanner = config::doris_scanner_row_num / config::vector_chunk_size;
    _chunks_per_scanner += static_cast<int>(config::doris_scanner_row_num % config::vector_chunk_size != 0);
    int concurrency = std::min<int>(kMaxConcurrency, _num_scanners);
    int chunks = _chunks_per_scanner * concurrency;
    _chunk_pool.reserve(chunks);
    _fill_chunk_pool(chunks);

    // start scanner
    std::lock_guard<std::mutex> l(_mtx);
    for (int i = 0; i < concurrency; i++) {
        CHECK(_submit_scanner(_pending_scanners.pop(), true));
    }

    return Status::OK();
}

Status HdfsScanNode::_create_and_init_scanner(RuntimeState* state, const HdfsFileDesc& hdfs_file_desc) {
    HdfsScannerParams scanner_params;
    scanner_params.runtime_filter_collector = &_runtime_filter_collector;
    scanner_params.scan_ranges = hdfs_file_desc.splits;
    scanner_params.fs = hdfs_file_desc.fs;
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

    HdfsScanner* scanner = nullptr;
    if (hdfs_file_desc.hdfs_file_format == THdfsFileFormat::PARQUET) {
        scanner = _pool->add(new HdfsParquetScanner());
    } else if (hdfs_file_desc.hdfs_file_format == THdfsFileFormat::ORC) {
        scanner = _pool->add(new HdfsOrcScanner());
    } else {
        string msg = "unsupported hdfs file format: " + hdfs_file_desc.hdfs_file_format;
        LOG(WARNING) << msg;
        return Status::NotSupported(msg);
    }

    RETURN_IF_ERROR(scanner->init(state, scanner_params));
    _pending_scanners.push(scanner);

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
                _pending_scanners.push(scanner);
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
                _pending_scanners.push(scanner);
            }
        } else if (status.ok()) {
            DCHECK(scanner == nullptr);
        } else if (status.is_end_of_file()) {
            scanner->close(_runtime_state);
            _closed_scanners.fetch_add(1, std::memory_order_release);
            std::lock_guard<std::mutex> l(_mtx);
            scanner = _pending_scanners.empty() ? nullptr : _pending_scanners.pop();
            if (scanner != nullptr && !_submit_scanner(scanner, false)) {
                _pending_scanners.push(scanner);
            }
        } else {
            _update_status(status);
            scanner->close(_runtime_state);
            _closed_scanners.fetch_add(1, std::memory_order_release);
            _close_pending_scanners();
        }
    } else {
        DCHECK(scanner != nullptr);
        scanner->close(_runtime_state);
        _closed_scanners.fetch_add(1, std::memory_order_release);
        _close_pending_scanners();
    }

    if (_closed_scanners.load(std::memory_order_acquire) == _num_scanners) {
        _result_chunks.shutdown();
    }
    _running_threads.fetch_sub(1, std::memory_order_release);
}

void HdfsScanNode::_close_pending_scanners() {
    std::lock_guard<std::mutex> l(_mtx);
    while (!_pending_scanners.empty()) {
        auto* scanner = _pending_scanners.pop();
        scanner->close(_runtime_state);
        _closed_scanners.fetch_add(1, std::memory_order_release);
    }
}

Status HdfsScanNode::_get_status() {
    std::lock_guard<SpinLock> lck(_status_mutex);
    return _status;
}

void HdfsScanNode::_fill_chunk_pool(int count) {
    std::lock_guard<std::mutex> l(_mtx);

    for (int i = 0; i < count; i++) {
        auto chunk = ChunkHelper::new_chunk(*_tuple_desc, config::vector_chunk_size);
        _chunk_pool.push(std::move(chunk));
    }
}

Status HdfsScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("HdfsScanNode don't support row_batch");
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
                (void)_submit_scanner(_pending_scanners.pop(), true);
            }
        }
    }

    if (_result_chunks.blocking_get(chunk)) {
        _fill_chunk_pool(1);

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

    for (auto* hdfsFile : _hdfs_files) {
        if (hdfsFile->hdfs_fs != nullptr && hdfsFile->hdfs_file != nullptr) {
            hdfsCloseFile(hdfsFile->hdfs_fs, hdfsFile->hdfs_file);
        }
    }

    _close_pending_scanners();

    Expr::close(_min_max_conjunct_ctxs, state);
    Expr::close(_partition_conjunct_ctxs, state);

    RETURN_IF_ERROR(ScanNode::close(state));
    return Status::OK();
}

Status HdfsScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    for (const auto& scan_range : scan_ranges) {
        _scan_ranges.emplace_back(scan_range.scan_range.hdfs_scan_range);
    }

    return Status::OK();
}

void HdfsScanNode::_init_partition_expr_map() {
    if (_scan_ranges.empty()) {
        return;
    }

    for (auto& scan_range : _scan_ranges) {
        auto* partition_desc = _hdfs_table->get_partition(scan_range.partition_id);

        if (_partition_values_map.find(scan_range.partition_id) == _partition_values_map.end()) {
            _partition_values_map[scan_range.partition_id] = partition_desc->partition_key_value_evals();
        }
    }

    if (_has_partition_columns && _has_partition_conjuncts) {
        auto it = _partition_values_map.begin();
        while (it != _partition_values_map.end()) {
            if (_filter_partition(it->second)) {
                _partition_values_map.erase(it++);
            } else {
                it++;
            }
        }
    }
}

bool HdfsScanNode::_filter_partition(const std::vector<ExprContext*>& partition_values) {
    _partition_chunk->reset();

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
    ExecNode::eval_conjuncts(_partition_conjunct_ctxs, _partition_chunk.get());
    if (_partition_chunk->has_rows()) {
        return false;
    }
    return true;
}

Status HdfsScanNode::_find_and_insert_hdfs_file(const THdfsScanRange& scan_range) {
    if (_partition_values_map.find(scan_range.partition_id) == _partition_values_map.end()) {
        // partition has been filtered
        return Status::OK();
    }

    // search file in hdfs file array
    // if found, add file splits to hdfs file desc
    // if not found, create
    for (auto& item : _hdfs_files) {
        if (item->partition_id == scan_range.partition_id && item->path == scan_range.relative_path) {
            item->splits.emplace_back(&scan_range);
            return Status::OK();
        }
    }

    auto* partition_desc = _hdfs_table->get_partition(scan_range.partition_id);

    SCOPED_TIMER(_open_file_timer);

    std::filesystem::path file_path(partition_desc->location());
    file_path /= scan_range.relative_path;
    const std::string& native_file_path = file_path.native();
    std::string namenode;
    RETURN_IF_ERROR(_get_name_node_from_path(native_file_path, &namenode));

    if (namenode.compare("default") == 0) {
        // local file, current only for test
        auto* env = Env::Default();
        std::unique_ptr<RandomAccessFile> file;
        env->new_random_access_file(native_file_path, &file);

        auto* hdfs_file_desc = _pool->add(new HdfsFileDesc());
        hdfs_file_desc->hdfs_fs = nullptr;
        hdfs_file_desc->hdfs_file = nullptr;
        hdfs_file_desc->fs = std::move(file);
        hdfs_file_desc->partition_id = scan_range.partition_id;
        hdfs_file_desc->path = scan_range.relative_path;
        hdfs_file_desc->file_length = scan_range.file_length;
        hdfs_file_desc->splits.emplace_back(&scan_range);
        hdfs_file_desc->hdfs_file_format = scan_range.file_format;
        _hdfs_files.emplace_back(hdfs_file_desc);
    } else {
        hdfsFS hdfs;
        RETURN_IF_ERROR(HdfsFsCache::instance()->get_connection(namenode, &hdfs));
        auto* file = hdfsOpenFile(hdfs, native_file_path.c_str(), O_RDONLY, 0, 0, 0);
        if (file == nullptr) {
            return Status::InternalError(strings::Substitute("open file failed, file=$0", native_file_path));
        }

        auto* hdfs_file_desc = _pool->add(new HdfsFileDesc());
        hdfs_file_desc->hdfs_fs = hdfs;
        hdfs_file_desc->hdfs_file = file;
        hdfs_file_desc->fs = std::make_shared<HdfsRandomAccessFile>(hdfs, file, native_file_path);
        hdfs_file_desc->partition_id = scan_range.partition_id;
        hdfs_file_desc->path = scan_range.relative_path;
        hdfs_file_desc->file_length = scan_range.file_length;
        hdfs_file_desc->splits.emplace_back(&scan_range);
        hdfs_file_desc->hdfs_file_format = scan_range.file_format;
        _hdfs_files.emplace_back(hdfs_file_desc);
    }

    return Status::OK();
}

Status HdfsScanNode::_get_name_node_from_path(const std::string& path, std::string* namenode) {
    const string local_fs("file:/");
    size_t n = path.find("://");

    if (n == string::npos) {
        if (path.compare(0, local_fs.length(), local_fs) == 0) {
            // Hadoop Path routines strip out consecutive /'s, so recognize 'file:/blah'.
            *namenode = "file:///";
        } else {
            // Path is not qualified, so use the default FS.
            *namenode = "default";
        }
    } else if (n == 0) {
        return Status::InternalError("Path missing schema");
    } else {
        // Path is qualified, i.e. "scheme://authority/path/to/file".  Extract
        // "scheme://authority/".
        n = path.find('/', n + 3);
        if (n == string::npos) {
            return Status::InternalError("Path missing '/' after authority");
        }
        // Include the trailing '/' for local filesystem case, i.e. "file:///".
        *namenode = path.substr(0, n + 1);
    }
    return Status::OK();
}

void HdfsScanNode::_update_status(const Status& status) {
    std::lock_guard<SpinLock> lck(_status_mutex);
    if (_status.ok()) {
        _status = status;
    }
}

void HdfsScanNode::_init_counter(RuntimeState* state) {
    _scan_timer = ADD_TIMER(_runtime_profile, "ScanTime");
    _reader_init_timer = ADD_TIMER(_runtime_profile, "ReaderInit");
    _open_file_timer = ADD_TIMER(_runtime_profile, "OpenFile");
    _raw_rows_counter = ADD_COUNTER(_runtime_profile, "RawRowsRead", TUnit::UNIT);
    _expr_filter_timer = ADD_TIMER(_runtime_profile, "ExprFilterTime");

    _io_timer = ADD_TIMER(_runtime_profile, "IoTime");
    _io_counter = ADD_COUNTER(_runtime_profile, "IoCounter", TUnit::UNIT);
    _bytes_read_from_disk_counter = ADD_COUNTER(_runtime_profile, "BytesReadFromDisk", TUnit::BYTES);
    _column_read_timer = ADD_TIMER(_runtime_profile, "ColumnReadTime");
    _level_decode_timer = ADD_TIMER(_runtime_profile, "LevelDecodeTime");
    _value_decode_timer = ADD_TIMER(_runtime_profile, "ValueDecodeTime");
    _page_read_timer = ADD_TIMER(_runtime_profile, "PageReadTime");
    _column_convert_timer = ADD_TIMER(_runtime_profile, "ColumnConvertTime");

    _bytes_total_read = ADD_COUNTER(_runtime_profile, "BytesTotalRead", TUnit::BYTES);
    _bytes_read_local = ADD_COUNTER(_runtime_profile, "BytesReadLocal", TUnit::BYTES);
    _bytes_read_short_circuit = ADD_COUNTER(_runtime_profile, "BytesReadShortCircuit", TUnit::BYTES);
    _bytes_read_dn_cache = ADD_COUNTER(_runtime_profile, "BytesReadDataNodeCache", TUnit::BYTES);
    _bytes_read_remote = ADD_COUNTER(_runtime_profile, "BytesReadRemote", TUnit::BYTES);

    // reader init
    _footer_read_timer = ADD_TIMER(_runtime_profile, "ReaderInitFooterRead");
    _column_reader_init_timer = ADD_TIMER(_runtime_profile, "ReaderInitColumnReaderInit");

    // dict filter
    _group_chunk_read_timer = ADD_TIMER(_runtime_profile, "GroupChunkRead");
    _group_dict_filter_timer = ADD_TIMER(_runtime_profile, "GroupDictFilter");
    _group_dict_decode_timer = ADD_TIMER(_runtime_profile, "GroupDictDecode");
}

} // namespace starrocks::vectorized
