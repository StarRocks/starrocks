// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/olap_scan_node.h"

#include <chrono>
#include <thread>

#include "column/column_pool.h"
#include "column/type_traits.h"
#include "common/global_types.h"
#include "common/status.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/olap_scan_operator.h"
#include "exec/vectorized/olap_scan_prepare.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/in_const_predicate.hpp"
#include "exprs/vectorized/runtime_filter_bank.h"
#include "glog/logging.h"
#include "gutil/map_util.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "runtime/primitive_type.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/vectorized/chunk_helper.h"
#include "util/defer_op.h"
#include "util/priority_thread_pool.hpp"
#include "util/runtime_profile.h"
namespace starrocks::vectorized {

OlapScanNode::OlapScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs), _olap_scan_node(tnode.olap_scan_node), _status(Status::OK()) {}

Status OlapScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(!tnode.olap_scan_node.__isset.sort_column) << "sorted result not supported any more";

    // init filtered_ouput_columns
    for (const auto& col_name : tnode.olap_scan_node.unused_output_column_name) {
        _unused_output_columns.emplace_back(col_name);
    }

    return Status::OK();
}

Status OlapScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ScanNode::prepare(state));

    _tablet_counter = ADD_COUNTER(runtime_profile(), "TabletCount ", TUnit::UNIT);
    _io_task_counter = ADD_COUNTER(runtime_profile(), "IOTaskCount ", TUnit::UNIT);
    _task_concurrency = ADD_COUNTER(runtime_profile(), "ScanConcurrency ", TUnit::UNIT);
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_olap_scan_node.tuple_id);
    _init_counter(state);
    if (_tuple_desc == nullptr) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }
    _runtime_profile->add_info_string("Table", _tuple_desc->table_desc()->name());
    if (_olap_scan_node.__isset.rollup_name) {
        _runtime_profile->add_info_string("Rollup", _olap_scan_node.rollup_name);
    }
    if (_olap_scan_node.__isset.sql_predicates) {
        _runtime_profile->add_info_string("Predicates", _olap_scan_node.sql_predicates);
    }
    _runtime_state = state;

    return Status::OK();
}

Status OlapScanNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(ExecNode::open(state));

    Status status;
    RETURN_IF_ERROR(OlapScanConjunctsManager::eval_const_conjuncts(_conjunct_ctxs, &status));
    _update_status(status);

    _dict_optimize_parser.set_mutable_dict_maps(state, state->mutable_query_global_dict_map());
    DictOptimizeParser::rewrite_descriptor(state, _conjunct_ctxs, _olap_scan_node.dict_string_id_to_int_ids,
                                           &(_tuple_desc->decoded_slots()));

    return Status::OK();
}

// Current get_next the chunk is nullptr when eos==true
// TODO: return the last chunk with eos=true, reduce one function call?
Status OlapScanNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    bool first_call = !_start;
    if (!_start && _status.ok()) {
        Status status = _start_scan(state);
        _update_status(status);
        LOG_IF(ERROR, !(status.ok() || status.is_end_of_file())) << "Failed to start scan node: " << status.to_string();
        _start = true;
        if (!status.ok()) {
            *eos = true;
            return status.is_end_of_file() ? Status::OK() : status;
        }
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
        std::unique_lock<std::mutex> l(_mtx);
        const int32_t num_closed = _closed_scanners.load(std::memory_order_acquire);
        const int32_t num_pending = _pending_scanners.size();
        const int32_t num_running = _num_scanners - num_pending - num_closed;
        if ((num_pending > 0) && (num_running < kMaxConcurrency)) {
            // before we submit a new scanner to run, check whether it can fetch
            // at least _chunks_per_scanner chunks from _chunk_pool.
            if (_chunk_pool.size() >= (num_running + 1) * _chunks_per_scanner) {
                TabletScanner* scanner = _pending_scanners.pop();
                l.unlock();
                (void)_submit_scanner(scanner, true);
            }
        }
    }

    if (_result_chunks.blocking_get(chunk)) {
        // If the second argument of `_fill_chunk_pool` is false *AND* the column pool is empty,
        // the column object in the chunk will be destroyed and its memory will be deallocated
        // when the last remaining shared_ptr owning it is destroyed, otherwise the column object
        // will be placed into the column pool.
        //
        // If all columns returned to the parent executor node can be returned back into the column
        // pool before the next calling of `get_next`, the column pool would be nonempty before the
        // calling of `_fill_chunk_pool`, except for the first time of calling `get_next`. So if this
        // is the first time of calling `get_next`, pass the second argument of `_fill_chunk_pool` as
        // true to ensure that the newly allocated column objects will be returned back into the column
        // pool.
        TRY_CATCH_BAD_ALLOC(_fill_chunk_pool(1, first_call));
        eval_join_runtime_filters(chunk);
        _num_rows_returned += (*chunk)->num_rows();
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
        // reach scan node limit
        if (reached_limit()) {
            int64_t num_rows_over = _num_rows_returned - _limit;
            DCHECK_GE((*chunk)->num_rows(), num_rows_over);
            (*chunk)->set_num_rows((*chunk)->num_rows() - num_rows_over);
            COUNTER_SET(_rows_returned_counter, _limit);
            _update_status(Status::EndOfFile("OlapScanNode has reach limit"));
            _result_chunks.shutdown();
        }
        *eos = false;
        DCHECK_CHUNK(*chunk);
        return Status::OK();
    } else {
        _update_status(Status::EndOfFile("EOF of OlapScanNode"));
        *eos = true;
        status = _get_status();
        return status.is_end_of_file() ? Status::OK() : status;
    }
}

Status OlapScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    exec_debug_action(TExecNodePhase::CLOSE);
    _update_status(Status::Cancelled("closed"));
    _result_chunks.shutdown();
    while (_running_threads.load(std::memory_order_acquire) > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    _close_pending_scanners();

    // Free chunks in _chunk_pool.
    _chunk_pool.clear();

    // Free chunks in _result_chunks.
    ChunkPtr chunk = nullptr;
    while (_result_chunks.blocking_get(&chunk)) {
        chunk.reset();
    }

    _dict_optimize_parser.close(state);

    if (runtime_state() != nullptr) {
        // Reduce the memory usage if the the average string size is greater than 512.
        release_large_columns<BinaryColumn>(runtime_state()->chunk_size() * 512);
    }

    return ScanNode::close(state);
}

OlapScanNode::~OlapScanNode() {
    if (runtime_state() != nullptr) {
        close(runtime_state());
    }
    DCHECK(is_closed());
}

void OlapScanNode::_fill_chunk_pool(int count, bool force_column_pool) {
    const size_t capacity = runtime_state()->chunk_size();
    for (int i = 0; i < count; i++) {
        ChunkPtr chunk(ChunkHelper::new_chunk_pooled(*_chunk_schema, capacity, force_column_pool));
        {
            std::lock_guard<std::mutex> l(_mtx);
            _chunk_pool.push(std::move(chunk));
        }
    }
}

void OlapScanNode::_scanner_thread(TabletScanner* scanner) {
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(scanner->runtime_state()->instance_mem_tracker());
    DeferOp op([&] {
        tls_thread_status.set_mem_tracker(prev_tracker);
        _running_threads.fetch_sub(1, std::memory_order_release);
    });
    tls_thread_status.set_query_id(scanner->runtime_state()->query_id());

    Status status = scanner->open(_runtime_state);
    if (!status.ok()) {
        QUERY_LOG_IF(ERROR, !status.is_end_of_file()) << status;
        _update_status(status);
    } else {
        status = scanner->runtime_state()->check_mem_limit("olap scanner");
        if (!status.ok()) {
            _update_status(status);
        }
    }
    scanner->set_keep_priority(false);
    // Because we use thread pool to scan data from storage. One scanner can't
    // use this thread too long, this can starve other query's scanner. So, we
    // need yield this thread when we do enough work. However, OlapStorage read
    // data in pre-aggregate mode, then we can't use storage returned data to
    // judge if we need to yield. So we record all raw data read in this round
    // scan, if this exceed threshold, we yield this thread.
    bool resubmit = false;
    int64_t raw_rows_threshold = scanner->raw_rows_read() + config::doris_scanner_row_num;
    while (status.ok()) {
        ChunkPtr chunk;
        {
            std::lock_guard<std::mutex> l(_mtx);
            if (_chunk_pool.empty()) {
                // NOTE: DO NOT move these operations out of current lock scope.
                scanner->set_keep_priority(true);
                _pending_scanners.push(scanner);
                scanner = nullptr;
                break;
            }
            chunk = _chunk_pool.pop();
        }
        DCHECK_EQ(chunk->num_rows(), 0);
        status = scanner->get_chunk(_runtime_state, chunk.get());
        if (!status.ok()) {
            QUERY_LOG_IF(ERROR, !status.is_end_of_file()) << status;
            std::lock_guard<std::mutex> l(_mtx);
            _chunk_pool.push(std::move(chunk));
            break;
        }
        DCHECK_CHUNK(chunk);
        // _result_chunks will be shutdown if error happened or has reached limit.
        if (!_result_chunks.put(std::move(chunk))) {
            status = Status::Aborted("_result_chunks has been shutdown");
            break;
        }
        // Improve for select * from table limit x;
        if (limit() != -1 && scanner->num_rows_read() >= limit()) {
            status = Status::EndOfFile("limit reach");
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
            // _chunk_pool is empty and scanner has been placed into _pending_scanners,
            // nothing to do here.
        } else if (status.is_end_of_file()) {
            scanner->close(_runtime_state);
            _closed_scanners.fetch_add(1, std::memory_order_release);
            // pick next scanner to run.
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
        if (scanner != nullptr) {
            scanner->close(_runtime_state);
            _closed_scanners.fetch_add(1, std::memory_order_release);
            _close_pending_scanners();
        } else {
            _close_pending_scanners();
        }
    }

    if (_closed_scanners.load(std::memory_order_acquire) == _num_scanners) {
        _result_chunks.shutdown();
    }
    tls_thread_status.set_query_id(TUniqueId());
    // DO NOT touch any shared variables since here, as they may have been destructed.
}

Status OlapScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    for (auto& scan_range : scan_ranges) {
        DCHECK(scan_range.scan_range.__isset.internal_scan_range);
        _scan_ranges.emplace_back(std::make_unique<TInternalScanRange>(scan_range.scan_range.internal_scan_range));
        COUNTER_UPDATE(_tablet_counter, 1);
    }

    RETURN_IF_ERROR(_capture_tablet_rowsets());

    return Status::OK();
}

Status OlapScanNode::collect_query_statistics(QueryStatistics* statistics) {
    RETURN_IF_ERROR(ExecNode::collect_query_statistics(statistics));
    QueryStatisticsItemPB stats_item;
    stats_item.set_scan_bytes(_read_compressed_counter->value());
    stats_item.set_scan_rows(_raw_rows_counter->value());
    stats_item.set_table_id(_tuple_desc->table_desc()->table_id());
    statistics->add_stats_item(stats_item);
    return Status::OK();
}

Status OlapScanNode::_start_scan(RuntimeState* state) {
    RETURN_IF_CANCELLED(state);

    OlapScanConjunctsManager& cm = _conjuncts_manager;
    cm.conjunct_ctxs_ptr = &_conjunct_ctxs;
    cm.tuple_desc = _tuple_desc;
    cm.obj_pool = _pool;
    cm.key_column_names = &_olap_scan_node.key_column_name;
    cm.runtime_filters = &_runtime_filter_collector;
    cm.runtime_state = state;

    const TQueryOptions& query_options = state->query_options();
    int32_t max_scan_key_num;
    if (query_options.__isset.max_scan_key_num && query_options.max_scan_key_num > 0) {
        max_scan_key_num = query_options.max_scan_key_num;
    } else {
        max_scan_key_num = config::doris_max_scan_key_num;
    }
    bool scan_keys_unlimited = (limit() == -1);
    bool enable_column_expr_predicate = false;
    if (_olap_scan_node.__isset.enable_column_expr_predicate) {
        enable_column_expr_predicate = _olap_scan_node.enable_column_expr_predicate;
    }
    RETURN_IF_ERROR(cm.parse_conjuncts(scan_keys_unlimited, max_scan_key_num, enable_column_expr_predicate));
    RETURN_IF_ERROR(_start_scan_thread(state));

    return Status::OK();
}

void OlapScanNode::_init_counter(RuntimeState* state) {
    _scan_timer = ADD_TIMER(_runtime_profile, "ScanTime");

    _scan_profile = _runtime_profile->create_child("SCAN", true, false);

    _create_seg_iter_timer = ADD_TIMER(_scan_profile, "CreateSegmentIter");

    _read_compressed_counter = ADD_COUNTER(_scan_profile, "CompressedBytesRead", TUnit::BYTES);
    _read_uncompressed_counter = ADD_COUNTER(_scan_profile, "UncompressedBytesRead", TUnit::BYTES);

    _raw_rows_counter = ADD_COUNTER(_scan_profile, "RawRowsRead", TUnit::UNIT);
    _read_pages_num_counter = ADD_COUNTER(_scan_profile, "ReadPagesNum", TUnit::UNIT);
    _cached_pages_num_counter = ADD_COUNTER(_scan_profile, "CachedPagesNum", TUnit::UNIT);
    _pushdown_predicates_counter = ADD_COUNTER(_scan_profile, "PushdownPredicates", TUnit::UNIT);

    /// SegmentInit
    _seg_init_timer = ADD_TIMER(_scan_profile, "SegmentInit");
    _bi_filter_timer = ADD_CHILD_TIMER(_scan_profile, "BitmapIndexFilter", "SegmentInit");
    _bi_filtered_counter = ADD_CHILD_COUNTER(_scan_profile, "BitmapIndexFilterRows", TUnit::UNIT, "SegmentInit");
    _bf_filtered_counter = ADD_CHILD_COUNTER(_scan_profile, "BloomFilterFilterRows", TUnit::UNIT, "SegmentInit");
    _seg_zm_filtered_counter = ADD_CHILD_COUNTER(_scan_profile, "SegmentZoneMapFilterRows", TUnit::UNIT, "SegmentInit");
    _zm_filtered_counter = ADD_CHILD_COUNTER(_scan_profile, "ZoneMapIndexFilterRows", TUnit::UNIT, "SegmentInit");
    _sk_filtered_counter = ADD_CHILD_COUNTER(_scan_profile, "ShortKeyFilterRows", TUnit::UNIT, "SegmentInit");

    /// SegmentRead
    _block_load_timer = ADD_TIMER(_scan_profile, "SegmentRead");
    _block_fetch_timer = ADD_CHILD_TIMER(_scan_profile, "BlockFetch", "SegmentRead");
    _block_load_counter = ADD_CHILD_COUNTER(_scan_profile, "BlockFetchCount", TUnit::UNIT, "SegmentRead");
    _block_seek_timer = ADD_CHILD_TIMER(_scan_profile, "BlockSeek", "SegmentRead");
    _block_seek_counter = ADD_CHILD_COUNTER(_scan_profile, "BlockSeekCount", TUnit::UNIT, "SegmentRead");
    _pred_filter_timer = ADD_CHILD_TIMER(_scan_profile, "PredFilter", "SegmentRead");
    _pred_filter_counter = ADD_CHILD_COUNTER(_scan_profile, "PredFilterRows", TUnit::UNIT, "SegmentRead");
    _del_vec_filter_counter = ADD_CHILD_COUNTER(_scan_profile, "DelVecFilterRows", TUnit::UNIT, "SegmentRead");
    _chunk_copy_timer = ADD_CHILD_TIMER(_scan_profile, "ChunkCopy", "SegmentRead");
    _decompress_timer = ADD_CHILD_TIMER(_scan_profile, "DecompressT", "SegmentRead");
    _index_load_timer = ADD_CHILD_TIMER(_scan_profile, "IndexLoad", "SegmentRead");
    _rowsets_read_count = ADD_CHILD_COUNTER(_scan_profile, "RowsetsReadCount", TUnit::UNIT, "SegmentRead");
    _segments_read_count = ADD_CHILD_COUNTER(_scan_profile, "SegmentsReadCount", TUnit::UNIT, "SegmentRead");
    _total_columns_data_page_count =
            ADD_CHILD_COUNTER(_scan_profile, "TotalColumnsDataPageCount", TUnit::UNIT, "SegmentRead");

    /// IOTime
    _io_timer = ADD_TIMER(_scan_profile, "IOTime");
}

// The more tasks you submit, the less priority you get.
int OlapScanNode::_compute_priority(int32_t num_submitted_tasks) {
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

bool OlapScanNode::_submit_scanner(TabletScanner* scanner, bool blockable) {
    PriorityThreadPool* thread_pool = _runtime_state->exec_env()->thread_pool();
    int delta = !scanner->keep_priority();
    int32_t num_submit = _scanner_submit_count.fetch_add(delta, std::memory_order_relaxed);
    PriorityThreadPool::Task task;
    task.work_function = [this, scanner] { _scanner_thread(scanner); };
    task.priority = _compute_priority(num_submit);
    _running_threads.fetch_add(1, std::memory_order_release);
    if (LIKELY(thread_pool->try_offer(task))) {
        return true;
    } else if (blockable) {
        CHECK(thread_pool->offer(task));
        return true;
    } else {
        LOG(WARNING) << "thread pool busy";
        _running_threads.fetch_sub(1, std::memory_order_release);
        _scanner_submit_count.fetch_sub(delta, std::memory_order_relaxed);
        return false;
    }
}

Status OlapScanNode::_start_scan_thread(RuntimeState* state) {
    if (_scan_ranges.empty()) {
        _update_status(Status::EndOfFile("empty scan ranges"));
        _result_chunks.shutdown();
        return Status::OK();
    }

    std::vector<std::unique_ptr<OlapScanRange>> key_ranges;
    RETURN_IF_ERROR(_conjuncts_manager.get_key_ranges(&key_ranges));
    std::vector<ExprContext*> conjunct_ctxs;
    _conjuncts_manager.get_not_push_down_conjuncts(&conjunct_ctxs);

    _dict_optimize_parser.rewrite_conjuncts<true>(&conjunct_ctxs, state);

    int tablet_count = _scan_ranges.size();
    for (int k = 0; k < tablet_count; ++k) {
        auto& scan_range = _scan_ranges[k];
        auto& tablet_rowset = _tablet_rowsets[k];

        size_t segment_nums = 0;
        for (const auto& rowset : tablet_rowset) {
            segment_nums += rowset->num_segments();
        }
        int scanners_per_tablet = std::min(segment_nums, kMaxScannerPerRange / _scan_ranges.size());
        scanners_per_tablet = std::max(1, scanners_per_tablet);

        int num_ranges = key_ranges.size();
        int ranges_per_scanner = std::max(1, num_ranges / scanners_per_tablet);
        for (int i = 0; i < num_ranges;) {
            std::vector<OlapScanRange*> agg_key_ranges;
            agg_key_ranges.push_back(key_ranges[i].get());
            i++;
            // each scanner could only handle TabletReaderParams, so each scanner could only
            // 'le' or 'lt' range
            // TODO:fix limit
            for (int j = 1; i < num_ranges && j < ranges_per_scanner &&
                            key_ranges[i]->end_include == key_ranges[i - 1]->end_include;
                 ++j, ++i) {
                agg_key_ranges.push_back(key_ranges[i].get());
            }

            TabletScannerParams scanner_params;
            scanner_params.scan_range = scan_range.get();
            scanner_params.key_ranges = &agg_key_ranges;
            scanner_params.conjunct_ctxs = &conjunct_ctxs;
            scanner_params.skip_aggregation = _olap_scan_node.is_preaggregation;
            scanner_params.need_agg_finalize = true;
            scanner_params.unused_output_columns = &_unused_output_columns;
            auto* scanner = _pool->add(new TabletScanner(this));
            RETURN_IF_ERROR(scanner->init(state, scanner_params));
            // Assume all scanners have the same schema.
            _chunk_schema = &scanner->chunk_schema();
            _pending_scanners.push(scanner);
            COUNTER_UPDATE(_io_task_counter, 1);
        }
    }
    _pending_scanners.reverse();
    _num_scanners = _pending_scanners.size();
    _chunks_per_scanner = config::doris_scanner_row_num / runtime_state()->chunk_size();
    _chunks_per_scanner += (config::doris_scanner_row_num % runtime_state()->chunk_size() != 0);
    // TODO: dynamic submit stragety
    int concurrency = _scanner_concurrency();
    COUNTER_SET(_task_concurrency, (int64_t)concurrency);
    int chunks = _chunks_per_scanner * concurrency;
    _chunk_pool.reserve(chunks);
    TRY_CATCH_BAD_ALLOC(_fill_chunk_pool(chunks, true));
    std::lock_guard<std::mutex> l(_mtx);
    for (int i = 0; i < concurrency; i++) {
        CHECK(_submit_scanner(_pending_scanners.pop(), true));
    }
    return Status::OK();
}

Status OlapScanNode::_capture_tablet_rowsets() {
    _tablet_rowsets.resize(_scan_ranges.size());
    for (int i = 0; i < _scan_ranges.size(); ++i) {
        const auto& scan_range = _scan_ranges[i];

        // Get version.
        int64_t version = strtoul(scan_range->version.c_str(), nullptr, 10);

        // Get tablet.
        TTabletId tablet_id = scan_range->tablet_id;
        std::string err;
        TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, true, &err);
        if (!tablet) {
            std::stringstream ss;
            SchemaHash schema_hash = strtoul(scan_range->schema_hash.c_str(), nullptr, 10);
            ss << "failed to get tablet. tablet_id=" << tablet_id << ", with schema_hash=" << schema_hash
               << ", reason=" << err;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        // Capture row sets of this version tablet.
        {
            std::shared_lock l(tablet->get_header_lock());
            RETURN_IF_ERROR(tablet->capture_consistent_rowsets(Version(0, version), &_tablet_rowsets[i]));
        }
    }

    return Status::OK();
}

struct TypeMemoryUsed {
    template <FieldType type>
    size_t operator()() {
        switch (type) {
        case OLAP_FIELD_TYPE_VARCHAR:
        case OLAP_FIELD_TYPE_CHAR:
        case OLAP_FIELD_TYPE_JSON:
            return 100;
#define M(TYPE) return 100;
            APPLY_FOR_COMPLEX_OLAP_FIELD_TYPE(M)
#undef M
        default:
            return 0;
        }
    }
};

size_t OlapScanNode::_scanner_concurrency() {
    int64_t query_limit = _runtime_state->query_mem_tracker_ptr()->limit();

    size_t chunk_mem_usage = 0;
    size_t row_mem_usage = 0;
    const auto& fields = _chunk_schema->fields();
    // we could use statistics
    for (const auto& field : fields) {
        row_mem_usage += field->type()->size();
        row_mem_usage += field_type_dispatch_column(field->type()->type(), TypeMemoryUsed());
    }
    // We temporarily assume that the memory tried in the storage layer
    // is the same size as the chunk
    row_mem_usage *= 2;
    chunk_mem_usage = row_mem_usage * _runtime_state->chunk_size();
    DCHECK_GT(chunk_mem_usage, 0);
    // limit scan memory usage not greater than 1/4 query limit
    int concurrency = std::max<int>(query_limit * config::scan_use_query_mem_ratio / chunk_mem_usage, 1);
    // limit concurrency not greater than scanner numbers
    concurrency = std::min<int>(concurrency, _num_scanners);
    concurrency = std::min<int>(concurrency, kMaxConcurrency);

    return concurrency;
}

Status OlapScanNode::set_scan_ranges(const std::vector<TInternalScanRange>& ranges) {
    for (auto& r : ranges) {
        _scan_ranges.emplace_back(std::make_unique<TInternalScanRange>(r));
    }
    return Status::OK();
}

Status OlapScanNode::set_scan_range(const TInternalScanRange& range) {
    return set_scan_ranges({range});
}

void OlapScanNode::_update_status(const Status& status) {
    std::lock_guard<SpinLock> lck(_status_mutex);
    if (_status.ok()) {
        _status = status;
    }
}

Status OlapScanNode::_get_status() {
    std::lock_guard<SpinLock> lck(_status_mutex);
    return _status;
}

void OlapScanNode::_close_pending_scanners() {
    std::lock_guard<std::mutex> l(_mtx);
    while (!_pending_scanners.empty()) {
        TabletScanner* scanner = _pending_scanners.pop();
        scanner->close(_runtime_state);
        _closed_scanners.fetch_add(1, std::memory_order_release);
    }
}

pipeline::OpFactories OlapScanNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    ScanNode* scan_node = this;
    OpFactories operators;
    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(1, std::move(this->runtime_filter_collector()));
    auto scan_operator = std::make_shared<pipeline::OlapScanOperatorFactory>(context->next_operator_id(), scan_node);
    // Initialize OperatorFactory's fields involving runtime filters.
    this->init_runtime_filter_for_operator(scan_operator.get(), context, rc_rf_probe_collector);
    auto& morsel_queues = context->fragment_context()->morsel_queues();
    auto source_id = scan_operator->plan_node_id();
    DCHECK(morsel_queues.count(source_id));
    auto& morsel_queue = morsel_queues[source_id];
    // ScanOperator's degree_of_parallelism is not more than the number of morsels
    // If table is empty, then morsel size is zero and we still set degree of parallelism to 1
    const auto degree_of_parallelism =
            std::min<size_t>(std::max<size_t>(1, morsel_queue->num_morsels()), context->degree_of_parallelism());
    scan_operator->set_degree_of_parallelism(degree_of_parallelism);
    operators.emplace_back(std::move(scan_operator));
    size_t limit = scan_node->limit();
    if (limit != -1) {
        operators.emplace_back(
                std::make_shared<pipeline::LimitOperatorFactory>(context->next_operator_id(), scan_node->id(), limit));
    }
    return operators;
}

} // namespace starrocks::vectorized
