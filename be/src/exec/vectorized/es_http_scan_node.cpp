// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/es_http_scan_node.h"

#include <fmt/format.h>

#include <memory>

#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "exec/es/es_predicate.h"
#include "exec/es/es_query_builder.h"
#include "exec/es/es_scan_reader.h"
#include "exec/es/es_scroll_query.h"
#include "runtime/current_thread.h"
#include "util/defer_op.h"
#include "util/spinlock.h"
#include "util/thread.h"

namespace starrocks::vectorized {
EsHttpScanNode::EsHttpScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : starrocks::ScanNode(pool, tnode, descs),
          _tuple_id(-1),
          _tuple_desc(nullptr),
          _num_running_scanners(0),
          _eos(false),
          _scan_finished(false),
          _result_chunks(config::doris_scanner_queue_size) {}

EsHttpScanNode::~EsHttpScanNode() {
    if (runtime_state() != nullptr) {
        close(runtime_state());
    }
}

Status EsHttpScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));

    DCHECK(tnode.__isset.es_scan_node);
    DCHECK(tnode.es_scan_node.__isset.properties);

    const auto& es_scan_node = tnode.es_scan_node;
    _tuple_id = es_scan_node.tuple_id;
    _properties = tnode.es_scan_node.properties;

    if (es_scan_node.__isset.docvalue_context) {
        _docvalue_context = es_scan_node.docvalue_context;
    }

    if (es_scan_node.__isset.fields_context) {
        _fields_context = es_scan_node.fields_context;
    }

    return Status::OK();
}

Status EsHttpScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ScanNode::prepare(state));

    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);

    if (_tuple_desc == nullptr) {
        return Status::InternalError(fmt::format("Failed to get tuple descriptor, _tuple_id={}", _tuple_id));
    }

    _column_names.reserve(_tuple_desc->slots().size());
    for (auto slot_desc : _tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        _column_names.push_back(slot_desc->col_name());
    }

    _wait_scanner_timer = ADD_TIMER(runtime_profile(), "WaitScannerTime");

    return Status::OK();
}

Status EsHttpScanNode::open(RuntimeState* state) {
    DCHECK(state != nullptr);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ScanNode::open(state));
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    RETURN_IF_CANCELLED(state);

    RETURN_IF_ERROR(_build_conjuncts());
    RETURN_IF_ERROR(_normalize_conjuncts());
    RETURN_IF_ERROR(_start_scan_thread(state));

    return Status::OK();
}

Status EsHttpScanNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    DCHECK(state != nullptr && chunk != nullptr && eos != nullptr);

    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));

    (*chunk).reset();
    Status status = _acquire_status();
    if (!status.ok()) {
        *eos = true;
        return status.is_end_of_file() ? Status::OK() : status;
    }

    if (_num_running_scanners > 0 || !_result_chunks.empty()) {
        if (_result_chunks.blocking_get(chunk)) {
            _num_rows_returned += (*chunk)->num_rows();
            COUNTER_SET(_rows_returned_counter, _num_rows_returned);
            if (reached_limit()) {
                int64_t num_rows_over = _num_rows_returned - _limit;
                (*chunk)->set_num_rows((*chunk)->num_rows() - num_rows_over);
                COUNTER_SET(_rows_returned_counter, _limit);
                _update_status(Status::EndOfFile("EsHttpScanNode has reach limit"));
                _result_chunks.shutdown();
            }
            *eos = false;
            DCHECK_CHUNK(*chunk);
            return Status::OK();
        }
    }

    _update_status(Status::EndOfFile("EOF of ESScanNode"));
    *eos = true;
    status = _acquire_status();
    return status.is_end_of_file() ? Status::OK() : status;
}

Status EsHttpScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    _update_status(Status::Cancelled("closed"));
    _result_chunks.shutdown();

    // wait thread
    for (auto& _scanner_thread : _scanner_threads) {
        _scanner_thread.join();
    }

    DCHECK_EQ(_num_running_scanners, 0);

    // Close Expr
    return ExecNode::close(state);
}

Status EsHttpScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    _scan_ranges = scan_ranges;
    return Status::OK();
}

Status EsHttpScanNode::_acquire_status() {
    std::lock_guard<SpinLock> lck(_status_mutex);
    return _process_status;
}

void EsHttpScanNode::_update_status(const Status& new_status) {
    std::lock_guard<SpinLock> lck(_status_mutex);
    if (_process_status.ok()) {
        _process_status = new_status;
    }
}

Status EsHttpScanNode::_build_conjuncts() {
    Status status = Status::OK();

    size_t conjunct_sz = _conjunct_ctxs.size();
    _predicates.reserve(conjunct_sz);
    _predicate_idx.reserve(conjunct_sz);

    for (int i = 0; i < _conjunct_ctxs.size(); ++i) {
        EsPredicate* predicate = _pool->add(new EsPredicate(_conjunct_ctxs[i], _tuple_desc, _pool));
        predicate->set_field_context(_fields_context);
        status = predicate->build_disjuncts_list(true);
        if (status.ok()) {
            _predicates.push_back(predicate);
            _predicate_idx.push_back(i);
        } else {
            status = predicate->get_es_query_status();
            if (!status.ok()) {
                LOG(WARNING) << status.get_error_msg();
                return status;
            }
        }
    }
    return status;
}

Status EsHttpScanNode::_try_skip_constant_conjuncts() {
    // TODO: skip constant true
    for (auto& _conjunct_ctx : _conjunct_ctxs) {
        if (_conjunct_ctx->root()->is_constant()) {
            // unreachable path
            // The new optimizer will rewrite `where always false` to `EMPTY_SET`
            ASSIGN_OR_RETURN(ColumnPtr value, _conjunct_ctx->evaluate(nullptr));
            DCHECK(value->is_constant());
            if (value->only_null() || value->get(0).get_uint8() == 0) {
                _eos = true;
            }
        }
    }
    return Status::OK();
}

Status EsHttpScanNode::_normalize_conjuncts() {
    RETURN_IF_ERROR(_try_skip_constant_conjuncts());

    std::vector<bool> validate_res;
    BooleanQueryBuilder::validate(_predicates, &validate_res);
    DCHECK(validate_res.size() == _predicates.size());

    int counter = 0;
    for (int i = 0; i < _predicates.size(); ++i) {
        if (validate_res[i]) {
            _predicate_idx[counter] = _predicate_idx[i];
            _predicates[counter++] = _predicates[i];
        }
    }
    _predicates.erase(_predicates.begin() + counter, _predicates.end());

    for (int i = _predicate_idx.size() - 1; i >= 0; i--) {
        int conjunct_index = _predicate_idx[i];
        _conjunct_ctxs[conjunct_index]->close(runtime_state());
        _conjunct_ctxs.erase(_conjunct_ctxs.begin() + conjunct_index);
    }
    return Status::OK();
}

Status EsHttpScanNode::_start_scan_thread(RuntimeState* state) {
    _num_running_scanners = _scan_ranges.size();
    _scanners_status.resize(_scan_ranges.size());

    // create scanner
    std::vector<std::unique_ptr<EsHttpScanner>> scanners(_scan_ranges.size());
    for (int i = 0; i < _scan_ranges.size(); i++) {
        RETURN_IF_ERROR(_create_scanner(i, &scanners[i]));
    }

    // start scan
    // TODO: use thread pool instead of new thread
    for (int i = 0; i < _scan_ranges.size(); i++) {
        _scanner_threads.emplace_back(&EsHttpScanNode::_scanner_scan, this, std::move(scanners[i]),
                                      std::ref(_scanners_status[i]));
        Thread::set_thread_name(_scanner_threads.back(), "es_http_scan");
    }
    return Status::OK();
}

static std::string get_host_port(const std::vector<TNetworkAddress>& es_hosts) {
    std::string host_port;
    std::string localhost = BackendOptions::get_localhost();

    TNetworkAddress host = es_hosts[0];
    for (auto& es_host : es_hosts) {
        if (es_host.hostname == localhost) {
            host = es_host;
            break;
        }
    }

    return fmt::format("{}:{}", host.hostname, host.port);
}

Status EsHttpScanNode::_create_scanner(int scanner_idx, std::unique_ptr<EsHttpScanner>* res) {
    std::vector<ExprContext*> scanner_expr_ctxs;
    auto status = Expr::clone_if_not_exists(runtime_state(), _pool, _conjunct_ctxs, &scanner_expr_ctxs);
    RETURN_IF_ERROR(status);

    const TEsScanRange& es_scan_range = _scan_ranges[scanner_idx].scan_range.es_scan_range;
    std::map<std::string, std::string> properties(_properties);

    properties[ESScanReader::KEY_INDEX] = es_scan_range.index;
    if (es_scan_range.__isset.type) {
        properties[ESScanReader::KEY_TYPE] = es_scan_range.type;
    }
    properties[ESScanReader::KEY_SHARD] = std::to_string(es_scan_range.shard_id);
    properties[ESScanReader::KEY_BATCH_SIZE] = std::to_string(runtime_state()->chunk_size());
    properties[ESScanReader::KEY_HOST_PORT] = get_host_port(es_scan_range.es_hosts);
    // push down limit to Elasticsearch
    if (limit() != -1 && limit() <= runtime_state()->chunk_size()) {
        properties[ESScanReader::KEY_TERMINATE_AFTER] = std::to_string(limit());
    }

    bool doc_value_mode = false;
    properties[ESScanReader::KEY_QUERY] =
            ESScrollQueryBuilder::build(properties, _column_names, _predicates, _docvalue_context, &doc_value_mode);

    *res = std::make_unique<EsHttpScanner>(runtime_state(), runtime_profile(), _tuple_id, std::move(properties),
                                           scanner_expr_ctxs, _docvalue_context, doc_value_mode);
    return Status::OK();
}

void EsHttpScanNode::_scanner_scan(std::unique_ptr<EsHttpScanner> scanner, std::promise<Status>& p_status) {
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(scanner->runtime_state()->instance_mem_tracker());
    DeferOp op([&] {
        tls_thread_status.set_mem_tracker(prev_tracker);

        // This scanner will finish
        _num_running_scanners--;

        if (_num_running_scanners == 0) {
            _result_chunks.shutdown();
        }
    });

    auto status = scanner->open();
    if (!status.ok()) {
        _update_status(status);
    }

    status = _acquire_chunks(scanner.get());

    _update_status(status);

    p_status.set_value(status);
}

Status EsHttpScanNode::_acquire_chunks(EsHttpScanner* scanner) {
    bool scanner_eof = false;
    while (!scanner_eof) {
        ChunkPtr chunk;
        // fill chunk
        while (!scanner_eof) {
            if (UNLIKELY(runtime_state()->is_cancelled())) {
                return Status::Cancelled("Cancelled because of runtime state is cancelled");
            }
            RETURN_IF_ERROR(scanner->get_next(runtime_state(), &chunk, &scanner_eof));
            if (chunk != nullptr && chunk->has_rows()) {
                break;
            }
        }
        // push to block queue
        if (chunk != nullptr && chunk->has_rows()) {
            if (!_result_chunks.blocking_put(std::move(chunk))) {
                return Status::Cancelled("result chunks has been shutdown");
            }
        }
    }
    return Status::OK();
}

} // namespace starrocks::vectorized
