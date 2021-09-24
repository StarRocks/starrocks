// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <atomic>
#include <future>
#include <map>
#include <mutex>
#include <string>
#include <thread>

#include "exec/scan_node.h"
#include "exec/vectorized/es_http_scanner.h"
#include "util/blocking_queue.hpp"

namespace starrocks {
class EsPredicate;

namespace vectorized {
class EsHttpScanNode final : public starrocks::ScanNode {
public:
    EsHttpScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~EsHttpScanNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    Status close(RuntimeState* state) override;

    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

private:
    Status _acquire_status();
    void _update_status(const Status& new_status);
    Status start_scanners();

    Status _build_conjuncts();
    // try to skip constant conjuncts is constant conjuncts
    // we will set eos to true if always false
    void _try_skip_constant_conjuncts();

    // validate predicate and remove expr that have been push down
    Status _normalize_conjuncts();

    Status _start_scan_thread(RuntimeState* state);
    Status _create_scanner(int scanner_idx, std::unique_ptr<EsHttpScanner>* res);
    void _scanner_scan(std::unique_ptr<EsHttpScanner> scanner, std::promise<Status>& p_status);
    Status _acquire_chunks(EsHttpScanner* scanner);

private:
    TupleId _tuple_id;
    RuntimeState* _runtime_state;
    TupleDescriptor* _tuple_desc;

    std::atomic<int> _num_running_scanners;
    bool _eos;
    int _max_buffered_batches;

    SpinLock _status_mutex;
    Status _process_status;

    std::atomic<bool> _scan_finished;
    std::mutex _mtx;

    std::vector<TScanRangeParams> _scan_ranges;

    std::map<std::string, std::string> _properties;
    std::map<std::string, std::string> _docvalue_context;
    std::map<std::string, std::string> _fields_context;

    std::vector<std::string> _column_names;

    // predicate index in the conjuncts
    std::vector<int> _predicate_idx;
    // Predicates will push down to ES
    std::vector<EsPredicate*> _predicates;

    RuntimeProfile::Counter* _wait_scanner_timer;

    std::vector<std::thread> _scanner_threads;
    std::vector<std::promise<Status>> _scanners_status;

    // queue size: config::doris_scanner_queue_size
    BlockingQueue<ChunkPtr> _result_chunks;
};
} // namespace vectorized
} // namespace starrocks
