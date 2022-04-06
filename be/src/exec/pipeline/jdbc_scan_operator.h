// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/pipeline/scan_operator.h"
#include "exec/vectorized/jdbc_scanner.h"
#include "util/blocking_queue.hpp"
#include "util/spinlock.h"

namespace starrocks {

namespace pipeline {

// Because the implementation of JDBCScanOperator is very different from ScanOperator and not easy to unify,
// for example, we fetch data in a seperate thread without using _io_threads...
// so we choose to rewrite almost all the core interfaces without directly reusing the interfaces in ScanOperator
// Maybe after we put the jdbc scanner into thread pool, the two can be unified.
class JDBCScanOperator final : public ScanOperator {
public:
    JDBCScanOperator(OperatorFactory* factory, int32_t id, ScanNode* scan_node, const TJDBCScanNode& jdbc_scan_node);
    ~JDBCScanOperator() override = default;

    bool has_output() const override;

    bool pending_finish() const override;

    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status do_prepare(RuntimeState* state) override;

    void do_close(RuntimeState* state) override;

    ChunkSourcePtr create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) override;

private:
    Status _start_scanner_thread(RuntimeState* state);

    void _start_scanner(RuntimeState* state);

    void _set_scanner_state(bool is_finished, const Status& new_status);
    // if scanner thread is finished, return true
    // Otherwise, return false
    bool _is_scanner_finished(Status* status) const;

    Status _fetch_chunks();

    std::string get_jdbc_sql(const std::string& table, const std::vector<std::string>& columns,
                             const std::vector<std::string>& filters, int64_t limit);

    RuntimeState* _state = nullptr;
    std::atomic<bool> _is_finished{false};

    const TJDBCScanNode& _jdbc_scan_node;
    const std::vector<ExprContext*>& _conjunct_ctxs;
    int64_t _limit;

    TupleDescriptor* _result_tuple_desc = nullptr;
    BlockingQueue<ChunkPtr> _result_chunks;

    // scanner related
    std::unique_ptr<vectorized::JDBCScanner> _scanner;
    std::unique_ptr<std::thread> _scanner_thread;
    // used for protecting _scanner_status and _is_scanner_finished
    mutable SpinLock _scanner_state_mutex;
    Status _scanner_status;
    bool _scanner_finished = false;
};

class JDBCScanOperatorFactory final : public ScanOperatorFactory {
public:
    JDBCScanOperatorFactory(int32_t id, ScanNode* scan_node, const TJDBCScanNode& jdbc_scan_node);

    ~JDBCScanOperatorFactory() override = default;

    Status do_prepare(RuntimeState* state) override;

    void do_close(RuntimeState* state) override;

    OperatorPtr do_create(int32_t dop, int32_t driver_sequence) override;

private:
    const TJDBCScanNode& _jdbc_scan_node;
};
} // namespace pipeline
} // namespace starrocks
