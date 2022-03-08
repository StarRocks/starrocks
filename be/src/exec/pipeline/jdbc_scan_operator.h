// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/pipeline/source_operator.h"
#include "exec/vectorized/jdbc_scanner.h"
#include "util/blocking_queue.hpp"
#include "util/spinlock.h"

namespace starrocks {

namespace pipeline {

class JDBCScanOperator final : public SourceOperator {
public:
    JDBCScanOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, const TJDBCScanNode& jdbc_scan_node,
                     const std::vector<ExprContext*>& conjunct_ctxs, int64_t limit);

    ~JDBCScanOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override;

    bool pending_finish() const override;

    bool is_finished() const override;

    void set_finishing(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

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

    TupleDescriptor* _result_tuple_desc;
    BlockingQueue<ChunkPtr> _result_chunks;

    // scanner related
    std::unique_ptr<vectorized::JDBCScanner> _scanner;
    std::unique_ptr<std::thread> _scanner_thread;
    // used for protecting _scanner_status and _is_scanner_finished
    mutable SpinLock _scanner_state_mutex;
    Status _scanner_status;
    bool _scanner_finished = false;
};

class JDBCScanOperatorFactory final : public SourceOperatorFactory {
public:
    JDBCScanOperatorFactory(int32_t id, int32_t plan_node_id, const TJDBCScanNode& jdbc_scan_node,
                            std::vector<ExprContext*>&& conjunct_ctxs, int64_t limit)
            : SourceOperatorFactory(id, "jdbc_scan", plan_node_id),
              _jdbc_scan_node(jdbc_scan_node),
              _conjunct_ctxs(conjunct_ctxs),
              _limit(limit) {}

    ~JDBCScanOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<JDBCScanOperator>(this, _id, _plan_node_id, _jdbc_scan_node, _conjunct_ctxs, _limit);
    }

    bool with_morsels() const override { return true; }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    const TJDBCScanNode& _jdbc_scan_node;
    std::vector<ExprContext*> _conjunct_ctxs;
    int64_t _limit;
};
} // namespace pipeline
} // namespace starrocks