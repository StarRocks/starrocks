// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <vector>

#include "column/chunk.h"
#include "common/status.h"
#include "exec/olap_utils.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/runtime_state.h"
#include "storage/tablet.h"
#include "storage/vectorized/conjunctive_predicates.h"
#include "storage/vectorized/reader.h"
#include "storage/vectorized/reader_params.h"

namespace starrocks::vectorized {

class OlapScanNode;

struct OlapScannerParams {
    const TInternalScanRange* scan_range = nullptr;
    const std::vector<OlapScanRange*>* key_ranges = nullptr;
    const std::vector<ExprContext*>* conjunct_ctxs = nullptr;

    bool skip_aggregation = false;
    bool need_agg_finalize = true;
};

class OlapScanner {
public:
    explicit OlapScanner(OlapScanNode* parent);
    ~OlapScanner() = default;

    OlapScanner(const OlapScanner&) = delete;
    OlapScanner(OlapScanner&&) = delete;
    void operator=(const OlapScanner&) = delete;
    void operator=(OlapScanner&&) = delete;

    Status init(RuntimeState* runtime_state, const OlapScannerParams& params);
    Status open([[maybe_unused]] RuntimeState* runtime_state);
    Status get_chunk([[maybe_unused]] RuntimeState* state, Chunk* chunk);
    Status close(RuntimeState* state);

    RuntimeState* runtime_state() { return _runtime_state; }
    int64_t raw_rows_read() const { return _raw_rows_read; }

    // REQUIRES: `init(RuntimeState*, const OlapScannerParams&)` has been called.
    const Schema& chunk_schema() const { return _prj_iter->schema(); }

    void set_keep_priority(bool v) { _keep_priority = v; }
    bool keep_priority() const { return _keep_priority; }

private:
    Status _get_tablet(const TInternalScanRange* scan_range);
    Status _init_reader_params(const std::vector<OlapScanRange*>* key_ranges);
    Status _init_return_columns();
    void _update_realtime_counter();
    void update_counter();

    RuntimeState* _runtime_state = nullptr;
    OlapScanNode* _parent = nullptr;

    using PredicatePtr = std::unique_ptr<ColumnPredicate>;

    std::vector<ExprContext*> _conjunct_ctxs;
    ConjunctivePredicates _predicates;
    std::vector<uint8_t> _selection;

    // for release memory.
    std::vector<PredicatePtr> _predicate_free_pool;

    bool _is_open = false;
    bool _is_closed = false;
    bool _skip_aggregation = false;
    bool _need_agg_finalize = false;
    bool _has_update_counter = false;

    ReaderParams _params;
    std::shared_ptr<Reader> _reader;

    TabletSharedPtr _tablet;
    int64_t _version = 0;

    // output columns of `this` OlapScanner, i.e, the final output columns of `get_chunk`.
    std::vector<uint32_t> _scanner_columns;
    // columns fetched from |_reader|.
    std::vector<uint32_t> _reader_columns;
    // projection iterator, doing the job of choosing |_scanner_columns| from |_reader_columns|.
    std::shared_ptr<ChunkIterator> _prj_iter;
    // slot descriptors for each one of |_scanner_columns|.
    std::vector<SlotDescriptor*> _query_slots;

    int64_t _num_rows_read = 0;
    int64_t _raw_rows_read = 0;
    int64_t _compressed_bytes_read = 0;

    // non-pushed-down predicates filter time.
    RuntimeProfile::Counter* _expr_filter_timer = nullptr;

    bool _keep_priority = false;
};
} // namespace starrocks::vectorized
