// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/pipeline/context_with_dependency.h"
#include "exec/vectorized/olap_scan_prepare.h"

namespace starrocks {

class ScanNode;
class Tablet;
using TabletSharedPtr = std::shared_ptr<Tablet>;
class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;

namespace vectorized {
class RuntimeFilterProbeCollector;
}

namespace pipeline {

class OlapScanContext;
using OlapScanContextPtr = std::shared_ptr<OlapScanContext>;

using namespace vectorized;

class OlapScanContext final : public ContextWithDependency {
public:
<<<<<<< HEAD
    explicit OlapScanContext(vectorized::OlapScanNode* scan_node) : _scan_node(scan_node) {}
=======
    explicit OlapScanContext(OlapScanNode* scan_node, int64_t scan_table_id, int32_t dop, bool shared_scan,
                             BalancedChunkBuffer& chunk_buffer)
            : _scan_node(scan_node),
              _scan_table_id(scan_table_id),
              _chunk_buffer(chunk_buffer),
              _shared_scan(shared_scan) {}
>>>>>>> 4265212f40 ([BugFix] fix incorrect scan metrics in FE (#27779))
    ~OlapScanContext() override = default;

    Status prepare(RuntimeState* state);
    void close(RuntimeState* state) override;

    void set_prepare_finished() { _is_prepare_finished.store(true, std::memory_order_release); }
    bool is_prepare_finished() const { return _is_prepare_finished.load(std::memory_order_acquire); }

    Status parse_conjuncts(RuntimeState* state, const std::vector<ExprContext*>& runtime_in_filters,
                           RuntimeFilterProbeCollector* runtime_bloom_filters);

    vectorized::OlapScanNode* scan_node() const { return _scan_node; }
    vectorized::OlapScanConjunctsManager& conjuncts_manager() { return _conjuncts_manager; }
    const std::vector<ExprContext*>& not_push_down_conjuncts() const { return _not_push_down_conjuncts; }
    const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges() const { return _key_ranges; }

    Status capture_tablet_rowsets(const std::vector<TInternalScanRange*>& olap_scan_ranges);
    const std::vector<TabletSharedPtr>& tablets() const { return _tablets; }
    const std::vector<std::vector<RowsetSharedPtr>>& tablet_rowsets() const { return _tablet_rowsets; };

<<<<<<< HEAD
private:
    vectorized::OlapScanNode* _scan_node;
=======
    const std::vector<ColumnAccessPathPtr>* column_access_paths() const;

    int64_t get_scan_table_id() const { return _scan_table_id; }

private:
    OlapScanNode* _scan_node;
    int64_t _scan_table_id;
>>>>>>> 4265212f40 ([BugFix] fix incorrect scan metrics in FE (#27779))

    std::vector<ExprContext*> _conjunct_ctxs;
    vectorized::OlapScanConjunctsManager _conjuncts_manager;
    // The conjuncts couldn't push down to storage engine
    std::vector<ExprContext*> _not_push_down_conjuncts;
    std::vector<std::unique_ptr<OlapScanRange>> _key_ranges;
    vectorized::DictOptimizeParser _dict_optimize_parser;
    ObjectPool _obj_pool;

    std::atomic<bool> _is_prepare_finished{false};

    // The row sets of tablets will become stale and be deleted, if compaction occurs
    // and these row sets aren't referenced, which will typically happen when the tablets
    // of the left table are compacted at building the right hash table. Therefore, reference
    // the row sets into _tablet_rowsets in the preparation phase to avoid the row sets being deleted.
    std::vector<TabletSharedPtr> _tablets;
    std::vector<std::vector<RowsetSharedPtr>> _tablet_rowsets;
};

<<<<<<< HEAD
=======
// OlapScanContextFactory creates different contexts for each scan operator, if _shared_scan is false.
// Otherwise, it outputs the same context for each scan operator.
class OlapScanContextFactory {
public:
    OlapScanContextFactory(OlapScanNode* const scan_node, int32_t dop, bool shared_morsel_queue, bool shared_scan,
                           ChunkBufferLimiterPtr chunk_buffer_limiter)
            : _scan_node(scan_node),
              _dop(dop),
              _shared_morsel_queue(shared_morsel_queue),
              _shared_scan(shared_scan),
              _chunk_buffer(shared_scan ? BalanceStrategy::kRoundRobin : BalanceStrategy::kDirect, dop,
                            std::move(chunk_buffer_limiter)),
              _contexts(shared_morsel_queue ? 1 : dop) {}

    OlapScanContextPtr get_or_create(int32_t driver_sequence);

    void set_scan_table_id(int64_t scan_table_id) { _scan_table_id = scan_table_id; }

private:
    OlapScanNode* const _scan_node;
    const int32_t _dop;
    const bool _shared_morsel_queue;   // Whether the scan operators share a morsel queue.
    const bool _shared_scan;           // Whether the scan operators share a chunk buffer.
    BalancedChunkBuffer _chunk_buffer; // Shared Chunk buffer for all the scan operators.

    int64_t _scan_table_id = -1;
    std::vector<OlapScanContextPtr> _contexts;
};

>>>>>>> 4265212f40 ([BugFix] fix incorrect scan metrics in FE (#27779))
} // namespace pipeline

} // namespace starrocks
