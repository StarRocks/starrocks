// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>

#include "column/column_access_path.h"
#include "exec/olap_scan_prepare.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/pipeline/schedule/observer.h"
#include "fs/fs.h"
#include "gutil/macros.h"
#include "runtime/global_dict/parser.h"
#include "storage/rowset/rowset.h"
#include "util/hash.h"
#include "util/phmap/phmap.h"
#include "util/phmap/phmap_fwd_decl.h"

namespace starrocks {

class ScanNode;
class Tablet;
using TabletSharedPtr = std::shared_ptr<Tablet>;
class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;
class Segment;
using SegmentPtr = std::shared_ptr<Segment>;

class RuntimeFilterProbeCollector;

namespace pipeline {

class OlapScanContext;
using OlapScanContextPtr = std::shared_ptr<OlapScanContext>;
class OlapScanContextFactory;
using OlapScanContextFactoryPtr = std::shared_ptr<OlapScanContextFactory>;

struct GlobalLateMaterilizationCtx {
    struct SegmentInfo {
        SegmentPtr segment;
        std::shared_ptr<FileSystem> fs;
    };
    using SegmentInfoPtr = std::shared_ptr<SegmentInfo>;
    GlobalLateMaterilizationCtx() = default;
    ~GlobalLateMaterilizationCtx();

    DISALLOW_COPY_AND_MOVE(GlobalLateMaterilizationCtx);

    uint32_t register_segment(SegmentInfo segment) {
        int32_t id = _next_id++;
        _segments.insert({id, std::move(segment)});
        return id;
    }
    SegmentInfo get_segment(uint32_t id) const {
        CHECK(_segments.contains(id)) << "segment id not exists: " << id;
        return _segments.at(id);
    }

    void add_captured_tablet_rowsets(std::vector<std::vector<RowsetSharedPtr>> rowsets);

    std::atomic<uint32_t> _next_id{0};
    phmap::parallel_flat_hash_map<uint32_t, SegmentInfo, StdHash<uint32_t>, std::equal_to<uint32_t>,
                                  std::allocator<std::pair<uint32_t, SegmentInfo>>, 5, std::shared_mutex, true>
            _segments;

    std::mutex _mu;
    std::vector<std::unique_ptr<MultiRowsetReleaseGuard>> _rowset_release_guards;

};

class ConcurrentJitRewriter {
public:
    ConcurrentJitRewriter() : _barrier(), _errors(0), _id(0) {}
    Status rewrite(std::vector<ExprContext*>& expr_ctxs, ObjectPool* pool, bool enable_jit);

private:
    // TODO: use c++20 barrier after upgrading gcc
    class Barrier {
    public:
        explicit Barrier() : _count(0), _current(0) {}

        void arrive() {
            std::unique_lock<std::mutex> lock(_mutex);
            ++_count;
        }

        void wait() {
            std::unique_lock<std::mutex> lock(_mutex);
            if (++_current >= _count) {
                _cv.notify_all();
            } else {
                _cv.wait(lock, [this] { return _current >= _count; });
            }
        }

    private:
        std::size_t _count;
        std::size_t _current;
        std::mutex _mutex;
        std::condition_variable _cv;
    };
    Barrier _barrier;
    std::atomic_int _errors;
    std::atomic_int _id = 0;
};

class OlapScanContext final : public ContextWithDependency {
public:
    explicit OlapScanContext(OlapScanNode* scan_node, int64_t scan_table_id, int32_t dop, bool shared_scan,
                             BalancedChunkBuffer& chunk_buffer, ConcurrentJitRewriter& jit_rewriter)
            : _scan_node(scan_node),
              _scan_table_id(scan_table_id),
              _chunk_buffer(chunk_buffer),
              _shared_scan(shared_scan),
              _jit_rewriter(jit_rewriter) {}
    ~OlapScanContext() override = default;

    Status prepare(RuntimeState* state);
    void close(RuntimeState* state) override;

    void set_prepare_finished() { _is_prepare_finished.store(true, std::memory_order_release); }
    bool is_prepare_finished() const { return _is_prepare_finished.load(std::memory_order_acquire); }

    Status parse_conjuncts(RuntimeState* state, const std::vector<ExprContext*>& runtime_in_filters,
                           RuntimeFilterProbeCollector* runtime_bloom_filters, int32_t driver_sequence);

    OlapScanNode* scan_node() const { return _scan_node; }
    ScanConjunctsManager& conjuncts_manager() { return *_conjuncts_manager; }
    const std::vector<ExprContext*>& not_push_down_conjuncts() const { return _not_push_down_conjuncts; }
    const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges() const { return _key_ranges; }
    BalancedChunkBuffer& get_chunk_buffer() { return _chunk_buffer; }

    // Shared scan states
    bool is_shared_scan() const { return _shared_scan; }
    // Attach and detach to account the active input for shared chunk buffer
    void attach_shared_input(int32_t operator_seq, int32_t source_index);
    void detach_shared_input(int32_t operator_seq, int32_t source_index);
    bool has_active_input() const;
    BalancedChunkBuffer& get_shared_buffer();

    Status capture_tablet_rowsets(const std::vector<TInternalScanRange*>& olap_scan_ranges);
    const std::vector<TabletSharedPtr>& tablets() const { return _tablets; }
    std::vector<TabletSharedPtr>* mutable_tablets() { return &_tablets; }
    const std::vector<std::vector<RowsetSharedPtr>>& tablet_rowsets() const {
        return _rowset_release_guard.tablet_rowsets();
    };

    const std::vector<ColumnAccessPathPtr>* column_access_paths() const;

    int64_t get_scan_table_id() const { return _scan_table_id; }

    void attach_observer(RuntimeState* state, PipelineObserver* observer) { _observable.add_observer(state, observer); }
    void notify_observers() { _observable.notify_source_observers(); }
    size_t only_one_observer() const { return _observable.num_observers() == 1; }
    bool active_inputs_empty_event() {
        if (!_active_inputs_empty.load(std::memory_order_acquire)) {
            return false;
        }
        bool val = true;
        return _active_inputs_empty.compare_exchange_strong(val, false);
    }

private:
    OlapScanNode* _scan_node;
    int64_t _scan_table_id;

    std::vector<ExprContext*> _conjunct_ctxs;
    std::unique_ptr<ScanConjunctsManager> _conjuncts_manager = nullptr;
    // The conjuncts couldn't push down to storage engine
    std::vector<ExprContext*> _not_push_down_conjuncts;
    std::vector<std::unique_ptr<OlapScanRange>> _key_ranges;
    ObjectPool _obj_pool;

    // For shared_scan mechanism
    using ActiveInputKey = std::pair<int32_t, int32_t>;
    using ActiveInputSet = phmap::parallel_flat_hash_set<
            ActiveInputKey, typename phmap::Hash<ActiveInputKey>, typename phmap::EqualTo<ActiveInputKey>,
            typename std::allocator<ActiveInputKey>, NUM_LOCK_SHARD_LOG, std::mutex, true>;
    BalancedChunkBuffer& _chunk_buffer; // Shared Chunk buffer for all scan operators, owned by OlapScanContextFactory.
    ActiveInputSet _active_inputs;      // Maintain the active chunksource
    std::atomic_int _num_active_inputs{};
    std::atomic_bool _active_inputs_empty{};
    bool _shared_scan; // Enable shared_scan

    std::atomic<bool> _is_prepare_finished{false};

    // The row sets of tablets will become stale and be deleted, if compaction occurs
    // and these row sets aren't referenced, which will typically happen when the tablets
    // of the left table are compacted at building the right hash table. Therefore, reference
    // the row sets into _tablet_rowsets in the preparation phase to avoid the row sets being deleted.
    std::vector<TabletSharedPtr> _tablets;
    MultiRowsetReleaseGuard _rowset_release_guard;
    ConcurrentJitRewriter& _jit_rewriter;

    // the scan operator observe when task finished
    Observable _observable;
};

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
              _contexts(shared_morsel_queue ? 1 : dop),
              _jit_rewriter() {}

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
    ConcurrentJitRewriter _jit_rewriter;
};

} // namespace pipeline

} // namespace starrocks
