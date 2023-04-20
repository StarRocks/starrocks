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

#include <mutex>
#include <unordered_map>
#include <utility>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exec/sorting/merge.h"
#include "exec/sorting/sorting.h"
#include "runtime/descriptors.h"
#include "util/runtime_profile.h"

namespace starrocks::merge_path {

struct InputSegment {
    InputSegment(SortedRuns runs, const size_t start, const size_t len)
            : runs(std::move(runs)), start(start), len(len) {}
    SortedRuns runs;

    // The start and len represent the specific segment of the chunk that we are going to process,
    // and the len may be smaller than the total lens of the runs in streaming mode.
    size_t start;
    size_t len;

    // For streaming merge processing, we need to record how many steps moving forward after merge.
    // We use atomic type here because the thread writing it and the thread reading it may be different threads.
    std::atomic<size_t> forward{0};

    void move_forward() {
        DCHECK_LE(forward, len);
        start += forward;
        len -= forward;
        forward = 0;

        while (runs.num_chunks() > 0 && runs.front().num_rows() <= start) {
            start -= runs.front().num_rows();
            runs.pop_front();
        }
    }
};
struct OutputSegment {
    OutputSegment(SortedRun run, std::vector<int32_t> orderby_indexes, const size_t total_len)
            : run(std::move(run)), orderby_indexes(std::move(orderby_indexes)), total_len(total_len) {}

    SortedRun run;
    // Auxiliary data structure to help void append orderby column twice if
    // orderby column is a ColumnRef of the corresponding chunk
    // orderby_indexes[i] < 0 means run.orderby[i] is not ColumnRef
    const std::vector<int32_t> orderby_indexes;

    // Length of the overall merged chunk, and each segment only contains part of it,
    // which means run.num_rows() <= total_len, and for most cases, run.num_rows() < total_len
    const size_t total_len;
};
using InputSegmentPtr = std::unique_ptr<InputSegment>;
using OutputSegmentPtr = std::unique_ptr<OutputSegment>;

/**
 * Merge the given two inputs to one.
 *
 * @param desc Ordering description.
 * @param left Left side input.
 * @param right Right side input.
 * @param dest Output used to store the merged data for this parallelism.
 * @param parallel_idx Index of current parallelism, densely ascending from 0 upwards to (degree_of_parallelism - 1).
 * @param degree_of_parallelism Total parallelism.
 */
void merge(const SortDescs& descs, InputSegment& left, InputSegment& right, OutputSegment& dest,
           const size_t parallel_idx, const size_t degree_of_parallelism);

namespace detail {

/**
 * Build auxiliary data structure to help avoid double append orderby columns
 */
std::vector<int32_t> _build_orderby_indexes(const ChunkPtr& chunk, const std::vector<ExprContext*>& sort_exprs);

/**
 * This method is used to compute the point of intersection between merge path and diagonal of current parallelism
 *
 * @param desc Ordering description.
 * @param left Left side input.
 * @param right Right side input.
 * @param d_size Total merge size among all parallelism.
 * @param parallel_idx Index of current parallelism.
 * @param degree_of_parallelism Total parallelism.
 * @param l_start Output param, start index in left for this parallel_idx.
 * @param r_start Output param, start index in right for this parallel_idx.
 */
void _eval_diagonal_intersection(const SortDescs& descs, const InputSegment& left, const InputSegment& right,
                                 const size_t d_size, const size_t parallel_idx, const size_t degree_of_parallelism,
                                 size_t* l_start, size_t* r_start);

/**
 * Check if the point (left[li], right[ri]) is intersection point.
 * Assuming M matrix is a matrix comprising of only boolean value
 *      if left[i] > right[j], then M[i, j] = true
 *      if left[i] <= right[j], then M[i, j] = false
 * For edge cases (i or j beyond the matrix), think about the merge path, with left in the vertical direction 
 * and right in the horizontal direction. Merge path goes from top left to bottom right, the positions below the merge path should be true, 
 * and otherwise should be false.
 *
 * And the following 4 points will be checked
 *      1. (left[li-1], right[ri-1])
 *      2. (left[li-1], right[ri])
 *      3. (left[li], right[ri-1])
 *      4. (left[li], right[ri])
 *
 *
 * @param desc Ordering description.
 * @param left Left side input.
 * @param li Current index in left.
 * @param right Right side input.
 * @param ri Current index in right.
 * @param has_true Output param, true if one of the above four points is true.
 * @param has_false Ouput param, true if one of the above four points is false.
 */
void _is_intersection(const SortDescs& descs, const InputSegment& left, const size_t li, const InputSegment& right,
                      const size_t ri, bool& has_true, bool& has_false);

/**
 * Do merge along the merge path.
 *
 * @param desc Ordering description.
 * @param left Left side input.
 * @param li Current index in left.
 * @param right Right side input.
 * @param ri Current index in right.
 * @param dest Output used to store the merged data for this parallelism.
 * @param start_di Start index of the whole merge result, NOT only the above dest.
 * @param length The step for this parallelism can forward along merge path.
 */
void _do_merge_along_merge_path(const SortDescs& descs, const InputSegment& left, size_t& li, const InputSegment& right,
                                size_t& ri, OutputSegment& dest, const size_t start_di, const size_t length);
} // namespace detail

class MergePathCascadeMerger;

/**
 * LeafNode will use this provider to get data.
 *
 * @param only_check_if_has_data If true, only check if it has more data for further fetching
 * @param chunk Output chunk if any
 * @param eos Set to true if this provider reaches eos
 * @return: Return true if it has new data
 */
using MergePathChunkProvider = std::function<bool(bool only_check_if_has_data, ChunkPtr* chunk, bool* eos)>;

namespace detail {

class MergeNode;

// A merge tree is composed by a group of node, node can be either LeafNode or MergeNode
// LeafNode gets data from the specific providers, and has no child
// MergeNode has two children, and combine two streams of data into one.
class Node {
public:
    Node(MergePathCascadeMerger* merger) : _merger(merger) {}
    virtual ~Node() = default;
    virtual void process_input(const int32_t parallel_idx) = 0;
    virtual void process_input_done(){};
    virtual bool is_leaf() { return false; }

    // Current node can produce more output if one of the following conditions are met.
    // 1. It's dependency is not reaching eos. For LeafNode, dependency can be MergePathChunkProvider;
    // For MergeNode, dependencies can be its left and right children.
    // 2. Input buffer has not exhausted yet.
    virtual bool has_more_output() = 0;

    // Return true if current node cannot produce any more data, and the output segments of the latest
    // processing has been fetched.
    bool eos() { return !has_more_output() && _output_segments.empty(); }
    std::vector<OutputSegmentPtr>& output_segments() { return _output_segments; }

    void bind_parent(MergeNode* parent) { _parent = parent; }
    bool parent_input_full();

    size_t degree_of_parallelism() { return _global_2_local_parallel_idx.size(); }
    void bind_parallel_idxs(std::unordered_map<int32_t, int32_t>&& global_2_local_parallel_idx) {
        _global_2_local_parallel_idx = std::move(global_2_local_parallel_idx);
    }

protected:
    MergePathCascadeMerger* _merger;
    MergeNode* _parent = nullptr;

    // There may be multiply workers work on the same Node. So wee need to know
    // the local parallel_idx which the global parallel_idx corresponds to.
    // For example, there are 4 workers with id 0-3, and there are 2 Node(A and B).
    // Each Node should have two workers, assuming Node A have worker_0 and worker_1,
    // Node B have worker_2 and worker_3. So Node A need to maintain the mapping of {0->0, 1->1},
    // and Node B need to maintain the mapping of {2->0, 3->1}, given that the local parallel_idx
    // always ascending from 0 upwards.
    // pair.first store global parallel_idx, pair.second store local parallel_idx
    std::unordered_map<int32_t, int32_t> _global_2_local_parallel_idx;

    // Output segments of current processing, it will be all fetched by its parent
    std::vector<OutputSegmentPtr> _output_segments;

    std::mutex _m;
};
using NodePtr = std::unique_ptr<Node>;

class MergeNode final : public Node {
public:
    MergeNode(MergePathCascadeMerger* merger, Node* left, Node* right)
            : Node(merger), _left(left), _right(right), _left_buffer({}, 0, 0), _right_buffer({}, 0, 0) {}

    void process_input(const int32_t parallel_idx) override;
    void process_input_done() override;
    bool has_more_output() override;
    bool input_full(Node* child);

private:
    void _setup_input();

    Node* _left;
    Node* _right;

    InputSegment _left_buffer;
    InputSegment _right_buffer;
    bool _input_ready = false;

    size_t _merge_length = 0;
};

class LeafNode final : public Node {
public:
    LeafNode(MergePathCascadeMerger* merger, bool late_materialization)
            : Node(merger), _late_materialization(late_materialization) {}

    void process_input(const int32_t parallel_idx) override;
    bool is_leaf() override { return true; }
    bool has_more_output() override { return !_provider_eos; }
    bool provider_pending() { return has_more_output() && !_provider(true, nullptr, nullptr); }

    void set_provider(MergePathChunkProvider provider) { _provider = std::move(provider); }

private:
    ChunkPtr _generate_ordinal(const size_t chunk_id, const size_t num_rows);

    const bool _late_materialization;
    MergePathChunkProvider _provider;
    bool _provider_eos = false;
};

// Transition graph is as follows:
//                   ┌────────── PENDING ◄──────────┐
//                   │                              │
//                   │                              │
//                   ├──────────────◄───────────────┤
//                   │                              │
//                   ▼                              │
//       INIT ──► PREPARE ──► SPLIT_CHUNK ──► FETCH_CHUNK ──► FINISHED
//                   ▲
//                   |
//                   | one traverse from leaf to root
//                   |
//                   ▼
//                PROCESS
//
// The overall merge process will contain multiply traverses of the merge tree. And for one
// traverse, the changelist of the stage might be:
//                  PREPARE(level=0, leaf)
//                     │
//                     ▼
//                  PROCESS(level=0)
//                     │
//                     ▼
//                  PREPARE(level=1)
//                     │
//                     ▼
//                  PROCESS(level=1)
//                     │
//                     ▼
//                   ......
//                     │
//                     ▼
//                  PREPARE(level=<root level>)
//                     │
//                     ▼
//                  PROCESS(level=<root level>)
//
enum Stage {
    // Some initiation work will be done in this stage, including building the cascade merge tree
    INIT = 0,

    // Before entering the PROCESS stage, we need to build relaionships between all the parallelism and
    // all the Nodes with the specific level id
    PREPARE = 1,

    // All the parallelism will concurrently do its own job assigned at the PREPARE stage
    PROCESS = 2,

    // All the merge processing are done here, but the rootNode::_output_segments may contains chunk with size
    // larger than the limited size required by pipeline engine, so wo need to further split it
    SPLIT_CHUNK = 3,

    // Chunks can be flowed out
    FETCH_CHUNK = 4,

    // One of the providers cannot provider more data at the moment, so we need to wait for it to produce new data
    PENDING = 5,

    // All works done
    FINISHED = 6,
};

inline std::string to_string(Stage stage) {
    switch (stage) {
    case INIT:
        return "INIT";
    case PREPARE:
        return "PREPARE";
    case PROCESS:
        return "PROCESS";
    case SPLIT_CHUNK:
        return "SPLIT_CHUNK";
    case FETCH_CHUNK:
        return "FETCH_CHUNK";
    case PENDING:
        return "PENDING";
    case FINISHED:
        return "FINISHED";
    default:
        CHECK(false);
    }
}

struct Metrics {
    RuntimeProfile* profile;

    RuntimeProfile::Counter* stage_timer;
    RuntimeProfile::Counter* stage_counter;
    // stage_timers[i] represents the timer for the correspond stage
    // stage_counters[i] represents the counter for the correspond stage
    // 0 -> Stage::INIT
    // 1 -> Stage::PREPARE
    // 2 -> Stage::PROCESS
    // 3 -> Stage::SPLIT_CHUNK
    // 4 -> Stage::FETCH_CHUNK
    // 5 -> Stage::PENDING
    // 6 -> Stage::FINISHED
    std::vector<RuntimeProfile::Counter*> stage_timers;
    std::vector<RuntimeProfile::Counter*> stage_counters;

    RuntimeProfile::Counter* _sorted_run_provider_timer;
    RuntimeProfile::Counter* _late_materialization_generate_ordinal_timer;
    RuntimeProfile::Counter* _late_materialization_restore_according_to_ordinal_timer;
    RuntimeProfile::Counter* _late_materialization_max_buffer_chunk_num;
};
} // namespace detail

class MergePathCascadeMerger {
public:
    MergePathCascadeMerger(const size_t chunk_size, const int32_t degree_of_parallelism,
                           std::vector<ExprContext*> sort_exprs, const SortDescs& sort_descs,
                           const TupleDescriptor* tuple_desc, const TTopNType::type topn_type, const int64_t offset,
                           const int64_t limit, std::vector<MergePathChunkProvider> chunk_providers);
    const std::vector<ExprContext*>& sort_exprs() const { return _sort_exprs; }
    const SortDescs& sort_descs() const { return _sort_descs; }

    size_t streaming_batch_size() { return _streaming_batch_size; }

    // There may be several parallelism working on the same stage
    // Return true if the current stage's work is done for the particular parallel_idx
    bool is_current_stage_finished(const int32_t parallel_idx, const bool sync);

    // All the data are coming from chunk_providers, which passes through ctor.
    // If one of the providers cannot provider new data at the moment, maybe waiting for network,
    // but the provider still alive(not reach eos), then merger will enter to pending stage.
    bool is_pending(const int32_t parallel_idx);

    // Return true if merge process is done
    bool is_finished();

    // All the merge process are triggered by this method
    ChunkPtr try_get_next(const int32_t parallel_idx);

    void bind_profile(const int32_t parallel_idx, RuntimeProfile* profile);

    size_t add_original_chunk(ChunkPtr&& chunk);
    detail::Metrics& get_metrics(const int32_t parallel_idx) { return _metrics[parallel_idx]; }

private:
    bool _is_current_stage_done();
    void _forward_stage(const detail::Stage& stage, int32_t worker_num, std::vector<size_t>* process_cnts = nullptr);

    void _init();
    void _prepare();
    void _process(const int32_t parallel_idx);
    void _split_chunk(const int32_t parallel_idx);
    void _fetch_chunk(const int32_t parallel_idx, ChunkPtr& chunk);
    void _finishing();

    void _init_late_materialization();
    ChunkPtr _restore_according_to_ordinal(const int32_t parallel_idx, const ChunkPtr& chunk, Columns orderby);

    void _process_limit(ChunkPtr& chunk);

    void _find_unfinished_level();
    using Action = std::function<void()>;
    void _finish_current_stage(
            const int32_t parallel_idx, const Action& stage_done_action = []() {});
    bool _has_pending_node();

    void _reset_output();

public:
    // The ordinal value is comprising the following two parts
    // 1. chunk_id, which is assigned by `add_original_chunk
    // 2. offset in chunk
    // And for sake of performance, we use 64-bit to store these two parts, chunk_id takes first 44-bits
    // and offset takes 20-bits.
    constexpr static size_t MAX_CHUNK_SIZE = 0xfffff;
    constexpr static size_t OFFSET_BITS = 20;

private:
    // For each MergeNode, there may be multiply threads working on the same merge processing.
    // And here we need to guarantee that each parallelism can process a certain amount of data. Assuming it
    // equals to (4 * chunk_size) right now (can be later optimized), so the total size of the merge should be
    // 4 * chunk_size * degree_of_parallelism, which is called _streaming_batch_size here.
    // And each MergeNode can hold left/right buffer of size within 2 * _streaming_batch_size.
    // So the total amount size the whole merge tree will hold is around _degree_of_parallelism * 2 * _streaming_batch_size
    // (The number of MergeNode approximately equals to _degree_of_parallelism)
    const size_t _chunk_size;
    const size_t _streaming_batch_size;

    const int32_t _degree_of_parallelism;
    const std::vector<ExprContext*> _sort_exprs;
    const SortDescs _sort_descs;
    const TupleDescriptor* _tuple_desc;
    const TTopNType::type _topn_type;
    const int64_t _offset;
    const int64_t _limit;
    const std::vector<MergePathChunkProvider> _chunk_providers;
    Action _finish_merge_action;

    // All operations of _stage and _process_cnts must under the protection of _status_m, the critical section
    // protected by the _status_m must won't last long, so it won't lead to drastic performance deduction.
    // And the final consistency of all the following fields are guaranteed by:
    //      1. read _process_cnts at the begining of try_get_next.
    //      2. write _process_cnts at the end of try_get_next.
    std::recursive_mutex _status_m;
    detail::Stage _stage;
    std::vector<size_t> _process_cnts;

    // Merge nodes
    std::vector<std::vector<detail::NodePtr>> _levels;
    int32_t _level_idx = 0;
    detail::Node* _root;
    std::vector<detail::LeafNode*> _leafs;

    // In some cases, one parallelism may have to process more than one node.
    // _working_nodes[i] represents the node list for parallel_idx=<i>
    std::vector<std::vector<detail::Node*>> _working_nodes;

    // Fields used for late materialization
    bool _late_materialization = false;
    std::mutex _late_materialization_m;
    size_t _chunk_id_generator = 0;
    size_t _max_buffer_chunk_num = 0;
    // Pointer stability is required here, so we must use node_hash_map
    phmap::node_hash_map<size_t, std::pair<ChunkPtr, size_t>> _original_chunk_buffer;
    std::vector<int32_t> _orderby_indexes;

    // Output chunks for each parallelism
    std::vector<std::vector<ChunkPtr>> _output_chunks;
    std::vector<ChunkPtr> _flat_output_chunks;
    size_t _output_idx = 0;
    size_t _output_row_num = 0;
    bool _short_circuit = false;

    std::vector<detail::Metrics> _metrics;
    std::chrono::steady_clock::time_point _pending_start;
    // First pending should not be recorded, because it all comes from the operator dependency
    bool _is_first_pending = true;
};

} // namespace starrocks::merge_path
