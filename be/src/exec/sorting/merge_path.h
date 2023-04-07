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

#include <utility>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exec/sorting/merge.h"
#include "exec/sorting/sorting.h"
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
    OutputSegment(SortedRun run, const size_t total_len) : run(std::move(run)), total_len(total_len) {}

    SortedRun run;

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
                                size_t& ri, OutputSegment& dest, size_t start_di, const size_t length);
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

// A merge tree is composed by a group of node, node can be either LeafNode or MergeNode
// LeafNode gets data from the specific providers, and has no child
// MergeNode has two children, and combine two streams of data into one.
class Node {
public:
    Node(MergePathCascadeMerger* merger) : _merger(merger) {}
    virtual ~Node() = default;
    virtual void process_input(int32_t parallel_idx) = 0;
    virtual void process_input_done(){};
    virtual bool is_leaf() { return false; }
    // Means the previous node reaches eos, no more data will flow to current node,
    // but there still may be some data in the input buffer to be consumed.
    virtual bool dependency_finished() = 0;
    // Means based on dependency_finished state, all the data in the input buffer has been consumed.
    virtual bool input_finished() = 0;
    // Return true if current node is active but cannot provide more data at this moment.
    virtual bool is_pending() { return false; }
    size_t degree_of_parallelism() { return _global_2_local_parallel_idx.size(); }
    void bind_parallel_idxs(std::unordered_map<int32_t, int32_t>&& global_2_local_parallel_idx) {
        _global_2_local_parallel_idx = std::move(global_2_local_parallel_idx);
    }
    bool output_empty() { return _output_segments.empty(); }
    bool eos() { return dependency_finished() && input_finished() && output_empty(); }
    std::vector<OutputSegmentPtr>&& output_segments() { return std::move(_output_segments); }

protected:
    MergePathCascadeMerger* _merger;

    // There may be multiply workers work on the same Node. So wee need to know
    // the local parallel_idx which the global parallel_idx corresponds to.
    // For example, there are 4 workers with id 0-3, and there are 2 Node(A and B).
    // Each Node should have two workers, assuming Node A have worker_0 and worker_1,
    // Node B have worker_2 and worker_3. So Node A need to maintain the mapping of {0->0, 1->1},
    // and Node B need to maintain the mapping of {2->0, 3->1}, given that the local parallel_idx
    // always ascending from 0 upwards.
    // pair.first store global parallel_idx, pair.second store local parallel_idx
    std::unordered_map<int32_t, int32_t> _global_2_local_parallel_idx;

    // Every time traversing, current node must all the output of its children
    std::vector<OutputSegmentPtr> _output_segments;

    std::mutex _m;
};
using NodePtr = std::unique_ptr<Node>;

class MergeNode final : public Node {
public:
    MergeNode(MergePathCascadeMerger* merger, Node* left, Node* right) : Node(merger), _left(left), _right(right) {}

    void process_input(int32_t parallel_idx) override;
    void process_input_done() override;
    bool dependency_finished() override { return _left->eos() && _right->eos(); }
    bool input_finished() override {
        return dependency_finished() && _left_input != nullptr && _left_input->len == 0 && _right_input != nullptr &&
               _right_input->len == 0;
    }

private:
    void _setup_input();

    Node* _left;
    Node* _right;

    InputSegmentPtr _left_input;
    InputSegmentPtr _right_input;
    bool _input_ready = false;

    size_t _merge_length;
};

class LeafNode final : public Node {
public:
    LeafNode(MergePathCascadeMerger* merger) : Node(merger) {}

    void process_input(int32_t parallel_idx) override;
    bool dependency_finished() override { return _provider_eos; }
    bool is_leaf() override { return true; }
    bool input_finished() override { return dependency_finished(); }
    bool is_pending() override { return !_provider_eos && !_provider(true, nullptr, nullptr); }

    void set_provider(MergePathChunkProvider&& provider) { _provider = std::move(provider); }

private:
    bool _provider_eos = false;
    MergePathChunkProvider _provider;
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
};
} // namespace detail

class MergePathCascadeMerger {
public:
    MergePathCascadeMerger(const int32_t degree_of_parallelism, std::vector<ExprContext*> sort_exprs,
                           const SortDescs& sort_descs, std::vector<MergePathChunkProvider> chunk_providers,
                           const size_t chunk_size);
    const std::vector<ExprContext*>& sort_exprs() const { return _sort_exprs; }
    const SortDescs& sort_descs() const { return _sort_descs; }

    size_t streaming_batch_size() { return _streaming_batch_size; }

    // There may be several parallelism working on the same stage
    // Return true if the current stage's work is done for the particular parallel_idx
    bool is_current_stage_finished(int32_t parallel_idx);

    // All the data are coming from chunk_providers, which passes through ctor.
    // If one of the providers cannot provider new data at the moment, maybe waiting for network,
    // but the provider still alive(not reach eos), then merger will enter to pending stage.
    bool is_pending(int32_t parallel_idx);

    // Return true if merge process is done
    bool is_finished();

    // All the merge process are triggered by this method
    ChunkPtr try_get_next(const int32_t parallel_idx);

    void bind_profile(int32_t parallel_idx, RuntimeProfile* profile);

private:
    bool _is_current_stage_done();
    void _forward_stage(const detail::Stage& stage, int32_t worker_num, std::vector<size_t>* process_cnts = nullptr);

    void _init();
    void _prepare();
    void _process(int32_t parallel_idx);
    void _split_chunk(int32_t parallel_idx);
    void _fetch_chunk(int32_t parallel_idx, ChunkPtr& chunk);

    void _find_unfinished_level();
    using Action = std::function<void()>;
    void _finish_current_stage(
            int32_t parallel_idx, const Action& stage_done_action = []() {});
    bool _has_pending_node();

    void _reset_output();

private:
    // For each MergeNode, there may be multiply threads working on the same merge processing.
    // And here we need to guarantee that each parallelism can process a certain amount of data. Assuming it
    // equals to chunk_size right now (can be later optimized), so the total size of the merge should be
    // chunk_size * degree_of_parallelism, which is called _streaming_batch_size here.
    const size_t _chunk_size;
    const size_t _streaming_batch_size;

    const int32_t _degree_of_parallelism;
    const std::vector<ExprContext*> _sort_exprs;
    const SortDescs _sort_descs;
    std::vector<MergePathChunkProvider> _chunk_providers;
    Action _finish_merge_action;

    // In order to get high performance, all the condition methods like `is_current_stage_finished/is_finished/is_pending`
    // don't use global mutex. But here is one critical section of forwarding stage, during which we need to mute all these
    // methods to avoid undefined concurrent behaviors.
    std::atomic<bool> _is_forwarding_stage = false;
    std::atomic<detail::Stage> _stage;
    // _process_cnts[i] represents how many times for parallel_idx=<i> that method `try_get_next` can be executed
    // The final consistency of all the following non-atomic fields are protected by _process_cnts
    // Through
    //      1. Read _process_cnts at the begining of try_get_next
    //      2. Write _process_cnts at tne end of try_get_next
    std::vector<std::atomic<size_t>> _process_cnts;

    // Merge nodes
    std::vector<std::vector<detail::NodePtr>> _levels;
    int32_t _level_idx = 0;
    detail::Node* _root;
    std::vector<detail::LeafNode*> _leafs;

    // In some cases when the original parallelism is not equal to power of 2, one level may contains both
    // MergeNode and LeafNode. And given that LeafNode can only bind to one operator, so there may be one
    // operator being idle when processing this level.
    int32_t _working_parallelism;
    // In some cases, one parallelism may have to process more than one node.
    // _working_nodes[i] represents the node list for parallel_idx=<i>
    std::vector<std::vector<detail::Node*>> _working_nodes;

    // Output chunks for each parallelism
    std::vector<std::vector<ChunkPtr>> _output_chunks;
    std::vector<ChunkPtr> _flat_output_chunks;
    size_t _output_idx = 0;

    std::vector<detail::Metrics> _metrics;
    std::chrono::steady_clock::time_point _pending_start;
    // First pending should not be recorded, because it all comes from the operator dependency
    bool _is_first_pending = true;

    std::mutex _m;
};

} // namespace starrocks::merge_path
