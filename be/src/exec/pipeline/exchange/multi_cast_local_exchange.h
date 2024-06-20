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

#include <deque>
#include <utility>

#include "column/chunk.h"
#include "exec/pipeline/source_operator.h"
#include "exec/spill/dir_manager.h"
#include "fs/fs.h"

namespace starrocks::pipeline {

// For uni cast stream sink, we just add a exchange sink operator
// at the end of the pipeline. It works like
// [source pipeline]  -> exchange_sink_operator

// But for multi cast stream sink, extra pipelines are added with exchanger
// It works like
// [source pipeline] -> mcast_local_sink   -> mcast_local_exchanger  -> [new pipeline1]
//                                                     |             -> [new pipeline2]
//                                                     |             -> [new pipeline3]
// and for each new pipeline, the internal structure is
// [new pileine]: mcast_local_source -> exchange_sink_operator

// The dataflow works like:
// 1. mcast_local_sink push chunks to exchanger
// 2. mcast_local_source pull chunks from exchanger

// The exchanger should take care of several things:
// 1. can accept chunk or not. we don't want to block any consumer. we can accept chunk only when a any consumer needs chunk.
// 2. can throw chunk or not. we can only throw any chunk when all consumers have consumed that chunk.
// 3. can pull chiunk. we maintain the progress of consumers.

class MultiCastLocalExchangeSinkOperator;
// ===== exchanger =====
class MultiCastLocalExchanger {
public:
    virtual ~MultiCastLocalExchanger() = default;
    virtual bool can_pull_chunk(int32_t mcast_consumer_index) const = 0;
    virtual bool can_push_chunk() const = 0;
    virtual Status push_chunk(const ChunkPtr& chunk, int32_t sink_driver_sequenc,
                      MultiCastLocalExchangeSinkOperator* sink_operator) = 0;
    virtual StatusOr<ChunkPtr> pull_chunk(RuntimeState* state, int32_t mcast_consuemr_index) = 0;
    virtual void open_source_operator(int32_t mcast_consumer_index) = 0; 
    virtual void close_source_operator(int32_t mcast_consumer_index) = 0;
    virtual void open_sink_operator() = 0;
    virtual void close_sink_operator() = 0;
    RuntimeProfile* runtime_profile() { return _runtime_profile.get(); }
protected:
    std::unique_ptr<RuntimeProfile> _runtime_profile;
};

class InMemoryMultiCastLocalExchanger final: public MultiCastLocalExchanger {
public:
    InMemoryMultiCastLocalExchanger(RuntimeState* runtime_state, size_t consumer_number);
    ~InMemoryMultiCastLocalExchanger() override;
    bool can_pull_chunk(int32_t mcast_consumer_index) const override;
    bool can_push_chunk() const override;
    Status push_chunk(const ChunkPtr& chunk, int32_t sink_driver_sequenc,
                      MultiCastLocalExchangeSinkOperator* sink_operator) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state, int32_t mcast_consuemr_index) override;
    void open_source_operator(int32_t mcast_consumer_index) override;
    void close_source_operator(int32_t mcast_consumer_index) override;
    void open_sink_operator() override;
    void close_sink_operator() override;
    RuntimeProfile* runtime_profile() { return _runtime_profile.get(); }

private:
    struct Cell {
        ChunkPtr chunk = nullptr;
        Cell* next = nullptr;
        size_t memory_usage = 0;
        size_t accumulated_row_size = 0;
        // how many consumers have used this chunk
        int32_t used_count = 0;
    };
    void _update_progress(Cell* fast = nullptr);
    void _closer_consumer(int32_t mcast_consumer_index);
    RuntimeState* _runtime_state;
    mutable std::mutex _mutex;
    size_t _consumer_number;
    size_t _current_accumulated_row_size = 0;
    std::vector<Cell*> _progress;
    std::vector<int32_t> _opened_source_opcount;
    // the fast consumer has consumed how many chunks.
    size_t _fast_accumulated_row_size = 0;
    int32_t _opened_source_number = 0;
    int32_t _opened_sink_number = 0;
    Cell* _head = nullptr;
    Cell* _tail = nullptr;
    size_t _current_memory_usage = 0;
    size_t _current_row_size = 0;
    std::unique_ptr<RuntimeProfile> _runtime_profile;
    RuntimeProfile::HighWaterMarkCounter* _peak_memory_usage_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _peak_buffer_row_size_counter = nullptr;
};

class MemLimitedChunkQueue;
class SpillableMultiCastLocalExchanger: public MultiCastLocalExchanger {
public:
    SpillableMultiCastLocalExchanger(RuntimeState* runtime_state, size_t consumer_number, int64_t memory_limit = 1);
    virtual ~SpillableMultiCastLocalExchanger();

    bool can_pull_chunk(int32_t mcast_consumer_index) const override;
    bool can_push_chunk() const override;
    Status push_chunk(const ChunkPtr& chunk, int32_t sink_driver_sequence, MultiCastLocalExchangeSinkOperator* sink_operator) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state, int32_t mcast_consumer_index) override;
    void open_source_operator(int32_t mcast_consumer_index) override;
    void close_source_operator(int32_t mcast_consumer_index) override;
    void open_sink_operator() override;
    void close_sink_operator() override;

private:
    // queue ? cell->cell->cell
    // cell in memory or on disk
    // evcit
    struct Cell {
        ChunkPtr chunk = nullptr;
        Cell* next = nullptr;
        size_t memory_usage = 0;
        size_t accumulated_row_size = 0;
        int32_t used_count = 0;
        // @TODO mark state, on_mem/flushing/on_disk
        bool in_mem = true;
        // disk info
        size_t offset = 0;
        size_t length = 0;
        //@TODO should have a index
    };
    struct FlushContext;

    // @TODO need a mapping ablout offset to spillBlock
    struct ChunkBlock {
        std::vector<ChunkPtr> chunks;
        ChunkBlock* next = nullptr;
        size_t memory_usage = 0;
        bool in_mem = true;
        size_t offset = 0;
        size_t length = 0;
    };
    // @TODO we can use std::deque to store Cell, but if we remove from end, idnex may change?
    // void _update_progress(std::deque<Cell>::iterator& fast);
    // void _update_progress(size_t index);
    void _update_progress(Cell* cell = nullptr);
    void _close_consumer(int32_t mcast_consumer_index);

    Status _create_file();
    // we may need rand read
    RuntimeState* _runtime_state = nullptr;
    mutable std::mutex _mutex;
    size_t _consumer_number;
    size_t _current_accumulated_row_size = 0;
    mutable size_t _current_memory_usage = 0;
    size_t _current_row_size = 0;
    std::deque<Cell> _cells;
    Cell* _head = nullptr;
    Cell* _tail = nullptr;
    size_t _fast_accumulated_row_size = 0;
    int32_t _opened_source_number = 0;
    int32_t _opened_sink_number = 0;

    size_t _last_index = 0;
    size_t _head_index = 0;
    // each consumsers offset
    // next Cell that each consumer will consume
    std::vector<Cell*> _progress;
    // std::vector<std::deque<Cell>::iterator> _progress;

    // std::vector<size_t> _progress;
    std::vector<int32_t> _opened_source_opcount;
    // std::atomic_bool on_flight_io;
    // @TODO should buffer data
    size_t _memory_limit = 1024 * 1024 * 16;
    RuntimeProfile::HighWaterMarkCounter* _peak_memory_usage_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _peak_buffer_row_size_counter = nullptr;

    mutable std::unique_ptr<WritableFile> _file;
    mutable bool _has_flush_io_task = false;
    mutable std::vector<bool> _has_load_io_task;
    mutable std::string _file_name;
    mutable std::shared_ptr<spill::Dir> _dir;
    // Cell* _flush_point = nullptr;
    mutable Cell* _flush_point = nullptr;
    std::shared_ptr<MemLimitedChunkQueue> _queue;
};



// ===== source op =====
class MultiCastLocalExchangeSourceOperator final : public SourceOperator {
public:
    MultiCastLocalExchangeSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                         int32_t driver_sequence, int32_t mcast_consumer_index,
                                         std::shared_ptr<MultiCastLocalExchanger> exchanger)
            : SourceOperator(factory, id, "multi_cast_local_exchange_source", plan_node_id, true, driver_sequence),
              _mcast_consumer_index(mcast_consumer_index),
              _exchanger(std::move(std::move(exchanger))) {}

    Status prepare(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override { return _is_finished; }

    Status set_finishing(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    bool _is_finished = false;
    int32_t _mcast_consumer_index;
    std::shared_ptr<MultiCastLocalExchanger> _exchanger;
};

class MultiCastLocalExchangeSourceOperatorFactory final : public SourceOperatorFactory {
public:
    MultiCastLocalExchangeSourceOperatorFactory(int32_t id, int32_t plan_node_id, int32_t mcast_consumer_index,
                                                std::shared_ptr<MultiCastLocalExchanger> exchanger)
            : SourceOperatorFactory(id, "multi_cast_local_exchange_source", plan_node_id),
              _mcast_consumer_index(mcast_consumer_index),
              _exchanger(std::move(std::move(exchanger))) {}
    ~MultiCastLocalExchangeSourceOperatorFactory() override = default;
    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<MultiCastLocalExchangeSourceOperator>(this, _id, _plan_node_id, driver_sequence,
                                                                      _mcast_consumer_index, _exchanger);
    }

private:
    int32_t _mcast_consumer_index;
    std::shared_ptr<MultiCastLocalExchanger> _exchanger;
};

// ===== sink op =====

class MultiCastLocalExchangeSinkOperator final : public Operator {
public:
    MultiCastLocalExchangeSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                       const int32_t driver_sequence,
                                       std::shared_ptr<MultiCastLocalExchanger> exchanger)
            : Operator(factory, id, "multi_cast_local_exchange_sink", plan_node_id, true, driver_sequence),
              _exchanger(std::move(std::move(exchanger))) {}

    ~MultiCastLocalExchangeSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override { return _is_finished; }

    Status set_finishing(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    void update_counter(size_t memory_usage, size_t buffer_row_size);

private:
    bool _is_finished = false;
    const std::shared_ptr<MultiCastLocalExchanger> _exchanger;
    RuntimeProfile::HighWaterMarkCounter* _peak_memory_usage_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _peak_buffer_row_size_counter = nullptr;
};

class MultiCastLocalExchangeSinkOperatorFactory final : public OperatorFactory {
public:
    MultiCastLocalExchangeSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                              std::shared_ptr<MultiCastLocalExchanger> exchanger)
            : OperatorFactory(id, "multi_cast_local_exchange_sink", plan_node_id),
              _exchanger(std::move(std::move(exchanger))) {}

    ~MultiCastLocalExchangeSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<MultiCastLocalExchangeSinkOperator>(this, _id, _plan_node_id, driver_sequence,
                                                                    _exchanger);
    }

private:
    std::shared_ptr<MultiCastLocalExchanger> _exchanger;
};

} // namespace starrocks::pipeline
