// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "column/chunk.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks {
namespace pipeline {

// ===== exchanger =====
class MCastLocalExchanger {
public:
    MCastLocalExchanger(size_t consumer_number);
    ~MCastLocalExchanger();
    bool can_pull_chunk(int32_t mcast_consumer_index) const;
    bool can_push_chunk() const;
    Status push_chunk(const vectorized::ChunkPtr& chunk, int32_t sink_driver_sequence);
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state, int32_t mcast_consuemr_index);
    void open_source_operator(int32_t mcast_consumer_index);
    void close_source_operator(int32_t mcast_consumer_index);
    void open_sink_operator();
    void close_sink_operator();

private:
    struct Cell {
        vectorized::ChunkPtr chunk = nullptr;
        Cell* next = nullptr;
        size_t acc_chunk_size = 0;
        int32_t used_count = 0;
    };
    void _update_progress(Cell* fast = nullptr);
    void _closer_consumer(int32_t mcast_consumer_index);
    mutable std::mutex _mutex;
    size_t _consumer_number;
    size_t _current_acc_chunk_size = 0;
    std::vector<Cell*> _progress;
    std::vector<int32_t> _opened_source_opcount;
    size_t _fast_acc_chunk_size = 0;
    int32_t _opened_source_number = 0;
    int32_t _opened_sink_number = 0;
    Cell* _head = nullptr;
    Cell* _tail = nullptr;
};

// ===== source op =====
class MCastLocalExchangeSourceOperator final : public SourceOperator {
public:
    MCastLocalExchangeSourceOperator(int32_t id, int32_t mcast_consumer_index,
                                     std::shared_ptr<MCastLocalExchanger> exchanger)
            : SourceOperator(id, "mcast_local_exchange_source", -1),
              _mcast_consumer_index(mcast_consumer_index),
              _exchanger(exchanger) {}

    Status prepare(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override { return _is_finished; }

    void set_finishing(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    bool _is_finished = false;
    int32_t _mcast_consumer_index;
    std::shared_ptr<MCastLocalExchanger> _exchanger;
};

class MCastLocalExchangeSourceOperatorFactory final : public SourceOperatorFactory {
public:
    MCastLocalExchangeSourceOperatorFactory(int32_t id, int32_t mcast_consumer_index,
                                            std::shared_ptr<MCastLocalExchanger> exchanger)
            : SourceOperatorFactory(id, "mcast_local_exchange_source", -1),
              _mcast_consumer_index(mcast_consumer_index),
              _exchanger(exchanger) {}
    ~MCastLocalExchangeSourceOperatorFactory() override = default;
    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<MCastLocalExchangeSourceOperator>(_id, _mcast_consumer_index, _exchanger);
    }

private:
    int32_t _mcast_consumer_index;
    std::shared_ptr<MCastLocalExchanger> _exchanger;
};

// ===== sink op =====

class MCastLocalExchangeSinkOperator final : public Operator {
public:
    MCastLocalExchangeSinkOperator(int32_t id, const int32_t driver_sequence,
                                   std::shared_ptr<MCastLocalExchanger> exchanger)
            : Operator(id, "mcast_local_exchange_sink", -1), _driver_sequence(driver_sequence), _exchanger(exchanger) {}

    ~MCastLocalExchangeSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override { return _is_finished; }

    void set_finishing(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    bool _is_finished = false;
    const int32_t _driver_sequence;
    const std::shared_ptr<MCastLocalExchanger> _exchanger;
};

class MCastLocalExchangeSinkOperatorFactory final : public OperatorFactory {
public:
    MCastLocalExchangeSinkOperatorFactory(int32_t id, std::shared_ptr<MCastLocalExchanger> exchanger)
            : OperatorFactory(id, "mcast_local_exchange_sink", -1), _exchanger(exchanger) {}

    ~MCastLocalExchangeSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<MCastLocalExchangeSinkOperator>(_id, driver_sequence, _exchanger);
    }

private:
    std::shared_ptr<MCastLocalExchanger> _exchanger;
};

} // namespace pipeline
} // namespace starrocks