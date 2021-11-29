// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/exchange/mcast_local_exchange.h"

#include "util/logging.h"

namespace starrocks {
namespace pipeline {

MCastLocalExchanger::MCastLocalExchanger(size_t consumer_number)
        : _mutex(),
          _consumer_number(consumer_number),
          _current_acc_chunk_size(0),
          _progress(consumer_number),
          _opened_source_opcount(consumer_number) {
    Cell* dummy = new Cell();
    _head = dummy;
    _tail = dummy;
    for (size_t i = 0; i < consumer_number; i++) {
        _progress[i] = _tail;
        _opened_source_opcount[i] = 0;
    }
}

MCastLocalExchanger::~MCastLocalExchanger() {
    while (_head != nullptr) {
        auto* t = _head->next;
        delete _head;
        _head = t;
    }
}

bool MCastLocalExchanger::can_push_chunk() const {
    std::unique_lock l(_mutex);
    if ((_current_acc_chunk_size - _fast_acc_chunk_size) > config::vector_chunk_size) {
        return false;
    }
    return true;
}

Status MCastLocalExchanger::push_chunk(const vectorized::ChunkPtr& chunk, int32_t sink_driver_sequence) {
    if (chunk->num_rows() == 0) return Status::OK();

    std::unique_lock l(_mutex);

    int32_t used_count = (_consumer_number - _opened_source_number);

    auto* cell = new Cell();
    cell->chunk = chunk;
    cell->used_count = used_count;
    cell->acc_chunk_size = _current_acc_chunk_size;
    cell->next = nullptr;

    _tail->next = cell;
    _tail = cell;
    _current_acc_chunk_size += chunk->num_rows();

    return Status::OK();
}

bool MCastLocalExchanger::can_pull_chunk(int32_t mcast_consumer_index) const {
    DCHECK(mcast_consumer_index < _consumer_number);

    std::unique_lock l(_mutex);
    DCHECK(_progress[mcast_consumer_index] != nullptr);
    if (_opened_sink_number == 0) return true;
    auto* cell = _progress[mcast_consumer_index];
    if (cell->next != nullptr) {
        return true;
    }
    return false;
}

StatusOr<vectorized::ChunkPtr> MCastLocalExchanger::pull_chunk(RuntimeState* state, int32_t mcast_consumer_index) {
    DCHECK(mcast_consumer_index < _consumer_number);

    std::unique_lock l(_mutex);
    DCHECK(_progress[mcast_consumer_index] != nullptr);
    Cell* cell = _progress[mcast_consumer_index];
    if (cell->next == nullptr) {
        if (_opened_sink_number == 0) return Status::EndOfFile("mcast_local_exchanger eof");
        return Status::OK();
    }
    cell = cell->next;
    _progress[mcast_consumer_index] = cell;
    cell->used_count += 1;

    _update_progress(cell);
    return cell->chunk;
}

void MCastLocalExchanger::open_source_operator(int32_t mcast_consumer_index) {
    std::unique_lock l(_mutex);
    if (_opened_source_opcount[mcast_consumer_index] == 0) {
        _opened_source_number += 1;
    }
    _opened_source_opcount[mcast_consumer_index] += 1;
}
void MCastLocalExchanger::close_source_operator(int32_t mcast_consumer_index) {
    std::unique_lock l(_mutex);
    _opened_source_opcount[mcast_consumer_index] -= 1;
    if (_opened_source_opcount[mcast_consumer_index] == 0) {
        _opened_source_number--;
        _closer_consumer(mcast_consumer_index);
    }
}

void MCastLocalExchanger::open_sink_operator() {
    std::unique_lock l(_mutex);
    _opened_sink_number++;
}
void MCastLocalExchanger::close_sink_operator() {
    std::unique_lock l(_mutex);
    _opened_sink_number--;
}

void MCastLocalExchanger::_closer_consumer(int32_t mcast_consumer_index) {
    Cell* now = _progress[mcast_consumer_index];
    now = now->next;
    while (now) {
        now->used_count += 1;
        now = now->next;
    }
    _progress[mcast_consumer_index] = nullptr;
    _update_progress();
}

void MCastLocalExchanger::_update_progress(Cell* fast) {
    if (fast != nullptr) {
        _fast_acc_chunk_size = std::max(_fast_acc_chunk_size, fast->acc_chunk_size);
    } else {
        // update the fastest consumer.
        _fast_acc_chunk_size = 0;
        for (size_t i = 0; i < _consumer_number; i++) {
            Cell* c = _progress[i];
            if (c == nullptr) continue;
            _fast_acc_chunk_size = std::max(_fast_acc_chunk_size, c->acc_chunk_size);
        }
    }
    // release chunk if no one needs it.
    while (_head && _head->used_count == _consumer_number) {
        Cell* t = _head->next;
        if (t == nullptr) break;
        delete _head;
        _head = t;
    }
}

// ===== source op =====
Status MCastLocalExchangeSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    _exchanger->open_source_operator(_mcast_consumer_index);
    return Status::OK();
}

void MCastLocalExchangeSourceOperator::set_finishing(RuntimeState* state) {
    if (!_is_finished) {
        _is_finished = true;
        _exchanger->close_source_operator(_mcast_consumer_index);
    }
}

StatusOr<vectorized::ChunkPtr> MCastLocalExchangeSourceOperator::pull_chunk(RuntimeState* state) {
    auto ret = _exchanger->pull_chunk(state, _mcast_consumer_index);
    if (ret.status().is_end_of_file()) {
        set_finishing(state);
    }
    return ret;
}

bool MCastLocalExchangeSourceOperator::has_output() const {
    return _exchanger->can_pull_chunk(_mcast_consumer_index);
}

// ===== sink op =====

Status MCastLocalExchangeSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _exchanger->open_sink_operator();
    return Status::OK();
}

bool MCastLocalExchangeSinkOperator::need_input() const {
    return _exchanger->can_push_chunk();
}

void MCastLocalExchangeSinkOperator::set_finishing(RuntimeState* state) {
    if (!_is_finished) {
        _is_finished = true;
        _exchanger->close_sink_operator();
    }
}

StatusOr<vectorized::ChunkPtr> MCastLocalExchangeSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Should not pull_chunk in mcast_local_exchange_sink");
}

Status MCastLocalExchangeSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    return _exchanger->push_chunk(chunk, _driver_sequence);
}

} // namespace pipeline
} // namespace starrocks