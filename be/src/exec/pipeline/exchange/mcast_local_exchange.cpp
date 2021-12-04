// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/exchange/mcast_local_exchange.h"

#include "util/logging.h"

namespace starrocks {
namespace pipeline {

static const size_t kBufferedRowSizeScaleFactor = 16;

MultiCastLocalExchanger::MultiCastLocalExchanger(size_t consumer_number)
        : _mutex(),
          _consumer_number(consumer_number),
          _current_accumulated_row_size(0),
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

MultiCastLocalExchanger::~MultiCastLocalExchanger() {
    while (_head != nullptr) {
        auto* t = _head->next;
        delete _head;
        _head = t;
    }
}

bool MultiCastLocalExchanger::can_push_chunk() const {
    std::unique_lock l(_mutex);
    // if for the fastest consumer, the exchanger still has enough chunk to be consumed.
    // the exchanger does not need any input.
    if ((_current_accumulated_row_size - _fast_accumulated_row_size) >
        config::vector_chunk_size * kBufferedRowSizeScaleFactor) {
        return false;
    }
    return true;
}

Status MultiCastLocalExchanger::push_chunk(const vectorized::ChunkPtr& chunk, int32_t sink_driver_sequence) {
    if (chunk->num_rows() == 0) return Status::OK();

    std::unique_lock l(_mutex);

    int32_t closed_source_number = (_consumer_number - _opened_source_number);

    auto* cell = new Cell();
    cell->chunk = chunk;
    cell->used_count = closed_source_number;
    cell->accumulated_row_size = _current_accumulated_row_size;
    cell->next = nullptr;

    _tail->next = cell;
    _tail = cell;
    _current_accumulated_row_size += chunk->num_rows();

    return Status::OK();
}

bool MultiCastLocalExchanger::can_pull_chunk(int32_t mcast_consumer_index) const {
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

StatusOr<vectorized::ChunkPtr> MultiCastLocalExchanger::pull_chunk(RuntimeState* state, int32_t mcast_consumer_index) {
    DCHECK(mcast_consumer_index < _consumer_number);

    std::unique_lock l(_mutex);
    DCHECK(_progress[mcast_consumer_index] != nullptr);
    Cell* cell = _progress[mcast_consumer_index];
    if (cell->next == nullptr) {
        if (_opened_sink_number == 0) return Status::EndOfFile("mcast_local_exchanger eof");
        return Status::OK();
    }
    cell = cell->next;
    VLOG_FILE << "MultiCastLocalExchanger: return chunk to " << mcast_consumer_index
              << ", row = " << cell->chunk->debug_row(0) << ", size = " << cell->chunk->num_rows();

    _progress[mcast_consumer_index] = cell;
    cell->used_count += 1;

    _update_progress(cell);
    return cell->chunk;
}

void MultiCastLocalExchanger::open_source_operator(int32_t mcast_consumer_index) {
    std::unique_lock l(_mutex);
    if (_opened_source_opcount[mcast_consumer_index] == 0) {
        _opened_source_number += 1;
    }
    _opened_source_opcount[mcast_consumer_index] += 1;
}
void MultiCastLocalExchanger::close_source_operator(int32_t mcast_consumer_index) {
    std::unique_lock l(_mutex);
    _opened_source_opcount[mcast_consumer_index] -= 1;
    if (_opened_source_opcount[mcast_consumer_index] == 0) {
        _opened_source_number--;
        _closer_consumer(mcast_consumer_index);
    }
}

void MultiCastLocalExchanger::open_sink_operator() {
    std::unique_lock l(_mutex);
    _opened_sink_number++;
}
void MultiCastLocalExchanger::close_sink_operator() {
    std::unique_lock l(_mutex);
    _opened_sink_number--;
}

void MultiCastLocalExchanger::_closer_consumer(int32_t mcast_consumer_index) {
    Cell* now = _progress[mcast_consumer_index];
    now = now->next;
    while (now) {
        now->used_count += 1;
        now = now->next;
    }
    _progress[mcast_consumer_index] = nullptr;
    _update_progress();
}

void MultiCastLocalExchanger::_update_progress(Cell* fast) {
    if (fast != nullptr) {
        _fast_accumulated_row_size = std::max(_fast_accumulated_row_size, fast->accumulated_row_size);
    } else {
        // update the fastest consumer.
        _fast_accumulated_row_size = 0;
        for (size_t i = 0; i < _consumer_number; i++) {
            Cell* c = _progress[i];
            if (c == nullptr) continue;
            _fast_accumulated_row_size = std::max(_fast_accumulated_row_size, c->accumulated_row_size);
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
Status MultiCastLocalExchangeSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    _exchanger->open_source_operator(_mcast_consumer_index);
    return Status::OK();
}

void MultiCastLocalExchangeSourceOperator::set_finishing(RuntimeState* state) {
    if (!_is_finished) {
        _is_finished = true;
        _exchanger->close_source_operator(_mcast_consumer_index);
    }
}

StatusOr<vectorized::ChunkPtr> MultiCastLocalExchangeSourceOperator::pull_chunk(RuntimeState* state) {
    auto ret = _exchanger->pull_chunk(state, _mcast_consumer_index);
    if (ret.status().is_end_of_file()) {
        set_finishing(state);
    }
    return ret;
}

bool MultiCastLocalExchangeSourceOperator::has_output() const {
    return _exchanger->can_pull_chunk(_mcast_consumer_index);
}

// ===== sink op =====

Status MultiCastLocalExchangeSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _exchanger->open_sink_operator();
    return Status::OK();
}

bool MultiCastLocalExchangeSinkOperator::need_input() const {
    return _exchanger->can_push_chunk();
}

void MultiCastLocalExchangeSinkOperator::set_finishing(RuntimeState* state) {
    if (!_is_finished) {
        _is_finished = true;
        _exchanger->close_sink_operator();
    }
}

StatusOr<vectorized::ChunkPtr> MultiCastLocalExchangeSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Should not pull_chunk in mcast_local_exchange_sink");
}

Status MultiCastLocalExchangeSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    return _exchanger->push_chunk(chunk, _driver_sequence);
}

} // namespace pipeline
} // namespace starrocks