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

#include "runtime/batch_write/txn_state_cache.h"

#include <utility>

#include "runtime/batch_write/batch_write_util.h"

namespace starrocks {

TxnStateHandler::~TxnStateHandler() {
    TEST_SYNC_POINT_CALLBACK("TxnStateHandler::destruct", this);
    TRACE_BATCH_WRITE << "evict txn state, " << debug_string();
}

void TxnStateHandler::update_state(TTransactionStatus::type new_status, const std::string& reason) {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    if (_stopped) {
        return;
    }

    TTransactionStatus::type old_status = _txn_state.txn_status;
    // actually FE will guarantee to update transaction status correctly,
    // but here still check the transition to avoid unexpected status change
    if (old_status == TTransactionStatus::VISIBLE || old_status == TTransactionStatus::ABORTED ||
        old_status == TTransactionStatus::UNKNOWN) {
        return;
    } else if (old_status == TTransactionStatus::PREPARED && new_status == TTransactionStatus::PREPARE) {
        return;
    } else if (old_status == TTransactionStatus::COMMITTED && new_status != TTransactionStatus::VISIBLE) {
        return;
    }
    TRACE_BATCH_WRITE << "update txn state, txn_id: " << _txn_id << ", old status: " << to_string(_txn_state.txn_status)
                      << ", reason: " << _txn_state.reason << ", new status: " << new_status << ", reason: " << reason;
    _txn_state.txn_status = new_status;
    _txn_state.reason = reason;
    if (_is_finished_txn_status()) {
        _cv.notify_all();
    }
}

StatusOr<TxnState> TxnStateHandler::wait_finished_state(const std::string& subscriber_name, int64_t timeout_us) {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    if (_is_finished_txn_status()) {
        return _txn_state;
    }
    if (_stopped) {
        return Status::ServiceUnavailable("Transaction state handler is stopped");
    }
    _num_waiting_subscriber++;
    DeferOp defer([&] { _num_waiting_subscriber--; });

    int64_t left_timeout_us = timeout_us;
    while (left_timeout_us > 0) {
        TRACE_BATCH_WRITE << "start to wait state, subscriber name: " << subscriber_name << ", txn_id: " << _txn_id
                          << ", timeout_us: " << left_timeout_us;
        auto start_us = MonotonicMicros();
        int ret = _cv.wait_for(lock, left_timeout_us);
        int64_t elapsed_us = MonotonicMicros() - start_us;
        TRACE_BATCH_WRITE << "finish to wait state, subscriber name: " << subscriber_name << ", txn_id: " << _txn_id
                          << ", elapsed: " << elapsed_us << " us, txn_status: " << to_string(_txn_state.txn_status)
                          << ", reason: " << _txn_state.reason << ", stopped: " << _stopped;
        if (_is_finished_txn_status()) {
            return _txn_state;
        } else if (_stopped) {
            return Status::ServiceUnavailable("Transaction state handler is stopped");
        } else if (ret == ETIMEDOUT) {
            break;
        }
        left_timeout_us = std::max((int64_t)0, left_timeout_us - elapsed_us);
    }
    return Status::TimedOut(fmt::format("Wait txn state timeout {} us", timeout_us));
}

void TxnStateHandler::stop() {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    if (_stopped) {
        return;
    }
    _stopped = true;
    _cv.notify_all();
}

std::string TxnStateHandler::debug_string() {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    return fmt::format(
            "txn_id: {}, txn_status: {}, reason: {}, num_subscriber: {}, num_waiting_subscriber: {}, stopped: {}",
            _txn_id.load(), to_string(_txn_state.txn_status), _txn_state.reason, _num_subscriber,
            _num_waiting_subscriber, _stopped);
}

bool TxnStateHandler::_is_finished_txn_status() {
    // The load can be successful or failed. When successful, the transaction status can be VISIBLE
    // or COMMITTED. COMMITTED means the transaction published timeout. When failed, the transaction
    // status can be ABORTED or UNKNOWN. UNKNOWN indicates the transaction may be cleaned up by the
    // FE because of expiration or reaching the max number of transactions to keep.
    switch (_txn_state.txn_status) {
    case TTransactionStatus::VISIBLE:
    case TTransactionStatus::ABORTED:
    case TTransactionStatus::UNKNOWN:
    case TTransactionStatus::COMMITTED:
        return true;
    default:
        return false;
    }
}

StatusOr<TxnState> TxnStateSubscriber::wait_finished_state(int64_t timeout_us) {
    return _entry->value().wait_finished_state(_name, timeout_us);
}

TxnState TxnStateSubscriber::current_state() {
    return _entry->value().txn_state();
}

TxnStateCache::TxnStateCache(size_t capacity) : _capacity(capacity) {
    size_t capacity_per_shard = (_capacity + (kNumShards - 1)) / kNumShards;
    for (int32_t i = 0; i < kNumShards; i++) {
        _shards[i] = std::make_unique<TxnStateDynamicCache>(capacity_per_shard);
    }
}

Status TxnStateCache::update_state(int64_t txn_id, TTransactionStatus::type status, const std::string& reason) {
    auto cache = _get_txn_cache(txn_id);
    ASSIGN_OR_RETURN(auto entry, _get_txn_entry(cache, txn_id, true));
    entry->value().update_state(status, reason);
    cache->release(entry);
    return Status::OK();
}

StatusOr<TxnState> TxnStateCache::get_state(int64_t txn_id) {
    auto cache = _get_txn_cache(txn_id);
    ASSIGN_OR_RETURN(auto entry, _get_txn_entry(cache, txn_id, false));
    if (entry == nullptr) {
        return Status::NotFound("Transaction state not found");
    }
    auto txn_state = entry->value().txn_state();
    cache->release(entry);
    return txn_state;
}

StatusOr<TxnStateSubscriberPtr> TxnStateCache::subscribe_state(int64_t txn_id, const std::string& subscriber_name) {
    auto cache = _get_txn_cache(txn_id);
    ASSIGN_OR_RETURN(auto entry, _get_txn_entry(cache, txn_id, true));
    return std::make_unique<TxnStateSubscriber>(cache, entry, subscriber_name);
}

void TxnStateCache::set_capacity(size_t new_capacity) {
    std::unique_lock<bthreads::BThreadSharedMutex> lock;
    if (_stopped) {
        return;
    }
    const size_t capacity_per_shard = (new_capacity + (kNumShards - 1)) / kNumShards;
    for (auto& _shard : _shards) {
        _shard->set_capacity(capacity_per_shard);
    }
    _capacity = new_capacity;
}

void TxnStateCache::stop() {
    {
        std::unique_lock<bthreads::BThreadSharedMutex> lock;
        if (_stopped) {
            return;
        }
        _stopped = true;
    }
    for (auto& cache : _shards) {
        auto entries = cache->get_all_entries();
        for (auto entry : entries) {
            entry->value().stop();
            cache->release(entry);
        }
    }
}

int32_t TxnStateCache::size() {
    int32_t size = 0;
    for (auto& cache : _shards) {
        size += cache->size();
    }
    return size;
}

StatusOr<TxnStateDynamicCacheEntry*> TxnStateCache::_get_txn_entry(TxnStateDynamicCache* cache, int64_t txn_id,
                                                                   bool create_if_not_exist) {
    // use lock to avoid creating new entry after stopped
    std::shared_lock<bthreads::BThreadSharedMutex> lock;
    if (_stopped) {
        return Status::ServiceUnavailable("Transaction state cache is stopped");
    }
    TxnStateDynamicCacheEntry* entry = create_if_not_exist ? cache->get_or_create(txn_id, 1) : cache->get(txn_id);
    if (create_if_not_exist) {
        DCHECK(entry != nullptr);
        // initialize txn_id
        entry->value().set_txn_id(txn_id);
    }
    return entry;
}

} // namespace starrocks