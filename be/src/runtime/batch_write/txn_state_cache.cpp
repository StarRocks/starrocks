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

#include "agent/master_info.h"
#include "gen_cpp/FrontendService.h"
#include "runtime/batch_write/batch_write_util.h"
#include "runtime/client_cache.h"
#include "util/thread.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

TxnStateHandler::~TxnStateHandler() {
    TEST_SYNC_POINT_CALLBACK("TxnStateHandler::destruct", this);
    TRACE_BATCH_WRITE << "evict txn state, " << debug_string();
}

void TxnStateHandler::push_state(TTransactionStatus::type new_status, const std::string& reason) {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    if (_stopped) {
        return;
    }
    _transition_txn_state(new_status, reason, true);
    if (_is_finished_txn_state()) {
        _cv.notify_all();
    }
}

bool TxnStateHandler::poll_state(const StatusOr<TxnState>& result) {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    if (_stopped) {
        return false;
    }
    if (!result.status().ok()) {
        _num_poll_failure += 1;
        TRACE_BATCH_WRITE << "handler poll failure, txn_id: " << _txn_id << ", num_poll_failure: " << _num_poll_failure
                          << ", status: " << result.status();
        // fast fail if there is failure between FE and BE
        if (_num_poll_failure >= config::merge_commit_txn_state_poll_max_fail_times) {
            _transition_txn_state(TTransactionStatus::UNKNOWN,
                                  fmt::format("poll txn state failure exceeds max times {}, last error: {}",
                                              _num_poll_failure, result.status().to_string(false)),
                                  false);
        }
    } else {
        TRACE_BATCH_WRITE << "handler poll success, txn_id: " << _txn_id << ", " << result.value();
        _num_poll_failure = 0;
        _transition_txn_state(result.value().txn_status, result.value().reason, false);
    }
    // stop polling if reach the finished state or there is no subscriber
    if (_is_finished_txn_state()) {
        _cv.notify_all();
        return false;
    } else {
        return _num_subscriber > 0;
    }
}

void TxnStateHandler::subscribe(bool& trigger_poll) {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    _num_subscriber++;
    // trigger polling if this is the first subscriber and not in finished state
    trigger_poll = _num_subscriber == 1 && !_is_finished_txn_state();
}

void TxnStateHandler::unsubscribe() {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    _num_subscriber--;
}

StatusOr<TxnState> TxnStateHandler::wait_finished_state(const std::string& subscriber_name, int64_t timeout_us) {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    if (_is_finished_txn_state()) {
        return _txn_state;
    }
    if (_stopped) {
        return Status::ServiceUnavailable("Transaction state handler is stopped");
    }
    _num_waiting_finished_state++;
    DeferOp defer([&] { _num_waiting_finished_state--; });

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
        if (_is_finished_txn_state()) {
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
            "txn_id: {}, txn_status: {}, reason: {}, num_subscriber: {}, num_waiting_finished_state: {}, stopped: {}",
            _txn_id.load(), to_string(_txn_state.txn_status), _txn_state.reason, _num_subscriber,
            _num_waiting_finished_state, _stopped);
}

void TxnStateHandler::_transition_txn_state(TTransactionStatus::type new_status, const std::string& reason,
                                            bool from_fe) {
    TTransactionStatus::type old_status = _txn_state.txn_status;
    TRACE_BATCH_WRITE << "receive new txn state, txn_id: " << _txn_id
                      << ", current status: " << to_string(_txn_state.txn_status)
                      << ", current reason: " << _txn_state.reason << ", new status: " << new_status
                      << ", new reason: " << reason << ", from_fe: " << from_fe;
    // special case for COMMITTED status. If it's notified by FE, it means the load finished with
    // publish timeout, _is_finished_txn_state() should return true, and notify subscribers.
    if (new_status == TTransactionStatus::COMMITTED && from_fe) {
        _committed_status_from_fe = from_fe;
    }
    if (old_status == TTransactionStatus::VISIBLE || old_status == TTransactionStatus::ABORTED ||
        old_status == TTransactionStatus::UNKNOWN) {
        return;
    } else if (old_status == TTransactionStatus::PREPARED && new_status == TTransactionStatus::PREPARE) {
        return;
    } else if (old_status == TTransactionStatus::COMMITTED &&
               (new_status != TTransactionStatus::VISIBLE && new_status != TTransactionStatus::UNKNOWN)) {
        return;
    }
    _txn_state.txn_status = new_status;
    _txn_state.reason = reason;
}

bool TxnStateHandler::_is_finished_txn_state() {
    // The load can be successful or failed. When successful, the transaction status is VISIBLE.
    // When failed, the transaction status can be COMMITTED, ABORTED, or UNKNOWN. COMMITTED is a
    // special status
    switch (_txn_state.txn_status) {
    case TTransactionStatus::VISIBLE:
    case TTransactionStatus::ABORTED:
    case TTransactionStatus::UNKNOWN:
        return true;
    case TTransactionStatus::COMMITTED:
        return _committed_status_from_fe;
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

inline int64_t get_current_ms() {
    int64_t current_ts = MonotonicMillis();
    TEST_SYNC_POINT_CALLBACK("TxnStatePoller::get_current_ms", &current_ts);
    return current_ts;
}

Status TxnStatePoller::init() {
    _schedule_thread = std::make_unique<std::thread>([this] { _schedule_func(); });
    Thread::set_thread_name(*_schedule_thread.get(), "txn_state_sche");
    return Status::OK();
}

void TxnStatePoller::submit(const TxnStatePollTask& task, int64_t delay_ms) {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    if (_stopped) {
        return;
    }
    if (_pending_txn_ids.find(task.txn_id) != _pending_txn_ids.end()) {
        return;
    }
    int64_t execute_time = get_current_ms() + delay_ms;
    _pending_txn_ids.emplace(task.txn_id);
    _pending_tasks.emplace(std::make_pair(execute_time, task));
    _cv.notify_all();
    TRACE_BATCH_WRITE << "submit poll task, txn_id: " << task.txn_id << ", db: " << task.db << ", tbl: " << task.tbl
                      << ", delay_ms: " << delay_ms;
}

void TxnStatePoller::stop() {
    {
        std::unique_lock<bthread::Mutex> lock(_mutex);
        if (_stopped) {
            return;
        }
        _stopped = true;
        _cv.notify_all();
    }
    if (_schedule_thread && _schedule_thread->joinable()) {
        _schedule_thread->join();
    }
}

void TxnStatePoller::_schedule_func() {
    std::vector<TxnStatePollTask> poll_tasks;
    std::unique_lock<bthread::Mutex> lock(_mutex);
    _is_scheduling = true;
    while (!_stopped) {
        int64_t current_ts = get_current_ms();
        auto it = _pending_tasks.begin();
        while (it != _pending_tasks.end()) {
            if (it->first <= current_ts) {
                _pending_txn_ids.erase(it->second.txn_id);
                poll_tasks.emplace_back(it->second);
                it = _pending_tasks.erase(it);
            } else {
                break;
            }
        }
        if (!poll_tasks.empty()) {
            lock.unlock();
            _schedule_poll_tasks(poll_tasks);
            poll_tasks.clear();
            lock.lock();
        }
        if (_stopped) {
            break;
        }
        if (_pending_tasks.empty()) {
            _cv.wait(lock);
        } else {
            // at least wait 50 ms to avoid busy loop
            int64_t wait_time_ms = std::max((int64_t)50, _pending_tasks.begin()->first - get_current_ms());
            _cv.wait_for(lock, wait_time_ms * 1000);
        }
    }
    _is_scheduling = false;
}

void TxnStatePoller::_schedule_poll_tasks(const std::vector<TxnStatePollTask>& poll_tasks) {
    for (const auto& task : poll_tasks) {
        // check current state of the txn, if it's not in cache or is final, skip poll
        auto current_state = _txn_state_cache->get_state(task.txn_id);
        if (!current_state.ok()) {
            TRACE_BATCH_WRITE << "skip poll task because fail to get txn state, txn_id: " << task.txn_id
                              << ", db: " << task.db << ", tbl: " << task.tbl << ", error: " << current_state.status();
            continue;
        }
        TTransactionStatus::type txn_status = current_state.value().txn_status;
        if (txn_status == TTransactionStatus::VISIBLE || txn_status == TTransactionStatus::ABORTED ||
            txn_status == TTransactionStatus::UNKNOWN) {
            TRACE_BATCH_WRITE << "skip poll task because txn state is final, txn_id: " << task.txn_id
                              << ", db: " << task.db << ", tbl: " << task.tbl << ", state status: " << txn_status
                              << ", reason: " << current_state.value().reason;
            continue;
        }
        Status status = _poll_token->submit_func([this, task] { _execute_poll(task); }, ThreadPool::HIGH_PRIORITY);
        if (!status.ok()) {
            _txn_state_cache->_notify_poll_result(
                    task, Status::InternalError("failed to submit poll txn state task, error: " + status.to_string()));
        } else {
            TRACE_BATCH_WRITE << "schedule poll task, txn_id: " << task.txn_id << ", db: " << task.db
                              << ", tbl: " << task.tbl;
        }
    }
}

void TxnStatePoller::_execute_poll(const TxnStatePollTask& task) {
    int64_t start_ts = MonotonicMicros();
    TGetLoadTxnStatusRequest request;
    request.__set_db(task.db);
    request.__set_tbl(task.tbl);
    request.__set_txnId(task.txn_id);
    set_request_auth(&request, task.auth);
    TGetLoadTxnStatusResult response;
    Status status;
#ifndef BE_TEST
    TNetworkAddress master_addr = get_master_address();
    status = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &response](FrontendServiceConnection& client) { client->getLoadTxnStatus(response, request); });
#else
    TEST_SYNC_POINT_CALLBACK("TxnStatePoller::_execute_poll::request", &request);
    TEST_SYNC_POINT_CALLBACK("TxnStatePoller::_execute_poll::status", &status);
    TEST_SYNC_POINT_CALLBACK("TxnStatePoller::_execute_poll::response", &response);
#endif
    TRACE_BATCH_WRITE << "execute poll task, txn_id: " << task.txn_id << ", db: " << task.db << ", tbl: " << task.tbl
                      << ", cost: " << (MonotonicMicros() - start_ts) << " us, rpc status: " << status
                      << ", response: " << response;
    if (status.ok()) {
        _txn_state_cache->_notify_poll_result(task, TxnState{response.status, response.reason});
    } else {
        _txn_state_cache->_notify_poll_result(
                task, Status::InternalError("poll txn state failed, error: " + status.to_string()));
    }
}

bool TxnStatePoller::TEST_is_txn_pending(int64_t txn_id) {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    return _pending_txn_ids.find(txn_id) != _pending_txn_ids.end();
}

StatusOr<int64_t> TxnStatePoller::TEST_pending_execution_time(int64_t txn_id) {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    auto it = _pending_tasks.begin();
    while (it != _pending_tasks.end()) {
        if (it->second.txn_id == txn_id) {
            return it->first;
        }
        ++it;
    }
    return Status::NotFound("no task found");
}

bool TxnStatePoller::TEST_is_scheduling() {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    return _is_scheduling;
}

TxnStateCache::TxnStateCache(size_t capacity, std::unique_ptr<ThreadPoolToken> poller_token)
        : _capacity(capacity), _poll_state_token(std::move(poller_token)) {
    size_t capacity_per_shard = (_capacity + (kNumShards - 1)) / kNumShards;
    for (int32_t i = 0; i < kNumShards; i++) {
        _shards[i] = std::make_unique<TxnStateDynamicCache>(capacity_per_shard);
    }
}

Status TxnStateCache::init() {
    _txn_state_poller = std::make_unique<TxnStatePoller>(this, _poll_state_token.get());
    RETURN_IF_ERROR(_txn_state_poller->init());
    _txn_state_clean_thread = std::make_unique<std::thread>([this] { _txn_state_clean_func(); });
    Thread::set_thread_name(*_txn_state_clean_thread.get(), "txn_state_clean");
    return Status::OK();
}

Status TxnStateCache::push_state(int64_t txn_id, TTransactionStatus::type status, const std::string& reason) {
    auto cache = _get_txn_cache(txn_id);
    ASSIGN_OR_RETURN(auto entry, _get_txn_entry(cache, txn_id, true));
    DCHECK(entry != nullptr);
    entry->value().push_state(status, reason);
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

StatusOr<TxnStateSubscriberPtr> TxnStateCache::subscribe_state(int64_t txn_id, const std::string& subscriber_name,
                                                               const std::string& db, const std::string& tbl,
                                                               const AuthInfo& auth) {
    auto cache = _get_txn_cache(txn_id);
    ASSIGN_OR_RETURN(auto entry, _get_txn_entry(cache, txn_id, true));
    DCHECK(entry != nullptr);
    bool trigger_poll = false;
    entry->value().subscribe(trigger_poll);
    TRACE_BATCH_WRITE << "create subscriber, txn_id: " << txn_id << ", name: " << subscriber_name << ", db: " << db
                      << ", tbl: " << tbl << ", trigger_poll: " << trigger_poll;
    if (trigger_poll) {
        _txn_state_poller->submit({txn_id, db, tbl, auth}, config::merge_commit_txn_state_poll_interval_ms);
    }
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
    if (_txn_state_poller) {
        _txn_state_poller->stop();
    }
    _poll_state_token->shutdown();
    _txn_state_clean_stop_latch.count_down();
    if (_txn_state_clean_thread && _txn_state_clean_thread->joinable()) {
        _txn_state_clean_thread->join();
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
    TxnStateDynamicCacheEntry* entry = nullptr;
    if (create_if_not_exist) {
        entry = cache->get_or_create(txn_id, 1);
        DCHECK(entry != nullptr);
        // initialize txn_id
        entry->value().set_txn_id(txn_id);
    } else {
        entry = cache->get(txn_id);
    }
    if (entry) {
        // expire time does not need very accurate and monotonic, so do not protect it from concurrent update
        entry->update_expire_time(get_current_ms() + config::merge_commit_txn_state_expire_time_sec * 1000);
    }
    return entry;
}

void TxnStateCache::_notify_poll_result(const TxnStatePollTask& task, const StatusOr<TxnState>& result) {
    auto cache = _get_txn_cache(task.txn_id);
    auto entry_st = _get_txn_entry(cache, task.txn_id, false);
    if (!entry_st.ok() || entry_st.value() == nullptr) {
        TRACE_BATCH_WRITE << "skip notify poll result, txn_id: " << task.txn_id << ", db: " << task.db
                          << ", tbl: " << task.tbl << ", entry status: "
                          << (entry_st.ok() ? Status::NotFound("not in cache") : entry_st.status());
        return;
    }
    auto entry = entry_st.value();
    DeferOp defer([&] { cache->release(entry); });
    bool continue_poll = entry->value().poll_state(result);
    TRACE_BATCH_WRITE << "notify poll result, txn_id: " << task.txn_id << ", db: " << task.db << ", tbl: " << task.tbl
                      << ", continue_poll: " << continue_poll;
    if (continue_poll) {
        _txn_state_poller->submit(task, config::merge_commit_txn_state_poll_interval_ms);
    }
}

void TxnStateCache::_txn_state_clean_func() {
    while (!_stopped) {
        int32_t clean_interval_sec = config::merge_commit_txn_state_clean_interval_sec;
        _txn_state_clean_stop_latch.wait_for(std::chrono::seconds(clean_interval_sec));
        if (_stopped) {
            break;
        }
        for (auto& cache : _shards) {
            cache->clear_expired();
        }
    }
}

} // namespace starrocks