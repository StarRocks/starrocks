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

#include <bthread/mutex.h>

#include <map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/utils.h"
#include "testutil/sync_point.h"
#include "util/bthreads/bthread_shared_mutex.h"
#include "util/countdown_latch.h"
#include "util/dynamic_cache.h"
#include "util/threadpool.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

class ThreadPoolToken;
class TxnStateHandler;
class TxnStateSubscriber;
class TxnStateCache;
using TxnStateDynamicCache = DynamicCache<int64_t, TxnStateHandler, bthread::Mutex>;
using TxnStateDynamicCachePtr = std::unique_ptr<TxnStateDynamicCache>;
using TxnStateDynamicCacheEntry = TxnStateDynamicCache::Entry;

struct TxnState {
    TTransactionStatus::type txn_status{TTransactionStatus::PREPARE};
    std::string reason;
};

inline std::ostream& operator<<(std::ostream& os, const TxnState& txn_state) {
    os << "txn_status: " << to_string(txn_state.txn_status) << ", reason: " << txn_state.reason;
    return os;
}

// Handle the transaction state. It's the value of the DynamicCache entry
// 1. transmit the txn state according to the new state pushed by FE or polled from FE
// 2. notify txn state subscribers whether it reaches the finished state
// 3. control the behaviour of txn state poll. The poll task starts to schedule when the
//    first subscriber comes(see subscribe()), and continue to schedule (see poll_state())
//    until the txn state reaches the finished state, and there is no subscriber
class TxnStateHandler {
public:
    ~TxnStateHandler();

    // update the txn state pushed by FE
    void push_state(TTransactionStatus::type new_status, const std::string& reason);
    // update the txn state polled by the cache. The returned value tell the caller to
    // continue or stop polling according the current result and the current txn state
    bool poll_state(const StatusOr<TxnState>& result);

    // Add a subscriber for the finished state. Handler will set 'trigger_poll'
    // to tell whether the caller should submit a txn state poll task
    void subscribe(bool& trigger_poll);
    // Remove a subscriber which has called subscribe() before
    void unsubscribe();
    // A subscriber calls this function to wait for the txn state to reach
    // a finished state or error happens.
    StatusOr<TxnState> wait_finished_state(const std::string& subscriber_name, int64_t timeout_us);
    int32_t num_waiting_finished_state();

    void set_txn_id(int64_t txn_id) { _txn_id.store(txn_id); }
    int64_t txn_id() { return _txn_id.load(); }
    TxnState txn_state();
    bool committed_status_from_fe();
    int32_t num_poll_failure();
    std::string debug_string();

    void stop();

    // For testing
    int32_t TEST_num_subscriber() { return _num_subscriber; }

private:
    void _transition_txn_state(TTransactionStatus::type new_status, const std::string& reason, bool from_fe);
    // Whether the current status indicate the load is finished
    bool _is_finished_txn_state();

    // lazy initialized
    std::atomic<int64_t> _txn_id{-1};
    bthread::Mutex _mutex;
    bthread::ConditionVariable _cv;
    TxnState _txn_state;
    // whether COMMITTED status is notified by FE. Only valid if txn status is COMMITTED.
    // If true, means publish timeout happens, and should notify subscribers. If false,
    // means its from poll should continue to wait.
    bool _committed_status_from_fe{false};
    int32_t _num_subscriber{0};
    int32_t _num_waiting_finished_state{0};
    int32_t _num_poll_failure{0};
    bool _stopped{false};
};

inline int32_t TxnStateHandler::num_waiting_finished_state() {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    return _num_waiting_finished_state;
}

inline TxnState TxnStateHandler::txn_state() {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    return _txn_state;
}

inline bool TxnStateHandler::committed_status_from_fe() {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    return _committed_status_from_fe;
}

inline int32_t TxnStateHandler::num_poll_failure() {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    return _num_poll_failure;
}

inline std::ostream& operator<<(std::ostream& os, TxnStateHandler& holder) {
    os << holder.debug_string();
    return os;
}

// A subscriber which will wait for the finished txn state. It holds a reference
// to the entry of DynamicCache so that the cache will not evict the txn state.
// The subscriber can call wait_finished_state() to wait for the finished state.
class TxnStateSubscriber {
public:
    TxnStateSubscriber(TxnStateDynamicCache* cache, TxnStateDynamicCacheEntry* entry, std::string name)
            : _cache(cache), _entry(entry), _name(std::move(name)){};

    ~TxnStateSubscriber() {
        _entry->value().unsubscribe();
        _cache->release(_entry);
    }

    const std::string& name() const { return _name; }
    StatusOr<TxnState> wait_finished_state(int64_t timeout_us);
    TxnState current_state();
    TxnStateDynamicCacheEntry* entry() { return _entry; }

private:
    TxnStateDynamicCache* _cache;
    TxnStateDynamicCacheEntry* _entry;
    std::string _name;
};
using TxnStateSubscriberPtr = std::unique_ptr<TxnStateSubscriber>;

struct TxnStatePollTask {
    int64_t txn_id;
    std::string db;
    std::string tbl;
    AuthInfo auth;
};

// Schedule and execute txn state poll tasks. The poller uses a single thread
// to schedule tasks according to their execution time, and submit them to the
// thread pool to run which will send rpc to FE to get txn state.
class TxnStatePoller {
public:
    TxnStatePoller(TxnStateCache* txn_state_cache, ThreadPoolToken* poll_token)
            : _txn_state_cache(txn_state_cache), _poll_token(poll_token) {}
    Status init();
    // submit a task which should be executed after the delay time
    void submit(const TxnStatePollTask& task, int64_t delay_ms);
    void stop();

    // For testing
    bool TEST_is_txn_pending(int64_t txn_id);
    StatusOr<int64_t> TEST_pending_execution_time(int64_t txn_id);
    bool TEST_is_scheduling();

private:
    void _schedule_func();
    void _schedule_poll_tasks(const std::vector<TxnStatePollTask>& poll_tasks);
    void _execute_poll(const TxnStatePollTask& task);

    TxnStateCache* _txn_state_cache;
    ThreadPoolToken* _poll_token;
    std::unique_ptr<std::thread> _schedule_thread;
    bthread::Mutex _mutex;
    bthread::ConditionVariable _cv;
    // txn ids to schedule, used to duplicate tasks for the same txn
    std::unordered_set<int64_t> _pending_txn_ids;
    // sorted execution time (milliseconds) -> task
    std::multimap<int64_t, TxnStatePollTask> _pending_tasks;
    bool _is_scheduling{false};
    bool _stopped{false};
};

// A cache for txn states. It can receive txn state in two ways: pushed by FE and polled from FE by itself.
// When the load finishes, FE will try to push the txn state to BE which is more efficient and realtime,
// but it does not always work because the push may fail for some reason, such as FE leader switch or crash.
// So BE will poll the txn state from FE periodically in a low frequency to detect those bad cases rather
// than just waiting until timeout. Apart from maintaining the txn state, the cache also provides a subscribe
// mechanism to notify the subscriber when the txn state reaches the finished state.
// The poll state task starts to schedule when the first subscriber comes, and continue to schedule when the
// last poll finishes. The schedule will end when the txn reaches the finished state or there is no subscriber.
class TxnStateCache {
public:
    TxnStateCache(size_t capacity, std::unique_ptr<ThreadPoolToken> poller_token);
    Status init();

    // update the txn state which is pushed by FE. It will create an entry
    // in the DynamicCache it the txn does not in the cache before.
    Status push_state(int64_t txn_id, TTransactionStatus::type status, const std::string& reason);

    // get the current state of txn_id. A TxnState will return if the txn is in the cache.
    // Status::NotFound will return if the txn is not in the cache. Other status will return
    // if error happens.
    StatusOr<TxnState> get_state(int64_t txn_id);

    // create a TxnStateSubscriber to subscribe the finished txn state. It will create an entry
    // in the DynamicCache it the txn does not in the cache before. The subscriber will hold a
    // reference to the entry, so the entry will not be evicted if any subscriber is using it.
    // The db/tbl/auth may be used to poll txn state.
    StatusOr<TxnStateSubscriberPtr> subscribe_state(int64_t txn_id, const std::string& subscriber_name,
                                                    const std::string& db, const std::string& tbl,
                                                    const AuthInfo& auth);

    void set_capacity(size_t new_capacity);
    int32_t size();

    void stop();

    // For testing
    std::vector<TxnStateDynamicCache*> get_cache_shards() {
        std::vector<TxnStateDynamicCache*> ret;
        for (auto& shard : _shards) {
            ret.push_back(shard.get());
        }
        return ret;
    }
    TxnStatePoller* txn_state_poller() { return _txn_state_poller.get(); }

private:
    static const int kNumShardBits = 5;
    static const int kNumShards = 1 << kNumShardBits;

    friend class TxnStatePoller;

    TxnStateDynamicCache* _get_txn_cache(int64_t txn_id);
    // if create_if_not_exist is true, must return non nullptr entry if status is ok.
    // if create_if_not_exist is false, return nullptr if txn is not in cache.
    // Return not ok status if error happens.
    StatusOr<TxnStateDynamicCacheEntry*> _get_txn_entry(TxnStateDynamicCache* cache, int64_t txn_id,
                                                        bool create_if_not_exist);
    void _notify_poll_result(const TxnStatePollTask& task, const StatusOr<TxnState>& result);
    void _txn_state_clean_func();

    size_t _capacity;
    std::unique_ptr<ThreadPoolToken> _poll_state_token;
    TxnStateDynamicCachePtr _shards[kNumShards];
    std::unique_ptr<TxnStatePoller> _txn_state_poller;
    // protect the cache from being accessed after it is stopped
    bthreads::BThreadSharedMutex _rw_mutex;
    std::atomic<bool> _stopped{false};

    std::unique_ptr<std::thread> _txn_state_clean_thread;
    // used to notify the clean thread to stop
    CountDownLatch _txn_state_clean_stop_latch{1};
};

inline TxnStateDynamicCache* TxnStateCache::_get_txn_cache(int64_t txn_id) {
    return _shards[txn_id & (kNumShards - 1)].get();
}
} // namespace starrocks
