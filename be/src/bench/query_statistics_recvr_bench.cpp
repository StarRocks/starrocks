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

#include <sched.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include <benchmark/benchmark.h>

#include "absl/container/inlined_vector.h"
#include "base/phmap/phmap.h"
#include "gen_cpp/data.pb.h"
#include "runtime/query_statistics.h"

namespace starrocks {
namespace {

// 1. Legacy naive spinlock (from main before optimize_spinlock_arm64).
class LegacySpinLock {
public:
    void lock() {
        if (!try_lock()) {
            slow_acquire();
        }
    }

    void unlock() {
        __sync_synchronize();
        _locked = false;
    }

    inline bool try_lock() { return __sync_bool_compare_and_swap(&_locked, false, true); }

private:
    __attribute__((noinline)) void slow_acquire() {
        while (true) {
            if (try_lock()) {
                return;
            }
            for (int i = 0; i < 70; ++i) {
#if (defined(__i386) || defined(__x86_64__))
                asm volatile("pause\n" : : : "memory");
#elif defined(__aarch64__)
                asm volatile("isb\n" ::: "memory");
#endif
            }
            if (try_lock()) {
                return;
            }
            sched_yield();
        }
    }

    bool _locked{false};
};

// 2. Persistent exponential-backoff TTAS SpinLock (from optimize_spinlock_arm64).
class BackoffSpinLock {
public:
    void lock() {
        if (!try_lock()) {
            slow_acquire();
        }
    }

    void unlock() { _locked.store(false, std::memory_order_release); }

    inline bool try_lock() {
        return !_locked.load(std::memory_order_relaxed) &&
               !_locked.exchange(true, std::memory_order_acquire);
    }

private:
    static inline void cpu_relax() {
#if (defined(__i386) || defined(__x86_64__))
        asm volatile("pause\n" : : : "memory");
#elif defined(__aarch64__)
        asm volatile("isb\n" ::: "memory");
#endif
    }

    __attribute__((noinline)) void slow_acquire() {
        int step = 1;
        while (true) {
            for (int probe = 0; probe < 8; ++probe) {
                for (int i = 0; i < step; ++i) {
                    cpu_relax();
                }
                if (try_lock()) {
                    return;
                }
                if (step < 64) {
                    step <<= 1;
                }
            }
            sched_yield();
            if (try_lock()) {
                return;
            }
        }
    }

    std::atomic<bool> _locked{false};
};

inline void bench_atomic_max(std::atomic_int64_t* target, int64_t value) {
    int64_t prev = target->load(std::memory_order_relaxed);
    while (prev < value &&
           !target->compare_exchange_weak(prev, value, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

// Legacy QueryStatistics implementation (pre-optimization):
// - std::unordered_map<..., std::shared_ptr<...>> for _stats_items and _exec_stats_items
// - swap() in merge() that destroys map buckets and shared_ptrs on every aggregate()
// - compare_exchange_strong(x, 0) scalar draining
template <typename LockType = LegacySpinLock>
class LegacyQueryStatisticsTpl {
public:
    void merge_pb(const PQueryStatistics& statistics) {
        if (statistics.has_scan_rows()) scan_rows += statistics.scan_rows();
        if (statistics.has_scan_bytes()) scan_bytes += statistics.scan_bytes();
        if (statistics.has_cpu_cost_ns()) cpu_ns += statistics.cpu_cost_ns();
        if (statistics.has_spill_bytes()) spill_bytes += statistics.spill_bytes();
        if (statistics.has_read_local_cnt()) read_local_cnt += statistics.read_local_cnt();
        if (statistics.has_read_remote_cnt()) read_remote_cnt += statistics.read_remote_cnt();
        if (statistics.has_mem_cost_bytes()) {
            mem_cost_bytes = std::max<int64_t>(mem_cost_bytes, statistics.mem_cost_bytes());
        }
        if (statistics.has_transmitted_bytes()) transmitted_bytes += statistics.transmitted_bytes();
        {
            std::lock_guard<LockType> l(_lock);
            for (int i = 0; i < statistics.stats_items_size(); ++i) {
                const auto& item = statistics.stats_items(i);
                update_stats_item(item.table_id(), item.scan_rows(), item.scan_bytes());
            }
            for (int i = 0; i < statistics.node_exec_stats_items_size(); ++i) {
                const auto& item = statistics.node_exec_stats_items(i);
                update_exec_stats_item(item.node_id(), item.push_rows(), item.pull_rows(), item.pred_filter_rows(),
                                       item.index_filter_rows(), item.rf_filter_rows());
            }
        }
    }

    void merge(int sender_id, LegacyQueryStatisticsTpl& other) {
        (void)sender_id;
        int64_t rows = other.scan_rows.load();
        if (other.scan_rows.compare_exchange_strong(rows, 0)) this->scan_rows += rows;
        int64_t bytes = other.scan_bytes.load();
        if (other.scan_bytes.compare_exchange_strong(bytes, 0)) this->scan_bytes += bytes;
        int64_t cpu = other.cpu_ns.load();
        if (other.cpu_ns.compare_exchange_strong(cpu, 0)) this->cpu_ns += cpu;
        int64_t mem = other.mem_cost_bytes.load();
        this->mem_cost_bytes = std::max<int64_t>(this->mem_cost_bytes, mem);
        int64_t spill = other.spill_bytes.load();
        if (other.spill_bytes.compare_exchange_strong(spill, 0)) this->spill_bytes += spill;
        int64_t local_cnt = other.read_local_cnt.load();
        if (other.read_local_cnt.compare_exchange_strong(local_cnt, 0)) this->read_local_cnt += local_cnt;
        int64_t remote_cnt = other.read_remote_cnt.load();
        if (other.read_remote_cnt.compare_exchange_strong(remote_cnt, 0)) this->read_remote_cnt += remote_cnt;
        int64_t trans = other.transmitted_bytes.load();
        if (other.transmitted_bytes.compare_exchange_strong(trans, 0)) this->transmitted_bytes += trans;

        std::unordered_map<int64_t, std::shared_ptr<ScanStats>> other_stats_item;
        std::unordered_map<uint32_t, std::shared_ptr<NodeExecStats>> other_exec_stats_items;
        {
            std::lock_guard<LockType> l(other._lock);
            other_stats_item.swap(other._stats_items);
            other_exec_stats_items.swap(other._exec_stats_items);
        }
        std::lock_guard<LockType> l(_lock);
        for (const auto& [table_id, stats_item] : other_stats_item) {
            update_stats_item(table_id, stats_item->scan_rows, stats_item->scan_bytes);
        }
        for (const auto& [node_id, exec_stats_item] : other_exec_stats_items) {
            update_exec_stats_item(node_id, exec_stats_item->push_rows, exec_stats_item->pull_rows,
                                   exec_stats_item->pred_filter_rows, exec_stats_item->index_filter_rows,
                                   exec_stats_item->rf_filter_rows);
        }
    }

private:
    struct ScanStats {
        ScanStats(int64_t r, int64_t b) : scan_rows(r), scan_bytes(b) {}
        int64_t scan_rows = 0;
        int64_t scan_bytes = 0;
    };
    struct NodeExecStats {
        std::atomic_int64_t push_rows;
        std::atomic_int64_t pull_rows;
        std::atomic_int64_t pred_filter_rows;
        std::atomic_int64_t index_filter_rows;
        std::atomic_int64_t rf_filter_rows;
        NodeExecStats(int64_t push, int64_t pull, int64_t pred, int64_t idx, int64_t rf)
                : push_rows(push),
                  pull_rows(pull),
                  pred_filter_rows(pred),
                  index_filter_rows(idx),
                  rf_filter_rows(rf) {}
    };

    void update_stats_item(int64_t table_id, int64_t rows, int64_t bytes) {
        if (table_id > 0 && (rows > 0 || bytes > 0)) {
            auto iter = _stats_items.find(table_id);
            if (iter == _stats_items.end()) {
                _stats_items.insert({table_id, std::make_shared<ScanStats>(rows, bytes)});
            } else {
                iter->second->scan_rows += rows;
                iter->second->scan_bytes += bytes;
            }
        }
    }

    void update_exec_stats_item(uint32_t node_id, int64_t push, int64_t pull, int64_t pred, int64_t idx, int64_t rf) {
        auto iter = _exec_stats_items.find(node_id);
        if (iter == _exec_stats_items.end()) {
            _exec_stats_items.insert({node_id, std::make_shared<NodeExecStats>(push, pull, pred, idx, rf)});
        } else {
            iter->second->push_rows += push;
            iter->second->pull_rows += pull;
            iter->second->pred_filter_rows += pred;
            iter->second->index_filter_rows += idx;
            iter->second->rf_filter_rows += rf;
        }
    }

    std::atomic_int64_t scan_rows{0};
    std::atomic_int64_t scan_bytes{0};
    std::atomic_int64_t cpu_ns{0};
    std::atomic_int64_t mem_cost_bytes{0};
    std::atomic_int64_t spill_bytes{0};
    std::atomic_int64_t read_local_cnt{0};
    std::atomic_int64_t read_remote_cnt{0};
    std::atomic_int64_t transmitted_bytes{0};
    LockType _lock;
    std::unordered_map<int64_t, std::shared_ptr<ScanStats>> _stats_items;
    std::unordered_map<uint32_t, std::shared_ptr<NodeExecStats>> _exec_stats_items;
};

using LegacyQueryStatistics = LegacyQueryStatisticsTpl<LegacySpinLock>;

// Candidate 1: Production dd5c030 baseline (std::map + outer SpinLock held across lookup & merge_pb)
template <typename LockType>
class LegacyMapRecvrTpl {
public:
    using StatsType = LegacyQueryStatisticsTpl<LockType>;

    ~LegacyMapRecvrTpl() {
        for (auto& pair : _query_statistics) {
            delete pair.second;
        }
        _query_statistics.clear();
    }

    void insert(const PQueryStatistics& statistics, int sender_id) {
        std::lock_guard<LockType> l(_lock);
        StatsType* qs = nullptr;
        auto iter = _query_statistics.find(sender_id);
        if (iter == _query_statistics.end()) {
            qs = new StatsType;
            _query_statistics[sender_id] = qs;
        } else {
            qs = iter->second;
        }
        qs->merge_pb(statistics);
    }

    void aggregate(StatsType* statistics) {
        std::lock_guard<LockType> l(_lock);
        for (auto& pair : _query_statistics) {
            statistics->merge(pair.first, *pair.second);
        }
    }

private:
    std::map<int, StatsType*> _query_statistics;
    LockType _lock;
};

using LegacyMapSpinLockRecvr = LegacyMapRecvrTpl<LegacySpinLock>;
using LegacyMapBackoffLockRecvr = LegacyMapRecvrTpl<BackoffSpinLock>;

// Candidate 2: PR #73796 baseline (phmap::parallel_flat_hash_map<..., 4, std::shared_mutex>)
template <typename LockType>
class PhmapSharedMutexRecvrTpl {
public:
    using StatsType = LegacyQueryStatisticsTpl<LockType>;

    ~PhmapSharedMutexRecvrTpl() {
        _query_statistics.for_each_m([](auto& pair) { delete pair.second; });
        _query_statistics.clear();
    }

    void insert(const PQueryStatistics& statistics, int sender_id) {
        StatsType* qs = nullptr;
        _query_statistics.lazy_emplace_l(
                sender_id, [&](StatsType* const& existing) { qs = existing; },
                [&](const auto& ctor) {
                    qs = new StatsType;
                    ctor(sender_id, qs);
                });
        qs->merge_pb(statistics);
    }

    void aggregate(StatsType* statistics) {
        _query_statistics.for_each([&statistics](const auto& pair) { statistics->merge(pair.first, *pair.second); });
    }

private:
    phmap::parallel_flat_hash_map<int, StatsType*, phmap::Hash<int>, phmap::EqualTo<int>,
                                  phmap::Allocator<phmap::Pair<const int, StatsType*>>, 4,
                                  std::shared_mutex>
            _query_statistics;
};

using PhmapSharedMutexRecvr = PhmapSharedMutexRecvrTpl<LegacySpinLock>;
using PhmapSharedMutexBackoffLockRecvr = PhmapSharedMutexRecvrTpl<BackoffSpinLock>;

// Parameterized FastQueryStatistics (inline phmap::flat_hash_map, in-place draining, .exchange(0))
template <typename LockType>
class FastQueryStatistics {
public:
    void update_max_mem_costs(int64_t bytes) { bench_atomic_max(&mem_cost_bytes, bytes); }

    void merge_pb(const PQueryStatistics& statistics) {
        if (statistics.has_scan_rows()) scan_rows.fetch_add(statistics.scan_rows(), std::memory_order_relaxed);
        if (statistics.has_scan_bytes()) scan_bytes.fetch_add(statistics.scan_bytes(), std::memory_order_relaxed);
        if (statistics.has_cpu_cost_ns()) cpu_ns.fetch_add(statistics.cpu_cost_ns(), std::memory_order_relaxed);
        if (statistics.has_spill_bytes()) spill_bytes.fetch_add(statistics.spill_bytes(), std::memory_order_relaxed);
        if (statistics.has_read_local_cnt())
            read_local_cnt.fetch_add(statistics.read_local_cnt(), std::memory_order_relaxed);
        if (statistics.has_read_remote_cnt())
            read_remote_cnt.fetch_add(statistics.read_remote_cnt(), std::memory_order_relaxed);
        if (statistics.has_mem_cost_bytes()) bench_atomic_max(&mem_cost_bytes, statistics.mem_cost_bytes());
        if (statistics.has_transmitted_bytes())
            transmitted_bytes.fetch_add(statistics.transmitted_bytes(), std::memory_order_relaxed);

        if (statistics.stats_items_size() > 0 || statistics.node_exec_stats_items_size() > 0) {
            std::lock_guard<LockType> l(_lock);
            for (int i = 0; i < statistics.stats_items_size(); ++i) {
                const auto& item = statistics.stats_items(i);
                update_stats_item(item.table_id(), item.scan_rows(), item.scan_bytes());
            }
            for (int i = 0; i < statistics.node_exec_stats_items_size(); ++i) {
                const auto& item = statistics.node_exec_stats_items(i);
                update_exec_stats_item(item.node_id(), item.push_rows(), item.pull_rows(), item.pred_filter_rows(),
                                       item.index_filter_rows(), item.rf_filter_rows());
            }
        }
    }

    void merge(int sender_id, FastQueryStatistics& other) {
        (void)sender_id;
        if (this == &other) return;
        if (int64_t v = other.scan_rows.exchange(0, std::memory_order_relaxed); v != 0)
            scan_rows.fetch_add(v, std::memory_order_relaxed);
        if (int64_t v = other.scan_bytes.exchange(0, std::memory_order_relaxed); v != 0)
            scan_bytes.fetch_add(v, std::memory_order_relaxed);
        if (int64_t v = other.cpu_ns.exchange(0, std::memory_order_relaxed); v != 0)
            cpu_ns.fetch_add(v, std::memory_order_relaxed);
        bench_atomic_max(&mem_cost_bytes, other.mem_cost_bytes.load(std::memory_order_relaxed));
        if (int64_t v = other.spill_bytes.exchange(0, std::memory_order_relaxed); v != 0)
            spill_bytes.fetch_add(v, std::memory_order_relaxed);
        if (int64_t v = other.read_local_cnt.exchange(0, std::memory_order_relaxed); v != 0)
            read_local_cnt.fetch_add(v, std::memory_order_relaxed);
        if (int64_t v = other.read_remote_cnt.exchange(0, std::memory_order_relaxed); v != 0)
            read_remote_cnt.fetch_add(v, std::memory_order_relaxed);
        if (int64_t v = other.transmitted_bytes.exchange(0, std::memory_order_relaxed); v != 0)
            transmitted_bytes.fetch_add(v, std::memory_order_relaxed);

        struct DrainedScan {
            int64_t table_id;
            ScanStats stats;
        };
        struct DrainedExec {
            uint32_t node_id;
            NodeExecStats stats;
        };
        absl::InlinedVector<DrainedScan, 8> drained_scans;
        absl::InlinedVector<DrainedExec, 32> drained_execs;
        {
            std::lock_guard<LockType> l(other._lock);
            if (other._has_map_updates) {
                for (auto& [tid, s] : other._stats_items) {
                    if (!s.empty()) {
                        drained_scans.push_back({tid, s});
                        s = ScanStats{};
                    }
                }
                for (auto& [nid, e] : other._exec_stats_items) {
                    if (e.active) {
                        drained_execs.push_back({nid, e});
                        e = NodeExecStats{};
                    }
                }
                other._has_map_updates = false;
            }
        }
        if (!drained_scans.empty() || !drained_execs.empty()) {
            std::lock_guard<LockType> l(_lock);
            for (const auto& item : drained_scans) {
                update_stats_item(item.table_id, item.stats.scan_rows, item.stats.scan_bytes);
            }
            for (const auto& item : drained_execs) {
                update_exec_stats_item(item.node_id, item.stats.push_rows, item.stats.pull_rows,
                                       item.stats.pred_filter_rows, item.stats.index_filter_rows,
                                       item.stats.rf_filter_rows);
            }
        }
    }

private:
    struct ScanStats {
        int64_t scan_rows = 0;
        int64_t scan_bytes = 0;
        bool empty() const { return scan_rows == 0 && scan_bytes == 0; }
    };
    struct NodeExecStats {
        int64_t push_rows = 0;
        int64_t pull_rows = 0;
        int64_t pred_filter_rows = 0;
        int64_t index_filter_rows = 0;
        int64_t rf_filter_rows = 0;
        bool active = false;
    };

    void update_stats_item(int64_t table_id, int64_t rows, int64_t bytes) {
        if (table_id > 0 && (rows > 0 || bytes > 0)) {
            auto& item = _stats_items[table_id];
            item.scan_rows += rows;
            item.scan_bytes += bytes;
            _has_map_updates = true;
        }
    }

    void update_exec_stats_item(uint32_t node_id, int64_t push, int64_t pull, int64_t pred, int64_t idx, int64_t rf) {
        auto& item = _exec_stats_items[node_id];
        item.push_rows += push;
        item.pull_rows += pull;
        item.pred_filter_rows += pred;
        item.index_filter_rows += idx;
        item.rf_filter_rows += rf;
        item.active = true;
        _has_map_updates = true;
    }

    std::atomic_int64_t scan_rows{0};
    std::atomic_int64_t scan_bytes{0};
    std::atomic_int64_t cpu_ns{0};
    std::atomic_int64_t mem_cost_bytes{0};
    std::atomic_int64_t spill_bytes{0};
    std::atomic_int64_t read_local_cnt{0};
    std::atomic_int64_t read_remote_cnt{0};
    std::atomic_int64_t transmitted_bytes{0};
    LockType _lock;
    bool _has_map_updates{false};
    phmap::flat_hash_map<int64_t, ScanStats> _stats_items;
    phmap::flat_hash_map<uint32_t, NodeExecStats> _exec_stats_items;
};

// Candidate 3 & 4: StripedRecvr<K, LockType> (K = 1 is SingleConsolidatedRecvr / Approach 2; K = 8, 16, 32 is Approach 1)
template <size_t K, typename LockType>
class StripedRecvr {
    static_assert((K & (K - 1)) == 0, "K must be a power of two");

public:
    using StatsType = FastQueryStatistics<LockType>;

    void insert(const PQueryStatistics& statistics, int sender_id) {
        (void)sender_id;
        if (statistics.has_mem_cost_bytes()) {
            bench_atomic_max(&_max_mem_cost_bytes, statistics.mem_cost_bytes());
        }
        auto& stripe = _stripes[stripe_index()];
        stripe.stats.merge_pb(statistics);
        stripe.dirty.store(true, std::memory_order_release);
    }

    void aggregate(StatsType* statistics) {
        statistics->update_max_mem_costs(_max_mem_cost_bytes.load(std::memory_order_relaxed));
        for (size_t i = 0; i < K; ++i) {
            if (_stripes[i].dirty.load(std::memory_order_relaxed) &&
                _stripes[i].dirty.exchange(false, std::memory_order_acq_rel)) {
                statistics->merge(static_cast<int>(i), _stripes[i].stats);
            }
        }
    }

private:
    static inline size_t stripe_index() {
        static std::atomic<size_t> next_id{0};
        thread_local const size_t idx = next_id.fetch_add(1, std::memory_order_relaxed) & (K - 1);
        return idx;
    }

    struct alignas(64) Stripe {
        StatsType stats;
        std::atomic<bool> dirty{false};
    };

    std::array<Stripe, K> _stripes;
    std::atomic_int64_t _max_mem_cost_bytes{0};
};

// Adapter for production starrocks::QueryStatisticsRecvr
class ProductionQueryStatisticsRecvrAdapter {
public:
    using StatsType = QueryStatistics;

    void insert(const PQueryStatistics& statistics, int sender_id) { _recvr.insert(statistics, sender_id); }

    void aggregate(QueryStatistics* statistics) { _recvr.aggregate(statistics); }

private:
    QueryStatisticsRecvr _recvr;
};

using SingleConsolidatedRecvr = StripedRecvr<1, LegacySpinLock>;
using Striped8LegacyLockRecvr = StripedRecvr<8, LegacySpinLock>;
using Striped16LegacyLockRecvr = StripedRecvr<16, LegacySpinLock>;
using Striped16BackoffLockRecvr = StripedRecvr<16, BackoffSpinLock>;
using Striped32BackoffLockRecvr = StripedRecvr<32, BackoffSpinLock>;

PQueryStatistics MakePayload(int num_plan_nodes) {
    PQueryStatistics pb;
    pb.set_scan_rows(4096);
    pb.set_scan_bytes(65536);
    pb.set_cpu_cost_ns(250000);
    pb.set_mem_cost_bytes(1048576);
    pb.set_spill_bytes(0);
    pb.set_read_local_cnt(4);
    pb.set_read_remote_cnt(1);
    pb.set_transmitted_bytes(32768);

    if (num_plan_nodes > 0) {
        int num_tables = std::min(4, std::max(1, num_plan_nodes / 4));
        for (int t = 0; t < num_tables; ++t) {
            auto* item = pb.add_stats_items();
            item->set_table_id(1000 + t);
            item->set_scan_rows(1024);
            item->set_scan_bytes(16384);
        }
        for (int n = 0; n < num_plan_nodes; ++n) {
            auto* exec = pb.add_node_exec_stats_items();
            exec->set_node_id(static_cast<uint32_t>(n));
            exec->set_push_rows(4096);
            exec->set_pull_rows(4096);
            exec->set_pred_filter_rows(128);
            exec->set_index_filter_rows(64);
            exec->set_rf_filter_rows(32);
        }
    }
    return pb;
}

template <typename Tag>
struct PooledLatencyAggregator {
    std::mutex mu;
    std::vector<uint64_t> pooled_samples;
    std::vector<uint64_t> pooled_insert_samples;
    std::vector<uint64_t> pooled_agg_samples;
    int completed_threads{0};

    static double compute_percentile(const std::vector<uint64_t>& sorted, double q) {
        const size_t idx = static_cast<size_t>(q * static_cast<double>(sorted.size() - 1));
        return static_cast<double>(sorted[idx]);
    }

    void record_and_publish(benchmark::State& state, const std::vector<uint64_t>& samples,
                            const std::vector<uint64_t>* insert_samples = nullptr,
                            const std::vector<uint64_t>* agg_samples = nullptr) {
        std::lock_guard<std::mutex> lock(mu);
        if (completed_threads == 0) {
            pooled_samples.clear();
            pooled_insert_samples.clear();
            pooled_agg_samples.clear();
        }
        pooled_samples.insert(pooled_samples.end(), samples.begin(), samples.end());
        if (insert_samples != nullptr) {
            pooled_insert_samples.insert(pooled_insert_samples.end(), insert_samples->begin(), insert_samples->end());
        }
        if (agg_samples != nullptr) {
            pooled_agg_samples.insert(pooled_agg_samples.end(), agg_samples->begin(), agg_samples->end());
        }
        ++completed_threads;
        if (completed_threads == state.threads()) {
            completed_threads = 0;
            if (!pooled_samples.empty()) {
                std::sort(pooled_samples.begin(), pooled_samples.end());
                state.counters["p50_ns"] = benchmark::Counter(compute_percentile(pooled_samples, 0.50));
                state.counters["p99_ns"] = benchmark::Counter(compute_percentile(pooled_samples, 0.99));
                state.counters["p999_ns"] = benchmark::Counter(compute_percentile(pooled_samples, 0.999));
            }
            if (!pooled_insert_samples.empty()) {
                std::sort(pooled_insert_samples.begin(), pooled_insert_samples.end());
                state.counters["insert_p50_ns"] = benchmark::Counter(compute_percentile(pooled_insert_samples, 0.50));
                state.counters["insert_p99_ns"] = benchmark::Counter(compute_percentile(pooled_insert_samples, 0.99));
            }
            if (!pooled_agg_samples.empty()) {
                std::sort(pooled_agg_samples.begin(), pooled_agg_samples.end());
                state.counters["agg_p50_ns"] = benchmark::Counter(compute_percentile(pooled_agg_samples, 0.50));
                state.counters["agg_p99_ns"] = benchmark::Counter(compute_percentile(pooled_agg_samples, 0.99));
            }
        }
    }
};

template <typename RecvrType>
struct SharedWorkloadEnv {
    std::mutex mu;
    std::condition_variable cv;
    int entered_threads{0};
    int finished_threads{0};
    int generation{0};
    std::unique_ptr<RecvrType> recvr;
    PQueryStatistics payload;

    void enter(int total_threads, int num_senders, int num_plan_nodes, bool pre_populate) {
        std::unique_lock<std::mutex> lock(mu);
        const int my_gen = generation;
        if (entered_threads == 0) {
            recvr = std::make_unique<RecvrType>();
            payload = MakePayload(num_plan_nodes);
            if (pre_populate) {
                for (int s = 0; s < num_senders; ++s) {
                    recvr->insert(payload, s);
                }
            }
        }
        if (++entered_threads == total_threads) {
            finished_threads = 0;
            cv.notify_all();
        } else {
            cv.wait(lock, [&]() { return entered_threads == total_threads || generation != my_gen; });
        }
    }

    void leave(int total_threads) {
        std::unique_lock<std::mutex> lock(mu);
        const int my_gen = generation;
        if (++finished_threads == total_threads) {
            entered_threads = 0;
            ++generation;
            cv.notify_all();
        } else {
            cv.wait(lock, [&]() { return generation != my_gen; });
        }
    }
};

// Workload 1: Pure bRPC insert() fan-in across num_senders and num_plan_nodes.
template <typename RecvrType>
void BM_InsertOnly(benchmark::State& state) {
    static SharedWorkloadEnv<RecvrType> env;
    static PooledLatencyAggregator<RecvrType> latency_agg;
    const int num_senders = static_cast<int>(state.range(0));
    const int num_plan_nodes = static_cast<int>(state.range(1));

    env.enter(state.threads(), num_senders, num_plan_nodes, false);

    static constexpr size_t kMaxSamples = 1U << 15;
    static constexpr uint64_t kSampleMask = 63;
    std::vector<uint64_t> samples;
    samples.reserve(kMaxSamples);
    uint64_t iter = 0;
    size_t overwrite_pos = 0;
    const int thread_offset = state.thread_index() * 7;

    for (auto _ : state) {
        const int sender_id = static_cast<int>((iter + thread_offset) % num_senders);
        if ((iter++ & kSampleMask) == 0) {
            const auto t0 = std::chrono::steady_clock::now();
            env.recvr->insert(env.payload, sender_id);
            const auto t1 = std::chrono::steady_clock::now();
            const uint64_t ns =
                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
            if (samples.size() < kMaxSamples) {
                samples.push_back(ns);
            } else {
                samples[overwrite_pos++ & (kMaxSamples - 1)] = ns;
            }
        } else {
            env.recvr->insert(env.payload, sender_id);
        }
    }

    latency_agg.record_and_publish(state, samples);
    env.leave(state.threads());
    state.SetItemsProcessed(state.iterations());
}

// Workload 2: Realistic intermediate fragment workload mixing insert() with periodic aggregate().
template <typename RecvrType>
void BM_MixedInsertAndAggregate(benchmark::State& state) {
    static SharedWorkloadEnv<RecvrType> env;
    static PooledLatencyAggregator<RecvrType> latency_agg;
    const int num_senders = static_cast<int>(state.range(0));
    const int num_plan_nodes = static_cast<int>(state.range(1));
    // state.range(2) (agg_every) is a power of two (8 or 32).
    const uint64_t agg_mask = static_cast<uint64_t>(state.range(2)) - 1;

    env.enter(state.threads(), num_senders, num_plan_nodes, false);

    static constexpr size_t kMaxSamples = 1U << 15;
    // Prime sampling stride coprime to power-of-two agg_every intervals (8, 32)
    // so sampling never phase-locks onto only aggregate() or only insert().
    static constexpr uint64_t kSampleStride = 67;
    std::vector<uint64_t> samples;
    std::vector<uint64_t> insert_samples;
    std::vector<uint64_t> agg_samples;
    samples.reserve(kMaxSamples);
    insert_samples.reserve(kMaxSamples);
    agg_samples.reserve(kMaxSamples >> 2);
    uint64_t iter = 0;
    uint64_t sample_countdown = static_cast<uint64_t>(state.thread_index() % kSampleStride) + 1;
    size_t overwrite_pos = 0;
    size_t insert_overwrite_pos = 0;
    size_t agg_overwrite_pos = 0;
    const int thread_offset = state.thread_index() * 13;

    for (auto _ : state) {
        const uint64_t seq = iter++;
        const int sender_id = static_cast<int>((seq + thread_offset) % num_senders);
        const bool do_agg = ((seq + state.thread_index()) & agg_mask) == 0;

        if (--sample_countdown == 0) {
            sample_countdown = kSampleStride;
            const auto t0 = std::chrono::steady_clock::now();
            if (do_agg) {
                typename RecvrType::StatsType out;
                env.recvr->aggregate(&out);
                benchmark::DoNotOptimize(out);
            } else {
                env.recvr->insert(env.payload, sender_id);
            }
            const auto t1 = std::chrono::steady_clock::now();
            const uint64_t ns =
                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
            if (samples.size() < kMaxSamples) {
                samples.push_back(ns);
            } else {
                samples[overwrite_pos++ & (kMaxSamples - 1)] = ns;
            }
            if (do_agg) {
                if (agg_samples.size() < kMaxSamples) {
                    agg_samples.push_back(ns);
                } else {
                    agg_samples[agg_overwrite_pos++ & (kMaxSamples - 1)] = ns;
                }
            } else {
                if (insert_samples.size() < kMaxSamples) {
                    insert_samples.push_back(ns);
                } else {
                    insert_samples[insert_overwrite_pos++ & (kMaxSamples - 1)] = ns;
                }
            }
        } else {
            if (do_agg) {
                typename RecvrType::StatsType out;
                env.recvr->aggregate(&out);
                benchmark::DoNotOptimize(out);
            } else {
                env.recvr->insert(env.payload, sender_id);
            }
        }
    }

    latency_agg.record_and_publish(state, samples, &insert_samples, &agg_samples);
    env.leave(state.threads());
    state.SetItemsProcessed(state.iterations());
}

// Workload 3: 90-destination EOS aggregate burst (simulates SinkBuffer::_try_to_send_rpc() EOS storm
// where multiple pipeline drivers call aggregate() repeatedly while tail packets arrive).
template <typename RecvrType>
void BM_EosAggregateBurst(benchmark::State& state) {
    static SharedWorkloadEnv<RecvrType> env;
    static PooledLatencyAggregator<RecvrType> latency_agg;
    const int num_senders = static_cast<int>(state.range(0));
    const int num_plan_nodes = static_cast<int>(state.range(1));

    env.enter(state.threads(), num_senders, num_plan_nodes, true);

    static constexpr size_t kMaxSamples = 1U << 15;
    // Prime sampling stride coprime to the period-4 operation selector so every thread
    // samples tail inserts (1/4) and EOS aggregates (3/4) in true workload proportion.
    static constexpr uint64_t kSampleStride = 37;
    std::vector<uint64_t> samples;
    std::vector<uint64_t> insert_samples;
    std::vector<uint64_t> agg_samples;
    samples.reserve(kMaxSamples);
    insert_samples.reserve(kMaxSamples >> 1);
    agg_samples.reserve(kMaxSamples);
    uint64_t iter = 0;
    uint64_t sample_countdown = static_cast<uint64_t>(state.thread_index() % kSampleStride) + 1;
    size_t overwrite_pos = 0;
    size_t insert_overwrite_pos = 0;
    size_t agg_overwrite_pos = 0;

    for (auto _ : state) {
        const uint64_t seq = iter++;
        // 3 out of 4 operations are EOS aggregate() calls; 1 out of 4 is a tail insert().
        const bool is_tail_insert = ((seq + state.thread_index()) & 3) == 0;
        const int sender_id = static_cast<int>(seq % num_senders);

        if (--sample_countdown == 0) {
            sample_countdown = kSampleStride;
            const auto t0 = std::chrono::steady_clock::now();
            if (is_tail_insert) {
                env.recvr->insert(env.payload, sender_id);
            } else {
                typename RecvrType::StatsType out;
                env.recvr->aggregate(&out);
                benchmark::DoNotOptimize(out);
            }
            const auto t1 = std::chrono::steady_clock::now();
            const uint64_t ns =
                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
            if (samples.size() < kMaxSamples) {
                samples.push_back(ns);
            } else {
                samples[overwrite_pos++ & (kMaxSamples - 1)] = ns;
            }
            if (is_tail_insert) {
                if (insert_samples.size() < kMaxSamples) {
                    insert_samples.push_back(ns);
                } else {
                    insert_samples[insert_overwrite_pos++ & (kMaxSamples - 1)] = ns;
                }
            } else {
                if (agg_samples.size() < kMaxSamples) {
                    agg_samples.push_back(ns);
                } else {
                    agg_samples[agg_overwrite_pos++ & (kMaxSamples - 1)] = ns;
                }
            }
        } else {
            if (is_tail_insert) {
                env.recvr->insert(env.payload, sender_id);
            } else {
                typename RecvrType::StatsType out;
                env.recvr->aggregate(&out);
                benchmark::DoNotOptimize(out);
            }
        }
    }

    latency_agg.record_and_publish(state, samples, &insert_samples, &agg_samples);
    env.leave(state.threads());
    state.SetItemsProcessed(state.iterations());
}

void ApplyInsertScales(benchmark::Benchmark* b) {
    for (int senders : {8, 90, 256}) {
        for (int nodes : {0, 16, 48}) {
            b->Args({senders, nodes});
        }
    }
    for (int threads : {1, 4, 16, 32, 64, 128}) {
        b->Threads(threads);
    }
    b->UseRealTime();
}

void ApplyMixedScales(benchmark::Benchmark* b) {
    for (int agg_every : {32, 8}) {
        b->Args({90, 16, agg_every});
    }
    for (int threads : {1, 4, 8, 16, 32, 64, 128}) {
        b->Threads(threads);
    }
    b->UseRealTime();
}

void ApplyEosBurstScales(benchmark::Benchmark* b) {
    b->Args({90, 16});
    for (int threads : {1, 4, 8, 16, 32, 64}) {
        b->Threads(threads);
    }
    b->UseRealTime();
}

BENCHMARK_TEMPLATE(BM_InsertOnly, LegacyMapSpinLockRecvr)->Apply(ApplyInsertScales);
BENCHMARK_TEMPLATE(BM_InsertOnly, LegacyMapBackoffLockRecvr)->Apply(ApplyInsertScales);
BENCHMARK_TEMPLATE(BM_InsertOnly, PhmapSharedMutexRecvr)->Apply(ApplyInsertScales);
BENCHMARK_TEMPLATE(BM_InsertOnly, PhmapSharedMutexBackoffLockRecvr)->Apply(ApplyInsertScales);
BENCHMARK_TEMPLATE(BM_InsertOnly, SingleConsolidatedRecvr)->Apply(ApplyInsertScales);
BENCHMARK_TEMPLATE(BM_InsertOnly, Striped8LegacyLockRecvr)->Apply(ApplyInsertScales);
BENCHMARK_TEMPLATE(BM_InsertOnly, Striped16LegacyLockRecvr)->Apply(ApplyInsertScales);
BENCHMARK_TEMPLATE(BM_InsertOnly, Striped16BackoffLockRecvr)->Apply(ApplyInsertScales);
BENCHMARK_TEMPLATE(BM_InsertOnly, Striped32BackoffLockRecvr)->Apply(ApplyInsertScales);
BENCHMARK_TEMPLATE(BM_InsertOnly, ProductionQueryStatisticsRecvrAdapter)->Apply(ApplyInsertScales);

BENCHMARK_TEMPLATE(BM_MixedInsertAndAggregate, LegacyMapSpinLockRecvr)->Apply(ApplyMixedScales);
BENCHMARK_TEMPLATE(BM_MixedInsertAndAggregate, LegacyMapBackoffLockRecvr)->Apply(ApplyMixedScales);
BENCHMARK_TEMPLATE(BM_MixedInsertAndAggregate, PhmapSharedMutexRecvr)->Apply(ApplyMixedScales);
BENCHMARK_TEMPLATE(BM_MixedInsertAndAggregate, PhmapSharedMutexBackoffLockRecvr)->Apply(ApplyMixedScales);
BENCHMARK_TEMPLATE(BM_MixedInsertAndAggregate, SingleConsolidatedRecvr)->Apply(ApplyMixedScales);
BENCHMARK_TEMPLATE(BM_MixedInsertAndAggregate, Striped8LegacyLockRecvr)->Apply(ApplyMixedScales);
BENCHMARK_TEMPLATE(BM_MixedInsertAndAggregate, Striped16LegacyLockRecvr)->Apply(ApplyMixedScales);
BENCHMARK_TEMPLATE(BM_MixedInsertAndAggregate, Striped16BackoffLockRecvr)->Apply(ApplyMixedScales);
BENCHMARK_TEMPLATE(BM_MixedInsertAndAggregate, Striped32BackoffLockRecvr)->Apply(ApplyMixedScales);
BENCHMARK_TEMPLATE(BM_MixedInsertAndAggregate, ProductionQueryStatisticsRecvrAdapter)->Apply(ApplyMixedScales);

BENCHMARK_TEMPLATE(BM_EosAggregateBurst, LegacyMapSpinLockRecvr)->Apply(ApplyEosBurstScales);
BENCHMARK_TEMPLATE(BM_EosAggregateBurst, LegacyMapBackoffLockRecvr)->Apply(ApplyEosBurstScales);
BENCHMARK_TEMPLATE(BM_EosAggregateBurst, PhmapSharedMutexRecvr)->Apply(ApplyEosBurstScales);
BENCHMARK_TEMPLATE(BM_EosAggregateBurst, PhmapSharedMutexBackoffLockRecvr)->Apply(ApplyEosBurstScales);
BENCHMARK_TEMPLATE(BM_EosAggregateBurst, SingleConsolidatedRecvr)->Apply(ApplyEosBurstScales);
BENCHMARK_TEMPLATE(BM_EosAggregateBurst, Striped8LegacyLockRecvr)->Apply(ApplyEosBurstScales);
BENCHMARK_TEMPLATE(BM_EosAggregateBurst, Striped16LegacyLockRecvr)->Apply(ApplyEosBurstScales);
BENCHMARK_TEMPLATE(BM_EosAggregateBurst, Striped16BackoffLockRecvr)->Apply(ApplyEosBurstScales);
BENCHMARK_TEMPLATE(BM_EosAggregateBurst, Striped32BackoffLockRecvr)->Apply(ApplyEosBurstScales);
BENCHMARK_TEMPLATE(BM_EosAggregateBurst, ProductionQueryStatisticsRecvrAdapter)->Apply(ApplyEosBurstScales);

} // namespace
} // namespace starrocks

BENCHMARK_MAIN();
