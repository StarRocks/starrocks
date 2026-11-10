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

#include "orchestration/runtime_filter_worker.h"

#include <gtest/gtest.h>

#include <chrono>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "base/testutil/assert.h"
#include "column/fixed_length_column.h"
#include "common/brpc/brpc_stub_cache.h"
#include "common/system/cpu_info.h"
#include "exec/exec_env.h"
#include "exec/runtime_filter_compat/runtime_filter_serde.h"
#include "orchestration/fragment_mgr.h"
#include "runtime/runtime_filter.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/service_contexts.h"
#include "types/type_descriptor.h"

namespace starrocks::orchestration {

namespace {

constexpr int kBuildBeNumber = 3;
constexpr const char* kSendTotalEvent = "SEND_TOTAL_RF_RPC";

TUniqueId make_query_id(int64_t hi, int64_t lo) {
    TUniqueId id;
    id.hi = hi;
    id.lo = lo;
    return id;
}

// Each build BE inserts its own values, so the merged filter has to see all of them.
std::string serialize_partial_rf(int be_number) {
    ComposedRuntimeBloomFilter<TYPE_INT> rf;
    rf.membership_filter().init(100);
    for (int i = be_number; i < 300; i += kBuildBeNumber) {
        rf.insert(i);
    }
    const size_t max_size = RuntimeFilterSerde::max_size(RF_VERSION_V3, &rf);
    std::string data(max_size, '\0');
    const size_t actual_size =
            RuntimeFilterSerde::serialize(RF_VERSION_V3, &rf, reinterpret_cast<uint8_t*>(data.data()));
    data.resize(actual_size);
    return data;
}

// The broadcast join's build keys of a skew join. Hash join key columns start at row 1.
std::string serialize_skew_broadcast_material() {
    auto column = Int32Column::create();
    for (int i = 0; i < 10; ++i) {
        column->append(i);
    }
    ColumnPtr key_column = std::move(column);
    std::string data(RuntimeFilterSerde::skew_max_size(key_column), '\0');
    auto size = RuntimeFilterSerde::skew_serialize(key_column, false, reinterpret_cast<uint8_t*>(data.data()));
    CHECK(size.ok()) << size.status();
    data.resize(size.value());
    return data;
}

} // namespace

class RuntimeFilterWorkerTest : public ::testing::Test {
protected:
    void SetUp() override {
        CpuInfo::init();
        _timer = std::make_unique<BthreadTimer>();
        ASSERT_OK(_timer->start());
        _stub_cache = std::make_unique<BrpcStubCache>(_timer.get());
        _rf_cache = std::make_unique<RuntimeFilterCache>(4);
        ASSERT_OK(_rf_cache->init());
        _rf_cache->set_enable_trace(true);
        _runtime_services.runtime_filter_cache = _rf_cache.get();
        _rpc_services.brpc_stub_cache = _stub_cache.get();
        _fragment_mgr = std::make_unique<FragmentMgr>(ExecEnv::GetInstance(), nullptr);
    }

    void TearDown() override {
        _fragment_mgr->close();
        _fragment_mgr.reset();
        _rf_cache.reset();
        _stub_cache.reset();
        _timer.reset();
    }

    std::unique_ptr<RuntimeFilterWorker> make_worker(int32_t num_threads) {
        return std::make_unique<RuntimeFilterWorker>(&_runtime_services, &_rpc_services, nullptr, _fragment_mgr.get(),
                                                     num_threads);
    }

    // Every filter expects kBuildBeNumber partial filters and has one probe target. The target port is invalid, so
    // no stub is created and the merged filter is dropped right after the SEND_TOTAL_RF_RPC event is traced.
    static TRuntimeFilterParams make_params(int num_filters) {
        TRuntimeFilterParams params;
        for (int filter_id = 0; filter_id < num_filters; ++filter_id) {
            TRuntimeFilterProberParams prober;
            prober.__set_fragment_instance_id(make_query_id(1000, filter_id));
            TNetworkAddress addr;
            addr.hostname = "127.0.0.1";
            addr.port = -1;
            prober.__set_fragment_instance_address(addr);
            params.id_to_prober_params[filter_id].push_back(prober);
            params.runtime_filter_builder_number[filter_id] = kBuildBeNumber;
        }
        params.__isset.id_to_prober_params = true;
        params.__isset.runtime_filter_builder_number = true;
        return params;
    }

    static PTransmitRuntimeFilterParams make_partial(const TUniqueId& query_id, int filter_id, int be_number,
                                                     const std::string& data) {
        PTransmitRuntimeFilterParams params;
        params.mutable_query_id()->set_hi(query_id.hi);
        params.mutable_query_id()->set_lo(query_id.lo);
        params.set_filter_id(filter_id);
        params.set_is_partial(true);
        params.set_is_pipeline(false);
        params.set_build_be_number(be_number);
        params.set_data(data);
        return params;
    }

    static PTransmitRuntimeFilterParams make_skew_broadcast(const TUniqueId& query_id, int filter_id,
                                                            int shuffle_filter_id, const std::string& data) {
        PTransmitRuntimeFilterParams params = make_partial(query_id, filter_id, 0, data);
        params.set_is_skew_broadcast_join(true);
        params.set_skew_shuffle_filter_id(shuffle_filter_id);
        *params.mutable_columntype() = TypeDescriptor(TYPE_INT).to_protobuf();
        return params;
    }

    // filter_id -> number of SEND_TOTAL_RF_RPC events of the query.
    std::map<int, int> sent_total_counts(const TUniqueId& query_id) {
        std::map<int, int> counts;
        auto events = _rf_cache->get_events();
        auto it = events.find(print_id(query_id));
        if (it == events.end()) {
            return counts;
        }
        for (const auto& event : it->second) {
            if (event.find(kSendTotalEvent) == std::string::npos) {
                continue;
            }
            // event format: [query_id][filter_id] ts: msg
            const size_t begin = event.find("][") + 2;
            const size_t end = event.find(']', begin);
            counts[std::stoi(event.substr(begin, end - begin))]++;
        }
        return counts;
    }

    // Opens a query on the worker, sends every partial filter from kBuildBeNumber threads at the same time, and
    // returns the SEND_TOTAL_RF_RPC counts once the worker has drained its queues.
    std::map<int, int> merge_all(RuntimeFilterWorker* worker, const TUniqueId& query_id, int num_filters) {
        worker->open_query(query_id, TQueryOptions(), make_params(num_filters), false);
        std::vector<std::thread> senders;
        for (int be = 0; be < kBuildBeNumber; ++be) {
            senders.emplace_back([&, be] {
                const std::string data = serialize_partial_rf(be);
                for (int filter_id = 0; filter_id < num_filters; ++filter_id) {
                    worker->receive_runtime_filter(make_partial(query_id, filter_id, be, data));
                }
            });
        }
        for (auto& t : senders) {
            t.join();
        }
        for (int i = 0; i < 1000 && sent_total_counts(query_id).size() < static_cast<size_t>(num_filters); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        worker->close_query(query_id);
        worker->close();
        return sent_total_counts(query_id);
    }

    std::unique_ptr<BthreadTimer> _timer;
    std::unique_ptr<BrpcStubCache> _stub_cache;
    std::unique_ptr<RuntimeFilterCache> _rf_cache;
    std::unique_ptr<FragmentMgr> _fragment_mgr;
    RuntimeServices _runtime_services;
    RpcServices _rpc_services;
};

TEST_F(RuntimeFilterWorkerTest, ShardIndexSpreadsFiltersOfOneQuery) {
    const TUniqueId query_id = make_query_id(0x1234, 0x5678);
    std::set<size_t> shards;
    for (int filter_id = 0; filter_id < 4; ++filter_id) {
        const size_t shard = RuntimeFilterWorker::shard_index(query_id, filter_id, 4);
        ASSERT_LT(shard, 4u);
        // The same filter always goes to the same thread.
        ASSERT_EQ(shard, RuntimeFilterWorker::shard_index(query_id, filter_id, 4));
        shards.insert(shard);
    }
    ASSERT_EQ(4u, shards.size());

    for (size_t num_shards : {size_t{0}, size_t{1}}) {
        ASSERT_EQ(0u, RuntimeFilterWorker::shard_index(query_id, 7, num_shards));
    }
}

TEST_F(RuntimeFilterWorkerTest, NonPositiveThreadNumUsesOneThread) {
    for (int32_t num_threads : {-1, 0, 1}) {
        auto worker = make_worker(num_threads);
        ASSERT_EQ(1u, worker->num_threads());
        worker->close();
    }
    auto worker = make_worker(4);
    ASSERT_EQ(4u, worker->num_threads());
    worker->close();
}

// Partial filters of many filters arrive interleaved from several BEs. With several threads every filter is still
// merged exactly once, the same as with one thread.
TEST_F(RuntimeFilterWorkerTest, MergeInterleavedPartialsOnSeveralThreads) {
    constexpr int kNumFilters = 32;
    for (int32_t num_threads : {1, 4}) {
        const TUniqueId query_id = make_query_id(42, num_threads);
        auto worker = make_worker(num_threads);
        auto counts = merge_all(worker.get(), query_id, kNumFilters);
        ASSERT_EQ(static_cast<size_t>(kNumFilters), counts.size()) << "num_threads=" << num_threads;
        for (const auto& [filter_id, count] : counts) {
            ASSERT_EQ(1, count) << "num_threads=" << num_threads << ", filter_id=" << filter_id;
        }
        ASSERT_EQ(0u, worker->queue_size());
    }
}

// A skew join's broadcast rf carries the broadcast join's own filter id but is merged into the shuffle join's rf. It
// has to reach the shuffle filter's thread, whether it arrives before or after the shuffle partials; on any other
// thread the shuffle filter would wait for it forever and never be sent.
TEST_F(RuntimeFilterWorkerTest, SkewBroadcastMeetsShufflePartials) {
    constexpr int kNumFilters = 8;
    constexpr int kBroadcastFilterIdOffset = 1001;
    const std::string material = serialize_skew_broadcast_material();
    for (bool broadcast_first : {true, false}) {
        const TUniqueId query_id = make_query_id(44, broadcast_first ? 1 : 2);
        // Otherwise routing by the broadcast filter id would pass too.
        ASSERT_NE(RuntimeFilterWorker::shard_index(query_id, kBroadcastFilterIdOffset, 4),
                  RuntimeFilterWorker::shard_index(query_id, 0, 4));

        TRuntimeFilterParams params = make_params(kNumFilters);
        for (int filter_id = 0; filter_id < kNumFilters; ++filter_id) {
            params.skew_join_runtime_filters.insert(filter_id);
        }
        params.__isset.skew_join_runtime_filters = true;

        auto worker = make_worker(4);
        worker->open_query(query_id, TQueryOptions(), params, false);
        auto send_broadcasts = [&] {
            for (int filter_id = 0; filter_id < kNumFilters; ++filter_id) {
                worker->receive_runtime_filter(
                        make_skew_broadcast(query_id, filter_id + kBroadcastFilterIdOffset, filter_id, material));
            }
        };
        if (broadcast_first) {
            send_broadcasts();
        }
        for (int be = 0; be < kBuildBeNumber; ++be) {
            const std::string data = serialize_partial_rf(be);
            for (int filter_id = 0; filter_id < kNumFilters; ++filter_id) {
                worker->receive_runtime_filter(make_partial(query_id, filter_id, be, data));
            }
        }
        if (!broadcast_first) {
            send_broadcasts();
        }
        // close() drains every queue before the threads exit.
        worker->close_query(query_id);
        worker->close();

        auto counts = sent_total_counts(query_id);
        ASSERT_EQ(static_cast<size_t>(kNumFilters), counts.size()) << "broadcast_first=" << broadcast_first;
        for (const auto& [filter_id, count] : counts) {
            ASSERT_LT(filter_id, kNumFilters);
            ASSERT_EQ(1, count) << "broadcast_first=" << broadcast_first << ", filter_id=" << filter_id;
        }
    }
}

// A partial filter that arrives after CLOSE_QUERY has no merger on its thread and is dropped.
TEST_F(RuntimeFilterWorkerTest, PartialAfterCloseQueryIsDropped) {
    const TUniqueId query_id = make_query_id(43, 1);
    auto worker = make_worker(4);
    worker->open_query(query_id, TQueryOptions(), make_params(4), false);
    worker->close_query(query_id);
    for (int be = 0; be < kBuildBeNumber; ++be) {
        const std::string data = serialize_partial_rf(be);
        for (int filter_id = 0; filter_id < 4; ++filter_id) {
            worker->receive_runtime_filter(make_partial(query_id, filter_id, be, data));
        }
    }
    // close() drains every queue before the threads exit.
    worker->close();
    ASSERT_TRUE(sent_total_counts(query_id).empty());
    ASSERT_EQ(0u, worker->queue_size());
}

} // namespace starrocks::orchestration
