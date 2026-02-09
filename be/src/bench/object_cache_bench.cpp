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

#include <benchmark/benchmark.h>

#include <cstdlib>
#include <random>

#include "cache/block_cache/block_cache.h"
#include "cache/cache_options.h"
#include "cache/disk_cache/starcache_engine.h"
#include "cache/mem_cache/lrucache_engine.h"
#include "cache/mem_cache/page_cache.h"
#include "common/config.h"
#include "common/system/disk_info.h"
#include "common/system/mem_info.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"

namespace starrocks {

enum class CacheType { LRU, STAR };

enum class TestMode { INSERT, QUERY_ALL_HIT, QUERY_50_PERCENT_HIT, QUERY_MULTI_THREAD, INSERT_MULTI_THREAD };

/* test result (touch rate 100%)
 |mode|LRU|Star|
 |----|----|----|
 |one thread insert|15.0s|29.4s|
 |one thread query, all hit|5.9s|13.6s|
 |one thread query, 50% hit|4.7s|8.8s|
 |10 thread random query|124s|163s|
 |5 thread random insert|52s|265s|
 */

class ObjectCacheBench {
public:
    void SetUp() {}
    void TearDown() {}

    static void init_env();
    static std::string get_cache_type_str(CacheType type);

    void init_cache(CacheType cache_type);
    void prepare_data(StoragePageCache* cache, int64_t count);
    void prepare_sequence_data(StoragePageCache* cache, int64_t count);
    void random_query(benchmark::State& state, StoragePageCache* cache, size_t ratio, int64_t iter_count,
                      int64_t count);

    static void random_query_multi_threads(benchmark::State* state, StoragePageCache* cache, size_t ratio,
                                           size_t count);
    static void random_insert_multi_threads(benchmark::State* state, StoragePageCache* cache, size_t count,
                                            size_t page_size);

    void prepare_data(BlockCache* cache, int64_t count);
    void prepare_sequence_data(BlockCache* cache, int64_t count);
    void random_query(benchmark::State& state, BlockCache* cache, size_t ratio, int64_t iter_count, int64_t count);
    static void random_query_multi_threads_block(benchmark::State* state, BlockCache* cache, size_t ratio, size_t count,
                                                 size_t page_size);
    static void random_insert_multi_threads_block(benchmark::State* state, BlockCache* cache, size_t count,
                                                  size_t page_size);

    void insert_to_cache(benchmark::State& state, CacheType cache_type, int64_t count);
    void random_query(benchmark::State& state, CacheType cache_type, size_t ratio, int64_t iter_count, int64_t count);
    void random_query_multi_threads_test(benchmark::State& state, CacheType cache_type, size_t ratio, int64_t count);
    void insert_cache_multi_threads_test(benchmark::State& state, CacheType cache_type, int64_t count);

private:
    MemPool _mem_pool;
    size_t _capacity = 100L * 1024 * 1024 * 1024;
    size_t _page_size = 1024;

    std::shared_ptr<BlockCache> _block_cache;
    std::shared_ptr<LRUCacheEngine> _lru_cache;
    std::shared_ptr<StarCacheEngine> _star_cache;
    std::shared_ptr<StoragePageCache> _page_cache;
};

void ObjectCacheBench::init_env() {
    static bool is_init = false;

    if (!is_init) {
        setenv("STARROCKS_HOME", "./", 1);
        setenv("UDF_RUNTIME_DIR", "./udf/", 1);
        (void)config::init(nullptr);
        config::mem_limit = "100G";
        CpuInfo::init();
        DiskInfo::init();
        MemInfo::init();
        GlobalEnv* env = GlobalEnv::GetInstance();
        Status st = env->init();
        is_init = true;
        LOG(INFO) << "int env: " << st;
    }
}

std::string ObjectCacheBench::get_cache_type_str(CacheType type) {
    if (type == CacheType::LRU) {
        return "LRU";
    } else {
        return "STAR";
    }
}

void ObjectCacheBench::init_cache(CacheType cache_type) {
    DiskCacheOptions opt;
    opt.mem_space_size = _capacity;
    opt.block_size = config::datacache_block_size;
    opt.max_flying_memory_mb = config::datacache_max_flying_memory_mb;
    opt.max_concurrent_inserts = config::datacache_max_concurrent_inserts;
    opt.enable_checksum = config::datacache_checksum_enable;
    opt.enable_direct_io = config::datacache_direct_io_enable;
    opt.skip_read_factor = config::datacache_skip_read_factor;
    opt.scheduler_threads_per_cpu = config::datacache_scheduler_threads_per_cpu;
    opt.enable_datacache_persistence = false;
    opt.inline_item_count_limit = config::datacache_inline_item_count_limit;
    opt.eviction_policy = config::datacache_eviction_policy;

    if (cache_type == CacheType::LRU) {
        _lru_cache = std::make_shared<LRUCacheEngine>();
        MemCacheOptions mem_opt;
        mem_opt.capacity = _capacity;
        Status st = _lru_cache->init(mem_opt);
        if (!st.ok()) {
            LOG(FATAL) << "init lru cache failed: " << st;
        }
        LOG(INFO) << "init lru cache success";
        _page_cache = std::make_shared<StoragePageCache>();
        _page_cache->init(_lru_cache.get());
    } else {
        _star_cache = std::make_shared<StarCacheEngine>();
        Status st = _star_cache->init(opt);
        if (!st.ok()) {
            LOG(FATAL) << "init star cache failed: " << st;
        }
        BlockCacheOptions block_opt;
        block_opt.block_size = _page_size;
        _block_cache = std::make_shared<BlockCache>();
        _block_cache->init(block_opt, _star_cache, nullptr);
        LOG(INFO) << "init star cache (block cache) success";
    }
}

void ObjectCacheBench::prepare_sequence_data(StoragePageCache* cache, int64_t count) {
    for (size_t i = 0; i < count; i++) {
        std::string key = "str:" + std::to_string(rand() % count);
        auto* ptr = new std::vector<uint8_t>(_page_size);
        (*ptr)[0] = 1;
        PageCacheHandle handle;
        MemCacheWriteOptions options;
        Status st = cache->insert(key, ptr, options, &handle);
        if (!st.ok()) {
            if (!st.is_already_exist()) {
                LOG(FATAL) << "insert failed: " << st;
            }
        }
    }
}

void ObjectCacheBench::prepare_data(StoragePageCache* cache, int64_t count) {
    for (size_t i = 0; i < count; i++) {
        std::string key = "str:" + std::to_string(rand());
        auto* ptr = new std::vector<uint8_t>(_page_size);
        (*ptr)[0] = 1;
        PageCacheHandle handle;
        MemCacheWriteOptions options;
        Status st = cache->insert(key, ptr, options, &handle);
        if (!st.ok()) {
            if (!st.is_already_exist()) {
                LOG(FATAL) << "insert failed: " << st;
            }
        }
    }
}

void ObjectCacheBench::random_query(benchmark::State& state, StoragePageCache* cache, size_t ratio, int64_t iter_count,
                                    int64_t count) {
    thread_local std::mt19937 gen(std::random_device{}());
    thread_local std::uniform_int_distribution<> dis(1, 1073741824);

    LOG(ERROR) << "random query start";
    for (size_t i = 0; i < iter_count; i++) {
        std::string key = "str:" + std::to_string(dis(gen) % (count * ratio));
        PageCacheHandle handle;
        (void)cache->lookup(key, &handle);
    }
    LOG(ERROR) << "random query end: lookup=" << cache->get_lookup_count() << ", hit=" << cache->get_hit_count();
}

void ObjectCacheBench::random_query_multi_threads(benchmark::State* state, StoragePageCache* cache, size_t ratio,
                                                  size_t count) {
    state->ResumeTiming();
    thread_local std::mt19937 gen(std::random_device{}());
    thread_local std::uniform_int_distribution<> dis(1, 1073741824);

    for (size_t i = 0; i < count; i++) {
        std::string key = "str:" + std::to_string(dis(gen) % (count * ratio));
        PageCacheHandle handle;
        (void)cache->lookup(key, &handle);
    }
    state->PauseTiming();
}

void ObjectCacheBench::random_insert_multi_threads(benchmark::State* state, StoragePageCache* cache, size_t count,
                                                   size_t page_size) {
    state->ResumeTiming();
    thread_local std::mt19937 gen(std::random_device{}());
    thread_local std::uniform_int_distribution<> dis(1, 1073741824);

    for (size_t i = 0; i < count; i++) {
        std::string key = "str:" + std::to_string(dis(gen));
        auto* ptr = new std::vector<uint8_t>(page_size);
        (*ptr)[0] = 1;
        PageCacheHandle handle;
        MemCacheWriteOptions options;
        Status st = cache->insert(key, ptr, options, &handle);
        if (!st.ok()) {
            if (!st.is_already_exist()) {
                LOG(FATAL) << "insert failed: " << st;
            }
        }
    }
    state->PauseTiming();
}

void ObjectCacheBench::prepare_sequence_data(BlockCache* cache, int64_t count) {
    std::vector<uint8_t> buf(_page_size, 1);
    for (size_t i = 0; i < count; i++) {
        std::string key = "str:" + std::to_string(rand() % count);
        Status st = cache->write_buffer(key, 0, _page_size, buf.data());
        if (!st.ok()) {
            if (!st.is_already_exist()) {
                LOG(FATAL) << "write_buffer failed: " << st;
            }
        }
    }
}

void ObjectCacheBench::prepare_data(BlockCache* cache, int64_t count) {
    std::vector<uint8_t> buf(_page_size, 1);
    for (size_t i = 0; i < count; i++) {
        std::string key = "str:" + std::to_string(rand());
        Status st = cache->write_buffer(key, 0, _page_size, buf.data());
        if (!st.ok()) {
            if (!st.is_already_exist()) {
                LOG(FATAL) << "write_buffer failed: " << st;
            }
        }
    }
}

void ObjectCacheBench::random_query(benchmark::State& state, BlockCache* cache, size_t ratio, int64_t iter_count,
                                    int64_t count) {
    thread_local std::mt19937 gen(std::random_device{}());
    thread_local std::uniform_int_distribution<> dis(1, 1073741824);
    std::vector<char> buf(_page_size);

    LOG(ERROR) << "random query start";
    for (size_t i = 0; i < iter_count; i++) {
        std::string key = "str:" + std::to_string(dis(gen) % (count * ratio));
        (void)cache->read_buffer(key, 0, _page_size, buf.data());
    }
    LOG(ERROR) << "random query end";
}

void ObjectCacheBench::random_query_multi_threads_block(benchmark::State* state, BlockCache* cache, size_t ratio,
                                                        size_t count, size_t page_size) {
    state->ResumeTiming();
    thread_local std::mt19937 gen(std::random_device{}());
    thread_local std::uniform_int_distribution<> dis(1, 1073741824);
    std::vector<char> buf(page_size);

    for (size_t i = 0; i < count; i++) {
        std::string key = "str:" + std::to_string(dis(gen) % (count * ratio));
        (void)cache->read_buffer(key, 0, page_size, buf.data());
    }
    state->PauseTiming();
}

void ObjectCacheBench::random_insert_multi_threads_block(benchmark::State* state, BlockCache* cache, size_t count,
                                                         size_t page_size) {
    state->ResumeTiming();
    thread_local std::mt19937 gen(std::random_device{}());
    thread_local std::uniform_int_distribution<> dis(1, 1073741824);
    std::vector<uint8_t> buf(page_size, 1);

    for (size_t i = 0; i < count; i++) {
        std::string key = "str:" + std::to_string(dis(gen));
        Status st = cache->write_buffer(key, 0, page_size, buf.data());
        if (!st.ok()) {
            if (!st.is_already_exist()) {
                LOG(FATAL) << "write_buffer failed: " << st;
            }
        }
    }
    state->PauseTiming();
}

void ObjectCacheBench::insert_to_cache(benchmark::State& state, CacheType cache_type, int64_t count) {
    int64_t old_mem_usage = CurrentThread::mem_tracker()->consumption();
    std::string type_str = get_cache_type_str(cache_type);
    init_cache(cache_type);

    state.ResumeTiming();
    if (cache_type == CacheType::LRU) {
        prepare_data(_page_cache.get(), count);
    } else {
        prepare_data(_block_cache.get(), count);
    }
    state.PauseTiming();

    int64_t new_mem_usage = CurrentThread::mem_tracker()->consumption();
    int64_t calc_usage = 0;
    if (cache_type == CacheType::LRU) {
        calc_usage = _page_cache->memory_usage() / 1024 / 1024;
    } else {
        calc_usage = _star_cache->mem_usage() / 1024 / 1024;
    }
    int64_t real_usage = (new_mem_usage - old_mem_usage) / 1024 / 1024;

    LOG(INFO) << "insert: type=" << type_str << ", metric=" << calc_usage << "M, lru=" << real_usage << "M";
}

void ObjectCacheBench::random_query(benchmark::State& state, CacheType cache_type, size_t ratio, int64_t iter_count,
                                    int64_t count) {
    init_cache(cache_type);
    if (cache_type == CacheType::LRU) {
        prepare_sequence_data(_page_cache.get(), count);
        state.ResumeTiming();
        random_query(state, _page_cache.get(), ratio, iter_count, count);
    } else {
        prepare_sequence_data(_block_cache.get(), count);
        state.ResumeTiming();
        random_query(state, _block_cache.get(), ratio, iter_count, count);
    }
    state.PauseTiming();
}

void ObjectCacheBench::random_query_multi_threads_test(benchmark::State& state, CacheType cache_type, size_t ratio,
                                                       int64_t count) {
    state.PauseTiming();
    init_cache(cache_type);

    LOG(INFO) << "start random query test";
    std::vector<std::thread> threads;
    if (cache_type == CacheType::LRU) {
        prepare_sequence_data(_page_cache.get(), count);
        for (size_t i = 0; i < 10; i++) {
            threads.emplace_back(random_query_multi_threads, &state, _page_cache.get(), ratio, count);
        }
    } else {
        prepare_sequence_data(_block_cache.get(), count);
        for (size_t i = 0; i < 10; i++) {
            threads.emplace_back(random_query_multi_threads_block, &state, _block_cache.get(), ratio, count,
                                 _page_size);
        }
    }

    for (auto& t : threads) {
        t.join();
    }

    if (cache_type == CacheType::LRU) {
        LOG(INFO) << "end random query test: lookup=" << _page_cache->get_lookup_count()
                  << ", hit=" << _page_cache->get_hit_count();
    } else {
        LOG(INFO) << "end random query test (BlockCache stats not directly available in this bench)";
    }

    threads.clear();
    state.ResumeTiming();
}

void ObjectCacheBench::insert_cache_multi_threads_test(benchmark::State& state, CacheType cache_type, int64_t count) {
    state.PauseTiming();
    init_cache(cache_type);
    int64_t old_mem_usage = CurrentThread::mem_tracker()->consumption();

    std::vector<std::thread> threads;
    for (size_t i = 0; i < 5; i++) {
        if (cache_type == CacheType::LRU) {
            threads.emplace_back(random_insert_multi_threads, &state, _page_cache.get(), count, _page_size);
        } else {
            threads.emplace_back(random_insert_multi_threads_block, &state, _block_cache.get(), count, _page_size);
        }
    }
    for (auto& t : threads) {
        t.join();
    }
    threads.clear();

    int64_t new_mem_usage = CurrentThread::mem_tracker()->consumption();
    LOG(INFO) << "MEM: " << (new_mem_usage - old_mem_usage) / 1024 / 1024 << "M";
    state.ResumeTiming();
}

static void bench_func(benchmark::State& state) {
    ObjectCacheBench::init_env();
    CacheType type = static_cast<CacheType>(state.range(0));
    TestMode mode = static_cast<TestMode>(state.range(1));
    int64_t iter_count = state.range(2);
    int64_t count = state.range(3);

    ObjectCacheBench perf;

    switch (mode) {
    case TestMode::INSERT:
        perf.insert_to_cache(state, type, count);
        break;
    case TestMode::QUERY_ALL_HIT:
        perf.random_query(state, type, 1, iter_count, count);
        break;
    case TestMode::QUERY_50_PERCENT_HIT:
        perf.random_query(state, type, 2, iter_count, count);
        break;
    case TestMode::QUERY_MULTI_THREAD:
        perf.random_query_multi_threads_test(state, type, 1, count);
        break;
    case TestMode::INSERT_MULTI_THREAD:
        perf.insert_cache_multi_threads_test(state, type, count);
        break;
    default:
        break;
    }
}

// all test case should run separately
static void process_args(benchmark::internal::Benchmark* b) {
    // one thread insert
    b->Args({0, 0, 10000000, 10000000})->Iterations(1);
    b->Args({1, 0, 10000000, 10000000})->Iterations(1);

    // one thread query, all hit
    b->Args({0, 1, 10000000, 10000000})->Iterations(1);
    b->Args({1, 1, 10000000, 10000000})->Iterations(1);

    // one thread query, 50% hit
    b->Args({0, 2, 10000000, 10000000})->Iterations(1);
    b->Args({1, 2, 10000000, 10000000})->Iterations(1);

    // multi thread query
    b->Args({0, 3, 10000000, 10000000})->Iterations(1);
    b->Args({1, 3, 10000000, 10000000})->Iterations(1);

    // multi thread insert
    b->Args({0, 4, 1000000, 5000000})->Iterations(1);
    b->Args({1, 4, 1000000, 5000000})->Iterations(1);
}

BENCHMARK(bench_func)->Apply(process_args);
} // namespace starrocks

BENCHMARK_MAIN();