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
#include <brpc/server.h>
#include <bvar/bvar.h>
#include <bvar/detail/agent_group.h>
#include <bvar/passive_status.h>
#include <bvar/reducer.h>
#include <gtest/gtest.h>
#include <testutil/assert.h>

#include <filesystem>
#include <memory>
#include <numeric>

#include "block_cache/starcache_wrapper.h"
#include "common/config.h"
#include "common/statusor.h"
#include "starcache/common/types.h"
#include "util/logging.h"
#include "util/random.h"
#include "util/time.h"
#ifdef WITH_CACHELIB
#include "block_cache/cachelib_wrapper.h"
#endif

namespace starrocks {

constexpr size_t KB = 1024;
constexpr size_t MB = KB * 1024;
constexpr size_t GB = MB * 1024;

constexpr std::string DISK_CACHE_PATH = "./bench_dir/block_disk_cache";

void delete_dir_content(const std::string& dir_path) {
    for (const auto& entry : std::filesystem::directory_iterator(dir_path)) {
        std::filesystem::remove_all(entry.path());
    }
}

enum class CacheEngine { CACHELIB, STARCACHE };

class BlockCacheBenchSuite {
public:
    struct BenchParams {
        CacheEngine cache_engine;
        size_t obj_count = 0;
        size_t obj_key_size = 0;
        size_t obj_value_size = 0;
        size_t read_size = 0;
        bool pre_populate = true;
        bool check_value = false;
        bool random_read_offset = false;
        bool random_obj_size = false;
        size_t total_op_count = 0;
        // means percentage in 100
        size_t remove_op_ratio = 0;
    };

    struct BenchContext {
        std::vector<std::string> obj_keys;
        std::vector<size_t> obj_sizes;
        std::atomic<size_t> finish_op_count = 0;

        std::unique_ptr<bvar::LatencyRecorder> write_latency;
        std::unique_ptr<bvar::LatencyRecorder> read_latency;
        std::unique_ptr<bvar::LatencyRecorder> remove_latency;

        std::unique_ptr<bvar::Adder<size_t>> write_op_count;
        std::unique_ptr<bvar::Adder<size_t>> read_op_count;
        std::unique_ptr<bvar::Adder<size_t>> remove_op_count;

        std::unique_ptr<bvar::Adder<size_t>> write_bytes;
        std::unique_ptr<bvar::PerSecond<bvar::Adder<size_t>>> write_throught;
        std::unique_ptr<bvar::Adder<size_t>> read_bytes;
        std::unique_ptr<bvar::PerSecond<bvar::Adder<size_t>>> read_throught;

        std::unique_ptr<bvar::IntRecorder> avg_read_size;

        std::unique_ptr<Random> rnd;

        BenchContext() {
            write_latency = std::make_unique<bvar::LatencyRecorder>("star_bench", "write_latency_us");
            read_latency = std::make_unique<bvar::LatencyRecorder>("star_bench", "read_latency_us");
            remove_latency = std::make_unique<bvar::LatencyRecorder>("star_bench", "remove_latency_us");

            write_op_count = std::make_unique<bvar::Adder<size_t>>("star_bench", "write_op_count");
            read_op_count = std::make_unique<bvar::Adder<size_t>>("star_bench", "read_op_count");
            remove_op_count = std::make_unique<bvar::Adder<size_t>>("star_bench", "remove_op_count");

            write_bytes = std::make_unique<bvar::Adder<size_t>>("star_bench", "write_bytes");
            write_throught = std::make_unique<bvar::PerSecond<bvar::Adder<size_t>>>("star_bench", "write_throught",
                                                                                    write_bytes.get());
            read_bytes = std::make_unique<bvar::Adder<size_t>>("star_bench", "read_bytes");
            read_throught = std::make_unique<bvar::PerSecond<bvar::Adder<size_t>>>("star_bench", "read_throught",
                                                                                   read_bytes.get());

            avg_read_size = std::make_unique<bvar::IntRecorder>("star_bench", "avg_read_size");

            rnd = std::make_unique<Random>(0);
        }
    };

    BlockCacheBenchSuite(const CacheOptions& options, const BenchParams& params) {
        _params = new BlockCacheBenchSuite::BenchParams(params);
        if (params.cache_engine == CacheEngine::STARCACHE) {
            _cache = new StarCacheWrapper;
#ifdef WITH_CACHELIB
        } else {
            _cache = new CacheLibWrapper;
#endif
        }
        else {
            DCHECK(false) << "Unsupported cache engine: " << params.cache_engine;
        }
        Status st = _cache->init(options);
        DCHECK(st.ok()) << st.message();
        _ctx = new BenchContext();
    }

    ~BlockCacheBenchSuite() {
        delete _params;
        delete _ctx;
        delete _cache;
    }

    static void Setup(const benchmark::State& state) { brpc::StartDummyServerAt(8080); }

    static void Teardown(const benchmark::State& state) { delete_dir_content(DISK_CACHE_PATH); }

    static char index2ch(size_t index) { return 'a' + index % 26; }

    static std::string gen_obj_key(size_t obj_index, size_t key_size, BenchContext* ctx) {
        const int range = 26;
        std::string key;
        key.push_back(index2ch(obj_index));
        for (size_t i = 0; i < key_size; ++i) {
            int32_t n = ctx->rnd->Uniform(range - 1);
            key.push_back('a' + n);
        }
        return key;
    }

    static std::string gen_obj_value(size_t obj_index, size_t value_size, BenchContext* ctx) {
        std::string value;
        value.assign(value_size, index2ch(obj_index));
        return value;
    }

    static bool check_buffer(const char* data, size_t length, char ch) {
        for (size_t i = 0; i < length; ++i) {
            if (data[i] != ch) {
                LOG(ERROR) << "check buffer failed, "
                           << "real: " << data[i] << ", expect: " << ch << ", data: " << (uint64_t)data << ", i: " << i;
                return false;
            }
        }
        return true;
    }

    void do_bench(benchmark::State& state) {
        for (size_t i = 0; i < _params->total_op_count; ++i) {
            int index = _ctx->rnd->Uniform(_params->obj_count);
            // remove
            if (_params->remove_op_ratio > 0 && _ctx->rnd->Uniform(100) < _params->remove_op_ratio) {
                int64_t start_us = MonotonicMicros();
                Status st = _cache->remove_cache(_ctx->obj_keys[index]);
                if (st.ok()) {
                    *(_ctx->remove_latency) << MonotonicMicros() - start_us;
                    *(_ctx->remove_op_count) << 1;
                }
                continue;
            }

            const size_t obj_value_size = _ctx->obj_sizes[index];
            // read
            char* value = new char[_params->read_size];
            off_t delta = obj_value_size - _params->read_size;
            off_t offset = 0;
            if (_params->random_read_offset && delta > 0) {
                offset = _ctx->rnd->Uniform(delta);
            }
            int64_t start_us = MonotonicMicros();
            auto res = _cache->read_cache(_ctx->obj_keys[index], value, offset, _params->read_size);
            if (res.ok()) {
                *(_ctx->read_latency) << MonotonicMicros() - start_us;
                *(_ctx->read_bytes) << res.value();
                *(_ctx->avg_read_size) << res.value();
                *(_ctx->read_op_count) << 1;
                if (_params->check_value && !check_buffer(value, res.value(), index2ch(index))) {
                    ASSERT_TRUE(false) << "check read data correctness failed"
                                       << ", index: " << index << ", key: " << _ctx->obj_keys[index];
                }
            } else if (res.status().is_not_found()) {
                std::string v = gen_obj_value(index, obj_value_size, _ctx);
                start_us = MonotonicMicros();
                Status st = _cache->write_cache(_ctx->obj_keys[index], v.data(), obj_value_size, 0);
                ASSERT_TRUE(st.ok()) << "write cache failed: " << st.message();
                *(_ctx->write_latency) << MonotonicMicros() - start_us;
                *(_ctx->write_bytes) << v.size();
                *(_ctx->write_op_count) << 1;

                char* read_value = new char[obj_value_size];
                auto res = _cache->read_cache(_ctx->obj_keys[index], read_value, 0, _params->read_size);
                delete[] read_value;
            } else {
                ASSERT_TRUE(false) << "read cache failed: " << res.status().message();
            }
            delete[] value;
        }
    }

    void do_prepare(bool pre_populate, bool random_obj_size) {
        _ctx->obj_keys.reserve(_params->obj_count);
        for (size_t i = 0; i < _params->obj_count; ++i) {
            std::string key = gen_obj_key(i, _params->obj_key_size, _ctx);
            size_t size = _params->obj_value_size;
            if (random_obj_size) {
                size = _ctx->rnd->Uniform(size);
            }
            if (pre_populate) {
                std::string value = gen_obj_value(i, size, _ctx);

                int64_t start_us = MonotonicMicros();
                Status st = _cache->write_cache(key, value.data(), size, 0);
                ASSERT_OK(st);

                char* read_value = new char[_params->obj_value_size];
                std::string read_key = i == 0 ? key : _ctx->obj_keys[0];
                auto res = _cache->read_cache(read_key, read_value, 0, _params->read_size);
                delete[] read_value;

                *(_ctx->write_latency) << MonotonicMicros() - start_us;
                *(_ctx->write_bytes) << value.size();
                *(_ctx->write_op_count) << 1;
            }
            _ctx->obj_keys.push_back(key);
            _ctx->obj_sizes.push_back(size);
        }
    }

    BenchParams* params() { return _params; }

    BenchContext* ctx() { return _ctx; }

private:
    BenchParams* _params = nullptr;
    BenchContext* _ctx = nullptr;
    KvCache* _cache = nullptr;
};

static void do_bench_cache(benchmark::State& state, const CacheOptions& options,
                           const BlockCacheBenchSuite::BenchParams& params) {
    static std::shared_ptr<BlockCacheBenchSuite> suite;
    if (state.thread_index == 0) {
        suite = std::make_shared<BlockCacheBenchSuite>(options, params);
        suite->Setup(state);
        suite->do_prepare(suite->params()->pre_populate, suite->params()->random_obj_size);
    }

    for (auto _ : state) {
        suite->do_bench(state);
    }

    if (state.thread_index == 0) {
        suite->Teardown(state);
    }
    state.counters["write_bytes"] = suite->ctx()->write_bytes->get_value();
    state.counters["read_bytes"] = suite->ctx()->read_bytes->get_value();
    //state.counters["write_throught"] = suite->ctx()->write_throught->get_value();
    //state.counters["read_throught"] = suite->ctx()->read_throught->get_value();
}

template <class... Args>
static void BM_bench_cachelib(benchmark::State& state, Args&&... args) {
    auto args_tuple = std::make_tuple(std::move(args)...);
    std::get<0>(args_tuple).second.cache_engine = CacheEngine::CACHELIB;
    do_bench_cache(state, std::get<0>(args_tuple).first, std::get<0>(args_tuple).second);
}

template <class... Args>
static void BM_bench_starcache(benchmark::State& state, Args&&... args) {
    auto args_tuple = std::make_tuple(std::move(args)...);
    std::get<0>(args_tuple).second.cache_engine = CacheEngine::STARCACHE;
    do_bench_cache(state, std::get<0>(args_tuple).first, std::get<0>(args_tuple).second);
}

[[maybe_unused]] static std::pair<CacheOptions, BlockCacheBenchSuite::BenchParams> read_mem_suite() {
    CacheOptions options;
    options.mem_space_size = 5 * GB;
    options.meta_path = DISK_CACHE_PATH;
    options.disk_spaces.push_back({.path = DISK_CACHE_PATH, .size = 1 * GB});
    options.block_size = 4 * MB;
    options.checksum = false;

    BlockCacheBenchSuite::BenchParams params;
    params.obj_count = 1000;
    params.obj_key_size = 20;
    params.obj_value_size = 1 * MB;
    params.read_size = 1 * MB;
    params.total_op_count = 300000;
    params.check_value = false;

    return std::make_pair(options, params);
}

[[maybe_unused]] static std::pair<CacheOptions, BlockCacheBenchSuite::BenchParams> read_disk_suite() {
    CacheOptions options;
    options.mem_space_size = 300 * MB;
    options.meta_path = DISK_CACHE_PATH;
    options.disk_spaces.push_back({.path = DISK_CACHE_PATH, .size = 10 * GB});
    options.block_size = 4 * MB;
    options.checksum = true;

    BlockCacheBenchSuite::BenchParams params;
    params.obj_count = 1000;
    params.obj_key_size = 20;
    params.obj_value_size = 1 * MB;
    params.read_size = 1 * MB;
    params.total_op_count = 1000;
    params.check_value = true;

    return std::make_pair(options, params);
}

[[maybe_unused]] static std::pair<CacheOptions, BlockCacheBenchSuite::BenchParams> read_write_remove_disk_suite() {
    CacheOptions options;
    options.mem_space_size = 300 * MB;
    options.meta_path = DISK_CACHE_PATH;
    options.disk_spaces.push_back({.path = DISK_CACHE_PATH, .size = 10 * GB});
    options.block_size = 4 * MB;
    options.checksum = true;

    BlockCacheBenchSuite::BenchParams params;
    params.obj_count = 1000;
    params.obj_key_size = 20;
    params.obj_value_size = 1 * MB;
    params.read_size = 1 * MB;
    params.total_op_count = 500;
    params.check_value = true;
    params.pre_populate = false;
    //params.remove_op_ratio = 30;

    return std::make_pair(options, params);
}

[[maybe_unused]] static std::pair<CacheOptions, BlockCacheBenchSuite::BenchParams> random_offset_read_suite() {
    CacheOptions options;
    options.mem_space_size = 300 * MB;
    //options.mem_space_size = 8000 * MB;
    options.meta_path = DISK_CACHE_PATH;
    options.disk_spaces.push_back({.path = DISK_CACHE_PATH, .size = 10 * GB});
    options.block_size = 1 * MB;
    options.checksum = true;

    BlockCacheBenchSuite::BenchParams params;
    params.obj_count = 1000;
    params.obj_key_size = 20;
    //params.obj_value_size = 1 * MB;
    params.obj_value_size = 2 * MB + 123;
    params.read_size = 257 * KB;
    params.random_obj_size = true;
    params.random_read_offset = true;
    params.total_op_count = 10000;
    params.check_value = true;

    return std::make_pair(options, params);
}

// Read Mem
BENCHMARK_CAPTURE(BM_bench_starcache, bench_read_mem, read_mem_suite())->Threads(16);

// Read Disk
BENCHMARK_CAPTURE(BM_bench_starcache, bench_read_disk, read_disk_suite())->Threads(16);

// Read+Write+Remove Disk
BENCHMARK_CAPTURE(BM_bench_starcache, bench_read_write_remove_disk, read_write_remove_disk_suite())->Threads(16);

// Random offset for Read+Write+Remove Disk
BENCHMARK_CAPTURE(BM_bench_starcache, bench_random_offset_read, random_offset_read_suite())->Threads(16);

#ifdef WITH_CACHELIB
BENCHMARK_CAPTURE(BM_bench_cachelib, bench_read_mem, read_mem_suite())->Threads(16);
BENCHMARK_CAPTURE(BM_bench_cachelib, bench_read_disk, read_disk_suite())->Threads(16);
BENCHMARK_CAPTURE(BM_bench_cachelib, bench_read_write_remove_disk, read_write_remove_disk_suite())->Threads(16);
BENCHMARK_CAPTURE(BM_bench_cachelib, bench_random_offset_read, random_offset_read_suite())->Threads(16);
#endif

} // namespace starrocks

//BENCHMARK_MAIN();
int main(int argc, char** argv) {
    starrocks::init_glog("be_bench", true);
    std::filesystem::create_directories(starrocks::DISK_CACHE_PATH);
    starrocks::delete_dir_content(starrocks::DISK_CACHE_PATH);

    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();

    //std::filesystem::remove_all("./bench_dir");
    return 0;
}
