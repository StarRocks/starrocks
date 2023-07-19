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

#include "fs/fs_memory.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/persistent_index.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset_update_state.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/update_manager.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"
#include "util/coding.h"
#include "util/faststring.h"

namespace starrocks {

struct BenchParams {
    size_t key_size;
    uint64_t total_record;
    uint64_t each_upsert_record;
};

#define ASSERT_CHECK(stmt)      \
    do {                        \
        Status st__ = stmt;     \
        if (!st__.ok()) {       \
            LOG(ERROR) << st__; \
        }                       \
        assert(st__.ok());      \
    } while (0)

class PersistentIndexBenchTest {
public:
    using Key = std::string;
    void SetUp() {}
    void TearDown() {}

    PersistentIndexBenchTest(BenchParams params) {
        FileSystem* fs = FileSystem::Default();
        _index_dir = "./PersistentIndexBenchTest";
        _params = params;
        const std::string kIndexFile = "./PersistentIndexBenchTest/index.l0.0.0";
        bool created;
        ASSERT_CHECK(fs->create_dir_if_missing(_index_dir, &created));

        {
            ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(kIndexFile));
            ASSERT_CHECK(wfile->close());
        }

        // build index
        EditVersion version(_cur_version++, 0);
        _index_meta.set_key_size(params.key_size);
        _index_meta.set_size(0);
        version.to_pb(_index_meta.mutable_version());
        MutableIndexMetaPB* l0_meta = _index_meta.mutable_l0_meta();
        IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
        version.to_pb(snapshot_meta->mutable_version());

        _index = std::make_unique<PersistentIndex>(_index_dir);
    }

    ~PersistentIndexBenchTest() { fs::remove_all(_index_dir); }

    void do_bench(benchmark::State& state);
    void do_verify();

private:
    PersistentIndexMetaPB _index_meta;
    std::string _index_dir;
    BenchParams _params;
    int64_t _cur_version = 0;
    std::unique_ptr<PersistentIndex> _index;
    std::vector<std::thread> _worker;

    IOStat _total_stat;
    uint64_t _long_tail_stat = 0;
};

void PersistentIndexBenchTest::do_verify() {
    // verify
    vector<Key> keys(_params.total_record);
    vector<Slice> key_slices(_params.total_record);
    vector<IndexValue> values(_params.total_record);
    for (int i = 0; i < _params.total_record; i++) {
        keys[i] = "persistent_index_bench_" + std::to_string(i);
        values[i] = i;
        key_slices[i] = keys[i];
    }
    std::vector<IndexValue> get_values(keys.size());
    ASSERT_CHECK(_index->get(keys.size(), key_slices.data(), get_values.data()));
    assert(keys.size() == get_values.size());
    for (int i = 0; i < values.size(); i++) {
        assert(values[i] == get_values[i]);
    }
    LOG(INFO) << "verify success";
}

void PersistentIndexBenchTest::do_bench(benchmark::State& state) {
    const uint64_t total_step = _params.total_record / _params.each_upsert_record;
    vector<Key> keys(_params.each_upsert_record);
    vector<Slice> key_slices(_params.each_upsert_record);
    vector<IndexValue> values(_params.each_upsert_record);
    auto incre_key = [&](int step) {
        for (int i = 0; i < _params.each_upsert_record; i++) {
            keys[i] = "persistent_index_bench_" + std::to_string(i + step * _params.each_upsert_record);
            values[i] = i + step * _params.each_upsert_record;
            key_slices[i] = keys[i];
        }
    };
    ASSERT_CHECK(_index->load(_index_meta));

    // upsert
    for (int i = 0; i < total_step; i++) {
        incre_key(i);
        IOStat stat;
        std::vector<IndexValue> old_values(_params.each_upsert_record, IndexValue(NullIndexValue));
        ASSERT_CHECK(_index->prepare(EditVersion(_cur_version++, 0), _params.each_upsert_record));
        MonotonicStopWatch watch;
        watch.start();
        ASSERT_CHECK(
                _index->upsert(_params.each_upsert_record, key_slices.data(), values.data(), old_values.data(), &stat));
        ASSERT_CHECK(_index->commit(&_index_meta, &stat));
        uint64_t tail = watch.elapsed_time();
        ASSERT_CHECK(_index->on_commited());
        if (config::enable_pindex_async_compaction) {
            if (_index->get_write_amp_score() > 0.0) {
                ASSERT_CHECK(_index->TEST_bg_compaction(_index_meta));
            }
        }
        if (stat.compaction_cost > 0) {
            LOG(INFO) << stat.print_str();
        }
        _long_tail_stat = std::max(tail, _long_tail_stat);
    }

    // print result
    LOG(INFO) << fmt::format(
            "PersistentIndexBench result, l0_write_cost: {} l1_l2_read_cost: {} flush_or_wal_cost: {} compaction_cost: "
            "{} reload_meta_cost: {} long_tail_cost: {}",
            _total_stat.l0_write_cost / total_step, _total_stat.l1_l2_read_cost / total_step,
            _total_stat.flush_or_wal_cost / total_step, _total_stat.compaction_cost / total_step,
            _total_stat.reload_meta_cost / total_step, _long_tail_stat);
    // verify
    do_verify();
}

static void bench_func(benchmark::State& state) {
    BenchParams params;
    params.key_size = 0;
    params.total_record = state.range(0);
    params.each_upsert_record = state.range(1);

    PersistentIndexBenchTest perf(params);
    perf.do_bench(state);
}

static void process_args(benchmark::internal::Benchmark* b) {
    config::l0_l1_merge_ratio = 10;
    config::l0_max_file_size = 209715200;
    config::l0_max_mem_usage = 67108864;
    config::l0_snapshot_size = 16777216;
    config::max_tmp_l1_num = 10;
    config::enable_parallel_get_and_bf = false;
    config::enable_pindex_async_compaction = true;
    config::max_allow_pindex_l2_num = 5;
    config::pindex_bg_compaction_num_threads = 2;
    b->Args({10000000, 5000, 1})->Iterations(1);
}

BENCHMARK(bench_func)->Apply(process_args);

} // namespace starrocks

BENCHMARK_MAIN();