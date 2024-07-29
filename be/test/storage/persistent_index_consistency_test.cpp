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

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <cassert>
#include <cstdlib>
#include <random>

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
#include "testutil/deterministic_test_utils.h"
#include "testutil/parallel_test.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "util/logging.h"

DEFINE_bool(debug, false, "debug mode");
DEFINE_int64(seed, -1, "random seed");
DEFINE_int64(second, 60, "custom run second");
DEFINE_bool(fixlen, false, "fix len or not");
DEFINE_int64(max_number, 1000000, "max number");
DEFINE_int64(max_n, 10000, "max N");

namespace starrocks {

enum PICT_OP {
    UPSERT = 0,
    ERASE = 1,
    GET = 2,
    MAJOR_COMPACT = 3,
    REPLACE = 4,
    RELOAD = 5,
    MAX = 6,
};

struct TestParams {
    int64_t max_number = 0;
    int64_t max_n = 0;
    size_t run_second = 0;
    bool is_fixed_key_sz = false;
    bool print_debug_info = false;
};

static const std::string kTestDirectory = "./test_persistent_index_consistency";

template <typename T>
class PersistentIndexWrapper {
public:
    PersistentIndexWrapper() {}

    ~PersistentIndexWrapper() { (void)fs::remove_all(kTestDirectory); }

    Status init() {
        FileSystem* fs = FileSystem::Default();
        CHECK_OK(fs->create_dir_if_missing(kTestDirectory));
        _index_dir = kTestDirectory + "/PersistentIndexConsistencyTest";
        _db_dir = kTestDirectory + "/PersistentIndexConsistencyTest_db";
        (void)fs::remove_all(_index_dir);
        (void)fs::remove_all(_db_dir);
        const std::string kIndexFile = kTestDirectory + "/PersistentIndexConsistencyTest/index.l0.0.0";
        bool created;
        CHECK_OK(fs->create_dir_if_missing(_index_dir, &created));
        CHECK_OK(fs->create_dir_if_missing(_db_dir, &created));
        _kv_store = std::make_unique<KVStore>(_db_dir);
        CHECK_OK(_kv_store->init());
        {
            ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(kIndexFile));
            CHECK_OK(wfile->close());
        }

        // build index
        EditVersion version(_cur_version, 0);
        if (std::is_same<T, std::string>::value) {
            _index_meta.set_key_size(0);
        } else {
            _index_meta.set_key_size(sizeof(T));
        }
        _index_meta.set_size(0);
        version.to_pb(_index_meta.mutable_version());
        MutableIndexMetaPB* l0_meta = _index_meta.mutable_l0_meta();
        l0_meta->set_format_version(PERSISTENT_INDEX_VERSION_4);
        IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
        version.to_pb(snapshot_meta->mutable_version());
        _index = std::make_unique<PersistentIndex>(_index_dir);
        CHECK_OK(_index->load(_index_meta));
        return Status::OK();
    }

    Status reload_index() {
        _index = std::make_unique<PersistentIndex>(_index_dir);
        return _index->load(_index_meta);
    }

    PersistentIndexMetaPB _index_meta;
    std::string _index_dir;
    std::string _db_dir;
    int64_t _cur_version = 0;
    std::unique_ptr<PersistentIndex> _index;
    std::unique_ptr<KVStore> _kv_store;
};

template <typename T>
class Replayer {
public:
    Replayer(bool print_debug_info) : _print_debug_info(print_debug_info) {}
    void upsert(int64_t N, const vector<T>& keys, const vector<IndexValue>& values,
                std::vector<IndexValue>* old_values) {
        for (int i = 0; i < N; i++) {
            auto result = _replayer_index.insert(std::make_pair(keys[i], values[i]));
            if (result.second) {
                // success
                (*old_values)[i] = IndexValue(NullIndexValue);
            } else {
                (*old_values)[i] = result.first->second;
                // cover it
                result.first->second = values[i];
            }
            if (_print_debug_info) {
                LOG(INFO) << "upsert: index: " << i << " key: " << keys[i] << " value: " << values[i].get_value();
            }
        }
        LOG(INFO) << "Replayer [Upsert] sequence: " << _seq_no++;
    }

    void erase(int64_t N, const vector<T>& keys, std::vector<IndexValue>* old_values) {
        for (int i = 0; i < N; i++) {
            auto it = _replayer_index.find(keys[i]);
            if (it != _replayer_index.end()) {
                // erase and set old value
                (*old_values)[i] = it->second;
                _replayer_index.erase(it);
            } else {
                (*old_values)[i] = IndexValue(NullIndexValue);
            }
            if (_print_debug_info) {
                LOG(INFO) << "erase: index: " << i << " key: " << keys[i];
            }
        }
        LOG(INFO) << "Replayer [Erase] sequence: " << _seq_no++;
    }

    void get(int64_t N, const vector<T>& keys, std::vector<IndexValue>* old_values) {
        for (int i = 0; i < N; i++) {
            auto it = _replayer_index.find(keys[i]);
            if (it != _replayer_index.end()) {
                (*old_values)[i] = it->second;
            } else {
                (*old_values)[i] = IndexValue(NullIndexValue);
            }
            if (_print_debug_info) {
                LOG(INFO) << "get: index: " << i << " key: " << keys[i] << " value: " << (*old_values)[i].get_value();
            }
        }
        LOG(INFO) << "Replayer [Get] sequence: " << _seq_no++;
    }

    void try_replace(int64_t N, const vector<T>& keys, const vector<IndexValue>& values,
                     std::vector<uint32_t>* failed) {
        for (int i = 0; i < N; i++) {
            auto it = _replayer_index.find(keys[i]);
            if (it != _replayer_index.end()) {
                // cover it
                it->second = values[i];
            } else {
                failed->emplace_back(values[i].get_value() & 0xFFFFFFFF);
            }
            if (_print_debug_info) {
                LOG(INFO) << "try_replace: index: " << i << " key: " << keys[i] << " value: " << values[i].get_value();
            }
        }
        LOG(INFO) << "Replayer [TryReplace] sequence: " << _seq_no++;
    }

private:
    std::map<T, IndexValue> _replayer_index;
    uint64_t _seq_no = 0;
    bool _print_debug_info = false;
};

class Checker {
public:
    Checker(int seed) : _seed(seed) {}
    ~Checker() = default;

    void check(const vector<IndexValue>& current, const vector<IndexValue>& expected) {
        if (current.size() != expected.size()) {
            LOG(FATAL) << "current size: " << current.size() << " expected size: " << expected.size()
                       << " seed: " << _seed;
        }
        for (int i = 0; i < current.size(); i++) {
            if (current[i] != expected[i]) {
                LOG(FATAL) << "index: " << i << " current : " << current[i].get_value()
                           << " expected : " << expected[i].get_value() << " seed: " << _seed;
            }
        }
    }
    void check(const vector<uint32_t>& current, const vector<uint32_t>& expected) {
        if (current.size() != expected.size()) {
            LOG(FATAL) << "current size: " << current.size() << " expected size: " << expected.size()
                       << " seed: " << _seed;
        }
        for (int i = 0; i < current.size(); i++) {
            if (current[i] != expected[i]) {
                LOG(FATAL) << "index: " << i << " current : " << current[i] << " expected : " << expected[i]
                           << " seed: " << _seed;
            }
        }
    }

private:
    int _seed = 0;
};

template <typename T>
class PictOpBase {
public:
    virtual ~PictOpBase() = 0;
    virtual Status run(DeterRandomGenerator<T, PICT_OP>* rand, PersistentIndexWrapper<T>* index, Replayer<T>* replayer,
                       Checker* checker) = 0;
};

template <typename T>
PictOpBase<T>::~PictOpBase() = default;

template <typename T>
class PictOpUpsert : public PictOpBase<T> {
public:
    Status run(DeterRandomGenerator<T, PICT_OP>* rand, PersistentIndexWrapper<T>* index, Replayer<T>* replayer,
               Checker* checker) override {
        index->_cur_version++;
        IOStat stat;
        int64_t N = rand->random_n();

        vector<T> keys;
        vector<Slice> key_slices;
        vector<IndexValue> values;
        rand->random_keys_values(N, &keys, &key_slices, &values);
        std::vector<IndexValue> old_values(N, IndexValue(NullIndexValue));
        CHECK_OK(index->_index->prepare(EditVersion(index->_cur_version, 0), N));
        CHECK_OK(index->_index->upsert(N, key_slices.data(), values.data(), old_values.data(), &stat));
        CHECK_OK(index->_index->commit(&(index->_index_meta), &stat));
        CHECK_OK(index->_index->on_commited());
        std::vector<IndexValue> expected_old_values(N, IndexValue(NullIndexValue));
        replayer->upsert(N, keys, values, &expected_old_values);
        checker->check(old_values, expected_old_values);
        return Status::OK();
    }
};

template <typename T>
class PictOpErase : public PictOpBase<T> {
public:
    Status run(DeterRandomGenerator<T, PICT_OP>* rand, PersistentIndexWrapper<T>* index, Replayer<T>* replayer,
               Checker* checker) override {
        index->_cur_version++;
        IOStat stat;
        int64_t N = rand->random_n();

        vector<T> keys;
        vector<Slice> key_slices;
        rand->random_keys_values(N, &keys, &key_slices, nullptr);
        std::vector<IndexValue> old_values(N, IndexValue(NullIndexValue));
        CHECK_OK(index->_index->prepare(EditVersion(index->_cur_version, 0), N));
        CHECK_OK(index->_index->erase(N, key_slices.data(), old_values.data()));
        CHECK_OK(index->_index->commit(&(index->_index_meta), &stat));
        CHECK_OK(index->_index->on_commited());
        std::vector<IndexValue> expected_old_values(N, IndexValue(NullIndexValue));
        replayer->erase(N, keys, &expected_old_values);
        checker->check(old_values, expected_old_values);
        return Status::OK();
    }
};

template <typename T>
class PictOpGet : public PictOpBase<T> {
public:
    Status run(DeterRandomGenerator<T, PICT_OP>* rand, PersistentIndexWrapper<T>* index, Replayer<T>* replayer,
               Checker* checker) override {
        int64_t N = rand->random_n();

        vector<T> keys;
        vector<Slice> key_slices;
        rand->random_keys_values(N, &keys, &key_slices, nullptr);
        std::vector<IndexValue> old_values(N, IndexValue(NullIndexValue));
        CHECK_OK(index->_index->get(N, key_slices.data(), old_values.data()));
        std::vector<IndexValue> expected_old_values(N, IndexValue(NullIndexValue));
        replayer->get(N, keys, &expected_old_values);
        checker->check(old_values, expected_old_values);
        return Status::OK();
    }
};

template <typename T>
class PictOpMajorCompact : public PictOpBase<T> {
public:
    Status run(DeterRandomGenerator<T, PICT_OP>* rand, PersistentIndexWrapper<T>* index, Replayer<T>* replayer,
               Checker* checker) override {
        return index->_index->TEST_major_compaction(index->_index_meta);
    }
};

template <typename T>
class PictOpReplace : public PictOpBase<T> {
public:
    Status run(DeterRandomGenerator<T, PICT_OP>* rand, PersistentIndexWrapper<T>* index, Replayer<T>* replayer,
               Checker* checker) override {
        index->_cur_version++;
        IOStat stat;
        int64_t N = rand->random_n();

        vector<T> keys;
        vector<Slice> key_slices;
        vector<IndexValue> values;
        rand->random_keys_values(N, &keys, &key_slices, &values);
        std::vector<uint32_t> failed;
        CHECK_OK(index->_index->prepare(EditVersion(index->_cur_version, 0), N));
        CHECK_OK(index->_index->try_replace(N, key_slices.data(), values.data(), UINT32_MAX, &failed));
        CHECK_OK(index->_index->commit(&(index->_index_meta), &stat));
        CHECK_OK(index->_index->on_commited());
        std::vector<uint32_t> expected_failed;
        replayer->try_replace(N, keys, values, &expected_failed);
        checker->check(failed, expected_failed);
        return Status::OK();
    }
};

template <typename T>
class PictOpReload : public PictOpBase<T> {
public:
    Status run(DeterRandomGenerator<T, PICT_OP>* rand, PersistentIndexWrapper<T>* index, Replayer<T>* replayer,
               Checker* checker) override {
        return index->reload_index();
    }
};

template <typename T>
class PersistentIndexConsistencyTest {
public:
    PersistentIndexConsistencyTest() {
        // 5% of whole data
        _old_l0_size = config::l0_max_mem_usage;
        config::l0_max_mem_usage = _params.max_number * 16 * 5 / 100;
    }

    ~PersistentIndexConsistencyTest() { config::l0_max_mem_usage = _old_l0_size; }

    void init(const TestParams& params, int seed) {
        _params = params;
        _rand = std::make_unique<DeterRandomGenerator<T, PICT_OP>>(params.max_number, params.max_n, seed, 0, INT64_MAX);
        _index_wrapper = std::make_unique<PersistentIndexWrapper<T>>();
        CHECK_OK(_index_wrapper->init());
        _replayer = std::make_unique<Replayer<T>>(params.print_debug_info);
        _checker = std::make_unique<Checker>(seed);
        std::vector<WeightedItem<PICT_OP>> items;
        items.emplace_back(PICT_OP::UPSERT, 30);
        items.emplace_back(PICT_OP::ERASE, 15);
        items.emplace_back(PICT_OP::GET, 30);
        items.emplace_back(PICT_OP::MAJOR_COMPACT, 5);
        items.emplace_back(PICT_OP::REPLACE, 15);
        items.emplace_back(PICT_OP::RELOAD, 5);
        _op_selector = std::make_unique<WeightedRandomOpSelector<T, PICT_OP>>(_rand.get(), items);
    }

    void run() {
        size_t start_second = time(nullptr);
        do {
            std::unique_ptr<PictOpBase<T>> op = generate_op();
            CHECK_OK(op->run(_rand.get(), _index_wrapper.get(), _replayer.get(), _checker.get()));
        } while (start_second + _params.run_second >= time(nullptr));
    }

    std::unique_ptr<PictOpBase<T>> generate_op() {
        PICT_OP op = _op_selector->select();
        if (op == UPSERT) {
            return std::make_unique<PictOpUpsert<T>>();
        } else if (op == ERASE) {
            return std::make_unique<PictOpErase<T>>();
        } else if (op == GET) {
            return std::make_unique<PictOpGet<T>>();
        } else if (op == MAJOR_COMPACT) {
            return std::make_unique<PictOpMajorCompact<T>>();
        } else if (op == REPLACE) {
            return std::make_unique<PictOpReplace<T>>();
        } else if (op == RELOAD) {
            return std::make_unique<PictOpReload<T>>();
        }
        return nullptr;
    }

private:
    TestParams _params;
    std::unique_ptr<DeterRandomGenerator<T, PICT_OP>> _rand;
    std::unique_ptr<PersistentIndexWrapper<T>> _index_wrapper;
    std::unique_ptr<Replayer<T>> _replayer;
    std::unique_ptr<Checker> _checker;
    std::unique_ptr<WeightedRandomOpSelector<T, PICT_OP>> _op_selector;
    int64_t _old_l0_size = 0;
};

class PersistentIndexConsistencyTestTest : public testing::Test {
public:
    PersistentIndexConsistencyTestTest() {}
    static void SetUpTestCase() {}

    static void TearDownTestCase() {}
};

static void test_func(int64_t max_number, int64_t max_n, size_t run_second, int seed, bool is_fixed_size) {
    TestParams params;
    params.max_number = max_number;
    params.max_n = max_n;
    params.run_second = run_second;
    params.print_debug_info = FLAGS_debug;
    if (is_fixed_size) {
        // fixed size
        PersistentIndexConsistencyTest<uint64_t> pict;
        pict.init(params, seed);
        pict.run();
    } else {
        PersistentIndexConsistencyTest<std::string> pict;
        pict.init(params, seed);
        pict.run();
    }
}

TEST_F(PersistentIndexConsistencyTestTest, test_local_persistent_index) {
    int seed = time(nullptr);
    test_func(FLAGS_max_number, FLAGS_max_n, FLAGS_second, seed, true);
    // regenerate seed
    seed = time(nullptr);
    test_func(FLAGS_max_number, FLAGS_max_n, FLAGS_second, seed, false);
}

} // namespace starrocks