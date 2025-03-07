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

#include "testutil/assert.h"
#include "testutil/parallel_test.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "util/logging.h"

namespace starrocks {

template <typename PICT_OP>
class WeightedItem {
public:
    PICT_OP id;
    double weight;

    WeightedItem(PICT_OP id, double weight) : id(id), weight(weight) {}
};

// There are several random val we need to generate:
// 1. op type
// 2. Batch entry count
// 3. Keys and Values
template <typename T, typename PICT_OP>
class DeterRandomGenerator {
public:
    DeterRandomGenerator(int64_t max_number, int64_t max_n, int seed, int64_t s, int64_t e)
            : _max_number(max_number), _max_n(max_n), _rng(seed), _distribution(s, e) {}
    ~DeterRandomGenerator() = default;

    int64_t random() { return _distribution(_rng); }

    int64_t random_n() { return (random() % _max_n) + 1; }

    int64_t random_number(std::set<int64_t>& filter) {
        int64_t r = 0;
        do {
            r = random() % _max_number;
        } while (filter.count(r) > 0);
        filter.insert(r);
        return r;
    }

    int64_t random_number() { return random() % _max_number; }

    // Template specialization for std::string
    template <typename U = T>
    typename std::enable_if<std::is_same<U, std::string>::value>::type assign_value(T& key, Slice& key_slice,
                                                                                    std::set<int64_t>& filter) {
        key = fmt::format("{:016x}", random_number(filter));
        key_slice = key;
    }

    // Template specialization for integer
    template <typename U = T>
    typename std::enable_if<std::is_integral<U>::value>::type assign_value(T& key, Slice& key_slice,
                                                                           std::set<int64_t>& filter) {
        key = random_number(filter);
        key_slice = Slice((uint8_t*)(&key), sizeof(T));
    }

    // vector<T> keys(N);
    // vector<IndexValue> values(N);
    void random_keys_values(int64_t N, vector<T>* keys, vector<Slice>* key_slices, vector<IndexValue>* values) {
        keys->resize(N);
        key_slices->resize(N);
        if (values != nullptr) values->resize(N);
        std::set<int64_t> filter;
        for (int i = 0; i < N; i++) {
            assign_value((*keys)[i], (*key_slices)[i], filter);
            if (values != nullptr) (*values)[i] = random_number();
        }
    }

    void random_cols(int64_t N, vector<vector<int>>* cols) {
        for (auto& each_col : *cols) {
            each_col.resize(N);
            for (int i = 0; i < N; i++) {
                each_col[i] = (int)random_number();
            }
        }
    }

private:
    int64_t _max_number = 0;
    int64_t _max_n = 0;
    std::mt19937 _rng;
    std::uniform_int_distribution<int64_t> _distribution;
};

template <typename T, typename PICT_OP>
class WeightedRandomOpSelector {
public:
    WeightedRandomOpSelector(DeterRandomGenerator<T, PICT_OP>* ran_generator,
                             const std::vector<WeightedItem<PICT_OP>>& items)
            : _ran_generator(ran_generator), _items(items.begin(), items.end()) {}

    PICT_OP select() {
        double totalWeight = 0.0;

        for (const auto& item : _items) {
            totalWeight += item.weight;
        }

        double randomValue = ((double)(_ran_generator->random() % 100) / (double)100) * totalWeight;

        double currentWeight = 0.0;
        for (const auto& item : _items) {
            currentWeight += item.weight;
            if (randomValue <= currentWeight) {
                return item.id;
            }
        }

        return _items.back().id;
    }

private:
    DeterRandomGenerator<T, PICT_OP>* _ran_generator;
    std::vector<WeightedItem<PICT_OP>> _items;
};

class IOFailureGuard {
public:
    IOFailureGuard() { SyncPoint::GetInstance()->EnableProcessing(); }
    ~IOFailureGuard() { SyncPoint::GetInstance()->DisableProcessing(); }
};

template <typename T, typename PICT_OP>
class IOFailureGenerator {
public:
    IOFailureGenerator(DeterRandomGenerator<T, PICT_OP>* ran_generator, int64_t percent)
            : _ran_generator(ran_generator), _percent(percent) {
        TEST_ENABLE_ERROR_POINT("PosixFileSystem::appendv", Status::IOError("injected appendv error"));
        TEST_ENABLE_ERROR_POINT("PosixFileSystem::pre_allocate", Status::IOError("injected pre_allocate error"));
        TEST_ENABLE_ERROR_POINT("PosixFileSystem::close", Status::IOError("injected close error"));
        TEST_ENABLE_ERROR_POINT("PosixFileSystem::flush", Status::IOError("injected flush error"));
        TEST_ENABLE_ERROR_POINT("PosixFileSystem::sync", Status::IOError("injected sync error"));
    }
    ~IOFailureGenerator() {
        TEST_DISABLE_ERROR_POINT("PosixFileSystem::appendv");
        TEST_DISABLE_ERROR_POINT("PosixFileSystem::pre_allocate");
        TEST_DISABLE_ERROR_POINT("PosixFileSystem::close");
        TEST_DISABLE_ERROR_POINT("PosixFileSystem::flush");
        TEST_DISABLE_ERROR_POINT("PosixFileSystem::sync");
    }

    std::unique_ptr<IOFailureGuard> generate() {
        auto r = _ran_generator->random() % 100;
        if (r < _percent) {
            return std::make_unique<IOFailureGuard>();
        }
        return nullptr;
    }

private:
    DeterRandomGenerator<T, PICT_OP>* _ran_generator;
    int64_t _percent = 0;
};

} // namespace starrocks