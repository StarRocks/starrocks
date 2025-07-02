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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/bloom_filter.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>
#include <functional>
#include <memory>

#include "common/status.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/strings/substitute.h"
#include "storage/utils.h"
#include "util/hash_util.hpp"
#include "util/murmur_hash3.h"

namespace starrocks {
class Slice;

static const std::string FPP_KEY = "bloom_filter_fpp";
static const std::string GRAM_NUM_KEY = "gram_num";
static const std::string CASE_SENSITIVE_KEY = "case_sensitive";

class Hasher {
public:
    enum HashStrategy {
        HASH_MURMUR3_X64_64 = 0,
        XXHASH64 = 1,
    };
    Hasher() = default;
    virtual ~Hasher() = default;
    virtual uint64_t hash(const void* buf, uint32_t size) const = 0;
    HashStrategy strategy() const { return _strategy; }

private:
    HashStrategy _strategy;
};

class MurmurHasher : public Hasher {
public:
    explicit MurmurHasher(uint32_t seed) : _seed(seed) {}
    uint64_t hash(const void* buf, uint32_t size) const override;

private:
    uint32_t _seed;
};

class XXHasher : public Hasher {
public:
    explicit XXHasher(uint32_t seed) : _seed(seed) {}
    uint64_t hash(const void* buf, uint32_t size) const override;

private:
    uint32_t _seed;
};

class HasherFactory {
public:
    static std::unique_ptr<Hasher> create(Hasher::HashStrategy strategy, uint32_t seed = 0);
};

// used in write
struct BloomFilterOptions {
    // false positive probablity
    double fpp = 0.05;
    HashStrategyPB strategy = HASH_MURMUR3_X64_64;
    bool use_ngram = false;
    // only use when use_ngram is true
    size_t gram_num = 0;
    bool case_sensitive = true;
};

// used in read from ngram bloom filter
struct NgramBloomFilterReaderOptions {
    size_t index_gram_num = 0;
    bool index_case_sensitive = true;
};

struct NgramBloomFilterState {
    bool initialized = false;
    // whether this index can be used for predicate or not
    bool index_useful = false;
    std::vector<std::string> ngram_set;
};

// Base class for bloom filter
// To support null value, the size of bloom filter is optimize bytes + 1.
// The last byte is for null value flag.
class BloomFilter {
public:
    // Default seed for the hash function. It comes from date +%s.
    static const uint32_t DEFAULT_SEED = 1575457558;

    // Minimum Bloom filter size, set to the size of a tiny Bloom filter block
    static const uint32_t MINIMUM_BYTES = 32;

    // Maximum Bloom filter size, set it to half of max segment file size
    static const uint32_t MAXIMUM_BYTES = 128 * 1024 * 1024;

    // Factory function for BloomFilter
    static Status create(BloomFilterAlgorithmPB algorithm, std::unique_ptr<BloomFilter>* bf);

    BloomFilter() = default;

    virtual ~BloomFilter() { delete[] _data; }
    Status init(uint64_t n, double fpp, Hasher::HashStrategy strategy, int seed) {
        _hasher = HasherFactory::create(strategy, seed);
        DCHECK(_hasher);
        _num_bytes = _optimal_bit_num(n, fpp) / 8;
        // make sure _num_bytes is power of 2
        DCHECK((_num_bytes & (_num_bytes - 1)) == 0);
        _size = _num_bytes + 1;
        // reserve last byte for null flag
        _data = new char[_size];
        memset(_data, 0, _size);
        _has_null = (bool*)(_data + _num_bytes);
        *_has_null = false;
        return Status::OK();
    }
    // for write
    Status init(uint64_t n, double fpp, HashStrategyPB strategy) {
        Status ret;
        if (strategy == HashStrategyPB::HASH_MURMUR3_X64_64) {
            ret = init(n, fpp, Hasher::HashStrategy::HASH_MURMUR3_X64_64, DEFAULT_SEED);
        } else {
            return Status::InvalidArgument(strings::Substitute("invalid strategy:$0", strategy));
        }
        return ret;
    }

    Status init(const char* buf, uint32_t size, Hasher::HashStrategy strategy, int seed) {
        DCHECK(size > 1);
        _hasher = HasherFactory::create(strategy, seed);
        DCHECK(_hasher);
        if (size == 0) {
            return Status::InvalidArgument(strings::Substitute("invalid size:$0", size));
        }
        _data = new char[size];
        memcpy(_data, buf, size);
        _size = size;
        _num_bytes = _size - 1;
        _has_null = (bool*)(_data + _num_bytes);
        return Status::OK();
    }

    // for read
    // use deep copy to acquire the data
    Status init(const char* buf, uint32_t size, HashStrategyPB strategy) {
        Status ret;
        if (strategy == HashStrategyPB::HASH_MURMUR3_X64_64) {
            ret = init(buf, size, Hasher::HashStrategy::HASH_MURMUR3_X64_64, DEFAULT_SEED);
        } else {
            return Status::InvalidArgument(strings::Substitute("invalid strategy:$0", strategy));
        }
        return ret;
    }

    void reset() { memset(_data, 0, _size); }

    uint64_t hash(const char* buf, uint32_t size) const { return _hasher->hash(buf, size); }

    void add_bytes(const char* buf, uint32_t size) {
        if (buf == nullptr) {
            *_has_null = true;
            return;
        }
        uint64_t code = hash(buf, size);
        add_hash(code);
    }

    bool test_bytes(const char* buf, uint32_t size) const {
        if (buf == nullptr) {
            return *_has_null;
        }
        uint64_t code = hash(buf, size);
        return test_hash(code);
    }

    char* data() const { return _data; }

    uint32_t num_bytes() const { return _num_bytes; }

    uint32_t size() const { return _size; }

    void set_has_null(bool has_null) { *_has_null = has_null; }

    bool has_null() const { return *_has_null; }

    virtual void add_hash(uint64_t hash) = 0;
    virtual bool test_hash(uint64_t hash) const = 0;

    static uint32_t estimate_bytes(uint64_t n, double fpp) { return _optimal_bit_num(n, fpp) / 8 + 1; }

private:
    // Compute the optimal bit number according to the following rule:
    //     m = -n * ln(fpp) / (ln(2) ^ 2)
    // n: expected distinct record number
    // fpp: false positive probablity
    // the result will be power of 2
    static uint32_t _optimal_bit_num(uint64_t n, double fpp);

protected:
    // bloom filter data
    // specially add one byte for null flag
    char* _data{nullptr};
    // optimal bloom filter num bytes
    // it is calculated by optimal_bit_num() / 8
    uint32_t _num_bytes{0};
    // equal to _num_bytes + 1
    // last byte is for has_null flag
    uint32_t _size{0};
    // last byte's pointer in data for null flag
    bool* _has_null{nullptr};

private:
    std::unique_ptr<Hasher> _hasher;
};

} // namespace starrocks
