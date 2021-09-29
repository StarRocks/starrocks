// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/hll.h

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

#ifndef STARROCKS_BE_SRC_OLAP_HLL_H
#define STARROCKS_BE_SRC_OLAP_HLL_H

#include <cmath>
#include <cstdio>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "common/compiler_util.h"
#include "common/logging.h"
#include "gutil/macros.h"
#include "runtime/memory/chunk.h"
#include "runtime/memory/chunk_allocator.h"
#include "util/phmap/phmap.h"

namespace starrocks {

class Slice;

const static int HLL_COLUMN_PRECISION = 14;
const static int HLL_ZERO_COUNT_BITS = (64 - HLL_COLUMN_PRECISION);
const static int HLL_EXPLICLIT_INT64_NUM = 160;
const static int HLL_SPARSE_THRESHOLD = 4096;
const static int HLL_REGISTERS_COUNT = 16 * 1024;
// maximum size in byte of serialized HLL: type(1) + registers (2^14)
const static int HLL_COLUMN_DEFAULT_LEN = HLL_REGISTERS_COUNT + 1;

// 1 for type; 1 for hash values count; 8 for hash value
const static int HLL_SINGLE_VALUE_SIZE = 10;
const static int HLL_EMPTY_SIZE = 1;

// Hyperloglog distinct estimate algorithm.
// See these papers for more details.
// 1) Hyperloglog: The analysis of a near-optimal cardinality estimation
// algorithm (2007)
// 2) HyperLogLog in Practice (paper from google with some improvements)

// Each HLL value is a set of values. To save space, StarRocks store HLL value
// in different format according to its cardinality.
//
// HLL_DATA_EMPTY: when set is empty.
//
// HLL_DATA_EXPLICIT: when there is only few values in set, store these values explicit.
// If the number of hash values is not greater than 160, set is encoded in this format.
// The max space occupied is (1 + 1 + 160 * 8) = 1282. I don't know why 160 is choosed,
// maybe can be other number. If you are interested, you can try other number and see
// if it will be better.
//
// HLL_DATA_SPARSE: only store non-zero registers. If the number of non-zero registers
// is not greater than 4096, set is encoded in this format. The max space occupied is
// (1 + 4 + 3 * 4096) = 12293.
//
// HLL_DATA_FULL: most space-consuming, store all registers
//
// A HLL value will change in the sequence empty -> explicit -> sparse -> full, and not
// allow reverse.
//
// NOTE: This values are persisted in storage devices, so don't change exist
// enum values.
enum HllDataType {
    HLL_DATA_EMPTY = 0,
    HLL_DATA_EXPLICIT = 1,
    HLL_DATA_SPARSE = 2,
    HLL_DATA_FULL = 3,
};

class HyperLogLog {
public:
    HyperLogLog() = default;

    HyperLogLog(const HyperLogLog& other) : _type(other._type), _hash_set(other._hash_set) {
        if (_registers.data != nullptr) {
            ChunkAllocator::instance()->free(_registers);
            _registers.data = nullptr;
        }

        if (other._registers.data != nullptr) {
            ChunkAllocator::instance()->allocate(HLL_REGISTERS_COUNT, &_registers);
            DCHECK_NE(_registers.data, nullptr);
            DCHECK_EQ(_registers.size, HLL_REGISTERS_COUNT);
            memcpy(_registers.data, other._registers.data, HLL_REGISTERS_COUNT);
        }
    }

    HyperLogLog& operator=(const HyperLogLog& other) {
        if (this != &other) {
            this->_type = other._type;
            this->_hash_set = other._hash_set;

            if (_registers.data != nullptr) {
                ChunkAllocator::instance()->free(_registers);
                _registers.data = nullptr;
            }

            if (other._registers.data != nullptr) {
                ChunkAllocator::instance()->allocate(HLL_REGISTERS_COUNT, &_registers);
                DCHECK_NE(_registers.data, nullptr);
                DCHECK_EQ(_registers.size, HLL_REGISTERS_COUNT);
                memcpy(_registers.data, other._registers.data, HLL_REGISTERS_COUNT);
            }
        }
        return *this;
    }

    HyperLogLog(HyperLogLog&& other) noexcept : _type(other._type), _hash_set(std::move(other._hash_set)) {
        if (_registers.data != nullptr) {
            ChunkAllocator::instance()->free(_registers);
        }
        _registers = other._registers;

        other._type = HLL_DATA_EMPTY;
        other._registers.data = nullptr;
    }

    HyperLogLog& operator=(HyperLogLog&& other) noexcept {
        if (this != &other) {
            this->_type = other._type;
            this->_hash_set = std::move(other._hash_set);

            if (_registers.data != nullptr) {
                ChunkAllocator::instance()->free(_registers);
            }
            _registers = other._registers;

            other._type = HLL_DATA_EMPTY;
            other._registers.data = nullptr;
        }
        return *this;
    }

    explicit HyperLogLog(uint64_t hash_value) : _type(HLL_DATA_EXPLICIT) { _hash_set.emplace(hash_value); }

    explicit HyperLogLog(const Slice& src);

    ~HyperLogLog();

    typedef uint8_t SetTypeValueType;
    typedef int32_t SparseLengthValueType;
    typedef uint16_t SparseIndexType;
    typedef uint8_t SparseValueType;

    // Add a hash value to this HLL value
    // NOTE: input must be a hash_value
    void update(uint64_t hash_value);

    void merge(const HyperLogLog& other);

    // Return max size of serialized binary
    size_t max_serialized_size() const;

    // Input slice should has enough capacity for serialize, which
    // can be get through max_serialized_size(). If insufficient buffer
    // is given, this will cause process crash.
    // Return actual size of serialized binary.
    size_t serialize(uint8_t* dst) const;

    // Now, only empty HLL support this funciton.
    bool deserialize(const Slice& slice);

    int64_t estimate_cardinality() const;

    static std::string empty() {
        static HyperLogLog hll;
        std::string buf;
        buf.resize(HLL_EMPTY_SIZE);
        hll.serialize((uint8_t*)buf.c_str());
        return buf;
    }

    // Check if input slice is a valid serialized binary of HyperLogLog.
    // This function only check the encoded type in slice, whose complex
    // function is O(1).
    static bool is_valid(const Slice& slice);

    // only for debug
    std::string to_string() const;

    uint64_t serialize_size() const { return max_serialized_size(); }

    // common interface
    void clear() {
        _type = HLL_DATA_EMPTY;
        _hash_set.clear();
    }

private:
    HllDataType _type = HLL_DATA_EMPTY;
    phmap::flat_hash_set<uint64_t> _hash_set;

    // This field is much space consumming(HLL_REGISTERS_COUNT), we create
    // it only when it is really needed.
    // Allocate memory by ChunkAllocator in order to reuse memory.
    Chunk _registers;

private:
    void _convert_explicit_to_register();

    // absorb other registers into this registers
    void _merge_registers(uint8_t* other_registers);

    // update one hash value into this registers
    void _update_registers(uint64_t hash_value) {
        // Use the lower bits to index into the number of streams and then
        // find the first 1 bit after the index bits.
        int idx = hash_value % HLL_REGISTERS_COUNT;
        hash_value >>= HLL_COLUMN_PRECISION;
        // make sure max first_one_bit is HLL_ZERO_COUNT_BITS + 1
        hash_value |= ((uint64_t)1 << HLL_ZERO_COUNT_BITS);
        uint8_t first_one_bit = __builtin_ctzl(hash_value) + 1;
        _registers.data[idx] = std::max((uint8_t)_registers.data[idx], first_one_bit);
    }
};

// todo(kks): remove this when dpp_sink class was removed
class HllSetResolver {
public:
    HllSetResolver()

    {}

    ~HllSetResolver() = default;

    typedef uint8_t SetTypeValueType;
    typedef uint8_t ExpliclitLengthValueType;
    typedef int32_t SparseLengthValueType;
    typedef uint16_t SparseIndexType;
    typedef uint8_t SparseValueType;

    // only save pointer
    void init(char* buf, int len) {
        this->_buf_ref = buf;
        this->_buf_len = len;
    }

    // hll set type
    HllDataType get_hll_data_type() { return _set_type; };

    // explicit value num
    int get_explicit_count() const { return (int)_explicit_num; };

    // get explicit index value 64bit
    uint64_t get_explicit_value(int index) {
        if (index >= _explicit_num) {
            return -1;
        }
        return _explicit_value[index];
    };

    // get full register value
    char* get_full_value() { return _full_value_position; };

    // get (index, value) map
    std::map<SparseIndexType, SparseValueType>& get_sparse_map() { return _sparse_map; };

    // parse set , call after copy() or init()
    void parse();

private:
    char* _buf_ref{nullptr};               // set
    int _buf_len{0};                       // set len
    HllDataType _set_type{HLL_DATA_EMPTY}; //set type
    char* _full_value_position{nullptr};
    uint64_t* _explicit_value{nullptr};
    ExpliclitLengthValueType _explicit_num{0};
    std::map<SparseIndexType, SparseValueType> _sparse_map;
    SparseLengthValueType* _sparse_count{nullptr};
};

// todo(kks): remove this when dpp_sink class was removed
class HllSetHelper {
public:
    static void set_sparse(char* result, const std::map<int, uint8_t>& index_to_value, int* len);
    static void set_explicit(char* result, const std::set<uint64_t>& hash_value_set, int* len);
    static void set_full(char* result, const std::map<int, uint8_t>& index_to_value, int set_len, int* len);
};

} // namespace starrocks

#endif // STARROCKS_BE_SRC_OLAP_HLL_H
