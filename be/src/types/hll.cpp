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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/hll.cpp

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

#include "types/hll.h"

#ifdef __x86_64__
#include <immintrin.h>
#endif

#include <cmath>
#include <map>

#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "runtime/string_value.h"
#include "util/coding.h"
#include "util/phmap/phmap.h"

using std::map;
using std::nothrow;
using std::string;
using std::stringstream;

namespace starrocks {

std::string HyperLogLog::empty() {
    std::string buf;
    buf.resize(HLL_EMPTY_SIZE);
    (*(uint8_t*)buf.c_str()) = HLL_DATA_EMPTY;
    return buf;
}

HyperLogLog::HyperLogLog(const HyperLogLog& other) : _type(other._type), _hash_set(other._hash_set) {
    if (other._registers.data != nullptr) {
        MemChunkAllocator::instance()->allocate(HLL_REGISTERS_COUNT, &_registers);
        DCHECK_NE(_registers.data, nullptr);
        DCHECK_EQ(_registers.size, HLL_REGISTERS_COUNT);
        memcpy(_registers.data, other._registers.data, HLL_REGISTERS_COUNT);
    }
}

HyperLogLog& HyperLogLog::operator=(const HyperLogLog& other) {
    if (this != &other) {
        this->_type = other._type;
        this->_hash_set = other._hash_set;

        if (_registers.data != nullptr) {
            MemChunkAllocator::instance()->free(_registers);
            _registers.data = nullptr;
        }

        if (other._registers.data != nullptr) {
            MemChunkAllocator::instance()->allocate(HLL_REGISTERS_COUNT, &_registers);
            DCHECK_NE(_registers.data, nullptr);
            DCHECK_EQ(_registers.size, HLL_REGISTERS_COUNT);
            memcpy(_registers.data, other._registers.data, HLL_REGISTERS_COUNT);
        }
    }
    return *this;
}

HyperLogLog::HyperLogLog(HyperLogLog&& other) noexcept : _type(other._type), _hash_set(std::move(other._hash_set)) {
    _registers = other._registers;

    other._type = HLL_DATA_EMPTY;
    other._registers.data = nullptr;
}

HyperLogLog& HyperLogLog::operator=(HyperLogLog&& other) noexcept {
    if (this != &other) {
        this->_type = other._type;
        this->_hash_set = std::move(other._hash_set);

        if (_registers.data != nullptr) {
            MemChunkAllocator::instance()->free(_registers);
        }
        _registers = other._registers;

        other._type = HLL_DATA_EMPTY;
        other._registers.data = nullptr;
    }
    return *this;
}

HyperLogLog::HyperLogLog(uint64_t hash_value) : _type(HLL_DATA_EXPLICIT) {
    _hash_set.emplace(hash_value);
}

HyperLogLog::HyperLogLog(const Slice& src) {
    // When deserialize return false, we make this object a empty
    if (!deserialize(src)) {
        _type = HLL_DATA_EMPTY;
    }
}

HyperLogLog::~HyperLogLog() {
    if (_registers.data != nullptr) {
        DCHECK_EQ(_registers.size, HLL_REGISTERS_COUNT);
        MemChunkAllocator::instance()->free(_registers);
    }
}

// Convert explicit values to register format, and clear explicit values.
// NOTE: this function won't modify _type.
void HyperLogLog::_convert_explicit_to_register() {
    DCHECK(_type == HLL_DATA_EXPLICIT) << "_type(" << _type << ") should be explicit(" << HLL_DATA_EXPLICIT << ")";
    DCHECK_EQ(_registers.data, nullptr);
    MemChunkAllocator::instance()->allocate(HLL_REGISTERS_COUNT, &_registers);
    DCHECK_NE(_registers.data, nullptr);
    DCHECK_EQ(_registers.size, HLL_REGISTERS_COUNT);
    memset(_registers.data, 0, HLL_REGISTERS_COUNT);

    for (auto value : _hash_set) {
        _update_registers(value);
    }

    // Clear _hash_set.
    phmap::flat_hash_set<uint64_t>().swap(_hash_set);
}

// Change HLL_DATA_EXPLICIT to HLL_DATA_FULL directly, because HLL_DATA_SPARSE
// is implemented in the same way in memory with HLL_DATA_FULL.
void HyperLogLog::update(uint64_t hash_value) {
    switch (_type) {
    case HLL_DATA_EMPTY:
        _hash_set.insert(hash_value);
        _type = HLL_DATA_EXPLICIT;
        break;
    case HLL_DATA_EXPLICIT:
        if (_hash_set.size() < HLL_EXPLICLIT_INT64_NUM) {
            _hash_set.insert(hash_value);
            break;
        }
        _convert_explicit_to_register();
        _type = HLL_DATA_FULL;
        // fall through
    case HLL_DATA_SPARSE:
    case HLL_DATA_FULL:
        _update_registers(hash_value);
        break;
    }
}

void HyperLogLog::merge(const HyperLogLog& other) {
    // fast path
    if (other._type == HLL_DATA_EMPTY) {
        return;
    }
    switch (_type) {
    case HLL_DATA_EMPTY: {
        // _type must change
        _type = other._type;
        switch (other._type) {
        case HLL_DATA_EXPLICIT:
            _hash_set = other._hash_set;
            break;
        case HLL_DATA_SPARSE:
        case HLL_DATA_FULL:
            DCHECK_EQ(_registers.data, nullptr);
            MemChunkAllocator::instance()->allocate(HLL_REGISTERS_COUNT, &_registers);
            DCHECK_NE(_registers.data, nullptr);
            DCHECK_EQ(_registers.size, HLL_REGISTERS_COUNT);
            memcpy(_registers.data, other._registers.data, HLL_REGISTERS_COUNT);
            break;
        default:
            break;
        }
        break;
    }
    case HLL_DATA_EXPLICIT: {
        switch (other._type) {
        case HLL_DATA_EXPLICIT:
            // Merge other's explicit values first, then check if the number is exccede
            // HLL_EXPLICLIT_INT64_NUM. This is OK because the max value is 2 * 160.
            _hash_set.insert(other._hash_set.begin(), other._hash_set.end());
            if (_hash_set.size() > HLL_EXPLICLIT_INT64_NUM) {
                _convert_explicit_to_register();
                _type = HLL_DATA_FULL;
            }
            break;
        case HLL_DATA_SPARSE:
        case HLL_DATA_FULL:
            _convert_explicit_to_register();
            _merge_registers(other._registers.data);
            _type = HLL_DATA_FULL;
            break;
        default:
            break;
        }
        break;
    }
    case HLL_DATA_SPARSE:
    case HLL_DATA_FULL: {
        switch (other._type) {
        case HLL_DATA_EXPLICIT:
            for (auto hash_value : other._hash_set) {
                _update_registers(hash_value);
            }
            break;
        case HLL_DATA_SPARSE:
        case HLL_DATA_FULL:
            _merge_registers(other._registers.data);
            break;
        default:
            break;
        }
        break;
    }
    }
}

size_t HyperLogLog::max_serialized_size() const {
    switch (_type) {
    case HLL_DATA_EMPTY:
    default:
        return 1;
    case HLL_DATA_EXPLICIT:
        return 2 + _hash_set.size() * 8;
    case HLL_DATA_SPARSE:
    case HLL_DATA_FULL:
        return 1 + HLL_REGISTERS_COUNT;
    }
}

size_t HyperLogLog::serialize(uint8_t* dst) const {
    uint8_t* ptr = dst;
    switch (_type) {
    case HLL_DATA_EMPTY:
    default: {
        // When the _type is unknown, which may not happen, we encode it as
        // Empty HyperLogLog object.
        *ptr++ = HLL_DATA_EMPTY;
        break;
    }
    case HLL_DATA_EXPLICIT: {
        DCHECK(_hash_set.size() <= HLL_EXPLICLIT_INT64_NUM)
                << "Number of explicit elements(" << _hash_set.size() << ") should be less or equal than "
                << HLL_EXPLICLIT_INT64_NUM;
        *ptr++ = _type;
        *ptr++ = (uint8_t)_hash_set.size();
        for (auto hash_value : _hash_set) {
            encode_fixed64_le(ptr, hash_value);
            ptr += 8;
        }
        break;
    }
    case HLL_DATA_SPARSE:
    case HLL_DATA_FULL: {
        uint32_t num_non_zero_registers = 0;
        for (int i = 0; i < HLL_REGISTERS_COUNT; i++) {
            num_non_zero_registers += (_registers.data[i] != 0);
        }
        // each register in sparse format will occupy 3bytes, 2 for index and
        // 1 for register value. So if num_non_zero_registers is greater than
        // 4K we use full encode format.
        if (num_non_zero_registers > HLL_SPARSE_THRESHOLD) {
            *ptr++ = HLL_DATA_FULL;
            memcpy(ptr, _registers.data, HLL_REGISTERS_COUNT);
            ptr += HLL_REGISTERS_COUNT;
        } else {
            *ptr++ = HLL_DATA_SPARSE;
            // 2-5(4 byte): number of registers
            encode_fixed32_le(ptr, num_non_zero_registers);
            ptr += 4;

            for (uint32_t i = 0; i < HLL_REGISTERS_COUNT; ++i) {
                if (_registers.data[i] == 0) {
                    continue;
                }
                // 2 bytes: register index
                // 1 byte: register value
                encode_fixed16_le(ptr, i);
                ptr += 2;
                *ptr++ = _registers.data[i];
            }
        }
        break;
    }
    }
    return ptr - dst;
}

bool HyperLogLog::is_valid(const Slice& slice) {
    if (slice.size < 1) {
        return false;
    }
    const uint8_t* ptr = (uint8_t*)slice.data;
    const uint8_t* end = (uint8_t*)slice.data + slice.size;
    auto type = (HllDataType)*ptr++;
    switch (type) {
    case HLL_DATA_EMPTY:
        break;
    case HLL_DATA_EXPLICIT: {
        if ((ptr + 1) > end) {
            return false;
        }
        uint8_t num_explicits = *ptr++;
        ptr += num_explicits * 8;
        break;
    }
    case HLL_DATA_SPARSE: {
        if ((ptr + 4) > end) {
            return false;
        }
        uint32_t num_registers = decode_fixed32_le(ptr);
        ptr += 4 + 3 * num_registers;
        break;
    }
    case HLL_DATA_FULL: {
        ptr += HLL_REGISTERS_COUNT;
        break;
    }
    default:
        return false;
    }
    return ptr == end;
}

// TODO(zc): check input string's length
bool HyperLogLog::deserialize(const Slice& slice) {
    // can be called only when type is empty
    DCHECK(_type == HLL_DATA_EMPTY);

    // NOTE(zc): Don't remove this check unless you known what
    // you are doing. Because of history bug, we ingest some
    // invalid HLL data in storge, which ptr is nullptr.
    // we must handle this case to avoid process crash.
    // This bug is in release 0.10, I think we can remove this
    // in release 0.12 or later.
    if (slice.data == nullptr || slice.size <= 0) {
        return false;
    }
    // check if input length is valid
    if (!is_valid(slice)) {
        return false;
    }

    const uint8_t* ptr = (uint8_t*)slice.data;
    // first byte : type
    _type = (HllDataType)*ptr++;
    switch (_type) {
    case HLL_DATA_EMPTY:
        break;
    case HLL_DATA_EXPLICIT: {
        // 2: number of explicit values
        // make sure that num_explicit is positive
        uint8_t num_explicits = *ptr++;
        // 3+: 8 bytes hash value
        for (int i = 0; i < num_explicits; ++i) {
            _hash_set.insert(decode_fixed64_le(ptr));
            ptr += 8;
        }
        break;
    }
    case HLL_DATA_SPARSE: {
        DCHECK_EQ(_registers.data, nullptr);
        MemChunkAllocator::instance()->allocate(HLL_REGISTERS_COUNT, &_registers);
        DCHECK_NE(_registers.data, nullptr);
        DCHECK_EQ(_registers.size, HLL_REGISTERS_COUNT);
        memset(_registers.data, 0, HLL_REGISTERS_COUNT);

        // 2-5(4 byte): number of registers
        uint32_t num_registers = decode_fixed32_le(ptr);
        ptr += 4;
        for (uint32_t i = 0; i < num_registers; ++i) {
            // 2 bytes: register index
            // 1 byte: register value
            uint16_t register_idx = decode_fixed16_le(ptr);
            ptr += 2;
            _registers.data[register_idx] = *ptr++;
        }
        break;
    }
    case HLL_DATA_FULL: {
        DCHECK_EQ(_registers.data, nullptr);
        MemChunkAllocator::instance()->allocate(HLL_REGISTERS_COUNT, &_registers);
        DCHECK_NE(_registers.data, nullptr);
        DCHECK_EQ(_registers.size, HLL_REGISTERS_COUNT);
        // 2+ : hll register value
        memcpy(_registers.data, ptr, HLL_REGISTERS_COUNT);
        break;
    }
    default:
        // revert type to EMPTY
        _type = HLL_DATA_EMPTY;
        return false;
    }
    return true;
}

static float harmomic_tables[65] = {
        1.0f / static_cast<float>(1L << 0),
        1.0f / static_cast<float>(1L << 1),
        1.0f / static_cast<float>(1L << 2),
        1.0f / static_cast<float>(1L << 3),
        1.0f / static_cast<float>(1L << 4),
        1.0f / static_cast<float>(1L << 5),
        1.0f / static_cast<float>(1L << 6),
        1.0f / static_cast<float>(1L << 7),
        1.0f / static_cast<float>(1L << 8),
        1.0f / static_cast<float>(1L << 9),
        1.0f / static_cast<float>(1L << 10),
        1.0f / static_cast<float>(1L << 11),
        1.0f / static_cast<float>(1L << 12),
        1.0f / static_cast<float>(1L << 13),
        1.0f / static_cast<float>(1L << 14),
        1.0f / static_cast<float>(1L << 15),
        1.0f / static_cast<float>(1L << 16),
        1.0f / static_cast<float>(1L << 17),
        1.0f / static_cast<float>(1L << 18),
        1.0f / static_cast<float>(1L << 19),
        1.0f / static_cast<float>(1L << 20),
        1.0f / static_cast<float>(1L << 21),
        1.0f / static_cast<float>(1L << 22),
        1.0f / static_cast<float>(1L << 23),
        1.0f / static_cast<float>(1L << 24),
        1.0f / static_cast<float>(1L << 25),
        1.0f / static_cast<float>(1L << 26),
        1.0f / static_cast<float>(1L << 27),
        1.0f / static_cast<float>(1L << 28),
        1.0f / static_cast<float>(1L << 29),
        1.0f / static_cast<float>(1L << 30),
        1.0f / static_cast<float>(1L << 31),
        1.0f / static_cast<float>(1L << 32),
        1.0f / static_cast<float>(1L << 33),
        1.0f / static_cast<float>(1L << 34),
        1.0f / static_cast<float>(1L << 35),
        1.0f / static_cast<float>(1L << 36),
        1.0f / static_cast<float>(1L << 37),
        1.0f / static_cast<float>(1L << 38),
        1.0f / static_cast<float>(1L << 39),
        1.0f / static_cast<float>(1L << 40),
        1.0f / static_cast<float>(1L << 41),
        1.0f / static_cast<float>(1L << 42),
        1.0f / static_cast<float>(1L << 43),
        1.0f / static_cast<float>(1L << 44),
        1.0f / static_cast<float>(1L << 45),
        1.0f / static_cast<float>(1L << 46),
        1.0f / static_cast<float>(1L << 47),
        1.0f / static_cast<float>(1L << 48),
        1.0f / static_cast<float>(1L << 49),
        1.0f / static_cast<float>(1L << 50),
        1.0f / static_cast<float>(1L << 51),
        1.0f / static_cast<float>(1L << 52),
        1.0f / static_cast<float>(1L << 53),
        1.0f / static_cast<float>(1L << 54),
        1.0f / static_cast<float>(1L << 55),
        1.0f / static_cast<float>(1L << 56),
        1.0f / static_cast<float>(1L << 57),
        1.0f / static_cast<float>(1L << 58),
        1.0f / static_cast<float>(1L << 59),
        1.0f / static_cast<float>(1L << 60),
        1.0f / static_cast<float>(1L << 61),
        1.0f / static_cast<float>(1L << 62),
        1.0f / static_cast<float>(1L << 63),
        5.421010862427522e-20f,
};

int64_t HyperLogLog::estimate_cardinality() const {
    if (_type == HLL_DATA_EMPTY) {
        return 0;
    }
    if (_type == HLL_DATA_EXPLICIT) {
        return _hash_set.size();
    }

    const int num_streams = HLL_REGISTERS_COUNT;
    // Empirical constants for the algorithm.
    float alpha = 0;

    if (num_streams == 16) {
        alpha = 0.673f;
    } else if (num_streams == 32) {
        alpha = 0.697f;
    } else if (num_streams == 64) {
        alpha = 0.709f;
    } else {
        alpha = 0.7213f / (1 + 1.079f / num_streams);
    }

    float harmonic_mean = 0;
    int num_zero_registers = 0;

    for (int i = 0; i < HLL_REGISTERS_COUNT; ++i) {
        harmonic_mean += harmomic_tables[_registers.data[i]];

        if (_registers.data[i] == 0) {
            ++num_zero_registers;
        }
    }

    harmonic_mean = 1.0f / harmonic_mean;
    double estimate = alpha * num_streams * num_streams * harmonic_mean;
    // according to HerperLogLog current correction, if E is cardinal
    // E =< num_streams * 2.5 , LC has higher accuracy.
    // num_streams * 2.5 < E , HerperLogLog has higher accuracy.
    // Generally , we can use HerperLogLog to produce value as E.
    if (estimate <= num_streams * 2.5 && num_zero_registers != 0) {
        // Estimated cardinality is too low. Hll is too inaccurate here, instead use
        // linear counting.
        estimate = num_streams * std::log(static_cast<float>(num_streams) / num_zero_registers);
    } else if (num_streams == 16384 && estimate < 72000) {
        // when Linear Couint change to HerperLoglog according to HerperLogLog Correction,
        // there are relatively large fluctuations, we fixed the problem refer to redis.
        double bias = 5.9119 * 1.0e-18 * (estimate * estimate * estimate * estimate) -
                      1.4253 * 1.0e-12 * (estimate * estimate * estimate) + 1.2940 * 1.0e-7 * (estimate * estimate) -
                      5.2921 * 1.0e-3 * estimate + 83.3216;
        estimate -= estimate * (bias / 100);
    }
    return std::lround(estimate);
}

std::string HyperLogLog::to_string() const {
    switch (_type) {
    case HLL_DATA_EMPTY:
        return {};
    case HLL_DATA_EXPLICIT:
        return strings::Substitute("hash set size: $0\ncardinality:$1\ntype:$2", _hash_set.size(),
                                   estimate_cardinality(), _type);
    case HLL_DATA_SPARSE:
    case HLL_DATA_FULL: {
        return strings::Substitute("cardinality:$1\ntype:$2", estimate_cardinality(), _type);
    }
    default:
        return {};
    }
}

void HyperLogLog::clear() {
    _type = HLL_DATA_EMPTY;
    _hash_set.clear();
}

void HyperLogLog::_merge_registers(uint8_t* other_registers) {
#ifdef __AVX2__
    int loop = HLL_REGISTERS_COUNT / 32;
    uint8_t* dst = _registers.data;
    const uint8_t* src = other_registers;
    for (int i = 0; i < loop; i++) {
        __m256i xa = _mm256_loadu_si256((const __m256i*)dst);
        __m256i xb = _mm256_loadu_si256((const __m256i*)src);
        _mm256_storeu_si256((__m256i*)dst, _mm256_max_epu8(xa, xb));
        src += 32;
        dst += 32;
    }
#else
    for (int i = 0; i < HLL_REGISTERS_COUNT; i++) {
        _registers.data[i] = std::max(_registers.data[i], other_registers[i]);
    }
#endif
}

} // namespace starrocks
