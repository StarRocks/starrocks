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
//   https://github.com/apache/orc/tree/main/c++/src/MemoryPool.cc

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "orc/MemoryPool.hh"

#include <cstdlib>
#include <cstring>

#include "orc/Int128.hh"

namespace orc {

MemoryPool::~MemoryPool() {
    // PASS
}

class MemoryPoolImpl : public MemoryPool {
public:
    ~MemoryPoolImpl() override;

    char* malloc(uint64_t size) override;
    void free(char* p) override;
};

char* MemoryPoolImpl::malloc(uint64_t size) {
    return static_cast<char*>(std::malloc(size));
}

void MemoryPoolImpl::free(char* p) {
    std::free(p);
}

MemoryPoolImpl::~MemoryPoolImpl() {
    // PASS
}

template <class T>
DataBuffer<T>::DataBuffer(MemoryPool& pool, uint64_t newSize)
        : memoryPool(pool), buf(nullptr), currentSize(0), currentCapacity(0) {
    resize(newSize);
}

template <class T>
DataBuffer<T>::DataBuffer(DataBuffer<T>&& buffer) noexcept
        : memoryPool(buffer.memoryPool),
          buf(buffer.buf),
          currentSize(buffer.currentSize),
          currentCapacity(buffer.currentCapacity) {
    buffer.buf = nullptr;
    buffer.currentSize = 0;
    buffer.currentCapacity = 0;
}

template <class T>
DataBuffer<T>::~DataBuffer() {
    for (uint64_t i = currentSize; i > 0; --i) {
        (buf + i - 1)->~T();
    }
    if (buf) {
        memoryPool.free(reinterpret_cast<char*>(buf));
    }
}

template <class T>
void DataBuffer<T>::resize(uint64_t newSize) {
    reserve(newSize);
    if (currentSize > newSize) {
        for (uint64_t i = currentSize; i > newSize; --i) {
            (buf + i - 1)->~T();
        }
    } else if (newSize > currentSize) {
        for (uint64_t i = currentSize; i < newSize; ++i) {
            new (buf + i) T();
        }
    }
    currentSize = newSize;
}

template <class T>
void DataBuffer<T>::reserve(uint64_t newCapacity) {
    if (newCapacity > currentCapacity || !buf) {
        if (buf) {
            T* buf_old = buf;
            buf = reinterpret_cast<T*>(memoryPool.malloc(sizeof(T) * newCapacity));
            memcpy(buf, buf_old, sizeof(T) * currentSize);
            memoryPool.free(reinterpret_cast<char*>(buf_old));
        } else {
            buf = reinterpret_cast<T*>(memoryPool.malloc(sizeof(T) * newCapacity));
        }
        currentCapacity = newCapacity;
    }
}

inline int CountTrailingZerosNonZero32(uint32_t n) {
    return __builtin_ctz(n);
}

#ifdef __x86_64__
#ifdef __AVX2__
#include <immintrin.h>
#endif
#elif defined(__aarch64__) && defined(USE_AVX2KI)
#include "avx2ki.h"
#endif

// it's copied from `filter_range` in column_helper.h with some minor changes.
template <class T>
void DataBuffer<T>::filter(const uint8_t* f_data, size_t f_size, size_t true_size) {
    size_t src = 0;
    size_t dst = 0;
    size_t end = src + f_size;

#if defined(__AVX2__) || defined(USE_AVX2KI)
    const int simd_bits = 256;
    const int batch_nums = simd_bits / (8 * (int)sizeof(uint8_t));
    __m256i all0 = _mm256_setzero_si256();

    while (src + batch_nums < end) {
        __m256i f = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(f_data + src));
        uint32_t mask = _mm256_movemask_epi8(_mm256_cmpgt_epi8(f, all0));

        if (mask == 0) {
            // all no hit, pass
        } else if (mask == 0xffffffff) {
            // all hit, copy all
            memmove(buf + dst, buf + src, batch_nums * sizeof(T));
            dst += batch_nums;
        } else {
            // skip not hit row, it's will reduce compare when filter layout is sparse,
            // like "00010001...", but is ineffective when the filter layout is dense.

            do {
                uint32_t i = CountTrailingZerosNonZero32(mask);
                memmove(buf + dst, buf + src + i, sizeof(T));
                dst += 1;
                mask &= ~(1 << i);
            } while (mask);
        }
        src += batch_nums;
    }
#endif
    for (size_t i = src; i < end; ++i) {
        if (f_data[i]) {
            *(buf + dst) = *(buf + i);
            dst++;
        }
    }

    // set current size directly instead of calling resize.
    // it's noted that currentSize will not be be updated when data is written
    // and if new size is larger than old size
    // the larger area will be initialized to 0
    currentSize = true_size;
}

// Specializations for char

template <>
DataBuffer<char>::~DataBuffer() {
    if (buf) {
        memoryPool.free(reinterpret_cast<char*>(buf));
    }
}

template <>
void DataBuffer<char>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize) {
        memset(buf + currentSize, 0, newSize - currentSize);
    }
    currentSize = newSize;
}

// Specializations for char*

template <>
DataBuffer<char*>::~DataBuffer() {
    if (buf) {
        memoryPool.free(reinterpret_cast<char*>(buf));
    }
}

template <>
void DataBuffer<char*>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize) {
        memset(buf + currentSize, 0, (newSize - currentSize) * sizeof(char*));
    }
    currentSize = newSize;
}

// Specializations for double

template <>
DataBuffer<double>::~DataBuffer() {
    if (buf) {
        memoryPool.free(reinterpret_cast<char*>(buf));
    }
}

template <>
void DataBuffer<double>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize) {
        memset(buf + currentSize, 0, (newSize - currentSize) * sizeof(double));
    }
    currentSize = newSize;
}

// Specializations for int64_t

template <>
DataBuffer<int64_t>::~DataBuffer() {
    if (buf) {
        memoryPool.free(reinterpret_cast<char*>(buf));
    }
}

template <>
void DataBuffer<int64_t>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize) {
        memset(buf + currentSize, 0, (newSize - currentSize) * sizeof(int64_t));
    }
    currentSize = newSize;
}

// Specializations for uint64_t

template <>
DataBuffer<uint64_t>::~DataBuffer() {
    if (buf) {
        memoryPool.free(reinterpret_cast<char*>(buf));
    }
}

template <>
void DataBuffer<uint64_t>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize) {
        memset(buf + currentSize, 0, (newSize - currentSize) * sizeof(uint64_t));
    }
    currentSize = newSize;
}

// Specializations for unsigned char

template <>
DataBuffer<unsigned char>::~DataBuffer() {
    if (buf) {
        memoryPool.free(reinterpret_cast<char*>(buf));
    }
}

template <>
void DataBuffer<unsigned char>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize) {
        memset(buf + currentSize, 0, newSize - currentSize);
    }
    currentSize = newSize;
}

#ifdef __clang__
#pragma clang diagnostic ignored "-Wweak-template-vtables"
#endif

template class DataBuffer<char>;
template class DataBuffer<char*>;
template class DataBuffer<double>;
template class DataBuffer<Int128>;
template class DataBuffer<int64_t>;
template class DataBuffer<uint64_t>;
template class DataBuffer<unsigned char>;

#ifdef __clang__
#pragma clang diagnostic ignored "-Wexit-time-destructors"
#endif

MemoryPool* getDefaultPool() {
    static MemoryPoolImpl internal;
    return &internal;
}
} // namespace orc
