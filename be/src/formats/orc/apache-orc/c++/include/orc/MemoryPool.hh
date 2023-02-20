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
//   https://github.com/apache/orc/tree/main/c++/include/orc/MemoryPool.hh

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

#pragma once

#include <memory>

#include "orc/Int128.hh"
#include "orc/orc-config.hh"

namespace orc {

class MemoryPool {
public:
    virtual ~MemoryPool();

    virtual char* malloc(uint64_t size) = 0;
    virtual void free(char* p) = 0;
};
MemoryPool* getDefaultPool();

template <class T>
class DataBuffer {
private:
    MemoryPool& memoryPool;
    T* buf;
    // current size
    uint64_t currentSize;
    // maximal capacity (actual allocated memory)
    uint64_t currentCapacity;

    // not implemented
    DataBuffer(DataBuffer& buffer) = delete;
    DataBuffer& operator=(DataBuffer& buffer) = delete;

public:
    DataBuffer(MemoryPool& pool, uint64_t _size = 0);

    DataBuffer(DataBuffer<T>&& buffer) ORC_NOEXCEPT;

    virtual ~DataBuffer();

    T* data() { return buf; }

    const T* data() const { return buf; }

    uint64_t size() { return currentSize; }

    uint64_t capacity() { return currentCapacity; }

    T& operator[](uint64_t i) { return buf[i]; }

    void reserve(uint64_t _size);
    void resize(uint64_t _size);

    void filter(const uint8_t* f_data, size_t f_size, size_t true_size);
};

// Specializations for char

template <>
DataBuffer<char>::~DataBuffer();

template <>
void DataBuffer<char>::resize(uint64_t newSize);

// Specializations for char*

template <>
DataBuffer<char*>::~DataBuffer();

template <>
void DataBuffer<char*>::resize(uint64_t newSize);

// Specializations for double

template <>
DataBuffer<double>::~DataBuffer();

template <>
void DataBuffer<double>::resize(uint64_t newSize);

// Specializations for int64_t

template <>
DataBuffer<int64_t>::~DataBuffer();

template <>
void DataBuffer<int64_t>::resize(uint64_t newSize);

// Specializations for uint64_t

template <>
DataBuffer<uint64_t>::~DataBuffer();

template <>
void DataBuffer<uint64_t>::resize(uint64_t newSize);

// Specializations for unsigned char

template <>
DataBuffer<unsigned char>::~DataBuffer();

template <>
void DataBuffer<unsigned char>::resize(uint64_t newSize);

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wweak-template-vtables"
#endif

extern template class DataBuffer<char>;
extern template class DataBuffer<char*>;
extern template class DataBuffer<double>;
extern template class DataBuffer<Int128>;
extern template class DataBuffer<int64_t>;
extern template class DataBuffer<uint64_t>;
extern template class DataBuffer<unsigned char>;

#ifdef __clang__
#pragma clang diagnostic pop
#endif
} // namespace orc
