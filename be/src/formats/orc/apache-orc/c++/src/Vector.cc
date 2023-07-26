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
//   https://github.com/apache/orc/tree/main/c++/src/Vector.cc

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

#include "orc/Vector.hh"

#include <cstdlib>
#include <iostream>
#include <sstream>

#include "Adaptor.hh"
#include "orc/Exceptions.hh"

namespace orc {

ColumnVectorBatch::ColumnVectorBatch(uint64_t cap, MemoryPool& pool)
        : capacity(cap), numElements(0), notNull(pool, cap), hasNulls(false), isEncoded(false), memoryPool(pool) {
    std::memset(notNull.data(), 1, capacity);
}

ColumnVectorBatch::~ColumnVectorBatch() {
    // PASS
}

void ColumnVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
        capacity = cap;
        notNull.resize(cap);
    }
}

void ColumnVectorBatch::clear() {
    numElements = 0;
}

uint64_t ColumnVectorBatch::getMemoryUsage() {
    return static_cast<uint64_t>(notNull.capacity() * sizeof(char));
}

bool ColumnVectorBatch::hasVariableLength() {
    return false;
}

void ColumnVectorBatch::filter(uint8_t* f_data, uint32_t f_size, uint32_t true_size) {
    notNull.filter(f_data, f_size, true_size);
    numElements = true_size;
    // if there is no null, then after selection, there is still no null.
    if (hasNulls) {
        char* data = notNull.data();
        hasNulls = false;
        for (uint32_t i = 0; i < true_size; i++) {
            if (data[i] == 0) {
                hasNulls = true;
                break;
            }
        }
    }
}

void ColumnVectorBatch::filterOnFields(uint8_t* f_data, uint32_t f_size, uint32_t true_size,
                                       const std::vector<int>& fields, bool onLazyLoad) {
    throw ParseError("ColumnVectorBatch::filterOnFields not implemented");
}

LongVectorBatch::LongVectorBatch(uint64_t _capacity, MemoryPool& pool)
        : ColumnVectorBatch(_capacity, pool), data(pool, _capacity) {
    // PASS
}

LongVectorBatch::~LongVectorBatch() {
    // PASS
}

std::string LongVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Long vector <" << numElements << " of " << capacity << ">";
    return buffer.str();
}

void LongVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
        ColumnVectorBatch::resize(cap);
        data.resize(cap);
    }
}

void LongVectorBatch::clear() {
    numElements = 0;
}

#define DO_FILTER_FIELD(field) (field).filter(f_data, f_size, true_size)

void LongVectorBatch::filter(uint8_t* f_data, uint32_t f_size, uint32_t true_size) {
    ColumnVectorBatch::filter(f_data, f_size, true_size);
    DO_FILTER_FIELD(data);
}

uint64_t LongVectorBatch::getMemoryUsage() {
    return ColumnVectorBatch::getMemoryUsage() + static_cast<uint64_t>(data.capacity() * sizeof(int64_t));
}

DoubleVectorBatch::DoubleVectorBatch(uint64_t _capacity, MemoryPool& pool)
        : ColumnVectorBatch(_capacity, pool), data(pool, _capacity) {
    // PASS
}

DoubleVectorBatch::~DoubleVectorBatch() {
    // PASS
}

std::string DoubleVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Double vector <" << numElements << " of " << capacity << ">";
    return buffer.str();
}

void DoubleVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
        ColumnVectorBatch::resize(cap);
        data.resize(cap);
    }
}

void DoubleVectorBatch::clear() {
    numElements = 0;
}

uint64_t DoubleVectorBatch::getMemoryUsage() {
    return ColumnVectorBatch::getMemoryUsage() + static_cast<uint64_t>(data.capacity() * sizeof(double));
}

void DoubleVectorBatch::filter(uint8_t* f_data, uint32_t f_size, uint32_t true_size) {
    ColumnVectorBatch::filter(f_data, f_size, true_size);
    DO_FILTER_FIELD(data);
}

StringDictionary::StringDictionary(MemoryPool& pool) : dictionaryBlob(pool), dictionaryOffset(pool) {
    // PASS
}

EncodedStringVectorBatch::EncodedStringVectorBatch(uint64_t _capacity, MemoryPool& pool)
        : StringVectorBatch(_capacity, pool), index(pool, _capacity) {
    // PASS
}

EncodedStringVectorBatch::~EncodedStringVectorBatch() {
    // PASS
}

std::string EncodedStringVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Encoded string vector <" << numElements << " of " << capacity << ">";
    return buffer.str();
}

void EncodedStringVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
        StringVectorBatch::resize(cap);
        index.resize(cap);
    }
}

StringVectorBatch::StringVectorBatch(uint64_t _capacity, MemoryPool& pool)
        : ColumnVectorBatch(_capacity, pool),
          data(pool, _capacity),
          length(pool, _capacity),
          blob(pool),
          codes(pool, _capacity),
          use_codes(false) {
    // PASS
}

StringVectorBatch::~StringVectorBatch() {
    // PASS
}

std::string StringVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Byte vector <" << numElements << " of " << capacity << ">";
    return buffer.str();
}

void StringVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
        ColumnVectorBatch::resize(cap);
        data.resize(cap);
        length.resize(cap);
    }
}

void StringVectorBatch::clear() {
    numElements = 0;
}

uint64_t StringVectorBatch::getMemoryUsage() {
    return ColumnVectorBatch::getMemoryUsage() +
           static_cast<uint64_t>(data.capacity() * sizeof(char*) + length.capacity() * sizeof(int64_t));
}

void StringVectorBatch::filter(uint8_t* f_data, uint32_t f_size, uint32_t true_size) {
    ColumnVectorBatch::filter(f_data, f_size, true_size);
    DO_FILTER_FIELD(data);
    DO_FILTER_FIELD(length);
    if (use_codes) {
        DO_FILTER_FIELD(codes);
    }
}

void EncodedStringVectorBatch::filter(uint8_t* f_data, uint32_t f_size, uint32_t true_size) {
    StringVectorBatch::filter(f_data, f_size, true_size);
    DO_FILTER_FIELD(index);
}

StructVectorBatch::StructVectorBatch(uint64_t cap, MemoryPool& pool) : ColumnVectorBatch(cap, pool) {
    // PASS
}

StructVectorBatch::~StructVectorBatch() {
    for (auto& field : this->fields) {
        delete field;
    }
}

std::string StructVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Struct vector <" << numElements << " of " << capacity << "; ";
    for (auto field : fields) {
        buffer << field->toString() << "; ";
    }
    buffer << ">";
    return buffer.str();
}

void StructVectorBatch::resize(uint64_t cap) {
    ColumnVectorBatch::resize(cap);
}

void StructVectorBatch::clear() {
    for (auto& field : fields) {
        field->clear();
    }
    numElements = 0;
}

uint64_t StructVectorBatch::getMemoryUsage() {
    uint64_t memory = ColumnVectorBatch::getMemoryUsage();
    for (auto& field : fields) {
        memory += field->getMemoryUsage();
    }
    return memory;
}

bool StructVectorBatch::hasVariableLength() {
    for (auto& field : fields) {
        if (field->hasVariableLength()) {
            return true;
        }
    }
    return false;
}

void StructVectorBatch::filter(uint8_t* f_data, uint32_t f_size, uint32_t true_size) {
    ColumnVectorBatch::filter(f_data, f_size, true_size);
    for (ColumnVectorBatch* cvb : fields) {
        cvb->filter(f_data, f_size, true_size);
    }
}

void StructVectorBatch::filterOnFields(uint8_t* f_data, uint32_t f_size, uint32_t true_size,
                                       const std::vector<int>& positions, bool onLazyLoad) {
    if (!onLazyLoad) {
        alreadyFilteredByActiveColumn = true;
        ColumnVectorBatch::filter(f_data, f_size, true_size);
    } else {
        if (!alreadyFilteredByActiveColumn) {
            throw orc::ParseError("We have to filter active column first");
        }
        numElements = true_size;
    }
    for (int p : positions) {
        ColumnVectorBatch* cvb = fields[p];
        cvb->filter(f_data, f_size, true_size);
    }
}

ListVectorBatch::ListVectorBatch(uint64_t cap, MemoryPool& pool)
        : ColumnVectorBatch(cap, pool), offsets(pool, cap + 1) {
    // PASS
}

ListVectorBatch::~ListVectorBatch() {
    // PASS
}

std::string ListVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "List vector <" << elements->toString() << " with " << numElements << " of " << capacity << ">";
    return buffer.str();
}

void ListVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
        ColumnVectorBatch::resize(cap);
        offsets.resize(cap + 1);
    }
}

void ListVectorBatch::clear() {
    numElements = 0;
    elements->clear();
}

uint64_t ListVectorBatch::getMemoryUsage() {
    return ColumnVectorBatch::getMemoryUsage() + static_cast<uint64_t>(offsets.capacity() * sizeof(int64_t)) +
           elements->getMemoryUsage();
}

bool ListVectorBatch::hasVariableLength() {
    return true;
}

uint32_t build_filter_on_offsets(uint8_t* f_data, uint32_t f_size, DataBuffer<int64_t>& offsets,
                                 std::vector<uint8_t>* p) {
    int64_t total_size = offsets[f_size];
    // by default it's zero.
    p->assign(total_size, 0);
    uint8_t* filter = p->data();
    int64_t true_size = 0;
    int32_t cur_select = 1;
    for (uint32_t i = 0; i < f_size; i++) {
        if (f_data[i] == 0) continue;
        memset(filter + offsets[i], 0x1, offsets[i + 1] - offsets[i]);
        true_size += offsets[i + 1] - offsets[i];
        // update the offsets
        offsets[cur_select] = offsets[cur_select - 1] + offsets[i + 1] - offsets[i];
        cur_select++;
    }
    return static_cast<uint32_t>(true_size);
}

void ListVectorBatch::filter(uint8_t* f_data, uint32_t f_size, uint32_t true_size) {
    ColumnVectorBatch::filter(f_data, f_size, true_size);
    std::vector<uint8_t> p;
    uint32_t true_count = build_filter_on_offsets(f_data, f_size, offsets, &p);
    auto size = static_cast<uint32_t>(p.size());
    elements->filter(p.data(), size, true_count);
}

MapVectorBatch::MapVectorBatch(uint64_t cap, MemoryPool& pool) : ColumnVectorBatch(cap, pool), offsets(pool, cap + 1) {
    // PASS
}

MapVectorBatch::~MapVectorBatch() {
    // PASS
}

std::string MapVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Map vector <" << (keys ? keys->toString() : "key not selected") << ", "
           << (elements ? elements->toString() : "value not selected") << " with " << numElements << " of " << capacity
           << ">";
    return buffer.str();
}

void MapVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
        ColumnVectorBatch::resize(cap);
        offsets.resize(cap + 1);
    }
}

void MapVectorBatch::clear() {
    keys->clear();
    elements->clear();
    numElements = 0;
}

uint64_t MapVectorBatch::getMemoryUsage() {
    return ColumnVectorBatch::getMemoryUsage() + static_cast<uint64_t>(offsets.capacity() * sizeof(int64_t)) +
           (keys ? keys->getMemoryUsage() : 0) + (elements ? elements->getMemoryUsage() : 0);
}

bool MapVectorBatch::hasVariableLength() {
    return true;
}

void MapVectorBatch::filter(uint8_t* f_data, uint32_t f_size, uint32_t true_size) {
    ColumnVectorBatch::filter(f_data, f_size, true_size);
    std::vector<uint8_t> p;
    uint32_t true_count = build_filter_on_offsets(f_data, f_size, offsets, &p);
    auto size = static_cast<uint32_t>(p.size());
    keys->filter(p.data(), size, true_count);
    elements->filter(p.data(), size, true_count);
}

UnionVectorBatch::UnionVectorBatch(uint64_t cap, MemoryPool& pool)
        : ColumnVectorBatch(cap, pool), tags(pool, cap), offsets(pool, cap) {
    // PASS
}

UnionVectorBatch::~UnionVectorBatch() {
    for (auto& i : children) {
        delete i;
    }
}

std::string UnionVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Union vector <";
    for (size_t i = 0; i < children.size(); ++i) {
        if (i != 0) {
            buffer << ", ";
        }
        buffer << children[i]->toString();
    }
    buffer << "; with " << numElements << " of " << capacity << ">";
    return buffer.str();
}

void UnionVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
        ColumnVectorBatch::resize(cap);
        tags.resize(cap);
        offsets.resize(cap);
    }
}

void UnionVectorBatch::clear() {
    for (auto& i : children) {
        i->clear();
    }
    numElements = 0;
}

uint64_t UnionVectorBatch::getMemoryUsage() {
    uint64_t memory =
            ColumnVectorBatch::getMemoryUsage() +
            static_cast<uint64_t>(tags.capacity() * sizeof(unsigned char) + offsets.capacity() * sizeof(uint64_t));
    for (auto& i : children) {
        memory += i->getMemoryUsage();
    }
    return memory;
}

bool UnionVectorBatch::hasVariableLength() {
    for (auto& i : children) {
        if (i->hasVariableLength()) {
            return true;
        }
    }
    return false;
}

void UnionVectorBatch::filter(uint8_t* f_data, uint32_t f_size, uint32_t true_size) {
    throw std::logic_error("UnionVectorBatch::filter not supported");
}

Decimal64VectorBatch::Decimal64VectorBatch(uint64_t cap, MemoryPool& pool)
        : ColumnVectorBatch(cap, pool), precision(0), scale(0), values(pool, cap), readScales(pool, cap) {
    // PASS
}

Decimal64VectorBatch::~Decimal64VectorBatch() {
    // PASS
}

std::string Decimal64VectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Decimal64 vector  with " << numElements << " of " << capacity << ">";
    return buffer.str();
}

void Decimal64VectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
        ColumnVectorBatch::resize(cap);
        values.resize(cap);
        readScales.resize(cap);
    }
}

void Decimal64VectorBatch::clear() {
    numElements = 0;
}

uint64_t Decimal64VectorBatch::getMemoryUsage() {
    return ColumnVectorBatch::getMemoryUsage() +
           static_cast<uint64_t>((values.capacity() + readScales.capacity()) * sizeof(int64_t));
}

void Decimal64VectorBatch::filter(uint8_t* f_data, uint32_t f_size, uint32_t true_size) {
    ColumnVectorBatch::filter(f_data, f_size, true_size);
    DO_FILTER_FIELD(values);
}

Decimal128VectorBatch::Decimal128VectorBatch(uint64_t cap, MemoryPool& pool)
        : ColumnVectorBatch(cap, pool), precision(0), scale(0), values(pool, cap), readScales(pool, cap) {
    // PASS
}

Decimal128VectorBatch::~Decimal128VectorBatch() {
    // PASS
}

std::string Decimal128VectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Decimal128 vector  with " << numElements << " of " << capacity << ">";
    return buffer.str();
}

void Decimal128VectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
        ColumnVectorBatch::resize(cap);
        values.resize(cap);
        readScales.resize(cap);
    }
}

void Decimal128VectorBatch::clear() {
    numElements = 0;
}

uint64_t Decimal128VectorBatch::getMemoryUsage() {
    return ColumnVectorBatch::getMemoryUsage() +
           static_cast<uint64_t>(values.capacity() * sizeof(Int128) + readScales.capacity() * sizeof(int64_t));
}

void Decimal128VectorBatch::filter(uint8_t* f_data, uint32_t f_size, uint32_t true_size) {
    ColumnVectorBatch::filter(f_data, f_size, true_size);
    DO_FILTER_FIELD(values);
}

Decimal::Decimal(const Int128& _value, int32_t _scale) : value(_value), scale(_scale) {
    // PASS
}

Decimal::Decimal(const std::string& str) {
    std::size_t foundPoint = str.find('.');
    // no decimal point, it is int
    if (foundPoint == std::string::npos) {
        value = Int128(str);
        scale = 0;
    } else {
        std::string copy(str);
        scale = static_cast<int32_t>(str.length() - foundPoint - 1);
        value = Int128(copy.replace(foundPoint, 1, ""));
    }
}

Decimal::Decimal() : value(0) {
    // PASS
}

std::string Decimal::toString(bool trimTrailingZeros) const {
    return value.toDecimalString(scale, trimTrailingZeros);
}

TimestampVectorBatch::TimestampVectorBatch(uint64_t _capacity, MemoryPool& pool)
        : ColumnVectorBatch(_capacity, pool), data(pool, _capacity), nanoseconds(pool, _capacity) {
    // PASS
}

TimestampVectorBatch::~TimestampVectorBatch() {
    // PASS
}

std::string TimestampVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Timestamp vector <" << numElements << " of " << capacity << ">";
    return buffer.str();
}

void TimestampVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
        ColumnVectorBatch::resize(cap);
        data.resize(cap);
        nanoseconds.resize(cap);
    }
}

void TimestampVectorBatch::clear() {
    numElements = 0;
}

uint64_t TimestampVectorBatch::getMemoryUsage() {
    return ColumnVectorBatch::getMemoryUsage() +
           static_cast<uint64_t>((data.capacity() + nanoseconds.capacity()) * sizeof(int64_t));
}

void TimestampVectorBatch::filter(uint8_t* f_data, uint32_t f_size, uint32_t true_size) {
    ColumnVectorBatch::filter(f_data, f_size, true_size);
    DO_FILTER_FIELD(data);
    DO_FILTER_FIELD(nanoseconds);
}
} // namespace orc
