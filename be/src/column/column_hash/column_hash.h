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

#pragma once

#include <cstdint>
#include <memory>

#include "column/column_visitor.h"
#include "common/status.h"

namespace starrocks {

// Forward declarations - implementations in column_hash.cpp
template <typename HashFunction>
class ColumnHashVisitorImpl;

// Hash function type tags - defined in column_hash.cpp
struct FNVHash;
struct CRC32Hash;
struct MurmurHash3Hash;

// ColumnHashVisitor - simple interface for hash computation
// All implementations are in column_hash.cpp
template <typename HashFunction>
class ColumnHashVisitor : public ColumnVisitor {
public:
    ColumnHashVisitor(uint32_t* hashes, uint32_t from, uint32_t to);
    ColumnHashVisitor(uint32_t* hashes, uint8_t* selection, uint16_t from, uint16_t to);
    ColumnHashVisitor(uint32_t* hashes, uint16_t* sel, uint16_t sel_size);
    ColumnHashVisitor(uint32_t* hash, uint32_t idx);
    
    ~ColumnHashVisitor() override;
    
    // ColumnVisitor interface - implementations in .cpp
    Status visit(const Int8Column& column) override;
    Status visit(const UInt8Column& column) override;
    Status visit(const Int16Column& column) override;
    Status visit(const UInt16Column& column) override;
    Status visit(const Int32Column& column) override;
    Status visit(const UInt32Column& column) override;
    Status visit(const Int64Column& column) override;
    Status visit(const UInt64Column& column) override;
    Status visit(const Int128Column& column) override;
    Status visit(const FloatColumn& column) override;
    Status visit(const DoubleColumn& column) override;
    Status visit(const DateColumn& column) override;
    Status visit(const TimestampColumn& column) override;
    Status visit(const DecimalColumn& column) override;
    Status visit(const Decimal32Column& column) override;
    Status visit(const Decimal64Column& column) override;
    Status visit(const Decimal128Column& column) override;
    Status visit(const Decimal256Column& column) override;
    Status visit(const FixedLengthColumn<int96_t>& column) override;
    Status visit(const FixedLengthColumn<uint24_t>& column) override;
    Status visit(const FixedLengthColumn<decimal12_t>& column) override;
    Status visit(const BinaryColumn& column) override;
    Status visit(const LargeBinaryColumn& column) override;
    Status visit(const NullableColumn& column) override;
    Status visit(const ConstColumn& column) override;
    Status visit(const ArrayColumn& column) override;
    Status visit(const MapColumn& column) override;
    Status visit(const StructColumn& column) override;
    Status visit(const ObjectColumn<JsonValue>& column) override;
    Status visit(const ObjectColumn<VariantValue>& column) override;

private:
    std::unique_ptr<ColumnHashVisitorImpl<HashFunction>> _impl;
};

} // namespace starrocks
