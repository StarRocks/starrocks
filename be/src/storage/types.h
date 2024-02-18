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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/types.h

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

#include <cinttypes>
#include <cmath>
#include <cstdio>
#include <limits>
#include <sstream>
#include <string>
#include <unordered_map>

#include "gen_cpp/segment.pb.h" // for ColumnMetaPB
#include "storage/collection.h"
#include "storage/olap_common.h"
#include "types/logical_type.h"
#include "util/unaligned_access.h"

namespace starrocks {

class MemPool;
class TabletColumn;
class TypeInfo;
class ScalarTypeInfo;
using TypeInfoPtr = std::shared_ptr<TypeInfo>;
class Datum;

class TypeInfo {
public:
    virtual void shallow_copy(void* dest, const void* src) const = 0;

    virtual void deep_copy(void* dest, const void* src, MemPool* mem_pool) const = 0;

    // map/struct/array have not yet implemented this interface.
    virtual void direct_copy(void* dest, const void* src) const = 0;

    virtual Status from_string(void* buf, const std::string& scan_key) const = 0;

    virtual std::string to_string(const void* src) const = 0;
    virtual void set_to_max(void* buf) const = 0;
    virtual void set_to_min(void* buf) const = 0;

    virtual size_t size() const = 0;

    virtual LogicalType type() const = 0;

    virtual int precision() const { return -1; }

    virtual int scale() const { return -1; }

    ////////// Datum-based methods

    Status from_string(Datum* buf, const std::string& scan_key) const = delete;
    std::string to_string(const Datum& datum) const = delete;

    int cmp(const Datum& left, const Datum& right) const;

protected:
    virtual int _datum_cmp_impl(const Datum& left, const Datum& right) const = 0;
};

// TypeComparator
// static compare functions for performance-critical scenario
template <LogicalType ftype>
struct TypeComparator {
    static int cmp(const void* lhs, const void* rhs);
};

const TypeInfo* get_scalar_type_info(LogicalType t);

TypeInfoPtr get_type_info(LogicalType field_type);

TypeInfoPtr get_type_info(const ColumnMetaPB& column_meta_pb);

TypeInfoPtr get_type_info(const TabletColumn& col);

TypeInfoPtr get_type_info(LogicalType field_type, [[maybe_unused]] int precision, [[maybe_unused]] int scale);

TypeInfoPtr get_type_info(const TypeInfo* type_info);

} // namespace starrocks
