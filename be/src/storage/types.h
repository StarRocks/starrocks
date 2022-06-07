// This file is made available under Elastic License 2.0.
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
#include "runtime/mem_pool.h"
#include "storage/collection.h"
#include "storage/olap_common.h"
#include "util/mem_util.hpp"
#include "util/unaligned_access.h"

namespace starrocks {

class TabletColumn;
class TypeInfo;
class ScalarTypeInfo;
using TypeInfoPtr = std::shared_ptr<TypeInfo>;

namespace vectorized {
class Datum;
}

class TypeInfo {
public:
    using Datum = vectorized::Datum;

    virtual bool equal(const void* left, const void* right) const = 0;
    virtual int cmp(const void* left, const void* right) const = 0;

    virtual void shallow_copy(void* dest, const void* src) const = 0;

    virtual void deep_copy(void* dest, const void* src, MemPool* mem_pool) const = 0;

    // See copy_row_in_memtable() in olap/row.h, will be removed in future.
    // It is same with deep_copy() for all type except for HLL and OBJECT type
    virtual void copy_object(void* dest, const void* src, MemPool* mem_pool) const = 0;

    // The mem_pool is used to allocate memory for array type.
    // The scalar type can copy the value directly
    virtual void direct_copy(void* dest, const void* src, MemPool* mem_pool) const = 0;

    //convert and deep copy value from other type's source
    virtual Status convert_from(void* dest, const void* src, const TypeInfoPtr& src_type, MemPool* mem_pool) const = 0;

    virtual Status convert_from(Datum& dest, const Datum& src, const TypeInfoPtr& src_type) const {
        return Status::NotSupported("Not supported function");
    }

    virtual Status from_string(void* buf, const std::string& scan_key) const = 0;

    virtual std::string to_string(const void* src) const = 0;
    virtual void set_to_max(void* buf) const = 0;
    virtual void set_to_min(void* buf) const = 0;

    virtual uint32_t hash_code(const void* data, uint32_t seed) const = 0;
    virtual size_t size() const = 0;

    virtual FieldType type() const = 0;

    virtual int precision() const { return -1; }

    virtual int scale() const { return -1; }

    ////////// Datum-based methods

    Status from_string(vectorized::Datum* buf, const std::string& scan_key) const = delete;
    std::string to_string(const vectorized::Datum& datum) const = delete;

    int cmp(const Datum& left, const Datum& right) const;

protected:
    virtual int _datum_cmp_impl(const Datum& left, const Datum& right) const = 0;
};

// TypeComparator
// static compare functions for performance-critical scenario
template <FieldType ftype>
struct TypeComparator {
    static int cmp(const void* lhs, const void* rhs);
};

bool is_scalar_field_type(FieldType field_type);

bool is_complex_metric_type(FieldType field_type);

const TypeInfo* get_scalar_type_info(FieldType t);

TypeInfoPtr get_type_info(FieldType field_type);

TypeInfoPtr get_type_info(const ColumnMetaPB& column_meta_pb);

TypeInfoPtr get_type_info(const TabletColumn& col);

TypeInfoPtr get_type_info(FieldType field_type, [[maybe_unused]] int precision, [[maybe_unused]] int scale);

TypeInfoPtr get_type_info(const TypeInfo* type_info);

} // namespace starrocks
