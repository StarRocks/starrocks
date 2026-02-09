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

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "types/logical_type.h"
#include "types/type_info_allocator.h"

namespace starrocks {

class TypeDescriptor;
class TypeInfo;
using TypeInfoPtr = std::shared_ptr<TypeInfo>;
class Datum;

class TypeInfo {
public:
    virtual ~TypeInfo() = default;

    virtual void shallow_copy(void* dest, const void* src) const = 0;

    virtual void deep_copy(void* dest, const void* src, const TypeInfoAllocator* allocator) const = 0;

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

    Status from_string(Datum* buf, const std::string& scan_key) const = delete;
    std::string to_string(const Datum& datum) const = delete;

    int cmp(const Datum& left, const Datum& right) const;

protected:
    virtual int _datum_cmp_impl(const Datum& left, const Datum& right) const = 0;
};

// Static compare functions for performance-critical scenarios.
template <LogicalType ftype>
struct TypeComparator {
    static int cmp(const void* lhs, const void* rhs);
};

const TypeInfo* get_scalar_type_info(LogicalType t);

TypeInfoPtr get_type_info(LogicalType field_type);
TypeInfoPtr get_type_info(const TypeDescriptor& type_desc);
TypeInfoPtr get_type_info(LogicalType field_type, int precision, int scale);
TypeInfoPtr get_type_info(const TypeInfo* type_info);

TypeInfoPtr get_array_type_info(const TypeInfoPtr& item_type);
const TypeInfoPtr& get_item_type_info(const TypeInfo* type_info);

TypeInfoPtr get_map_type_info(const TypeInfoPtr& key_type, const TypeInfoPtr& value_type);
const TypeInfoPtr& get_key_type_info(const TypeInfo* type_info);
const TypeInfoPtr& get_value_type_info(const TypeInfo* type_info);

TypeInfoPtr get_struct_type_info(std::vector<TypeInfoPtr> field_types);
const std::vector<TypeInfoPtr>& get_struct_field_types(const TypeInfo* type_info);

TypeInfoPtr get_decimal_type_info(LogicalType type, int precision, int scale);
std::string get_decimal_zone_map_string(TypeInfo* type_info, const void* value);

} // namespace starrocks
