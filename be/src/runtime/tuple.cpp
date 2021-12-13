// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/tuple.cpp

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

#include "runtime/tuple.h"

#include <vector>

#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/raw_value.h"
#include "runtime/string_value.h"
#include "util/mem_util.hpp"

namespace starrocks {

int64_t Tuple::total_byte_size(const TupleDescriptor& desc) const {
    int64_t result = desc.byte_size();
    if (!desc.has_varlen_slots()) {
        return result;
    }
    result += varlen_byte_size(desc);
    return result;
}

int64_t Tuple::varlen_byte_size(const TupleDescriptor& desc) const {
    return 0;
}

Tuple* Tuple::deep_copy(const TupleDescriptor& desc, MemPool* pool, bool convert_ptrs) {
    Tuple* result = reinterpret_cast<Tuple*>(pool->allocate(desc.byte_size()));
    deep_copy(result, desc, pool, convert_ptrs);
    return result;
}

void Tuple::deep_copy(Tuple* dst, const TupleDescriptor& desc, MemPool* pool, bool convert_ptrs) {
    memory_copy(dst, this, desc.byte_size());
}

Tuple* Tuple::dcopy_with_new(const TupleDescriptor& desc, MemPool* pool, int64_t* bytes) {
    Tuple* result = reinterpret_cast<Tuple*>(pool->allocate(desc.byte_size()));
    *bytes = dcopy_with_new(result, desc);
    return result;
}

int64_t Tuple::dcopy_with_new(Tuple* dst, const TupleDescriptor& desc) {
    memory_copy(dst, this, desc.byte_size());

    int64_t bytes = 0;
    return bytes;
}

int64_t Tuple::release_string(const TupleDescriptor& desc) {
    int64_t bytes = 0;
    return bytes;
}

void Tuple::deep_copy(const TupleDescriptor& desc, char** data, int* offset, bool convert_ptrs) {
    Tuple* dst = reinterpret_cast<Tuple*>(*data);
    memory_copy(dst, this, desc.byte_size());
    *data += desc.byte_size();
    *offset += desc.byte_size();
}

std::string Tuple::to_string(const TupleDescriptor& d) const {
    std::stringstream out;
    out << "(";

    bool first_value = true;
    for (auto slot : d.slots()) {
        if (!slot->is_materialized()) {
            continue;
        }
        if (first_value) {
            first_value = false;
        } else {
            out << " ";
        }

        if (is_null(slot->null_indicator_offset())) {
            out << "null";
        } else {
            std::string value_str;
            RawValue::print_value(get_slot(slot->tuple_offset()), slot->type(), -1, &value_str);
            out << value_str;
        }
    }

    out << ")";
    return out.str();
}

std::string Tuple::to_string(const Tuple* t, const TupleDescriptor& d) {
    if (t == nullptr) {
        return "null";
    }
    return t->to_string(d);
}

} // namespace starrocks
