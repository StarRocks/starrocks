// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/tuple_row.h

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

#ifndef STARROCKS_BE_RUNTIME_TUPLE_ROW_H
#define STARROCKS_BE_RUNTIME_TUPLE_ROW_H

#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/tuple.h"

namespace starrocks {

// A TupleRow encapsulates a contiguous sequence of Tuple pointers which
// together make up a row.
class TupleRow {
public:
    Tuple* get_tuple(int tuple_idx) { return _tuples[tuple_idx]; }

    void set_tuple(int tuple_idx, Tuple* tuple) { _tuples[tuple_idx] = tuple; }

    static TupleRow* create(const std::vector<TupleDescriptor*>& descs, MemPool* pool) {
        int size = descs.size() * sizeof(Tuple*);
        return reinterpret_cast<TupleRow*>(pool->allocate(size));
    }

    // Create a deep copy of this TupleRow.  deep_copy will allocate from  the pool.
    TupleRow* deep_copy(const std::vector<TupleDescriptor*>& descs, MemPool* pool) {
        int size = descs.size() * sizeof(Tuple*);
        TupleRow* result = reinterpret_cast<TupleRow*>(pool->allocate(size));
        deep_copy(result, descs, pool, false);
        return result;
    }

    // Create a deep copy of this TupleRow into 'dst'.  deep_copy will allocate from
    // the MemPool and copy the tuple pointers, the tuples and the string data in the
    // tuples.
    // If reuse_tuple_mem is true, it is assumed the dst TupleRow has already allocated
    // tuple memory and that memory will be reused.  Otherwise, new tuples will be allocated
    // and stored in 'dst'.
    void deep_copy(TupleRow* dst, const std::vector<TupleDescriptor*>& descs, MemPool* pool, bool reuse_tuple_mem) {
        for (int i = 0; i < descs.size(); ++i) {
            if (this->get_tuple(i) != nullptr) {
                if (reuse_tuple_mem && dst->get_tuple(i) != nullptr) {
                    this->get_tuple(i)->deep_copy(dst->get_tuple(i), *descs[i], pool);
                } else {
                    dst->set_tuple(i, this->get_tuple(i)->deep_copy(*descs[i], pool));
                }
            } else {
                // TODO: this is wasteful.  If we have 'reuse_tuple_mem', we should be able
                // to save the tuple buffer and reuse it (i.e. freelist).
                dst->set_tuple(i, nullptr);
            }
        }
    }

    std::string to_string(const RowDescriptor& d);

private:
    Tuple* _tuples[1];
};

} // namespace starrocks

#endif
