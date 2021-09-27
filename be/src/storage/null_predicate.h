// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/null_predicate.h

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

#ifndef STARROCKS_BE_SRC_OLAP_NULL_PREDICATE_H
#define STARROCKS_BE_SRC_OLAP_NULL_PREDICATE_H

#include <cstdint>
#include <roaring/roaring.hh>

#include "storage/column_predicate.h"

namespace starrocks {

class VectorizedRowBatch;

class NullPredicate : public ColumnPredicate {
public:
    NullPredicate(uint32_t column_id, bool is_null);
    ~NullPredicate() override;

    void evaluate(VectorizedRowBatch* batch) const override;

    void evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const override;

    Status evaluate(const Schema& schema, const vector<BitmapIndexIterator*>& iterators, uint32_t num_rows,
                    Roaring* roaring) const override;

    Status convert_to(const ColumnPredicate** outpout, FieldType from_type, FieldType to_type,
                      ObjectPool* obj_pool) const override {
        *outpout = this;
        return Status::OK();
    }

private:
    bool _is_null; //true for null, false for not null
};

} //namespace starrocks

#endif //STARROCKS_BE_SRC_OLAP_NULL_PREDICATE_H
