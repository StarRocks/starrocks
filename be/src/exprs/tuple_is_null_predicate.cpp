// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/tuple_is_null_predicate.cpp

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

#include "exprs/tuple_is_null_predicate.h"

#include <sstream>

#include "column/column_helper.h"
#include "gen_cpp/Exprs_types.h"

namespace starrocks {

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30

TupleIsNullPredicate::TupleIsNullPredicate(const TExprNode& node)
        : Predicate(node),
          _tuple_ids(node.tuple_is_null_pred.tuple_ids.begin(), node.tuple_is_null_pred.tuple_ids.end()) {}

Status TupleIsNullPredicate::prepare(RuntimeState* state, const RowDescriptor& row_desc, ExprContext* ctx) {
    RETURN_IF_ERROR(Expr::prepare(state, row_desc, ctx));
    DCHECK_EQ(0, _children.size());

    // Resolve tuple ids to tuple indexes.
    for (int _tuple_id : _tuple_ids) {
        int32_t tuple_idx = row_desc.get_tuple_idx(_tuple_id);
        if (row_desc.tuple_is_nullable(tuple_idx)) {
            _tuple_idxs.push_back(tuple_idx);
        }
    }

    return Status::OK();
}

BooleanVal TupleIsNullPredicate::get_boolean_val(ExprContext* ctx, TupleRow* row) {
    int count = 0;
    for (int _tuple_idx : _tuple_idxs) {
        count += row->get_tuple(_tuple_idx) == nullptr;
    }
    return BooleanVal(!_tuple_idxs.empty() && count == _tuple_idxs.size());
}

ColumnPtr TupleIsNullPredicate::evaluate(ExprContext* context, vectorized::Chunk* chunk) {
    vectorized::ColumnPtr dest_column = vectorized::BooleanColumn::create(chunk->num_rows(), 0);
    if (_tuple_ids.empty()) {
        return dest_column;
    } else if (_tuple_ids.size() == 1) {
        if (!chunk->is_tuple_exist(_tuple_ids[0])) {
            return dest_column;
        }
        vectorized::ColumnPtr& src_column = chunk->get_tuple_column_by_id(_tuple_ids[0]);
        auto* src_data =
                vectorized::ColumnHelper::as_raw_column<vectorized::BooleanColumn>(src_column)->get_data().data();
        auto* dest_data =
                vectorized::ColumnHelper::as_raw_column<vectorized::BooleanColumn>(dest_column)->get_data().data();

        size_t size = src_column->size();

        for (size_t i = 0; i < size; i++) {
            dest_data[i] = !src_data[i];
        }
        return dest_column;
    } else {
        auto* dest_data =
                vectorized::ColumnHelper::as_raw_column<vectorized::BooleanColumn>(dest_column)->get_data().data();

        for (int tuple_id : _tuple_ids) {
            if (!chunk->is_tuple_exist(tuple_id)) {
                return vectorized::BooleanColumn::create(chunk->num_rows(), 0);
            }

            vectorized::ColumnPtr& src_column = chunk->get_tuple_column_by_id(tuple_id);
            auto* src_data =
                    vectorized::ColumnHelper::as_raw_column<vectorized::BooleanColumn>(src_column)->get_data().data();

            // Expr that returns true if all of the given tuples are NULL, otherwise false.
            // Used to make exprs originating from an inline view nullable in an outer join.
            // The given tupleIds must be materialized and nullable at the appropriate PlanNode.
            // If logic is |, it will be splited to two TupleIsNull expr
            size_t size = chunk->num_rows();
            for (size_t i = 0; i < size; i++) {
                dest_data[i] = dest_data[i] & src_data[i];
            }
        }
        return dest_column;
    }
}

std::string TupleIsNullPredicate::debug_string() const {
    std::stringstream out;
    out << "TupleIsNullPredicate(tupleids=[";

    for (int i = 0; i < _tuple_ids.size(); ++i) {
        out << (i == 0 ? "" : " ") << _tuple_ids[i];
    }

    out << "])";
    return out.str();
}

} // namespace starrocks