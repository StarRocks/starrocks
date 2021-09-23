// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/slot_ref.h

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

#ifndef STARROCKS_BE_SRC_QUERY_EXPRS_SLOT_REF_H
#define STARROCKS_BE_SRC_QUERY_EXPRS_SLOT_REF_H

#include "common/object_pool.h"
#include "exprs/expr.h"

namespace starrocks {

// Reference to a single slot of a tuple.
// We inline this here in order for Expr::get_value() to be able
// to reference SlotRef::compute_fn() directly.
// Splitting it up into separate .h files would require circular #includes.

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30

class SlotRef final : public Expr {
public:
    SlotRef(const TExprNode& node);
    SlotRef(const SlotDescriptor* desc);
    Expr* clone(ObjectPool* pool) const override { return pool->add(new SlotRef(*this)); }

    // TODO: this is a hack to allow aggregation nodes to work around NULL slot
    // descriptors. Ideally the FE would dictate the type of the intermediate SlotRefs.
    SlotRef(const SlotDescriptor* desc, const TypeDescriptor& type);

    // Used for testing.  get_value will return tuple + offset interpreted as 'type'
    SlotRef(const TypeDescriptor& type, int offset, SlotId slot = -1);

    Status prepare(const SlotDescriptor* slot_desc, const RowDescriptor& row_desc);

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc, ExprContext* ctx) override;
    static void* get_value(Expr* expr, TupleRow* row);
    void* get_slot(TupleRow* row);
    Tuple* get_tuple(TupleRow* row);
    bool is_null_bit_set(TupleRow* row);
    static bool vector_compute_fn(Expr* expr, VectorizedRowBatch* batch);
    static bool is_nullable(Expr* expr);
    std::string debug_string() const override;
    bool is_constant() const override { return false; }
    bool is_vectorized() const override { return true; }
    bool is_bound(const std::vector<TupleId>& tuple_ids) const override;
    int get_slot_ids(std::vector<SlotId>* slot_ids) const override;
    SlotId slot_id() const { return _slot_id; }
    TupleId tuple_id() const { return _tuple_id; }
    inline NullIndicatorOffset null_indicator_offset() const { return _null_indicator_offset; }

    starrocks_udf::BooleanVal get_boolean_val(ExprContext* context, TupleRow*) override;
    starrocks_udf::TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow*) override;
    starrocks_udf::SmallIntVal get_small_int_val(ExprContext* context, TupleRow*) override;
    starrocks_udf::IntVal get_int_val(ExprContext* context, TupleRow*) override;
    starrocks_udf::BigIntVal get_big_int_val(ExprContext* context, TupleRow*) override;
    starrocks_udf::LargeIntVal get_large_int_val(ExprContext* context, TupleRow*) override;
    starrocks_udf::FloatVal get_float_val(ExprContext* context, TupleRow*) override;
    starrocks_udf::DoubleVal get_double_val(ExprContext* context, TupleRow*) override;
    starrocks_udf::StringVal get_string_val(ExprContext* context, TupleRow*) override;
    starrocks_udf::DateTimeVal get_datetime_val(ExprContext* context, TupleRow*) override;
    starrocks_udf::DecimalVal get_decimal_val(ExprContext* context, TupleRow*) override;
    starrocks_udf::DecimalV2Val get_decimalv2_val(ExprContext* context, TupleRow*) override;
    // virtual starrocks_udf::ArrayVal GetArrayVal(ExprContext* context, TupleRow*);

    // vector query engine
    ColumnPtr evaluate(ExprContext* context, vectorized::Chunk* ptr) override;

    static ColumnPtr& get_column(Expr* expr, vectorized::Chunk* chunk);

private:
    int _tuple_idx = 0;                         // within row
    int _slot_offset = 0;                       // within tuple
    NullIndicatorOffset _null_indicator_offset; // within tuple
    const SlotId _slot_id;
    TupleId _tuple_id = 0; // used for desc this slot from
    bool _is_nullable = false;
};

inline bool SlotRef::vector_compute_fn(Expr* expr, VectorizedRowBatch* /* batch */) {
    return true;
}

inline void* SlotRef::get_value(Expr* expr, TupleRow* row) {
    SlotRef* ref = (SlotRef*)expr;
    Tuple* t = row->get_tuple(ref->_tuple_idx);
    if (t == nullptr || t->is_null(ref->_null_indicator_offset)) {
        return nullptr;
    }
    return t->get_slot(ref->_slot_offset);
}

inline void* SlotRef::get_slot(TupleRow* row) {
    Tuple* t = row->get_tuple(_tuple_idx);
    DCHECK(t != nullptr);
    return t->get_slot(_slot_offset);
}

inline Tuple* SlotRef::get_tuple(TupleRow* row) {
    Tuple* t = row->get_tuple(_tuple_idx);
    return t;
}

inline bool SlotRef::is_null_bit_set(TupleRow* row) {
    Tuple* t = row->get_tuple(_tuple_idx);
    DCHECK(t != nullptr);
    return t->is_null(_null_indicator_offset);
}

inline bool SlotRef::is_nullable(Expr* expr) {
    SlotRef* ref = (SlotRef*)expr;
    DCHECK(ref != nullptr);
    return ref->_is_nullable;
}

inline ColumnPtr& SlotRef::get_column(Expr* expr, vectorized::Chunk* chunk) {
    SlotRef* ref = (SlotRef*)expr;
    return (chunk)->get_column_by_slot_id(ref->slot_id());
}
} // namespace starrocks

#endif
