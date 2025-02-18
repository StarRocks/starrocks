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

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "exprs/expr.h"
#include "exprs/runtime_filter.h"
#include "types/large_int_value.h"

namespace starrocks {
template <LogicalType Type>
class MinMaxPredicate : public Expr {
public:
    using CppType = RunTimeCppType<Type>;
    MinMaxPredicate(SlotId slot_id, const CppType& min_value, const CppType& max_value, bool has_null)
            : Expr(TypeDescriptor(Type), false),
              _slot_id(slot_id),
              _min_value(min_value),
              _max_value(max_value),
              _has_null(has_null) {
        _node_type = TExprNodeType::RUNTIME_FILTER_MIN_MAX_EXPR;
    }
    ~MinMaxPredicate() override = default;
    Expr* clone(ObjectPool* pool) const override {
        return pool->add(new MinMaxPredicate<Type>(_slot_id, _min_value, _max_value, _has_null));
    }

    bool is_constant() const override { return false; }
    bool is_bound(const std::vector<TupleId>& tuple_ids) const override { return false; }
    bool has_null() const { return _has_null; }

    CppType get_min_value() const { return _min_value; }
    CppType get_max_value() const { return _max_value; }

    StatusOr<ColumnPtr> evaluate_with_filter(ExprContext* context, Chunk* ptr, uint8_t* filter) override {
        const ColumnPtr col = ptr->get_column_by_slot_id(_slot_id);
        size_t size = col->size();

        std::shared_ptr<BooleanColumn> result(new BooleanColumn(size, 1));
        uint8_t* res = result->get_data().data();

        if (col->only_null()) {
            if (!_has_null) {
                memset(res, 0x0, size);
            }
            return result;
        }

        if (col->is_constant()) {
            CppType value = ColumnHelper::get_const_value<Type>(col);
            if (!(value >= _min_value && value <= _max_value)) {
                memset(res, 0x0, size);
            }
            return result;
        }

        // NOTE(yan): make sure following code can be compiled into SIMD instructions:
        // in original version, we use
        //   1. memcpy filter -> res
        //   2. res[i] = res[i] && (null_data[i] || (data[i] >= _min_value && data[i] <= _max_value));
        // but they can not be compiled into SIMD instructions.
        if (col->is_nullable() && col->has_null()) {
            auto tmp = ColumnHelper::as_raw_column<NullableColumn>(col);
            uint8_t* __restrict__ null_data = tmp->null_column_data().data();
            CppType* __restrict__ data = ColumnHelper::cast_to_raw<Type>(tmp->data_column())->get_data().data();
            for (int i = 0; i < size; i++) {
                res[i] = (data[i] >= _min_value && data[i] <= _max_value);
            }

            if (_has_null) {
                for (int i = 0; i < size; i++) {
                    res[i] = res[i] | null_data[i];
                }
            } else {
                for (int i = 0; i < size; i++) {
                    res[i] = res[i] & !null_data[i];
                }
            }
        } else {
            const CppType* __restrict__ data =
                    ColumnHelper::get_data_column_by_type<Type>(col.get())->get_data().data();
            for (int i = 0; i < size; i++) {
                res[i] = (data[i] >= _min_value && data[i] <= _max_value);
            }
        }

        // NOTE(yan): filter can be used optionally.
        // if (filter != nullptr) {
        //     for (int i = 0; i < size; i++) {
        //         res[i] = res[i] & filter[i];
        //     }
        // }

        return result;
    }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {
        return evaluate_with_filter(context, ptr, nullptr);
    }

    int get_slot_ids(std::vector<SlotId>* slot_ids) const override {
        slot_ids->emplace_back(_slot_id);
        return 1;
    }

    std::string debug_string() const override {
        std::stringstream out;
        auto expr_debug_string = Expr::debug_string();
        out << "MinMaxPredicate (type=" << Type << ", slot_id=" << _slot_id << ", has_null=" << _has_null
            << ", min=" << _min_value << ", max=" << _max_value << ", expr(" << expr_debug_string << "))";
        return out.str();
    }

private:
    SlotId _slot_id;
    const CppType _min_value;
    const CppType _max_value;
    bool _has_null = false;
};

class MinMaxPredicateBuilder {
public:
    MinMaxPredicateBuilder(ObjectPool* pool, SlotId slot_id, const RuntimeFilter* filter)
            : _pool(pool), _slot_id(slot_id), _filter(filter) {}

    template <LogicalType ltype>
    Expr* operator()() {
        auto* minmax = down_cast<const MinMaxRuntimeFilter<ltype>*>(_filter->get_min_max_filter());
        return _pool->add(new MinMaxPredicate<ltype>(_slot_id, minmax->min_value(_pool), minmax->max_value(_pool),
                                                     _filter->has_null()));
    }

private:
    ObjectPool* _pool;
    SlotId _slot_id;
    const RuntimeFilter* _filter;
};

} // namespace starrocks
