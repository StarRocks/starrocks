// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/hash_set.h"
#include "common/object_pool.h"
#include "exprs/predicate.h"

namespace starrocks {
namespace vectorized {

namespace in_const_pred_detail {
template <PrimitiveType Type, typename Enable = void>
struct PHashSet {
    using PType = HashSet<RunTimeCppType<Type>>;
};

template <PrimitiveType Type>
struct PHashSet<Type, std::enable_if_t<isSlicePT<Type>>> {
    using PType = SliceHashSet;
};

template <PrimitiveType Type>
using PHashSetType = typename PHashSet<Type>::PType;
} // in_const_pred_detail

/**
 * Support In predicate which right-values only contains const value.
 * like:
 *  a in (1, 2, 3), a in ('a', 'b', 'c')
 *
 *  Not support:
 *  a in (column1, 'a', column3), a in (select * from ....)...
 */
template <PrimitiveType Type>
class VectorizedInConstPredicate final : public Predicate {
public:
    VectorizedInConstPredicate(const TExprNode& node)
            : Predicate(node), _is_not_in(node.in_predicate.is_not_in), _is_prepare(false), _null_in_set(false) {}

    VectorizedInConstPredicate(const VectorizedInConstPredicate& other)
            : Predicate(other), _is_not_in(other._is_not_in), _null_in_set(false) {}

    ~VectorizedInConstPredicate() {}

    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedInConstPredicate(*this)); }

    Status prepare([[maybe_unused]] RuntimeState* state) {
        if (_is_prepare) {
            return Status::OK();
        }
        _hash_set.clear();
        _is_prepare = true;
        return Status::OK();
    }

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc, ExprContext* context) override {
        Expr::prepare(state, row_desc, context);

        if (_is_prepare) {
            return Status::OK();
        }

        if (Type == TYPE_NULL) {
            return Status::InternalError("Unknown NULL Type column.");
        }

        if (_children.size() < 1) {
            return Status::InternalError("VectorizedInPredicate has no arguments.");
        }

        _hash_set.clear();
        _is_prepare = true;
        return Status::OK();
    }

    Status open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override {
        RETURN_IF_ERROR(Expr::open(state, context, scope));

        if (Type != _children[0]->type().type) {
            if (!isSlicePT<Type> || !_children[0]->type().is_string_type()) {
                return Status::InternalError("VectorizedInPredicate type is error");
            }
        }

        for (int i = 1; i < _children.size(); ++i) {
            if ((_children[0]->type().is_string_type() && _children[i]->type().is_string_type()) ||
                (_children[0]->type().type == _children[i]->type().type) ||
                (PrimitiveType::TYPE_NULL == _children[i]->type().type)) {
                // pass
            } else {
                return Status::InternalError("VectorizedInPredicate type not same");
            }

            ColumnPtr value = _children[i]->evaluate(context, nullptr);
            if (!value->is_constant() && !value->only_null()) {
                return Status::InternalError("VectorizedInPredicate value not const");
            }

            ColumnViewer<Type> viewer(value);
            if (viewer.is_null(0)) {
                _null_in_set = true;
                continue;
            }

            // insert into set
            if constexpr (isSlicePT<Type>) {
                if (_hash_set.emplace(viewer.value(0)).second) {
                    _string_values.emplace_back(value);
                }
            } else {
                _hash_set.emplace(viewer.value(0));
            }
        }

        return Status::OK();
    }

    ColumnPtr eval_on_chunk_both_column_and_set_not_has_null(const ColumnPtr& lhs) {
        DCHECK(!_null_in_set);

        const bool yes_value = !_is_not_in;
        const bool no_value = _is_not_in;
        auto size = lhs->size();

        auto lhs_data = lhs->is_constant() ? ColumnHelper::as_raw_column<ConstColumn>(lhs)->data_column() : lhs;

        // input data
        auto data = ColumnHelper::cast_to_raw<Type>(lhs_data)->get_data().data();

        // output data
        auto result = RunTimeColumnType<TYPE_BOOLEAN>::create();
        result->resize_uninitialized(size);
        auto data3 = result->get_data().data();

        if (!lhs->is_constant()) {
            for (int row = 0; row < size; ++row) {
                if (_hash_set.contains(data[row])) {
                    data3[row] = yes_value;
                } else {
                    data3[row] = no_value;
                }
            }
        } else {
            if (size > 0) {
                bool value = _hash_set.contains(data[0]) ? yes_value : no_value;
                data3[0] = value;
                for (int row = 1; row < size; ++row) {
                    data3[row] = value;
                }
            }
        }

        if (lhs->is_constant()) {
            return ConstColumn::create(result, 1);
        }
        return result;
    }

    // null_in_set: true means null is a value of _hash_set.
    // equal_null: true means that 'null' in column and 'null' in set is equal.
    template <bool null_in_set, bool equal_null>
    ColumnPtr eval_on_chunk(const ColumnPtr& lhs) {
        ColumnBuilder<TYPE_BOOLEAN> builder;
        ColumnViewer<Type> viewer(lhs);

        const bool yes_value = !_is_not_in;
        [[maybe_unused]] const bool no_value = _is_not_in;
        for (int row = 0; row < viewer.size(); ++row) {
            if (viewer.is_null(row)) {
                if constexpr (equal_null) {
                    builder.append(yes_value);
                } else {
                    builder.append_null();
                }
                continue;
            }
            // find value
            if (_hash_set.contains(viewer.value(row))) {
                builder.append(yes_value);
                continue;
            }
            if constexpr (!null_in_set || equal_null) {
                builder.append(no_value);
            } else {
                builder.append_null();
            }
        }

        return builder.build(lhs->is_constant());
    }

    ColumnPtr evaluate(ExprContext* context, vectorized::Chunk* ptr) override {
        ColumnPtr lhs = _children[0]->evaluate(context, ptr);
        if (ColumnHelper::count_nulls(lhs) == lhs->size()) {
            return ColumnHelper::create_const_null_column(lhs->size());
        }

        if (_null_in_set) {
            if (_eq_null) {
                return this->template eval_on_chunk<true, true>(lhs);
            } else {
                return this->template eval_on_chunk<true, false>(lhs);
            }
        } else if (lhs->is_nullable()) {
            return this->template eval_on_chunk<false, false>(lhs);
        } else {
            return eval_on_chunk_both_column_and_set_not_has_null(lhs);
        }
    }

    void insert(typename RunTimeTypeTraits<Type>::CppType* value) {
        if (value == nullptr) {
            _null_in_set = true;
        } else {
            _hash_set.emplace(*value);
        }
    }

    const in_const_pred_detail::PHashSetType<Type>& hash_set() const { return _hash_set; }

    bool is_not_in() const { return _is_not_in; }

    bool null_in_set() const { return _null_in_set; }

    bool is_join_runtime_filter() const { return _is_join_runtime_filter; }

    void set_is_join_runtime_filter() { _is_join_runtime_filter = true; }

    void set_eq_null(bool value) { _eq_null = value; }

private:
    const bool _is_not_in;
    bool _is_prepare;
    bool _null_in_set;
    bool _is_join_runtime_filter = false;
    bool _eq_null = false;

    in_const_pred_detail::PHashSetType<Type> _hash_set;
    // Ensure the string memory don't early free
    std::vector<ColumnPtr> _string_values;
};

} // namespace vectorized
} // namespace starrocks
