// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/hash_set.h"
#include "common/object_pool.h"
#include "exprs/predicate.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

class RuntimeState;
class ObjectPool;
class Expr;
class ExprContext;

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

} // namespace in_const_pred_detail

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
    using ValueType = typename RunTimeTypeTraits<Type>::CppType;

    VectorizedInConstPredicate(const TExprNode& node)
            : Predicate(node), _is_not_in(node.in_predicate.is_not_in), _is_prepare(false), _null_in_set(false) {}

    VectorizedInConstPredicate(const VectorizedInConstPredicate& other)
            : Predicate(other),
              _is_not_in(other._is_not_in),
              _is_prepare(other._is_prepare),
              _null_in_set(other._null_in_set),
              _is_join_runtime_filter(other._is_join_runtime_filter),
              _eq_null(other._eq_null),
              _array_size(other._array_size) {}

    ~VectorizedInConstPredicate() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedInConstPredicate(*this)); }

    static constexpr bool can_use_array() {
        return Type == TYPE_BOOLEAN || Type == TYPE_TINYINT || Type == TYPE_SMALLINT || Type == TYPE_INT ||
               Type == TYPE_BIGINT;
    }

    Status prepare([[maybe_unused]] RuntimeState* state) {
        if (_is_prepare) {
            return Status::OK();
        }
        _hash_set.clear();
        _init_array_buffer();
        _is_prepare = true;
        return Status::OK();
    }

    Status merge(Predicate* predicate) override {
        if (auto* that = dynamic_cast<typeof(this)>(predicate)) {
            const auto& hash_set = that->hash_set();
            _hash_set.insert(hash_set.begin(), hash_set.end());
            _null_in_set = _null_in_set || that->null_in_set();
            return Status::OK();
        } else {
            return Status::NotSupported(strings::Substitute("$0 cannot be merged with VectorizedInConstPredicate",
                                                            predicate->debug_string()));
        }
    }

    Status prepare(RuntimeState* state, ExprContext* context) override {
        Expr::prepare(state, context);

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
        _init_array_buffer();
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

        bool use_array = is_use_array();
        for (int i = 1; i < _children.size(); ++i) {
            if ((_children[0]->type().is_string_type() && _children[i]->type().is_string_type()) ||
                (_children[0]->type().type == _children[i]->type().type) ||
                (PrimitiveType::TYPE_NULL == _children[i]->type().type)) {
                // pass
            } else {
                return Status::InternalError("VectorizedInPredicate type not same");
            }

            ASSIGN_OR_RETURN(ColumnPtr value, _children[i]->evaluate_checked(context, nullptr));
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
                continue;
            }

            if (use_array) {
                if constexpr (can_use_array()) {
                    _set_array_index(viewer.value(0));
                }
            } else {
                _hash_set.emplace(viewer.value(0));
            }
        }
        return Status::OK();
    }

    template <bool use_array>
    ColumnPtr eval_on_chunk_both_column_and_set_not_has_null(const ColumnPtr& lhs, uint8_t* filter) {
        DCHECK(!_null_in_set);
        auto size = lhs->size();

        // input data
        auto lhs_data = lhs->is_constant() ? ColumnHelper::as_raw_column<ConstColumn>(lhs)->data_column() : lhs;
        auto data = ColumnHelper::cast_to_raw<Type>(lhs_data)->get_data().data();

        // output data
        auto result = RunTimeColumnType<TYPE_BOOLEAN>::create();
        result->resize_uninitialized(size);
        uint8_t* data3 = result->get_data().data();

        if (!lhs->is_constant()) {
            if (filter) {
                for (int row = 0; row < size; ++row) {
                    data3[row] = (filter[row] && check_value_existence<use_array>(data[row]));
                }
            } else {
                for (int row = 0; row < size; ++row) {
                    data3[row] = check_value_existence<use_array>(data[row]);
                }
            }
            if (_is_not_in) {
                for (int i = 0; i < size; i++) {
                    data3[i] = 1 - data3[i];
                }
            }
        } else {
            if (size > 0) {
                uint8_t ret = check_value_existence<use_array>(data[0]);
                if (_is_not_in) {
                    ret = 1 - ret;
                }
                memset(data3, ret, size);
            }
        }

        if (lhs->is_constant()) {
            return ConstColumn::create(result, size);
        }
        return result;
    }

    // null_in_set: true means null is a value of _hash_set.
    // equal_null: true means that 'null' in column and 'null' in set is equal.
    template <bool null_in_set, bool equal_null, bool use_array>
    ColumnPtr eval_on_chunk(const ColumnPtr& lhs, uint8_t* filter) {
        ColumnViewer<Type> viewer(lhs);
        size_t size = viewer.size();
        ColumnBuilder<TYPE_BOOLEAN> builder(size);
        builder.resize_uninitialized(size);

        uint8_t* null_data = builder.null_column()->get_data().data();
        memset(null_data, 0x0, size);
        uint8_t* output = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(builder.data_column())->get_data().data();

        auto update_row = [&](int row) {
            if (viewer.is_null(row)) {
                if constexpr (equal_null) {
                    output[row] = 1;
                } else {
                    null_data[row] = 1;
                }
                return;
            }
            // find value
            if (check_value_existence<use_array>(viewer.value(row))) {
                output[row] = 1;
                return;
            }
            if constexpr (!null_in_set || equal_null) {
                output[row] = 0;
            } else {
                null_data[row] = 1;
            }
        };

        if (filter != nullptr) {
            memset(output, 0x0, size);
            for (int row = 0; row < size; ++row) {
                if (filter[row]) {
                    update_row(row);
                }
            }
        } else {
            for (int row = 0; row < size; ++row) {
                update_row(row);
            }
        }

        if (_is_not_in) {
            for (int i = 0; i < size; i++) {
                output[i] = 1 - output[i];
            }
        }

        if (std::memchr(null_data, 0x1, size) != nullptr) {
            builder.set_has_null(true);
        }

        auto result = builder.build(lhs->is_constant());
        if (result->is_constant()) {
            result->resize(lhs->size());
        }
        return result;
    }

    StatusOr<ColumnPtr> evaluate_with_filter(ExprContext* context, vectorized::Chunk* ptr, uint8_t* filter) override {
        ASSIGN_OR_RETURN(ColumnPtr lhs, _children[0]->evaluate_checked(context, ptr));
        if (!_eq_null && ColumnHelper::count_nulls(lhs) == lhs->size()) {
            return ColumnHelper::create_const_null_column(lhs->size());
        }
        bool use_array = is_use_array();

        if (_null_in_set) {
            if (_eq_null) {
                if (!use_array) {
                    return this->template eval_on_chunk<true, true, false>(lhs, filter);
                } else {
                    return this->template eval_on_chunk<true, true, true>(lhs, filter);
                }
            } else {
                if (!use_array) {
                    return this->template eval_on_chunk<true, false, false>(lhs, filter);
                } else {
                    return this->template eval_on_chunk<true, false, true>(lhs, filter);
                }
            }
        } else if (lhs->is_nullable()) {
            if (!use_array) {
                return this->template eval_on_chunk<false, false, false>(lhs, filter);
            } else {
                return this->template eval_on_chunk<false, false, true>(lhs, filter);
            }
        } else {
            if (!use_array) {
                return eval_on_chunk_both_column_and_set_not_has_null<false>(lhs, filter);
            } else {
                return eval_on_chunk_both_column_and_set_not_has_null<true>(lhs, filter);
            }
        }
    }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override {
        return evaluate_with_filter(context, ptr, nullptr);
    }

    void insert(const ValueType* value) {
        if (value == nullptr) {
            _null_in_set = true;
        } else {
            _hash_set.emplace(*value);
        }
    }

    void insert_array(const ValueType* value) {
        if (value == nullptr) {
            _null_in_set = true;
        } else {
            if constexpr (can_use_array()) {
                _set_array_index(*value);
            }
        }
    }

    template <bool use_array>
    uint8_t check_value_existence(const ValueType& value) const {
        if constexpr (use_array && can_use_array()) {
            return _get_array_index(value);
        } else {
            return static_cast<uint8_t>(_hash_set.contains(value));
        }
    }

    const in_const_pred_detail::PHashSetType<Type>& hash_set() const { return _hash_set; }

    bool is_not_in() const { return _is_not_in; }

    bool null_in_set() const { return _null_in_set; }

    void set_null_in_set(bool v) { _null_in_set = v; }

    bool is_join_runtime_filter() const { return _is_join_runtime_filter; }

    void set_is_join_runtime_filter(bool v) { _is_join_runtime_filter = v; }

    void set_eq_null(bool value) { _eq_null = value; }

    void set_array_size(int array_size) { _array_size = array_size; }

    bool is_use_array() const { return _array_size != 0; }

private:
    // Note(yan): It's very tempting to use real bitmap, but the real scenario is, the array size is usually small like dict codes.
    // To usse real bitmap involves bit shift, and/or ops, which eats much cpu cycles.
    // Since the bitmap size is quite small, we can use trade memory usage for performance
    // According to experiments, there is 20% performance gain.

    void _set_array_index(int64_t index) { _array_buffer[index] = 1; }
    uint8_t _get_array_index(int64_t index) const { return _array_buffer[index]; }

    void _init_array_buffer() {
        if constexpr (can_use_array()) {
            if (is_use_array()) {
                _array_buffer.assign(_array_size, 0);
            }
        }
    }

    const bool _is_not_in;
    bool _is_prepare;
    bool _null_in_set;
    bool _is_join_runtime_filter = false;
    bool _eq_null = false;
    int _array_size = 0;
    std::vector<uint8_t> _array_buffer;

    in_const_pred_detail::PHashSetType<Type> _hash_set;
    // Ensure the string memory don't early free
    std::vector<ColumnPtr> _string_values;
};

class VectorizedInConstPredicateBuilder {
public:
    VectorizedInConstPredicateBuilder(RuntimeState* state, ObjectPool* pool, Expr* expr)
            : _state(state),
              _pool(pool),
              _expr(expr),
              _eq_null(false),
              _null_in_set(false),
              _is_not_in(false),
              _is_join_runtime_filter(false),
              _array_size(0),
              _in_pred_ctx(nullptr) {}

    Status create();
    Status add_values(const ColumnPtr& column, size_t column_offset);
    void use_array_set(size_t array_size) { _array_size = array_size; }
    void use_as_join_runtime_filter() { _is_join_runtime_filter = true; }
    void set_eq_null(bool v) { _eq_null = v; }
    void set_null_in_set(bool v) { _null_in_set = v; }
    void set_is_not_in(bool v) { _is_not_in = v; }
    ExprContext* get_in_const_predicate() const { return _in_pred_ctx; }

private:
    ExprContext* _create();
    RuntimeState* _state;
    ObjectPool* _pool;
    Expr* _expr;
    bool _eq_null;
    bool _null_in_set;
    bool _is_not_in;
    bool _is_join_runtime_filter;
    int _array_size;
    ExprContext* _in_pred_ctx;
    Status _st;
};

} // namespace vectorized
} // namespace starrocks
