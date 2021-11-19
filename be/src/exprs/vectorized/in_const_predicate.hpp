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

template <PrimitiveType Type>
constexpr bool can_use_bitmap() {
    return Type == TYPE_BOOLEAN || Type == TYPE_TINYINT || Type == TYPE_SMALLINT || Type == TYPE_INT ||
           Type == TYPE_BIGINT;
}

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
    VectorizedInConstPredicate(const TExprNode& node)
            : Predicate(node), _is_not_in(node.in_predicate.is_not_in), _is_prepare(false), _null_in_set(false) {}

    VectorizedInConstPredicate(const VectorizedInConstPredicate& other)
            : Predicate(other),
              _is_not_in(other._is_not_in),
              _is_prepare(other._is_prepare),
              _null_in_set(other._null_in_set),
              _is_join_runtime_filter(other._is_join_runtime_filter),
              _eq_null(other._eq_null),
              _bitmap_size(other._bitmap_size) {}

    ~VectorizedInConstPredicate() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedInConstPredicate(*this)); }

    Status prepare([[maybe_unused]] RuntimeState* state) {
        if (_is_prepare) {
            return Status::OK();
        }
        _hash_set.clear();
        _init_bitmap_buffer();
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
        _init_bitmap_buffer();
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

        bool use_bitmap = is_use_bitmap();
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
                continue;
            }

            if (use_bitmap) {
                if constexpr (in_const_pred_detail::can_use_bitmap<Type>()) {
                    _set_bitmap_index(viewer.value(0));
                }
            } else {
                _hash_set.emplace(viewer.value(0));
            }
        }
        return Status::OK();
    }

    template <bool use_bitmap>
    ColumnPtr eval_on_chunk_both_column_and_set_not_has_null(const ColumnPtr& lhs) {
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
            for (int row = 0; row < size; ++row) {
                if constexpr (use_bitmap && in_const_pred_detail::can_use_bitmap<Type>()) {
                    data3[row] = _get_bitmap_index(data[row]);
                } else {
                    data3[row] = static_cast<uint8_t>(_hash_set.contains(data[row]));
                }
            }
            if (_is_not_in) {
                for (int i = 0; i < size; i++) {
                    data3[i] = 1 - data3[i];
                }
            }
        } else {
            if (size > 0) {
                uint8_t ret = 0;
                if constexpr (use_bitmap && in_const_pred_detail::can_use_bitmap<Type>()) {
                    ret = _get_bitmap_index(data[0]);
                } else {
                    ret = static_cast<uint8_t>(_hash_set.contains(data[0]));
                }
                if (_is_not_in) {
                    ret = 1 - ret;
                }
                memset(data3, ret, size);
            }
        }

        if (lhs->is_constant()) {
            return ConstColumn::create(result, 1);
        }
        return result;
    }

    // null_in_set: true means null is a value of _hash_set.
    // equal_null: true means that 'null' in column and 'null' in set is equal.
    template <bool null_in_set, bool equal_null, bool use_bitmap>
    ColumnPtr eval_on_chunk(const ColumnPtr& lhs) {
        ColumnBuilder<TYPE_BOOLEAN> builder;
        ColumnViewer<Type> viewer(lhs);

        uint8_t* output = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(builder.data_column())->get_data().data();
        size_t size = viewer.size();

        for (int row = 0; row < size; ++row) {
            if (viewer.is_null(row)) {
                if constexpr (equal_null) {
                    builder.append(1);
                } else {
                    builder.append_null();
                }
                continue;
            }
            // find value
            if constexpr (use_bitmap && in_const_pred_detail::can_use_bitmap<Type>()) {
                if (_get_bitmap_index(viewer.value(row))) {
                    builder.append(1);
                    continue;
                }
            } else {
                if (_hash_set.contains(viewer.value(row))) {
                    builder.append(1);
                    continue;
                }
            }
            if constexpr (!null_in_set || equal_null) {
                builder.append(0);
            } else {
                builder.append_null();
            }
        }

        if (_is_not_in) {
            for (int i = 0; i < size; i++) {
                output[i] = 1 - output[i];
            }
        }

        return builder.build(lhs->is_constant());
    }

    ColumnPtr evaluate(ExprContext* context, vectorized::Chunk* ptr) override {
        ColumnPtr lhs = _children[0]->evaluate(context, ptr);
        if (ColumnHelper::count_nulls(lhs) == lhs->size()) {
            return ColumnHelper::create_const_null_column(lhs->size());
        }
        bool use_bitmap = is_use_bitmap();

        if (_null_in_set) {
            if (_eq_null) {
                if (!use_bitmap) {
                    return this->template eval_on_chunk<true, true, false>(lhs);
                } else {
                    return this->template eval_on_chunk<true, true, true>(lhs);
                }
            } else {
                if (!use_bitmap) {
                    return this->template eval_on_chunk<true, false, false>(lhs);
                } else {
                    return this->template eval_on_chunk<true, false, true>(lhs);
                }
            }
        } else if (lhs->is_nullable()) {
            if (!use_bitmap) {
                return this->template eval_on_chunk<false, false, false>(lhs);
            } else {
                return this->template eval_on_chunk<false, false, true>(lhs);
            }
        } else {
            if (!use_bitmap) {
                return eval_on_chunk_both_column_and_set_not_has_null<false>(lhs);
            } else {
                return eval_on_chunk_both_column_and_set_not_has_null<true>(lhs);
            }
        }
    }

    void insert(typename RunTimeTypeTraits<Type>::CppType* value) {
        if (value == nullptr) {
            _null_in_set = true;
        } else {
            _hash_set.emplace(*value);
        }
    }

    void insert_bitmap(typename RunTimeTypeTraits<Type>::CppType* value) {
        if (value == nullptr) {
            _null_in_set = true;
        } else {
            if constexpr (in_const_pred_detail::can_use_bitmap<Type>()) {
                _set_bitmap_index(*value);
            }
        }
    }

    const in_const_pred_detail::PHashSetType<Type>& hash_set() const { return _hash_set; }

    bool is_not_in() const { return _is_not_in; }

    bool null_in_set() const { return _null_in_set; }

    void set_null_in_set(bool v) { _null_in_set = v; }

    bool is_join_runtime_filter() const { return _is_join_runtime_filter; }

    void set_is_join_runtime_filter() { _is_join_runtime_filter = true; }

    void set_eq_null(bool value) { _eq_null = value; }

    void set_bitmap_size(int bitmap_size) { _bitmap_size = bitmap_size; }

    bool is_use_bitmap() const { return _bitmap_size != 0; }

private:
    // Note(yan): It's very tempting to use real bitmap. But the scenario is, the bitmap size is usually small like dict codes
    // Use real bitmap involves bit shift, and/or ops. Since the bitmap size is quite small, we can use trade memory usage for performance
    // And according to experiments, there is 20% performance gain.
    // inline void _set_bitmap_index(int64_t index) {
    //     uint8_t* buf = _bitmap_buffer.data();
    //     buf[index >> 3] |= (1 << (index & 0x7));
    // }
    // inline uint8_t _get_bitmap_index(int64_t index) const {
    //     const uint8_t* buf = _bitmap_buffer.data();
    //     return (buf[index >> 3] >> (index & 0x7)) & 0x1;
    // }
    // void _init_bitmap_buffer() {
    //     if constexpr (in_const_pred_detail::can_use_bitmap<Type>()) {
    //         if (is_use_bitmap()) {
    //             size_t alloc_size = (_bitmap_size + 7) / 8;
    //             _bitmap_buffer.assign(alloc_size, 0);
    //         }
    //     }
    // }

    inline void _set_bitmap_index(int64_t index) { _bitmap_buffer[index] = 1; }
    inline uint8_t _get_bitmap_index(int64_t index) const { return _bitmap_buffer[index]; }

    void _init_bitmap_buffer() {
        if constexpr (in_const_pred_detail::can_use_bitmap<Type>()) {
            if (is_use_bitmap()) {
                _bitmap_buffer.assign(_bitmap_size, 0);
            }
        }
    }

    const bool _is_not_in;
    bool _is_prepare;
    bool _null_in_set;
    bool _is_join_runtime_filter = false;
    bool _eq_null = false;
    int _bitmap_size = 0;
    std::vector<uint8_t> _bitmap_buffer;

    in_const_pred_detail::PHashSetType<Type> _hash_set;
    // Ensure the string memory don't early free
    std::vector<ColumnPtr> _string_values;
}; // namespace vectorized

} // namespace vectorized
} // namespace starrocks
