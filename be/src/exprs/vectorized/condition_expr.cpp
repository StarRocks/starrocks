// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/condition_expr.h"

#include "column/chunk.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/const_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "gutil/casts.h"
#include "runtime/primitive_type.h"
#include "runtime/types.h"
#include "simd/selector.h"
#include "util/dispatch.h"
#include "util/percentile_value.h"

namespace starrocks::vectorized {

template <bool isConstC0, bool isConst1, PrimitiveType Type>
struct SelectIfOP {
    static ColumnPtr eval(ColumnPtr& value0, ColumnPtr& value1, ColumnPtr& selector, const TypeDescriptor& type_desc) {
        [[maybe_unused]] Column::Filter& select_vec = ColumnHelper::merge_nullable_filter(selector.get());
        [[maybe_unused]] auto* input_data0 = ColumnHelper::get_data_column(value0.get());
        [[maybe_unused]] auto* input_data1 = ColumnHelper::get_data_column(value1.get());

        ColumnPtr res = ColumnHelper::create_column(type_desc, false);
        auto* res_col = down_cast<RunTimeColumnType<Type>*>(res.get());
        auto& res_data = res_col->get_data();
        res_data.resize(select_vec.size());
        if constexpr (isConstC0 && isConst1) {
            auto v0 = ColumnHelper::get_const_value<Type>(value0);
            auto v1 = ColumnHelper::get_const_value<Type>(value1);
            SIMD_selector<Type>::select_if(select_vec.data(), res_data, v0, v1);
        } else if constexpr (isConstC0 && !isConst1) {
            auto v0 = ColumnHelper::get_const_value<Type>(value0);
            auto* raw_col1 = down_cast<RunTimeColumnType<Type>*>(input_data1);
            SIMD_selector<Type>::select_if(select_vec.data(), res_data, v0, raw_col1->get_data());
        } else if constexpr (!isConstC0 && isConst1) {
            auto* raw_col0 = down_cast<RunTimeColumnType<Type>*>(input_data0);
            auto v1 = ColumnHelper::get_const_value<Type>(value1);
            SIMD_selector<Type>::select_if(select_vec.data(), res_data, raw_col0->get_data(), v1);
        } else if constexpr (!isConstC0 && !isConst1) {
            auto* raw_col0 = down_cast<RunTimeColumnType<Type>*>(input_data0);
            auto* raw_col1 = down_cast<RunTimeColumnType<Type>*>(input_data1);
            SIMD_selector<Type>::select_if(select_vec.data(), res_data, raw_col0->get_data(), raw_col1->get_data());
        }
        return res;
    }
};

#define DEFINE_CLASS_CONSTRUCT_FN(NAME)         \
    NAME(const TExprNode& node) : Expr(node) {} \
                                                \
    ~NAME() {}                                  \
                                                \
    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new NAME(*this)); }

template <PrimitiveType Type>
class VectorizedIfNullExpr : public Expr {
public:
    DEFINE_CLASS_CONSTRUCT_FN(VectorizedIfNullExpr);

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override {
        ASSIGN_OR_RETURN(auto lhs, _children[0]->evaluate_checked(context, ptr));

        int null_count = ColumnHelper::count_nulls(lhs);
        if (null_count == 0) {
            return lhs->clone();
        }

        ASSIGN_OR_RETURN(auto rhs, _children[1]->evaluate_checked(context, ptr));
        if (null_count == lhs->size()) {
            return rhs->clone();
        }

        Columns list = {lhs, rhs};
        return _evaluate_general(list);
    }

private:
    ColumnPtr _evaluate_general(const Columns& columns) {
        ColumnViewer<Type> lhs_viewer(columns[0]);
        ColumnViewer<Type> rhs_viewer(columns[1]);
        auto [all_const, num_rows] = ColumnHelper::num_packed_rows(columns);

        ColumnBuilder<Type> result(num_rows, this->type().precision, this->type().scale);

        for (int row = 0; row < num_rows; ++row) {
            if (lhs_viewer.is_null(row)) {
                result.append(rhs_viewer.value(row), rhs_viewer.is_null(row));
            } else {
                result.append(lhs_viewer.value(row), lhs_viewer.is_null(row));
            }
        }

        return result.build(ColumnHelper::is_all_const(columns));
    }
};

template <PrimitiveType Type>
class VectorizedNullIfExpr : public Expr {
public:
    DEFINE_CLASS_CONSTRUCT_FN(VectorizedNullIfExpr);

    // NullIF: return null if lhs == rhs else return lhs
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override {
        ASSIGN_OR_RETURN(auto lhs, _children[0]->evaluate_checked(context, ptr));
        if (ColumnHelper::count_nulls(lhs) == lhs->size()) {
            return ColumnHelper::create_const_null_column(lhs->size());
        }

        ASSIGN_OR_RETURN(auto rhs, _children[1]->evaluate_checked(context, ptr));
        if (ColumnHelper::count_nulls(rhs) == rhs->size()) {
            return lhs->clone();
        }

        Columns list = {lhs, rhs};
        return _evaluate_general(list);
    }

private:
    ColumnPtr _evaluate_general(const Columns& columns) {
        ColumnViewer<Type> lhs_viewer(columns[0]);
        ColumnViewer<Type> rhs_viewer(columns[1]);

        size_t size = columns[0]->size();
        ColumnBuilder<Type> result(size, this->type().precision, this->type().scale);
        for (int row = 0; row < size; ++row) {
            if (lhs_viewer.is_null(row)) {
                result.append_null();
                continue;
            }

            if (!rhs_viewer.is_null(row) && (rhs_viewer.value(row) == lhs_viewer.value(row))) {
                result.append_null();
                continue;
            }

            result.append(lhs_viewer.value(row), lhs_viewer.is_null(row));
        }

        return result.build(ColumnHelper::is_all_const(columns));
    }
};

template <PrimitiveType Type>
class VectorizedIfExpr : public Expr {
public:
    DEFINE_CLASS_CONSTRUCT_FN(VectorizedIfExpr);

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override {
        ASSIGN_OR_RETURN(auto bhs, _children[0]->evaluate_checked(context, ptr));
        int true_count = ColumnHelper::count_true_with_notnull(bhs);

        ASSIGN_OR_RETURN(auto lhs, _children[1]->evaluate_checked(context, ptr));
        if (true_count == bhs->size()) {
            return lhs->clone();
        }

        ASSIGN_OR_RETURN(auto rhs, _children[2]->evaluate_checked(context, ptr));
        if (true_count == 0) {
            return rhs->clone();
        }

        if (lhs->only_null() && rhs->only_null()) {
            return lhs->clone();
        }

        Columns list = {bhs, lhs, rhs};

        auto bhs_nulls = ColumnHelper::count_nulls(bhs);
        auto lhs_nulls = ColumnHelper::count_nulls(lhs);
        auto rhs_nulls = ColumnHelper::count_nulls(rhs);

        // optimization for 3 columns all not null.
        if (bhs_nulls == 0 && lhs_nulls == 0 && rhs_nulls == 0) {
            // only arithmetic type could use SIMD optimization
            if (bhs->is_constant() || !isArithmeticPT<Type>) {
                return _evaluate_general<false>(list);
            } else if constexpr (isArithmeticPT<Type>) {
                return dispatch_nonull_template<SelectIfOP, Type>(lhs, rhs, bhs, type());
            } else {
                __builtin_unreachable();
            }
        } else {
            if constexpr (isArithmeticPT<Type>) {
                // SIMD branch
                size_t num_rows = list[0]->size();
                // get null data
                auto lns = get_null_column(num_rows, lhs);
                auto rns = get_null_column(num_rows, rhs);
                // get data columns
                auto lds = get_data_column(num_rows, lhs);
                auto rds = get_data_column(num_rows, rhs);
                // call select if
                auto selector = bhs->only_null() ? UInt8Column::create(num_rows) : bhs;
                auto select_data = dispatch_nonull_template<SelectIfOP, Type>(lds, rds, bhs, type());
                auto select_null =
                        dispatch_nonull_template<SelectIfOP, TYPE_BOOLEAN>(lns, rns, bhs, TypeDescriptor(TYPE_BOOLEAN));
                auto res = NullableColumn::create(select_data, ColumnHelper::as_column<NullColumn>(select_null));
                return res;
            } else {
                return _evaluate_general<true>(list);
            }
        }
    }

private:
    ColumnPtr get_null_column(int num_rows, ColumnPtr& input_col) {
        if (input_col->only_null()) {
            auto res = UInt8Column::create(num_rows);
            res->get_data().assign(num_rows, 1);
            return res;
        } else if (input_col->is_nullable()) {
            return down_cast<NullableColumn*>(input_col.get())->null_column();
        } else {
            return UInt8Column::create(num_rows);
        }
    }
    ColumnPtr get_data_column(int num_rows, ColumnPtr& input_col) {
        if (input_col->only_null()) {
            auto res = ColumnHelper::create_column(type(), false);
            res->resize(num_rows);
            return res;
        } else if (input_col->is_nullable()) {
            return down_cast<NullableColumn*>(input_col.get())->data_column();
        } else {
            return input_col;
        }
    }

    template <bool check_null>
    ColumnPtr _evaluate_general(const Columns& columns) {
        auto [all_const, num_rows] = ColumnHelper::num_packed_rows(columns);
        ColumnViewer<TYPE_BOOLEAN> bhs_viewer(columns[0]);
        ColumnViewer<Type> lhs_viewer(columns[1]);
        ColumnViewer<Type> rhs_viewer(columns[2]);
        ColumnBuilder<Type> result(num_rows, this->type().precision, this->type().scale);
        if constexpr (check_null) {
            for (int row = 0; row < num_rows; ++row) {
                if (bhs_viewer.is_null(row) || !bhs_viewer.value(row)) {
                    if (rhs_viewer.is_null(row)) {
                        result.append_null();
                    } else {
                        result.append(rhs_viewer.value(row));
                    }
                } else {
                    if (lhs_viewer.is_null(row)) {
                        result.append_null();
                    } else {
                        result.append(lhs_viewer.value(row));
                    }
                }
            }
        } else {
            for (int row = 0; row < num_rows; ++row) {
                if (!bhs_viewer.value(row)) {
                    result.append(rhs_viewer.value(row));
                } else {
                    result.append(lhs_viewer.value(row));
                }
            }
        }
        return result.build(all_const);
    }
};

template <PrimitiveType Type>
class VectorizedCoalesceExpr : public Expr {
public:
    DEFINE_CLASS_CONSTRUCT_FN(VectorizedCoalesceExpr);

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override {
        std::vector<ColumnViewer<Type>> viewers;
        std::vector<ColumnPtr> columns;
        for (int i = 0; i < _children.size(); ++i) {
            ASSIGN_OR_RETURN(auto value, _children[i]->evaluate_checked(context, ptr));
            auto null_count = ColumnHelper::count_nulls(value);

            // 1.return if first column is all not null.
            // 2.if there is a column all not null, It at least maybe the choice.
            // 3.don't need check if value is all null
            if (null_count == 0) {
                if (viewers.size() == 0) {
                    return value->clone();
                }

                // There is a column all not null.
                viewers.push_back(ColumnViewer<Type>(value));
                columns.push_back(value);

                // put it in viewers as last column.
                break;
            } else if (null_count != value->size()) {
                viewers.push_back(ColumnViewer<Type>(value));
                columns.push_back(value);
            }
        }

        // return only null
        if (columns.size() == 0) {
            return ColumnHelper::create_const_null_column(ptr != nullptr ? ptr->num_rows() : 1);
        }

        // direct return if only one
        if (columns.size() == 1) {
            // TODO: don't copy column if chunk support copy on write
            return columns[0]->clone();
        }

        // choose not null
        int size = columns[0]->size();
        int col_size = viewers.size();
        ColumnBuilder<Type> builder(size, this->type().precision, this->type().scale);

        for (int row = 0; row < size; ++row) {
            int col;
            for (col = 0; col < (col_size - 1); ++col) {
                // if not null
                if (!viewers[col].is_null(row)) {
                    builder.append(viewers[col].value(row));
                    break;
                }
            }

            // if last column
            if (col == (col_size - 1)) {
                if (viewers[col].is_null(row)) {
                    builder.append_null();
                } else {
                    builder.append(viewers[col].value(row));
                }
            }
        }

        return builder.build(ColumnHelper::is_all_const(columns));
    }
};

#undef DEFINE_CLASS_CONSTRUCT_FN

#define CASE_TYPE(TYPE, CLASS)        \
    case TYPE: {                      \
        return new CLASS<TYPE>(node); \
    }

#define CASE_ALL_TYPE(CLASS)           \
    CASE_TYPE(TYPE_BOOLEAN, CLASS);    \
    CASE_TYPE(TYPE_TINYINT, CLASS);    \
    CASE_TYPE(TYPE_SMALLINT, CLASS);   \
    CASE_TYPE(TYPE_INT, CLASS);        \
    CASE_TYPE(TYPE_BIGINT, CLASS);     \
    CASE_TYPE(TYPE_LARGEINT, CLASS);   \
    CASE_TYPE(TYPE_FLOAT, CLASS);      \
    CASE_TYPE(TYPE_DOUBLE, CLASS);     \
    CASE_TYPE(TYPE_CHAR, CLASS);       \
    CASE_TYPE(TYPE_VARCHAR, CLASS);    \
    CASE_TYPE(TYPE_TIME, CLASS);       \
    CASE_TYPE(TYPE_DATE, CLASS);       \
    CASE_TYPE(TYPE_DATETIME, CLASS);   \
    CASE_TYPE(TYPE_DECIMALV2, CLASS);  \
    CASE_TYPE(TYPE_OBJECT, CLASS);     \
    CASE_TYPE(TYPE_HLL, CLASS);        \
    CASE_TYPE(TYPE_PERCENTILE, CLASS); \
    CASE_TYPE(TYPE_DECIMAL32, CLASS);  \
    CASE_TYPE(TYPE_DECIMAL64, CLASS);  \
    CASE_TYPE(TYPE_DECIMAL128, CLASS);

Expr* VectorizedConditionExprFactory::create_if_null_expr(const starrocks::TExprNode& node) {
    PrimitiveType resultType = TypeDescriptor::from_thrift(node.type).type;
    switch (resultType) {
        CASE_ALL_TYPE(VectorizedIfNullExpr);
    default:
        LOG(WARNING) << "vectorized engine not support type: " << resultType;
        return nullptr;
    }
}

Expr* VectorizedConditionExprFactory::create_null_if_expr(const TExprNode& node) {
    PrimitiveType resultType = TypeDescriptor::from_thrift(node.type).type;
    switch (resultType) {
        CASE_ALL_TYPE(VectorizedNullIfExpr);
    default:
        LOG(WARNING) << "vectorized engine not support type: " << resultType;
        return nullptr;
    }
}

Expr* VectorizedConditionExprFactory::create_if_expr(const TExprNode& node) {
    PrimitiveType resultType = TypeDescriptor::from_thrift(node.type).type;

    switch (resultType) {
        CASE_ALL_TYPE(VectorizedIfExpr);
    default:
        LOG(WARNING) << "vectorized engine not support type: " << resultType;
        return nullptr;
    }
}

Expr* VectorizedConditionExprFactory::create_coalesce_expr(const TExprNode& node) {
    PrimitiveType resultType = TypeDescriptor::from_thrift(node.type).type;
    switch (resultType) {
        CASE_ALL_TYPE(VectorizedCoalesceExpr);
    default:
        LOG(WARNING) << "vectorized engine not support type: " << resultType;
        return nullptr;
    }
}

#undef CASE_TYPE
#undef CASE_ALL_TYPE

} // namespace starrocks::vectorized
