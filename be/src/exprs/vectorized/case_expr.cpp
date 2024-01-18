// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/case_expr.h"

#include <cstdint>

#include "column/chunk.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "gutil/casts.h"
#include "runtime/primitive_type_infra.h"
#include "simd/mulselector.h"
#include "util/percentile_value.h"

namespace starrocks::vectorized {

/**
 * Support Case expression, like:
 *  CASE sex
 *      WHEN '1' THEN 'man'
 *      WHEN '2' THEN 'woman'
 *  ELSE 'other' END
 *
 *  or
 *
 *  CASE WHEN sex = '1' THEN 'man'
 *       WHEN sex = '2' THEN 'woman'
 *  ELSE 'other' END
 *
 *  ELSE is not necessary.
 */

template <PrimitiveType WhenType, PrimitiveType ResultType>
class VectorizedCaseExpr final : public Expr {
public:
    explicit VectorizedCaseExpr(const TExprNode& node)
            : Expr(node), _has_case_expr(node.case_expr.has_case_expr), _has_else_expr(node.case_expr.has_else_expr) {}

    ~VectorizedCaseExpr() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedCaseExpr(*this)); }

    Status open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override {
        RETURN_IF_ERROR(Expr::open(state, context, scope));

        // children size check
        if ((_has_case_expr ^ _has_else_expr) == 0) {
            return _children.size() % 2 == 0 ? Status::OK() : Status::InvalidArgument("case when children is error!");
        }

        return _children.size() % 2 == 1 ? Status::OK() : Status::InvalidArgument("case when children is error!");
    }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* chunk) override {
        if (_has_case_expr) {
            return evaluate_case(context, chunk);
        } else {
            return evaluate_no_case(context, chunk);
        }
    }

private:
    // CASE 1:
    //   CASE sex
    //       WHEN '1' THEN 'man'
    //       WHEN '2' THEN 'woman'
    //   ELSE 'other' END
    //
    //   If `CASE` is null, return `ELSE`
    //   If ALL `WHEN` is null, return `ELSE`
    //   If `CASE` equals `WHEN`, return `THEN`
    //   If `CASE` can't match ANY `WHEN`, return `ELSE`
    //
    // CASE 2:
    //   CASE sex
    //       WHEN '1' THEN 'man'
    //       WHEN '2' THEN 'woman'
    //
    //   If `CASE` is null, return NULL
    //   If ALL `WHEN` is null, return NULL
    //   If `CASE` equals `WHEN`, return `THEN`
    //   If `CASE` can't match ANY `WHEN`, return NULL
    StatusOr<ColumnPtr> evaluate_case(ExprContext* context, vectorized::Chunk* chunk) {
        ColumnPtr else_column = nullptr;
        if (!_has_else_expr) {
            else_column = ColumnHelper::create_const_null_column(chunk != nullptr ? chunk->num_rows() : 1);
        } else {
            ASSIGN_OR_RETURN(else_column, _children[_children.size() - 1]->evaluate_checked(context, chunk));
        }

        ASSIGN_OR_RETURN(ColumnPtr case_column, _children[0]->evaluate_checked(context, chunk));
        if (ColumnHelper::count_nulls(case_column) == case_column->size()) {
            return else_column->clone();
        }

        int loop_end = _children.size() - 1;

        Columns when_columns;
        when_columns.reserve(loop_end);

        Columns then_columns;
        then_columns.reserve(loop_end);

        std::vector<ColumnViewer<WhenType>> when_viewers;
        when_viewers.reserve(loop_end);

        std::vector<ColumnViewer<ResultType>> then_viewers;
        then_viewers.reserve(loop_end);

        for (int i = 1; i < loop_end; i += 2) {
            ASSIGN_OR_RETURN(ColumnPtr when_column, _children[i]->evaluate_checked(context, chunk));

            // skip if all null
            if (ColumnHelper::count_nulls(when_column) == when_column->size()) {
                continue;
            }

            ASSIGN_OR_RETURN(ColumnPtr then_column, _children[i + 1]->evaluate_checked(context, chunk));

            when_viewers.emplace_back(when_column);
            then_viewers.emplace_back(then_column);

            when_columns.emplace_back(when_column);
            then_columns.emplace_back(then_column);
        }

        if (when_viewers.empty()) {
            return else_column->clone();
        }
        when_columns.emplace_back(case_column);
        then_columns.emplace_back(else_column);

        ColumnViewer<WhenType> case_viewer(case_column);
        then_viewers.emplace_back(else_column);

        size_t size = when_columns[0]->size();
        ColumnBuilder<ResultType> builder(size, this->type().precision, this->type().scale);

        bool columns_has_null = false;
        for (ColumnPtr& column : when_columns) {
            columns_has_null |= column->has_null();
        }

        size_t view_size = when_viewers.size();
        if (!columns_has_null) {
            for (int row = 0; row < size; ++row) {
                int i = 0;
                while ((i < view_size) && (when_viewers[i].value(row) != case_viewer.value(row))) {
                    i += 1;
                }
                if (!then_viewers[i].is_null(row)) {
                    builder.append(then_viewers[i].value(row));
                } else {
                    builder.append_null();
                }
            }
        } else {
            for (int row = 0; row < size; ++row) {
                int i = view_size;
                if (!case_viewer.is_null(row)) {
                    i = 0;
                    while ((i < view_size) &&
                           (when_viewers[i].is_null(row) || when_viewers[i].value(row) != case_viewer.value(row))) {
                        i += 1;
                    }
                }
                if (!then_viewers[i].is_null(row)) {
                    builder.append(then_viewers[i].value(row));
                } else {
                    builder.append_null();
                }
            }
        }

        return builder.build(ColumnHelper::is_all_const(when_columns) && ColumnHelper::is_all_const(then_columns));
    }

    // CASE 1:
    //    CASE WHEN sex = '1' THEN 'man'
    //         WHEN sex = '2' THEN 'woman'
    //    ELSE 'other' END
    //
    //  Special CASE-WHEN statment, and `WHEN` clause must be boolean.
    //  If all `WHEN` is null/false, return `ELSE`
    //  If `WHEN` is not null and true, return `THEN`
    //
    // CASE 2:
    //    CASE WHEN sex = '1' THEN 'man'
    //         WHEN sex = '2' THEN 'woman'
    //
    //  Special CASE-WHEN statment, and `WHEN` clause must be boolean.
    //  If all `WHEN` is null/false, return NULL
    //  If `WHEN` is not null and true, return `THEN`
    StatusOr<ColumnPtr> evaluate_no_case(ExprContext* context, vectorized::Chunk* chunk) {
        ColumnPtr else_column = nullptr;
        if (!_has_else_expr) {
            else_column = ColumnHelper::create_const_null_column(chunk != nullptr ? chunk->num_rows() : 1);
        } else {
            ASSIGN_OR_RETURN(else_column, _children[_children.size() - 1]->evaluate_checked(context, chunk));
        }

        int loop_end = _children.size() - 1;

        Columns when_columns;
        when_columns.reserve(loop_end);

        Columns then_columns;
        then_columns.reserve(loop_end);

        std::vector<ColumnViewer<TYPE_BOOLEAN>> when_viewers;
        when_viewers.reserve(loop_end);

        std::vector<ColumnViewer<ResultType>> then_viewers;
        then_viewers.reserve(loop_end);

        for (int i = 0; i < loop_end; i += 2) {
            ASSIGN_OR_RETURN(ColumnPtr when_column, _children[i]->evaluate_checked(context, chunk));

            size_t trues_count = ColumnHelper::count_true_with_notnull(when_column);

            // skip if all false or all null
            if (trues_count == 0) {
                continue;
            }

            ASSIGN_OR_RETURN(ColumnPtr then_column, _children[i + 1]->evaluate_checked(context, chunk));

            // direct return if first when is all true
            if (when_viewers.empty() && trues_count == when_column->size()) {
                return then_column->clone();
            }

            when_columns.emplace_back(when_column);
            then_columns.emplace_back(then_column);

            when_viewers.emplace_back(when_column);
            then_viewers.emplace_back(then_column);
        }

        if (when_viewers.empty()) {
            return else_column->clone();
        }
        then_columns.emplace_back(else_column);
        then_viewers.emplace_back(else_column);

        size_t size = when_columns[0]->size();
        ColumnBuilder<ResultType> builder(size, this->type().precision, this->type().scale);

        bool when_columns_has_null = false;
        for (ColumnPtr& column : when_columns) {
            when_columns_has_null |= column->has_null();
        }

        // max case size in use SIMD CASE WHEN implements
        constexpr int max_simd_case_when_size = 8;

        // optimization for no-nullable Arithmetic Type
        if constexpr (isArithmeticPT<ResultType>) {
            bool then_columns_has_null = false;
            for (const auto& column : then_columns) {
                then_columns_has_null |= column->has_null();
            }

            bool check_could_use_multi_simd_selector =
                    !when_columns_has_null && when_columns.size() <= max_simd_case_when_size && !then_columns_has_null;

            if (check_could_use_multi_simd_selector) {
                int then_column_size = then_columns.size();
                int when_column_size = when_columns.size();
                // TODO: avoid unpack const column
                for (int i = 0; i < then_column_size; ++i) {
                    then_columns[i] = ColumnHelper::unpack_and_duplicate_const_column(size, then_columns[i]);
                }
                for (int i = 0; i < when_column_size; ++i) {
                    when_columns[i] = ColumnHelper::unpack_and_duplicate_const_column(size, when_columns[i]);
                }
                for (int i = 0; i < when_column_size; ++i) {
                    ColumnHelper::merge_nullable_filter(when_columns[i].get());
                }

                using ResultContainer = typename RunTimeColumnType<ResultType>::Container;

                ResultContainer* select_list[then_column_size];
                for (int i = 0; i < then_column_size; ++i) {
                    auto* data_column = ColumnHelper::get_data_column(then_columns[i].get());
                    select_list[i] = &down_cast<RunTimeColumnType<ResultType>*>(data_column)->get_data();
                }

                uint8_t* select_vec[when_column_size];
                for (int i = 0; i < when_column_size; ++i) {
                    auto* data_column = ColumnHelper::get_data_column(when_columns[i].get());
                    select_vec[i] = down_cast<BooleanColumn*>(data_column)->get_data().data();
                }

                auto res = RunTimeColumnType<ResultType>::create();

                if constexpr (pt_is_decimal<ResultType>) {
                    res->set_scale(this->type().scale);
                    res->set_precision(this->type().precision);
                }

                auto& container = res->get_data();
                container.resize(size);
                SIMD_muti_selector<ResultType>::multi_select_if(select_vec, when_column_size, container, select_list,
                                                                then_column_size);
                return res;
            }
        }

        size_t view_size = when_viewers.size();
        if (!when_columns_has_null) {
            for (int row = 0; row < size; ++row) {
                int i = 0;
                while (i < view_size && !(when_viewers[i].value(row))) {
                    i += 1;
                }
                if (!then_viewers[i].is_null(row)) {
                    builder.append(then_viewers[i].value(row));
                } else {
                    builder.append_null();
                }
            }
        } else {
            for (int row = 0; row < size; ++row) {
                int i = 0;
                while ((i < view_size) && (when_viewers[i].is_null(row) || !when_viewers[i].value(row))) {
                    i += 1;
                }

                if (!then_viewers[i].is_null(row)) {
                    builder.append(then_viewers[i].value(row));
                } else {
                    builder.append_null();
                }
            }
        }

        return builder.build(ColumnHelper::is_all_const(when_columns) && ColumnHelper::is_all_const(then_columns));
    }

private:
    const bool _has_case_expr;
    const bool _has_else_expr;
};

#define CASE_WHEN_RESULT_TYPE(WHEN_TYPE, RESULT_TYPE)                \
    case WHEN_TYPE: {                                                \
        return new VectorizedCaseExpr<WHEN_TYPE, RESULT_TYPE>(node); \
    }

#define SWITCH_ALL_WHEN_TYPE(RESULT_TYPE)                                                 \
    switch (whenType) {                                                                   \
        CASE_WHEN_RESULT_TYPE(TYPE_BOOLEAN, RESULT_TYPE);                                 \
        CASE_WHEN_RESULT_TYPE(TYPE_TINYINT, RESULT_TYPE);                                 \
        CASE_WHEN_RESULT_TYPE(TYPE_SMALLINT, RESULT_TYPE);                                \
        CASE_WHEN_RESULT_TYPE(TYPE_INT, RESULT_TYPE);                                     \
        CASE_WHEN_RESULT_TYPE(TYPE_BIGINT, RESULT_TYPE);                                  \
        CASE_WHEN_RESULT_TYPE(TYPE_LARGEINT, RESULT_TYPE);                                \
        CASE_WHEN_RESULT_TYPE(TYPE_FLOAT, RESULT_TYPE);                                   \
        CASE_WHEN_RESULT_TYPE(TYPE_DOUBLE, RESULT_TYPE);                                  \
        CASE_WHEN_RESULT_TYPE(TYPE_CHAR, RESULT_TYPE);                                    \
        CASE_WHEN_RESULT_TYPE(TYPE_VARCHAR, RESULT_TYPE);                                 \
        CASE_WHEN_RESULT_TYPE(TYPE_DATE, RESULT_TYPE);                                    \
        CASE_WHEN_RESULT_TYPE(TYPE_DATETIME, RESULT_TYPE);                                \
        CASE_WHEN_RESULT_TYPE(TYPE_TIME, RESULT_TYPE);                                    \
        CASE_WHEN_RESULT_TYPE(TYPE_DECIMALV2, RESULT_TYPE);                               \
        CASE_WHEN_RESULT_TYPE(TYPE_DECIMAL32, RESULT_TYPE);                               \
        CASE_WHEN_RESULT_TYPE(TYPE_DECIMAL64, RESULT_TYPE);                               \
        CASE_WHEN_RESULT_TYPE(TYPE_DECIMAL128, RESULT_TYPE);                              \
        CASE_WHEN_RESULT_TYPE(TYPE_JSON, RESULT_TYPE);                                    \
    default: {                                                                            \
        LOG(WARNING) << "vectorized engine case expr no support when type: " << whenType; \
        return nullptr;                                                                   \
    }                                                                                     \
    }

#define CASE_RESULT_TYPE(RESULT_TYPE)      \
    case RESULT_TYPE: {                    \
        SWITCH_ALL_WHEN_TYPE(RESULT_TYPE); \
    }

Expr* VectorizedCaseExprFactory::from_thrift(const starrocks::TExprNode& node) {
    PrimitiveType resultType = TypeDescriptor::from_thrift(node.type).type;
    PrimitiveType whenType = thrift_to_type(node.child_type);

    if (resultType == TYPE_NULL) {
        resultType = TYPE_BOOLEAN;
    }

    switch (resultType) {
        APPLY_FOR_ALL_SCALAR_TYPE(CASE_RESULT_TYPE)
        CASE_RESULT_TYPE(TYPE_OBJECT)
        CASE_RESULT_TYPE(TYPE_HLL)
        CASE_RESULT_TYPE(TYPE_PERCENTILE)
    default: {
        LOG(WARNING) << "vectorized engine case expr no support result type: " << resultType;
        return nullptr;
    }
    }
}

#undef CASE_WHEN_RESULT_TYPE
#undef SWITCH_ALL_WHEN_TYPE
#undef CASE_RESULT_TYPE

} // namespace starrocks::vectorized
