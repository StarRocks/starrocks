#include "exprs/debug_expr.h"

#include <stdexcept>

#include "column/chunk.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "types/logical_type.h"

namespace starrocks {
Status DebugExpr::prepare(RuntimeState* state, ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, context));
    if (!_fn.__isset.fid) {
        return Status::InternalError("unset fid for DebugExpr: " + _fn.name.function_name);
    }
    if (_fn.name.function_name == "chunk_memusage") {
        _func_caller = &DebugFunctions::chunk_memusage;
    } else if (_fn.name.function_name == "chunk_check_valid") {
        _func_caller = &DebugFunctions::chunk_check_valid;
    }
    return Status::NotSupported("unsupported function name");
}

StatusOr<ColumnPtr> DebugExpr::evaluate_checked(ExprContext* context, Chunk* ptr) {
    return _func_caller(context, ptr);
}

StatusOr<ColumnPtr> DebugFunctions::chunk_memusage(ExprContext* context, Chunk* ptr) {
    if (ptr == nullptr) {
        return ColumnHelper::create_const_column<TYPE_BIGINT>(0, 1);
    }
    size_t mem_usage = ptr->memory_usage();
    size_t num_rows = ptr->num_rows();
    return ColumnHelper::create_const_column<TYPE_BIGINT>(mem_usage, num_rows);
}

StatusOr<ColumnPtr> DebugFunctions::chunk_check_valid(ExprContext* context, Chunk* ptr) {
    // check chunk valid
    if (ptr == nullptr) {
        return ColumnHelper::create_const_column<TYPE_BOOLEAN>(true, 1);
    }
    ptr->check_or_die();
    size_t num_rows = ptr->num_rows();
    for (const auto& column : ptr->columns()) {
        // check column size capacity
        std::string msg;
        column->capacity_limit_reached(&msg);
        if (!msg.empty()) {
            DCHECK(false) << "not expected";
            throw std::runtime_error(msg);
        }
        // check column size matched
        if (column->size() != num_rows) {
            DCHECK(false) << "not expected";
            throw std::runtime_error(fmt::format("unmatched rows detected"));
        }
    }
    return ColumnHelper::create_const_column<TYPE_BOOLEAN>(true, ptr->num_rows());
}

} // namespace starrocks