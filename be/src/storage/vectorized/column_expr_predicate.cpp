
// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/vectorized/column_expr_predicate.h"

#include "column/column_helper.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/cast_expr.h"
#include "exprs/vectorized/column_ref.h"
#include "runtime/descriptors.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "storage/vectorized/column_predicate.h"
namespace starrocks::vectorized {

ColumnExprPredicate::ColumnExprPredicate(TypeInfoPtr type_info, ColumnId column_id, RuntimeState* state,
                                         ExprContext* expr_ctx, const SlotDescriptor* slot_desc)
        : ColumnPredicate(type_info, column_id), _state(state), _slot_desc(slot_desc), _monotonic(true) {
    // note: conjuncts would be shared by multiple scanners
    // so here we have to clone one to keep thread safe.
    _add_expr_ctx(expr_ctx);
    _is_expr_predicate = true;
}

ColumnExprPredicate::~ColumnExprPredicate() {
    for (ExprContext* ctx : _expr_ctxs) {
        ctx->close(_state);
    }
}

void ColumnExprPredicate::_add_expr_ctx(ExprContext* expr_ctx) {
    if (expr_ctx != nullptr) {
        DCHECK(expr_ctx->opened());
        ExprContext* ctx = nullptr;
        DCHECK_IF_ERROR(expr_ctx->clone(_state, &_pool, &ctx));
        _expr_ctxs.emplace_back(ctx);
        _monotonic &= ctx->root()->is_monotonic();
    }
}

void ColumnExprPredicate::evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const {
    // Does not support range evaluatation.
    DCHECK(from == 0);

    Chunk chunk;
    // `column` is owned by storage layer
    // we don't have ownership
    ColumnPtr bits(const_cast<Column*>(column), [](auto p) {});
    chunk.append_column(bits, _slot_desc->id());

    // theoretically there will be a chain of expr contexts.
    // The first one is expr context from planner
    // and others will be some cast exprs from one type to another
    // eg. [x as int >= 10]  [string->int] <- column(x as string)
    for (int i = _expr_ctxs.size() - 1; i >= 0; i--) {
        ExprContext* ctx = _expr_ctxs[i];
        chunk.update_column(bits, _slot_desc->id());
        bits = ctx->evaluate(&chunk);
    }

    // deal with constant.
    if (bits->is_constant()) {
        uint8_t value = 0;
        // if null, we don't select it.
        if (!bits->has_null()) {
            value = ColumnHelper::get_const_value<TYPE_BOOLEAN>(bits);
        }
        memset(selection + from, value, (to - from));
        return;
    }

    // deal with nullable.
    if (bits->is_nullable()) {
        NullableColumn* null_column = ColumnHelper::as_raw_column<NullableColumn>(bits);
        uint8_t* null_value = null_column->null_column_data().data();
        uint8_t* data_value = ColumnHelper::get_cpp_data<TYPE_BOOLEAN>(null_column->data_column());
        for (uint16_t i = from; i < to; i++) {
            selection[i] = (!null_value[i]) & (data_value[i]);
        }
        return;
    }

    // deal with non-nullable.
    uint8_t* data_value = ColumnHelper::get_cpp_data<TYPE_BOOLEAN>(bits);
    memcpy(selection + from, data_value, (to - from));
    return;
}

void ColumnExprPredicate::evaluate_and(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const {
    // Does not support range evaluatation.
    DCHECK(from == 0);

    uint16_t size = to - from;
    _tmp_select.reserve(size);
    uint8_t* tmp = _tmp_select.data();
    evaluate(column, tmp, 0, size);
    for (uint16_t i = 0; i < size; i++) {
        sel[i + from] &= tmp[i];
    }
}

void ColumnExprPredicate::evaluate_or(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const {
    // Does not support range evaluatation.
    DCHECK(from == 0);

    uint16_t size = to - from;
    _tmp_select.reserve(size);
    uint8_t* tmp = _tmp_select.data();
    evaluate(column, tmp, 0, size);
    for (uint16_t i = 0; i < size; i++) {
        sel[i + from] |= tmp[i];
    }
}

bool ColumnExprPredicate::zone_map_filter(const ZoneMapDetail& detail) const {
    // if expr does not satisfy monotonicity, we can not apply zone map.
    if (!_monotonic) return true;
    // construct column and chunk by zone map
    TypeDescriptor type_desc = TypeDescriptor::from_storage_type_info(_type_info.get());
    ColumnPtr col = ColumnHelper::create_column(type_desc, detail.has_null());
    // null, min, max
    uint16_t size = 0;
    uint8_t selection[3];
    if (detail.has_null()) {
        col->append_default();
        size += 1;
    }
    if (detail.has_not_null()) {
        col->append_datum(detail.min_value());
        col->append_datum(detail.max_value());
        size += 2;
    }
    // if all of them are evaluated to false, we don't need this zone.
    evaluate(col.get(), selection, 0, size);
    for (uint16_t i = 0; i < size; i++) {
        if (selection[i] != 0) {
            return true;
        }
    }
    VLOG_FILE << "ColumnExprPredicate: zone_map_filter succeeded. # of skipped rows = " << detail.num_rows();
    return false;
}

Status ColumnExprPredicate::convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                                       ObjectPool* obj_pool) const {
    TypeDescriptor input_type = TypeDescriptor::from_storage_type_info(target_type_info.get());
    TypeDescriptor to_type = TypeDescriptor::from_storage_type_info(_type_info.get());
    Expr* column_ref = obj_pool->add(new ColumnRef(_slot_desc));
    Expr* cast_expr = VectorizedCastExprFactory::from_type(input_type, to_type, column_ref, obj_pool);
    column_ref->set_monotonic(true);
    cast_expr->set_monotonic(true);
    ExprContext* cast_expr_ctx = obj_pool->add(new ExprContext(cast_expr));

    RowDescriptor row_desc; // I think we don't need to use it at all.
    RETURN_IF_ERROR(cast_expr_ctx->prepare(_state, row_desc));
    RETURN_IF_ERROR(cast_expr_ctx->open(_state));

    ColumnExprPredicate* pred =
            obj_pool->add(new ColumnExprPredicate(target_type_info, _column_id, _state, nullptr, _slot_desc));
    for (ExprContext* ctx : _expr_ctxs) {
        pred->_add_expr_ctx(ctx);
    }
    pred->_add_expr_ctx(cast_expr_ctx);
    *output = pred;
    return Status::OK();
}

std::string ColumnExprPredicate::debug_string() const {
    std::stringstream ss;
    ss << "(ColumnExprPredicate: ";
    for (ExprContext* ctx : _expr_ctxs) {
        ss << "[" << ctx->root()->debug_string() << "]";
    }
    ss << ")";
    return ss.str();
}

void ColumnTruePredicate::evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const {
    memset(selection + from, 0x1, to - from);
}
void ColumnTruePredicate::evaluate_and(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const {
    return;
}
void ColumnTruePredicate::evaluate_or(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const {
    memset(sel + from, 0x1, to - from);
}

Status ColumnTruePredicate::convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                                       ObjectPool* obj_pool) const {
    *output = this;
    return Status::OK();
}

std::string ColumnTruePredicate::debug_string() const {
    return "(ColumnTruePredicate)";
}

} // namespace starrocks::vectorized
