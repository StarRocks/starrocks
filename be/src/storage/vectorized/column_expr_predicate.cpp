
// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "column/column_helper.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/primitive_type.h"
#include "storage/vectorized/column_predicate.h"

namespace starrocks::vectorized {

using starrocks::ExprContext;

class ColumnExprPredicate : public ColumnPredicate {
public:
    ColumnExprPredicate(TypeInfoPtr type_info, ColumnId column_id, ExprContext* expr_ctx, SlotId slot_id)
            : ColumnPredicate(type_info, column_id), _expr_ctx(expr_ctx), _slot_id(slot_id) {}
    ~ColumnExprPredicate() override = default;

    void evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        Chunk chunk;
        ColumnPtr column_ptr(const_cast<Column*>(column));
        chunk.update_column(column_ptr, _slot_id);
        ColumnPtr bits = _expr_ctx->evaluate(&chunk);

        // deal with constant.
        if (bits->is_constant()) {
            uint8_t value = 0;
            // if null, we don't select it.
            if (!bits->has_null()) {
                value = ColumnHelper::get_const_value<TYPE_BOOLEAN>(bits);
            }
            memcpy(selection + from, &value, (to - from));
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

    void evaluate_and(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const override {
        uint16_t size = to - from;
        std::vector<uint8_t> tmp_sel(size);
        uint8_t* tmp = tmp_sel.data();
        evaluate(column, tmp, 0, size);
        for (uint16_t i = 0; i < size; i++) {
            sel[i + from] &= tmp[i];
        }
    }

    void evaluate_or(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const override {
        uint16_t size = to - from;
        std::vector<uint8_t> tmp_sel(size);
        uint8_t* tmp = tmp_sel.data();
        evaluate(column, tmp, 0, size);
        for (uint16_t i = 0; i < size; i++) {
            sel[i + from] |= tmp[i];
        }
    }

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        // todo(yan): once expr supports is_monotonic.
        return true;
    }

    Status seek_bitmap_dictionary(segment_v2::BitmapIndexIterator* iter, SparseRange* range) const override {
        range->clear();
        return Status::OK();
    }

    bool support_bloom_filter() const override { return false; }
    PredicateType type() const override { return PredicateType::kExpr; }
    bool can_vectorized() const override { return true; }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override {
        // todo(yan): do we really need convert to.
        return Status::NotSupported("ColumnExprPredicate does not support `convert_to`");
    }

    std::string debug_string() const override {
        std::stringstream ss;
        ss << "(ColumnExprPredicate: " << _expr_ctx->root()->debug_string() << ")";
        return ss.str();
    }

private:
    ExprContext* _expr_ctx;
    SlotId _slot_id;
};

ColumnPredicate* new_column_expr_predicate(const TypeInfoPtr& type, ColumnId column_id, ExprContext* expr_ctx,
                                           SlotId slot_id) {
    return new ColumnExprPredicate(type, column_id, expr_ctx, slot_id);
}

} // namespace starrocks::vectorized