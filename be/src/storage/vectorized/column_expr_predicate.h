
// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/olap_common.h"
#include "storage/types.h"
#include "storage/vectorized/column_predicate.h"

namespace starrocks {
class ZoneMapDetail;
class RuntimeState;
class SlotDescriptor;
class SparseRange;
class ExprContext;
class BitmapIndexIterator;
} // namespace starrocks

namespace starrocks::vectorized {

class Column;

// This class is a bridge to connect ColumnPredicatew which is used in scan/storage layer, and ExprContext which is
// used in computation layer. By bridging that, we can push more predicates from computation layer onto storage layer,
// hopefully to scan less data and boost performance.

// This class is supposed to be thread-safe, because
// 1. ExprContext* requires it and
// 2. we use `_tmp_select` when doing `evaluate_and` and `evaluate_or`.

// And this class has a big limitation that it does not support range evaluatation. In another word, `from` supposed to be 0 always.
// The fundamental reason is `ExprContext` requires `Column*` as a total piece, unless we can create a class to represent `ColumnSlice`.
// And that task is almost impossible.
class ColumnExprPredicate : public ColumnPredicate {
public:
    ColumnExprPredicate(TypeInfoPtr type_info, ColumnId column_id, RuntimeState* state, ExprContext* expr_ctx,
                        const SlotDescriptor* slot_desc);

    ~ColumnExprPredicate() override;

    void evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override;
    void evaluate_and(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const override;
    void evaluate_or(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const override;

    bool zone_map_filter(const ZoneMapDetail& detail) const override;
    bool support_bloom_filter() const override { return false; }
    PredicateType type() const override { return PredicateType::kExpr; }
    bool can_vectorized() const override { return true; }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override;
    std::string debug_string() const override;
    RuntimeState* runtime_state() const { return _state; }
    const SlotDescriptor* slot_desc() const { return _slot_desc; }

private:
    void _add_expr_ctx(ExprContext* expr_ctx);

    ObjectPool _pool;
    RuntimeState* _state;
    std::vector<ExprContext*> _expr_ctxs;
    const SlotDescriptor* _slot_desc;
    bool _monotonic;
    mutable std::vector<uint8_t> _tmp_select;
};

class ColumnTruePredicate : public ColumnPredicate {
public:
    ColumnTruePredicate(TypeInfoPtr type_info, ColumnId column_id) : ColumnPredicate(type_info, column_id) {}
    ~ColumnTruePredicate() override = default;
    void evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override;
    void evaluate_and(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const override;
    void evaluate_or(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const override;
    bool zone_map_filter(const ZoneMapDetail& detail) const override { return true; }
    bool support_bloom_filter() const override { return false; }
    PredicateType type() const override { return PredicateType::kTrue; }
    bool can_vectorized() const override { return true; }
    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override;
    std::string debug_string() const override;
};

} // namespace starrocks::vectorized
