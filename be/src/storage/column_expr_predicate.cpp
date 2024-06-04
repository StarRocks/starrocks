#include "storage/column_expr_predicate.h"

#include <utility>

#include "column/column_helper.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/binary_predicate.h"
#include "exprs/cast_expr.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/function_call_expr.h"
#include "exprs/literal.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/column_predicate.h"
#include "types/logical_type.h"

namespace starrocks {

ColumnExprPredicate::ColumnExprPredicate(TypeInfoPtr type_info, ColumnId column_id, RuntimeState* state,
                                         const SlotDescriptor* slot_desc)
        : ColumnPredicate(std::move(type_info), column_id), _state(state), _slot_desc(slot_desc), _monotonic(true) {
    _is_expr_predicate = true;
}

StatusOr<ColumnExprPredicate*> ColumnExprPredicate::make_column_expr_predicate(TypeInfoPtr type_info,
                                                                               ColumnId column_id, RuntimeState* state,
                                                                               ExprContext* expr_ctx,
                                                                               const SlotDescriptor* slot_desc) {
    auto* expr_predicate = new ColumnExprPredicate(std::move(type_info), column_id, state, slot_desc);
    // note: conjuncts would be shared by multiple scanners
    // so here we have to clone one to keep thread safe.
    expr_predicate->_add_expr_ctx(expr_ctx);
    // passed by FE
    if (expr_ctx != nullptr) {
        expr_predicate->set_index_filter_only(expr_ctx->is_index_only_filter());
    }
    return expr_predicate;
}

ColumnExprPredicate::~ColumnExprPredicate() {
    for (ExprContext* ctx : _expr_ctxs) {
        ctx->close(_state);
    }
}

void ColumnExprPredicate::_add_expr_ctxs(const std::vector<ExprContext*>& expr_ctxs) {
    for (auto& expr : expr_ctxs) {
        _add_expr_ctx(expr);
    }
}

void ColumnExprPredicate::_add_expr_ctx(std::unique_ptr<ExprContext> expr_ctx) {
    if (expr_ctx != nullptr) {
        DCHECK(expr_ctx->opened());
        // Transfer the ownership to object pool
        auto* ctx = _pool.add(expr_ctx.release());
        _expr_ctxs.emplace_back(ctx);
        _monotonic &= ctx->root()->is_monotonic();
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

Status ColumnExprPredicate::evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const {
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
    TRY_CATCH_ALLOC_SCOPE_START()
    for (int i = _expr_ctxs.size() - 1; i >= 0; i--) {
        ExprContext* ctx = _expr_ctxs[i];
        chunk.update_column(bits, _slot_desc->id());
        ASSIGN_OR_RETURN(bits, ctx->evaluate(&chunk));
    }
    TRY_CATCH_ALLOC_SCOPE_END()

    // deal with constant.
    if (bits->is_constant()) {
        uint8_t value = 0;
        // if null, we don't select it.
        if (!bits->has_null()) {
            value = ColumnHelper::get_const_value<TYPE_BOOLEAN>(bits);
        }
        memset(selection + from, value, (to - from));
        return Status::OK();
    }

    // deal with nullable.
    if (bits->is_nullable()) {
        auto* null_column = ColumnHelper::as_raw_column<NullableColumn>(bits);
        uint8_t* null_value = null_column->null_column_data().data();
        uint8_t* data_value = ColumnHelper::get_cpp_data<TYPE_BOOLEAN>(null_column->data_column());
        for (uint16_t i = from; i < to; i++) {
            selection[i] = (!null_value[i]) & (data_value[i]);
        }
        return Status::OK();
    }

    // deal with non-nullable.
    uint8_t* data_value = ColumnHelper::get_cpp_data<TYPE_BOOLEAN>(bits);
    memcpy(selection + from, data_value, (to - from));
    return Status::OK();
}

Status ColumnExprPredicate::evaluate_and(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const {
    // Does not support range evaluatation.
    DCHECK(from == 0);

    uint16_t size = to - from;
    _tmp_select.reserve(size);
    uint8_t* tmp = _tmp_select.data();
    RETURN_IF_ERROR(evaluate(column, tmp, 0, size));
    for (uint16_t i = 0; i < size; i++) {
        sel[i + from] &= tmp[i];
    }
    return Status::OK();
}

Status ColumnExprPredicate::evaluate_or(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const {
    // Does not support range evaluatation.
    DCHECK(from == 0);

    uint16_t size = to - from;
    _tmp_select.reserve(size);
    uint8_t* tmp = _tmp_select.data();
    RETURN_IF_ERROR(evaluate(column, tmp, 0, size));
    for (uint16_t i = 0; i < size; i++) {
        sel[i + from] |= tmp[i];
    }

    return Status::OK();
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
    if (!evaluate(col.get(), selection, 0, size).ok()) {
        return true;
    }

    for (uint16_t i = 0; i < size; i++) {
        if (selection[i] != 0) {
            return true;
        }
    }
    VLOG_FILE << "ColumnExprPredicate: zone_map_filter succeeded. # of skipped rows = " << detail.num_rows();
    return false;
}

bool ColumnExprPredicate::ngram_bloom_filter(const BloomFilter* bf,
                                             const NgramBloomFilterReaderOptions& reader_options) const {
    return _expr_ctxs[0]->ngram_bloom_filter(bf, reader_options);
}

Status ColumnExprPredicate::convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                                       ObjectPool* obj_pool) const {
    TypeDescriptor input_type = TypeDescriptor::from_storage_type_info(target_type_info.get());
    TypeDescriptor to_type = TypeDescriptor::from_storage_type_info(_type_info.get());
    Expr* column_ref = obj_pool->add(new ColumnRef(_slot_desc));
    Expr* cast_expr = VectorizedCastExprFactory::from_type(input_type, to_type, column_ref, obj_pool);
    column_ref->set_monotonic(true);
    cast_expr->set_monotonic(true);

    auto cast_expr_ctx = std::make_unique<ExprContext>(cast_expr);
    RETURN_IF_ERROR(cast_expr_ctx->prepare(_state));
    RETURN_IF_ERROR(cast_expr_ctx->open(_state));

    ASSIGN_OR_RETURN(auto pred, ColumnExprPredicate::make_column_expr_predicate(target_type_info, _column_id, _state,
                                                                                nullptr, _slot_desc));

    pred->_add_expr_ctxs(_expr_ctxs);
    pred->_add_expr_ctx(std::move(cast_expr_ctx));
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

Status ColumnExprPredicate::try_to_rewrite_for_zone_map_filter(starrocks::ObjectPool* pool,
                                                               std::vector<const ColumnExprPredicate*>* output) const {
    DCHECK(pool != nullptr);
    DCHECK(output != nullptr);
    ExprContext* pred_from_planner = _expr_ctxs[0];
    Expr* root = pred_from_planner->root();

    std::vector<Expr*> exprs_after_rewrite;

    if (root->op() == TExprOpcode::EQ) {
        if (root->get_num_children() != 2) {
            DCHECK(false) << "unexpected number of children in equal binary predicate, expected is 2, actual is "
                          << root->get_num_children();
            return Status::OK();
        }
        if (root->get_child(0)->is_monotonic() && root->get_child(1)->is_monotonic()) {
            // rewrite = to >= and <=
            auto build_binary_predicate_func = [pool, root](TExprOpcode::type new_op) {
                TExprNode node;
                node.node_type = TExprNodeType::BINARY_PRED;
                node.type = root->type().to_thrift();
                node.child_type = to_thrift(root->get_child(0)->type().type);
                node.__set_opcode(new_op);

                Expr* new_root = VectorizedBinaryPredicateFactory::from_thrift(node);
                DCHECK(new_root != nullptr);
                new_root->add_child(Expr::copy(pool, root->get_child(0)));
                new_root->add_child(Expr::copy(pool, root->get_child(1)));
                new_root->set_monotonic(true);
                return new_root;
            };

            Expr* le = build_binary_predicate_func(TExprOpcode::LE);
            Expr* ge = build_binary_predicate_func(TExprOpcode::GE);
            pool->add(le);
            pool->add(ge);
            exprs_after_rewrite.emplace_back(le);
            exprs_after_rewrite.emplace_back(ge);
        }
    }

    if (exprs_after_rewrite.empty()) {
        // no need to rewrite
        return Status::OK();
    }
    // build new ColumnExprPredicates
    for (auto& expr : exprs_after_rewrite) {
        auto expr_rewrite = std::make_unique<ExprContext>(expr);
        RETURN_IF_ERROR(expr_rewrite->prepare(_state));
        RETURN_IF_ERROR(expr_rewrite->open(_state));
        ASSIGN_OR_RETURN(ColumnExprPredicate * new_pred, ColumnExprPredicate::make_column_expr_predicate(
                                                                 _type_info, _column_id, _state, nullptr, _slot_desc));
        new_pred = pool->add(new_pred);
        new_pred->_add_expr_ctx(std::move(expr_rewrite));
        for (int i = 1; i < _expr_ctxs.size(); i++) {
            new_pred->_add_expr_ctx(_expr_ctxs[i]);
        }
        output->emplace_back(new_pred);
    }

    return Status::OK();
}
Status ColumnExprPredicate::seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                                                roaring::Roaring* row_bitmap) const {
    // Only support simple (NOT) LIKE/MATCH predicate for now
    // Root must be (NOT) LIKE/MATCH, and left child must be ColumnRef, which satisfy simple (NOT) LIKE/MATCH predicate
    // format as: col (NOT) LIKE/MATCH xxx, xxx must be string literal
    // For the untokenized mode, LIKE and MATCH are both available.
    // Otherwise, only MATCH is available.
    bool vaild_pred = false;
    bool vaild_like = false;
    bool vaild_match = false;
    bool with_not = false;

    with_not = _expr_ctxs[0]->root()->node_type() == TExprNodeType::COMPOUND_PRED &&
               _expr_ctxs[0]->root()->op() == TExprOpcode::COMPOUND_NOT;
    DCHECK((with_not && _expr_ctxs[0]->root()->get_num_children() == 1) || !with_not);

    Expr* expr = with_not ? _expr_ctxs[0]->root()->get_child(0) : _expr_ctxs[0]->root();

    auto* vectorized_function_call =
            expr->node_type() == TExprNodeType::FUNCTION_CALL ? down_cast<VectorizedFunctionCallExpr*>(expr) : nullptr;

    // check if satisfy vaild LIKE format
    vaild_like = vectorized_function_call != nullptr &&
                 LIKE_FN_NAME == boost::to_lower_copy(vectorized_function_call->get_function_desc()->name) &&
                 expr->get_num_children() == 2 && expr->get_child(0)->node_type() == TExprNodeType::SLOT_REF &&
                 expr->get_child(1)->node_type() == TExprNodeType::STRING_LITERAL;

    // check if satisfy vaild MATCH format
    vaild_match = vectorized_function_call == nullptr && expr->node_type() == TExprNodeType::MATCH_EXPR &&
                  expr->get_num_children() == 2 && expr->get_child(0)->node_type() == TExprNodeType::SLOT_REF &&
                  expr->get_child(1)->node_type() == TExprNodeType::STRING_LITERAL;

    vaild_pred = iterator->is_untokenized() ? (vaild_like || vaild_match) : vaild_match;

    if (!vaild_pred) {
        std::stringstream ss;
        ss << "Not supported function call in inverted index, expr predicate: "
           << (_expr_ctxs[0]->root() != nullptr ? _expr_ctxs[0]->root()->debug_string() : "");
        LOG(WARNING) << ss.str();
        return Status::NotSupported(ss.str());
    }

    auto* like_target = dynamic_cast<VectorizedLiteral*>(expr->get_child(1));
    DCHECK(like_target != nullptr);
    ASSIGN_OR_RETURN(auto literal_col, like_target->evaluate_checked(_expr_ctxs[0], nullptr));
    Slice padded_value(literal_col->get(0).get_slice());
    // MATCH a empty string should always return empty set.
    if (padded_value.empty()) {
        *row_bitmap -= *row_bitmap;
        return Status::OK();
    }
    std::string str_v = padded_value.to_string();
    InvertedIndexQueryType query_type = InvertedIndexQueryType::UNKNOWN_QUERY;
    if (str_v.find('*') == std::string::npos && str_v.find('%') == std::string::npos) {
        query_type = InvertedIndexQueryType::EQUAL_QUERY;
    } else {
        query_type = InvertedIndexQueryType::MATCH_WILDCARD_QUERY;
    }
    roaring::Roaring roaring;
    RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, &padded_value, query_type, &roaring));
    if (with_not) {
        *row_bitmap -= roaring;
    } else {
        *row_bitmap &= roaring;
    }
    return Status::OK();
}

Status ColumnTruePredicate::evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const {
    memset(selection + from, 0x1, to - from);
    return Status::OK();
}
Status ColumnTruePredicate::evaluate_and(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const {
    return Status::OK();
}
Status ColumnTruePredicate::evaluate_or(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const {
    memset(sel + from, 0x1, to - from);
    return Status::OK();
}

Status ColumnTruePredicate::convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                                       ObjectPool* obj_pool) const {
    *output = this;
    return Status::OK();
}

std::string ColumnTruePredicate::debug_string() const {
    return "(ColumnTruePredicate)";
}

} // namespace starrocks
