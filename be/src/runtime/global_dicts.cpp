#include "runtime/global_dicts.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/column_ref.h"
#include "glog/logging.h"
#include "gutil/casts.h"
#include "runtime/mem_tracker.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"

namespace starrocks::vectorized {
class DictConjunctExpr final : public Expr {
public:
    DictConjunctExpr(Expr& expr, DictOptimizeContext* dict_ctxs)
            : Expr(expr), _origin_expr(expr), _dict_opt_ctx(dict_ctxs) {}

    virtual ColumnPtr evaluate(ExprContext* context, vectorized::Chunk* ptr) {
        auto res = BooleanColumn::create();
        auto& input = ptr->get_column_by_slot_id(_dict_opt_ctx->slot_id);
        auto& res_data = res->get_data();
        int size = input->size();

        if (input->is_constant()) {
            int code = ColumnHelper::get_const_value<TYPE_INT>(input);
            return ColumnHelper::create_const_column<TYPE_BOOLEAN>(_dict_opt_ctx->filter[code], input->size());
        } else if (input->is_nullable()) {
            res_data.resize(size);
            const auto* null_column = down_cast<NullableColumn*>(input.get());
            const auto* data_column = down_cast<LowCardDictColumn*>(null_column->data_column().get());
            const auto& null_data = null_column->immutable_null_column_data();
            const auto& input_data = data_column->get_data();
            for (int i = 0; i < size; ++i) {
                res_data[i] = _dict_opt_ctx->filter[input_data[i]];
            }
            for (int i = 0; i < size; ++i) {
                if (null_data[i]) {
                    res_data[i] = 0;
                }
            }
        } else {
            res_data.resize(size);
            const auto* data_column = down_cast<LowCardDictColumn*>(input.get());
            const auto& input_data = data_column->get_data();
            for (int i = 0; i < size; ++i) {
                res_data[i] = _dict_opt_ctx->filter[input_data[i]];
            }
        }
        return res;
    }
    virtual Expr* clone(ObjectPool* pool) const { return pool->add(new DictConjunctExpr(_origin_expr, _dict_opt_ctx)); }

private:
    Expr& _origin_expr;
    DictOptimizeContext* _dict_opt_ctx;
};

class DictStringFuncExpr final : public Expr {
public:
    DictStringFuncExpr(Expr& expr, DictOptimizeContext* dict_ctxs)
            : Expr(expr), _origin_expr(expr), _dict_opt_ctx(dict_ctxs) {}

    virtual ColumnPtr evaluate(ExprContext* context, vectorized::Chunk* ptr) {
        auto& input = ptr->get_column_by_slot_id(_dict_opt_ctx->slot_id);
        int row_size = input->size();

        auto res = LowCardDictColumn::create_mutable();
        auto& res_data = res->get_data();
        res_data.resize(row_size);

        ColumnPtr output = nullptr;

        if (input->is_nullable()) {
            const auto* nullable_column = down_cast<const NullableColumn*>(input.get());
            const auto* null_column = down_cast<const NullColumn*>(nullable_column->null_column().get());
            const auto* data_column = down_cast<const LowCardDictColumn*>(nullable_column->data_column().get());
            const auto& input_data = data_column->get_data();

            for (int i = 0; i < row_size; ++i) {
                DCHECK(input_data[i] >= 0 && input_data[i] <= DICT_DECODE_MAX_SIZE);
                res_data[i] = _dict_opt_ctx->code_convert_map[input_data[i]];
            }

            if (_dict_opt_ctx->result_nullable) {
                auto res_null_column = null_column->clone();
                auto& res_null_data = down_cast<NullColumn*>(res_null_column.get())->get_data();

                for (int i = 0; i < row_size; ++i) {
                    res_null_data[i] |= (res_data[i] == 0);
                }

                output = NullableColumn::create(std::move(res), std::move(res_null_column));
            } else {
                output = NullableColumn::create(std::move(res), null_column->clone());
            }
        } else {
            const auto* data_column = down_cast<const LowCardDictColumn*>(input.get());
            const auto& input_data = data_column->get_data();

            for (int i = 0; i < row_size; ++i) {
                res_data[i] = _dict_opt_ctx->code_convert_map[input_data[i]];
            }

            if (_dict_opt_ctx->result_nullable) {
                auto res_null = NullColumn::create_mutable();
                auto& res_null_data = res_null->get_data();
                res_null_data.resize(row_size);

                for (int i = 0; i < row_size; ++i) {
                    res_null_data[i] = (res_data[i] == 0);
                }

                output = NullableColumn::create(std::move(res), std::unique_ptr<Column>(res_null.release()));
            } else {
                output = std::move(res);
            }
        }
        return output;
    }

    virtual Expr* clone(ObjectPool* pool) const {
        return pool->add(new DictStringFuncExpr(_origin_expr, _dict_opt_ctx));
    }

private:
    Expr& _origin_expr;
    DictOptimizeContext* _dict_opt_ctx;
};

void DictOptimizeParser::eval_expr(RuntimeState* state, ExprContext* expr_ctx, DictOptimizeContext* dict_opt_ctx,
                                   int32_t targetSlotId) {
    DCHECK(dict_opt_ctx->could_apply_dict_optimize);
    SlotId need_decode_slot_id = dict_opt_ctx->slot_id;
    DCHECK(_mutable_dict_maps->count(need_decode_slot_id) > 0);
    // Slice -> dict-code
    auto& column_dict_map = _mutable_dict_maps->at(need_decode_slot_id).first;

    auto [binary_column, codes] = extract_column_with_codes(column_dict_map);

    ChunkPtr temp_chunk = std::make_shared<Chunk>();
    temp_chunk->append_column(binary_column, need_decode_slot_id);

    auto result_column = expr_ctx->evaluate(temp_chunk.get());

    ColumnViewer<TYPE_VARCHAR> viewer(result_column);
    int row_sz = viewer.size();

    dict_opt_ctx->code_convert_map.resize(DICT_DECODE_MAX_SIZE + 1);
    std::fill(dict_opt_ctx->code_convert_map.begin(), dict_opt_ctx->code_convert_map.end(), 0);
    auto& code_convert_map = dict_opt_ctx->code_convert_map;

    GlobalDictMap result_map;
    RGlobalDictMap rresult_map;
    int id_allocator = 1;
    for (int i = 0; i < row_sz; ++i) {
        if (viewer.is_null(i)) {
            code_convert_map[codes[i]] = 0;
            dict_opt_ctx->result_nullable = true;
        } else {
            auto value = viewer.value(i);
            Slice slice(value.data, value.size);
            auto res = result_map.lazy_emplace(slice, [&](const auto& ctor) {
                id_allocator++;
                auto data = state->instance_mem_pool()->allocate(value.size);
                memcpy(data, value.data, value.size);
                slice = Slice(data, slice.size);
                ctor(slice, id_allocator);
            });

            code_convert_map[codes[i]] = res->second;
            rresult_map.emplace(res->second, slice);
        }
    }

    DCHECK_EQ(_mutable_dict_maps->count(targetSlotId), 0);
    _mutable_dict_maps->emplace(targetSlotId, std::make_pair(std::move(result_map), std::move(rresult_map)));
}

template <bool is_predicate>
void DictOptimizeParser::_check_could_apply_dict_optimize(ExprContext* expr_ctx, DictOptimizeContext* dict_opt_ctx) {
    if (expr_ctx->root()->is_slotref()) {
        dict_opt_ctx->could_apply_dict_optimize = false;
        return;
    }

    if constexpr (!is_predicate) {
        if (!expr_ctx->root()->fn().could_apply_dict_optimize) {
            return;
        }
    }

    std::vector<SlotId> slot_ids;
    expr_ctx->root()->get_slot_ids(&slot_ids);
    if (slot_ids.size() == 1 && _mutable_dict_maps->count(slot_ids.back())) {
        dict_opt_ctx->slot_id = slot_ids.back();
        dict_opt_ctx->could_apply_dict_optimize = true;
    }
}

void DictOptimizeParser::eval_conjuncts(ExprContext* conjunct, DictOptimizeContext* dict_opt_ctx) {
    DCHECK_EQ(conjunct->root()->type().type, TYPE_BOOLEAN);
    SlotId need_decode_slot_id = dict_opt_ctx->slot_id;
    DCHECK(_mutable_dict_maps->count(need_decode_slot_id) > 0);
    // Slice -> dict-code
    auto& column_dict_map = _mutable_dict_maps->at(need_decode_slot_id).first;

    auto [binary_column, codes] = extract_column_with_codes(column_dict_map);

    ChunkPtr temp_chunk = std::make_shared<Chunk>();
    temp_chunk->append_column(binary_column, need_decode_slot_id);

    auto result_column = conjunct->evaluate(temp_chunk.get());
    bool result_nullable = result_column->is_nullable();
    ColumnPtr data_column = result_column;
    if (result_nullable) {
        data_column = down_cast<NullableColumn*>(result_column.get())->data_column();
    }
    auto& result_data = down_cast<BooleanColumn*>(data_column.get())->get_data();

    dict_opt_ctx->filter.resize(DICT_DECODE_MAX_SIZE + 1);
    for (int i = 0; i < result_data.size(); ++i) {
        dict_opt_ctx->filter[codes[i]] = result_data[i];
    }

    if (result_nullable) {
        // null value will be treated as False
        const auto& null_data = down_cast<NullableColumn*>(result_column.get())->null_column_data();
        for (int i = 0; i < result_data.size(); ++i) {
            dict_opt_ctx->filter[codes[i]] &= !(null_data[i] == true);
        }
    }
}

template <bool is_predicate, typename ExprType>
void DictOptimizeParser::_rewrite_expr_ctxs(std::vector<ExprContext*>* pexpr_ctxs, RuntimeState* state,
                                            const std::vector<SlotId>& slot_ids) {
    auto& expr_ctxs = *pexpr_ctxs;
    for (int i = 0; i < expr_ctxs.size(); ++i) {
        auto& expr_ctx = expr_ctxs[i];
        DictOptimizeContext dict_ctx;
        _check_could_apply_dict_optimize<is_predicate>(expr_ctx, &dict_ctx);
        if (dict_ctx.could_apply_dict_optimize) {
            if constexpr (is_predicate) {
                eval_conjuncts(expr_ctx, &dict_ctx);
            } else {
                eval_expr(state, expr_ctx, &dict_ctx, slot_ids[i]);
            }
            auto* dict_ctx_handle = _free_pool.add(new DictOptimizeContext(std::move(dict_ctx)));
            auto* replaced_expr = _free_pool.add(new ExprType(*expr_ctx->root(), dict_ctx_handle));
            // Because the ExprContext is close safe,
            // Add both pre- and post-rewritten expressions to
            // the free_list to ensure they are closed correctly
            _expr_close_list.emplace_back(expr_ctx);
            expr_ctx = _free_pool.add(new ExprContext(replaced_expr));
            expr_ctx->prepare(state, RowDescriptor{});
            expr_ctx->open(state);
            _expr_close_list.emplace_back(expr_ctx);
        }
    }
}

void DictOptimizeParser::rewrite_conjuncts(std::vector<ExprContext*>* pconjuncts_ctxs, RuntimeState* state) {
    _rewrite_expr_ctxs<true, DictConjunctExpr>(pconjuncts_ctxs, state, std::vector<SlotId>{});
}

void DictOptimizeParser::rewrite_exprs(std::vector<ExprContext*>* pexpr_ctxs, RuntimeState* state,
                                       const std::vector<SlotId>& target_slotids) {
    _rewrite_expr_ctxs<false, DictStringFuncExpr>(pexpr_ctxs, state, target_slotids);
}

void DictOptimizeParser::close(RuntimeState* state) noexcept {
    Expr::close(_expr_close_list, state);
}

void DictOptimizeParser::check_could_apply_dict_optimize(ExprContext* expr_ctx, DictOptimizeContext* dict_opt_ctx) {
    _check_could_apply_dict_optimize<false>(expr_ctx, dict_opt_ctx);
}

std::pair<std::shared_ptr<BinaryColumn>, std::vector<int32_t>> extract_column_with_codes(
        const GlobalDictMap& dict_map) {
    std::vector<Slice> slices;
    std::vector<int> codes;

    slices.reserve(dict_map.size());
    codes.reserve(dict_map.size());

    for (auto& [slice, code] : dict_map) {
        slices.emplace_back(slice);
        codes.emplace_back(code);
    }

    auto binary_column = BinaryColumn::create();
    binary_column->append_strings(slices);

    return std::make_pair(std::move(binary_column), std::move(codes));
}

void DictOptimizeParser::rewrite_descriptor(RuntimeState* runtime_state, std::vector<SlotDescriptor*> slot_descs,
                                            std::vector<ExprContext*>& conjunct_ctxs,
                                            const std::map<int32_t, int32_t>& dict_slots_mapping) {
    const auto& global_dict = runtime_state->get_global_dict_map();
    if (global_dict.empty()) return;

    for (auto& slot : slot_descs) {
        if (global_dict.count(slot->id())) {
            slot->type().type = TYPE_VARCHAR;
        }
    }

    // rewrite slot-id for conjunct
    std::vector<SlotId> slots;
    for (auto& conjunct : conjunct_ctxs) {
        slots.clear();
        Expr* expr_root = conjunct->root();
        if (expr_root->get_slot_ids(&slots) == 1) {
            if (auto iter = dict_slots_mapping.find(slots[0]); iter != dict_slots_mapping.end()) {
                ColumnRef* column_ref = expr_root->get_column_ref();
                column_ref->set_slot_id(iter->second);
            }
        }
    }
}

} // namespace starrocks::vectorized