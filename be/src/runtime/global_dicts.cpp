// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "runtime/global_dicts.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "column/binary_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/const_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/column_ref.h"
#include "exprs/vectorized/dictmapping_expr.h"
#include "glog/logging.h"
#include "gutil/casts.h"
#include "runtime/mem_tracker.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "simd/gather.h"

namespace starrocks::vectorized {

// Dict Function Expr.
// The original Expr will be rewritten to DictFunctionExpr in the global dictionary optimization.
// Support Process any output type.
// FunctionCallExpr(StringColumn) -> DictFuncExpr(LowcardColumn)
// DictMappingExpr(LowcardColumn, FunctionCallExpr(PlaceHolder)) -> DictFuncExpr(LowcardColumn)
class DictFuncExpr final : public Expr {
public:
    DictFuncExpr(Expr& expr, DictOptimizeContext* dict_ctxs)
            : Expr(expr), _origin_expr(expr), _dict_opt_ctx(dict_ctxs) {
        _always_null = _dict_opt_ctx->convert_column->only_null();
        _always_const = _dict_opt_ctx->convert_column->is_constant();
        _always_nonull = !_dict_opt_ctx->convert_column->is_nullable();

        if (!_always_null && !_always_const) {
            _is_nullable_column = _dict_opt_ctx->convert_column->is_nullable();
            if (_dict_opt_ctx->convert_column->is_nullable()) {
                auto convert_col = down_cast<NullableColumn*>(_dict_opt_ctx->convert_column.get());
                const auto& null_data = convert_col->null_column_data();
                _is_strict = null_data[0] == 1 &&
                             std::all_of(null_data.begin() + 1, null_data.end(), [](auto a) { return a == 0; });
                _null_column_ptr = convert_col->null_column();
                _data_column_ptr = convert_col->data_column();
            } else {
                _data_column_ptr = _dict_opt_ctx->convert_column;
            }
        }
    }

    ColumnPtr evaluate(ExprContext* context, vectorized::Chunk* ptr) override {
        size_t num_rows = ptr->num_rows();
        if (_always_null) {
            return ColumnHelper::create_const_null_column(num_rows);
        }
        if (_always_const) {
            auto res = _dict_opt_ctx->convert_column->clone();
            res->resize(num_rows);
            return res;
        }
        auto& input = ptr->get_column_by_slot_id(_dict_opt_ctx->slot_id);
        // is const column
        if (input->only_null() || input->is_constant()) {
            if (_null_column_ptr && _null_column_ptr.get()->is_null(0)) {
                return ColumnHelper::create_const_null_column(num_rows);
            } else {
                auto idx = input->get(0);
                auto res = _data_column_ptr->clone_empty();
                res->append_datum(_data_column_ptr->get(idx.get_int32()));
                return ConstColumn::create(std::move(res));
            }
        } else if (input->is_nullable()) {
            // is nullable
            const auto* null_column = down_cast<NullableColumn*>(input.get());
            const auto* data_column = down_cast<LowCardDictColumn*>(null_column->data_column().get());
            // we could use data_column to avoid check null
            // because 0 in LowCardDictColumn means null
            const auto& container = data_column->get_data();
            auto res = _dict_opt_ctx->convert_column->clone_empty();

            res->append_selective(*_dict_opt_ctx->convert_column,
                                  _code_convert(container, _dict_opt_ctx->code_convert_map));
            return res;
        } else {
            // is not nullable
            const auto* data_column = down_cast<const LowCardDictColumn*>(input.get());
            const auto& container = data_column->get_data();
            if (_is_strict) {
                auto res = _data_column_ptr->clone_empty();
                res->append_selective(*_data_column_ptr, _code_convert(container, _dict_opt_ctx->code_convert_map));
                return res;
            } else {
                auto res = _dict_opt_ctx->convert_column->clone_empty();
                res->append_selective(*_dict_opt_ctx->convert_column,
                                      _code_convert(container, _dict_opt_ctx->code_convert_map));
                return res;
            }
        }

        return nullptr;
    }

    Expr* clone(ObjectPool* pool) const override { return pool->add(new DictFuncExpr(_origin_expr, _dict_opt_ctx)); }

private:
    // res[i] = mapping[index[i]]
    std::vector<uint32_t> _code_convert(const std::vector<int32_t>& index, const std::vector<int16_t>& mapping) {
        std::vector<uint32_t> res(index.size());
        SIMDGather::gather(res.data(), mapping.data(), index.data(), mapping.size(), index.size());
        return res;
    }

    Expr& _origin_expr;
    bool _always_const = false;
    bool _always_null = false;
    // any input value couldn't produce null result
    bool _always_nonull = false;
    // type of convert_column was nullable column
    bool _is_nullable_column = false;
    // if input was null return null
    // if input was not null return not null
    bool _is_strict = false;
    // null column ptr
    ColumnPtr _null_column_ptr;
    // data column ptr
    ColumnPtr _data_column_ptr;

    DictOptimizeContext* _dict_opt_ctx;
};

Status DictOptimizeParser::_check_could_apply_dict_optimize(ExprContext* expr_ctx, DictOptimizeContext* dict_opt_ctx) {
    if (auto f = dynamic_cast<DictMappingExpr*>(expr_ctx->root())) {
        dict_opt_ctx->slot_id = f->slot_id();
        dict_opt_ctx->could_apply_dict_optimize = true;
        return Status::OK();
    }

    // if expr was slot reference, we don't have to rewrite predicate
    if (expr_ctx->root()->is_slotref()) {
        dict_opt_ctx->could_apply_dict_optimize = false;
        return Status::OK();
    }

    // TODO: remove these check after 2.4
    std::vector<SlotId> slot_ids;
    expr_ctx->root()->get_slot_ids(&slot_ids);

    // Some string functions have multiple input slots, but their input slots are the same
    // eg: concat(slot1, slot1)
    auto only_one_slots = [](auto& slots) {
        int prev_slot = -1;
        for (auto slot : slots) {
            if (prev_slot != -1 && prev_slot != slot) return false;
            prev_slot = slot;
        }
        return !slots.empty();
    };

    if (only_one_slots(slot_ids) && _mutable_dict_maps->count(slot_ids.back())) {
        dict_opt_ctx->slot_id = slot_ids.back();
        dict_opt_ctx->could_apply_dict_optimize = true;
    }
    return Status::OK();
}

Status DictOptimizeParser::eval_expression(ExprContext* expr_ctx, DictOptimizeContext* dict_opt_ctx,
                                           SlotId targetSlotId) {
    // DCHECK_NE(expr_ctx->root()->type().type, TYPE_VARCHAR);
    SlotId need_decode_slot_id = dict_opt_ctx->slot_id;
    SlotId expr_slot_id = need_decode_slot_id;

    bool is_old_version = true;
    Expr* origin_expr = expr_ctx->root();
    if (auto f = dynamic_cast<DictMappingExpr*>(expr_ctx->root())) {
        origin_expr = f->get_child(1);
        std::vector<SlotId> slots;
        f->get_slot_ids(&slots);
        expr_slot_id = slots.back();
        is_old_version = false;
    }

    DCHECK(_mutable_dict_maps->count(need_decode_slot_id) > 0);
    if (_mutable_dict_maps->count(need_decode_slot_id) == 0) {
        return Status::InternalError(fmt::format("couldn't found dict cid:{}", need_decode_slot_id));
    }
    auto& column_dict_map = _mutable_dict_maps->at(need_decode_slot_id).first;
    auto [binary_column, codes] = extract_column_with_codes(column_dict_map);
    ChunkPtr temp_chunk = std::make_shared<Chunk>();
    temp_chunk->append_column(binary_column, expr_slot_id);
    // call inner expr with input column
    auto result_column = expr_ctx->evaluate(origin_expr, temp_chunk.get());
    // assign convert mapping column
    dict_opt_ctx->convert_column = result_column;
    // build code convert map
    dict_opt_ctx->code_convert_map.resize(DICT_DECODE_MAX_SIZE + 1);
    for (int i = 0; i < codes.size(); ++i) {
        dict_opt_ctx->code_convert_map[codes[i]] = i;
    }
    // insert dict result to global dicts

    // old lowcardinality optimization origin_expr return type was TYPE_INT
    // we want make old_version also generate new dict
    if (origin_expr->type().type == TYPE_VARCHAR || is_old_version) {
        DCHECK_GE(targetSlotId, 0);
        ColumnViewer<TYPE_VARCHAR> viewer(result_column);
        int num_rows = codes.size();

        GlobalDictMap result_map;
        RGlobalDictMap rresult_map;
        std::vector<Slice> values;
        values.reserve(num_rows);

        // distinct result values
        int id_allocator = 1;
        for (int i = 0; i < num_rows; ++i) {
            if (!viewer.is_null(i)) {
                auto value = viewer.value(i);
                Slice slice(value.data, value.size);
                result_map.lazy_emplace(slice, [&](const auto& ctor) {
                    id_allocator++;
                    auto data = _runtime_state->instance_mem_pool()->allocate(value.size);
                    memcpy(data, value.data, value.size);
                    slice = Slice(data, slice.size);
                    ctor(slice, id_allocator);
                    values.emplace_back(slice);
                });
            } else {
                dict_opt_ctx->result_nullable = true;
            }
        }

        // sort and build result map
        // no-null value
        std::sort(values.begin(), values.end(), Slice::Comparator());
        int sorted_id = 1;
        for (int i = 0; i < values.size(); ++i) {
            auto slice = values[i];
            result_map[slice] = sorted_id;
            rresult_map[sorted_id++] = slice;
        }

        ColumnBuilder<LowCardDictType> builder(codes.size());
        // build code convert map
        for (int i = 0; i < num_rows; ++i) {
            if (viewer.is_null(i)) {
                dict_opt_ctx->code_convert_map[codes[i]] = 0;
                builder.append_null();
            } else {
                dict_opt_ctx->code_convert_map[codes[i]] = i;
                builder.append(result_map.find(viewer.value(i))->second);
            }
        }

        dict_opt_ctx->convert_column = builder.build(false);
        DCHECK_EQ(_mutable_dict_maps->count(targetSlotId), 0);
        _mutable_dict_maps->emplace(targetSlotId, std::make_pair(std::move(result_map), std::move(rresult_map)));
    }
    return Status::OK();
}

template <bool close_original_expr>
Status DictOptimizeParser::_rewrite_expr_ctxs(std::vector<ExprContext*>* pexpr_ctxs, RuntimeState* state,
                                              const std::vector<SlotId>& slot_ids) {
    auto& expr_ctxs = *pexpr_ctxs;
    for (int i = 0; i < expr_ctxs.size(); ++i) {
        auto& expr_ctx = expr_ctxs[i];
        DictOptimizeContext dict_ctx;
        _check_could_apply_dict_optimize(expr_ctx, &dict_ctx);
        if (dict_ctx.could_apply_dict_optimize) {
            eval_expression(expr_ctx, &dict_ctx, slot_ids[i]);
            auto* dict_ctx_handle = _free_pool.add(new DictOptimizeContext(std::move(dict_ctx)));
            Expr* replaced_expr = _free_pool.add(new DictFuncExpr(*expr_ctx->root(), dict_ctx_handle));

            // Because the ExprContext is close safe,
            // Add both pre- and post-rewritten expressions to
            // the free_list to ensure they are closed correctly
            if constexpr (close_original_expr) {
                _expr_close_list.emplace_back(expr_ctx);
            }
            expr_ctx = _free_pool.add(new ExprContext(replaced_expr));
            expr_ctx->prepare(state, RowDescriptor{});
            expr_ctx->open(state);
            _expr_close_list.emplace_back(expr_ctx);
        }
    }
    return Status::OK();
}

template <bool close_original_expr>
Status DictOptimizeParser::rewrite_conjuncts(std::vector<ExprContext*>* pconjuncts_ctxs, RuntimeState* state) {
    return _rewrite_expr_ctxs<close_original_expr>(pconjuncts_ctxs, state,
                                                   std::vector<SlotId>(pconjuncts_ctxs->size(), -1));
}

template Status DictOptimizeParser::rewrite_conjuncts<true>(std::vector<ExprContext*>* conjuncts_ctxs,
                                                            RuntimeState* state);
template Status DictOptimizeParser::rewrite_conjuncts<false>(std::vector<ExprContext*>* conjuncts_ctxs,
                                                             RuntimeState* state);

Status DictOptimizeParser::rewrite_exprs(std::vector<ExprContext*>* pexpr_ctxs, RuntimeState* state,
                                         const std::vector<SlotId>& target_slotids) {
    return _rewrite_expr_ctxs<true>(pexpr_ctxs, state, target_slotids);
}

void DictOptimizeParser::close(RuntimeState* state) noexcept {
    Expr::close(_expr_close_list, state);
}

Status DictOptimizeParser::check_could_apply_dict_optimize(ExprContext* expr_ctx, DictOptimizeContext* dict_opt_ctx) {
    return _check_could_apply_dict_optimize(expr_ctx, dict_opt_ctx);
}

std::pair<std::shared_ptr<NullableColumn>, std::vector<int32_t>> extract_column_with_codes(
        const GlobalDictMap& dict_map) {
    auto res = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    res->reserve(dict_map.size() + 1);

    std::vector<Slice> slices;
    std::vector<int> codes;

    slices.reserve(dict_map.size() + 1);
    codes.reserve(dict_map.size() + 1);

    slices.emplace_back(Slice());
    codes.emplace_back(0);

    for (auto& [slice, code] : dict_map) {
        slices.emplace_back(slice);
        codes.emplace_back(code);
    }
    res->append_strings(slices);
    res->set_null(0);
    return std::make_pair(std::move(res), std::move(codes));
}

void DictOptimizeParser::rewrite_descriptor(RuntimeState* runtime_state, const std::vector<ExprContext*>& conjunct_ctxs,
                                            const std::map<int32_t, int32_t>& dict_slots_mapping,
                                            std::vector<SlotDescriptor*>* slot_descs) {
    const auto& global_dict = runtime_state->get_query_global_dict_map();
    if (global_dict.empty()) return;

    for (size_t i = 0; i < slot_descs->size(); ++i) {
        if (global_dict.count((*slot_descs)[i]->id())) {
            SlotDescriptor* newSlot = runtime_state->obj_pool()->add(new SlotDescriptor(*(*slot_descs)[i]));
            newSlot->type().type = TYPE_VARCHAR;
            (*slot_descs)[i] = newSlot;
        }
    }

    // TODO: remove this code in 2.4
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