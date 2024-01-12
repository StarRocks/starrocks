// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "runtime/global_dict/parser.h"

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "common/global_types.h"
#include "common/statusor.h"
#include "exprs/dictmapping_expr.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/placeholder_ref.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/descriptors.h"
#include "runtime/global_dict/config.h"
#include "runtime/global_dict/dict_column.h"
#include "runtime/global_dict/miscs.h"
#include "runtime/global_dict/types.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "simd/gather.h"
#include "types/logical_type.h"

namespace starrocks {

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

        DCHECK_GE(_origin_expr.get_num_children(), 2);
        auto place = get_place_holder(_origin_expr.get_child(1));
        auto type = place->type();
        if (type.type == LogicalType::TYPE_VARCHAR) {
            _input_type = LogicalType::TYPE_VARCHAR;
        } else if (type.is_array_type() && type.children[0].type == LogicalType::TYPE_VARCHAR) {
            _input_type = LogicalType::TYPE_ARRAY;
        }
    }

    PlaceHolderRef* get_place_holder(Expr* root) {
        if (auto f = dynamic_cast<PlaceHolderRef*>(root)) {
            return down_cast<PlaceHolderRef*>(f);
        }
        for (auto child : root->children()) {
            PlaceHolderRef* p = nullptr;
            if ((p = get_place_holder(child)) != nullptr) {
                return p;
            }
        }
        return nullptr;
    };

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {
        if (_input_type != LogicalType::TYPE_ARRAY && _input_type != LogicalType::TYPE_VARCHAR) {
            return Status::InternalError(fmt::format("dictFuncExpr can't resolve type: {}", _dict_opt_ctx->slot_id));
        }

        auto& input = ptr->get_column_by_slot_id(_dict_opt_ctx->slot_id);
        size_t num_rows = ptr->num_rows();

        if (_input_type == LogicalType::TYPE_VARCHAR) {
            return _translate_string(input, num_rows);
        } else {
            return _translate_array(input, num_rows);
        }

        return Status::InternalError(fmt::format("dictFuncExpr error on dict: {}", _dict_opt_ctx->slot_id));
    }

    Expr* clone(ObjectPool* pool) const override { return pool->add(new DictFuncExpr(_origin_expr, _dict_opt_ctx)); }

private:
    ColumnPtr _translate_string(ColumnPtr& input, size_t num_rows) {
        if (_always_null) {
            return ColumnHelper::create_const_null_column(num_rows);
        }

        if (_always_const) {
            auto res = _dict_opt_ctx->convert_column->clone();
            res->resize(num_rows);
            return res;
        }

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
            auto* null_column = down_cast<NullableColumn*>(input.get());
            // fill data to 0 if input value is null
            null_column->fill_null_with_default();
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
    }

    ColumnPtr _translate_array(ColumnPtr& array, size_t num_rows) {
        if ((array->only_null())) {
            return ColumnHelper::create_const_null_column(num_rows);
        }

        ArrayColumn* array_col = nullptr;
        TypeDescriptor stringType;
        stringType.type = TYPE_VARCHAR;
        if (array->is_constant()) {
            auto* const_column = down_cast<ConstColumn*>(array.get());
            array_col = down_cast<ArrayColumn*>(const_column->data_column().get());

            auto element = array_col->elements_column();
            auto offsets = UInt32Column::create(array_col->offsets());

            ColumnPtr string_col = _translate_string(element, element->size());
            string_col = ColumnHelper::unfold_const_column(stringType, element->size(), string_col);
            return ConstColumn::create(ArrayColumn::create(string_col, offsets), num_rows);
        } else if (array->is_nullable()) {
            auto nullable = down_cast<NullableColumn*>(array.get());
            array_col = down_cast<ArrayColumn*>(nullable->data_column().get());
            NullColumnPtr array_null = NullColumn::create(*nullable->null_column());

            auto element = array_col->elements_column();
            auto offsets = UInt32Column::create(array_col->offsets());

            ColumnPtr string_col = _translate_string(element, element->size());
            string_col = ColumnHelper::unfold_const_column(stringType, element->size(), string_col);
            return NullableColumn::create(ArrayColumn::create(string_col, offsets), array_null);
        } else {
            array_col = down_cast<ArrayColumn*>(array.get());
            auto element = array_col->elements_column();
            auto offsets = UInt32Column::create(array_col->offsets());

            ColumnPtr string_col = _translate_string(element, element->size());
            string_col = ColumnHelper::unfold_const_column(stringType, element->size(), string_col);
            return ArrayColumn::create(string_col, offsets);
        }
    }

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

    // mark intput column type
    LogicalType _input_type = TYPE_UNKNOWN;

    DictOptimizeContext* _dict_opt_ctx;
};

void DictOptimizeParser::_check_could_apply_dict_optimize(Expr* expr, DictOptimizeContext* dict_opt_ctx) {
    if (auto f = dynamic_cast<DictMappingExpr*>(expr)) {
        dict_opt_ctx->slot_id = f->slot_id();
        dict_opt_ctx->could_apply_dict_optimize = true;
    }
}

void DictOptimizeParser::close() noexcept {
    for (auto& [k, v] : _dict_exprs) {
        v->close(_runtime_state);
    }
}

Status DictOptimizeParser::_eval_and_rewrite(ExprContext* ctx, Expr* expr, DictOptimizeContext* dict_opt_ctx,
                                             int32_t targetSlotId) {
    auto* dict_mapping = down_cast<DictMappingExpr*>(expr);
    auto* origin_expr = dict_mapping->get_child(1);
    std::vector<SlotId> slots;
    dict_mapping->get_slot_ids(&slots);

    auto need_decode_slot_id = dict_mapping->slot_id();
    dict_opt_ctx->slot_id = need_decode_slot_id;
    SlotId expr_slot_id = slots.back();

    if (_mutable_dict_maps->count(need_decode_slot_id) == 0) {
        if (_dict_exprs.count(need_decode_slot_id) == 0) {
            return Status::InternalError(fmt::format("couldn't found dict cid:{}", need_decode_slot_id));
        } else {
            DictOptimizeContext doc;
            RETURN_IF_ERROR(eval_expression(_dict_exprs[need_decode_slot_id], &doc, need_decode_slot_id));
        }
    }

    auto& column_dict_map = _mutable_dict_maps->at(need_decode_slot_id).first;
    auto [binary_column, codes] = extract_column_with_codes(column_dict_map);
    ChunkPtr temp_chunk = std::make_shared<Chunk>();
    temp_chunk->append_column(binary_column, expr_slot_id);
    // call inner expr with input column
    ASSIGN_OR_RETURN(auto result_column, ctx->evaluate(origin_expr, temp_chunk.get()));
    // assign convert mapping column
    dict_opt_ctx->convert_column = result_column;
    // build code convert map
    dict_opt_ctx->code_convert_map.resize(DICT_DECODE_MAX_SIZE + 1);
    for (int i = 0; i < codes.size(); ++i) {
        dict_opt_ctx->code_convert_map[codes[i]] = i;
    }

    // insert dict result to global dicts

    // if dict expr return type not equels to origin expr return type
    // it means dict expr return a lowcardinality column. we need insert it
    // to global dicts
    if ((origin_expr->type().type != dict_mapping->type().type) ||
        (origin_expr->type().is_array_type() && dict_mapping->type().is_array_type() &&
         origin_expr->type().children[0].type != dict_mapping->type().children[0].type)) {
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
        for (auto slice : values) {
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

        if (_mutable_dict_maps->count(targetSlotId) == 0) {
            _mutable_dict_maps->emplace(targetSlotId, std::make_pair(std::move(result_map), std::move(rresult_map)));
        }
    }
    return Status::OK();
}

Status DictOptimizeParser::eval_expression(ExprContext* expr_ctx, DictOptimizeContext* dict_opt_ctx,
                                           SlotId targetSlotId) {
    return _eval_and_rewrite(expr_ctx, expr_ctx->root(), dict_opt_ctx, targetSlotId);
}

Status DictOptimizeParser::rewrite_expr(ExprContext* ctx, Expr* expr, SlotId slot_id) {
    // call rewrite for each DictMappingExpr
    if (auto f = dynamic_cast<DictMappingExpr*>(expr)) {
        return f->rewrite([&]() -> StatusOr<Expr*> {
            auto* dict_ctx_handle = _runtime_state->obj_pool()->add(new DictOptimizeContext());
            RETURN_IF_ERROR(_eval_and_rewrite(ctx, f, dict_ctx_handle, slot_id));
            return _runtime_state->obj_pool()->add(new DictFuncExpr(*f, dict_ctx_handle));
        });
    }

    for (auto child : expr->children()) {
        RETURN_IF_ERROR(rewrite_expr(ctx, child, -1));
    }
    return Status::OK();
}

Status DictOptimizeParser::eval_dict_expr(SlotId id) {
    if (_dict_exprs.count(id) == 0) {
        // none expr
        return Status::InternalError(fmt::format("not found dict expr on slot: {}", id));
    }
    DictOptimizeContext doc;
    return eval_expression(_dict_exprs[id], &doc, id);
}

void DictOptimizeParser::set_output_slot_id(std::vector<ExprContext*>* pexpr_ctxs,
                                            const std::vector<SlotId>& slot_ids) {
    auto& expr_ctxs = *pexpr_ctxs;
    for (int i = 0; i < expr_ctxs.size(); ++i) {
        auto& expr_ctx = expr_ctxs[i];
        auto expr = expr_ctx->root();
        if (auto f = dynamic_cast<DictMappingExpr*>(expr)) {
            f->set_output_id(slot_ids[i]);
        }
    }
}

static void expr_disable_open_rewrite(Expr* root) {
    if (auto f = dynamic_cast<DictMappingExpr*>(root)) {
        f->disable_open_rewrite();
    }

    for (auto& child : root->children()) {
        expr_disable_open_rewrite(child);
    }
}

void DictOptimizeParser::disable_open_rewrite(const std::vector<ExprContext*>* pexpr_ctxs) {
    auto& expr_ctxs = *pexpr_ctxs;
    for (int i = 0; i < expr_ctxs.size(); ++i) {
        auto& expr_ctx = expr_ctxs[i];
        auto expr = expr_ctx->root();
        expr_disable_open_rewrite(expr);
    }
}

Status DictOptimizeParser::init_dict_exprs(const std::map<int, TExpr>& exprs) {
    for (auto& [k, v] : exprs) {
        ExprContext* expr_ctx = nullptr;
        RETURN_IF_ERROR(Expr::create_expr_tree(&_free_pool, v, &expr_ctx, _runtime_state));
        auto expr = expr_ctx->root();
        if (auto f = dynamic_cast<DictMappingExpr*>(expr)) {
            f->set_output_id(k);
            f->disable_open_rewrite();
            RETURN_IF_ERROR(expr_ctx->prepare(_runtime_state));
            RETURN_IF_ERROR(expr_ctx->open(_runtime_state));
            _dict_exprs.emplace(k, expr_ctx);
        }
    }

    return Status::OK();
}

Status DictOptimizeParser::_rewrite_expr_ctxs(std::vector<ExprContext*>* pexpr_ctxs,
                                              const std::vector<SlotId>& slot_ids) {
    auto& expr_ctxs = *pexpr_ctxs;
    for (int i = 0; i < expr_ctxs.size(); ++i) {
        auto& expr_ctx = expr_ctxs[i];
        auto expr = expr_ctx->root();
        RETURN_IF_ERROR(rewrite_expr(expr_ctx, expr, slot_ids[i]));
    }
    return Status::OK();
}

Status DictOptimizeParser::rewrite_conjuncts(std::vector<ExprContext*>* pconjuncts_ctxs) {
    auto& expr_ctxs = *pconjuncts_ctxs;
    for (int i = 0; i < expr_ctxs.size(); ++i) {
        auto& expr_ctx = expr_ctxs[i];
        auto expr = expr_ctx->root();
        RETURN_IF_ERROR(rewrite_expr(expr_ctx, expr, -1));
    }
    return Status::OK();
}

void DictOptimizeParser::check_could_apply_dict_optimize(ExprContext* expr_ctx, DictOptimizeContext* dict_opt_ctx) {
    _check_could_apply_dict_optimize(expr_ctx->root(), dict_opt_ctx);
}

void DictOptimizeParser::rewrite_descriptor(RuntimeState* runtime_state, const std::vector<ExprContext*>& conjunct_ctxs,
                                            const std::map<int32_t, int32_t>& dict_slots_mapping,
                                            std::vector<SlotDescriptor*>* slot_descs) {
    const auto& global_dict = runtime_state->get_query_global_dict_map();
    if (global_dict.empty()) return;

    for (auto& slot_desc : *slot_descs) {
        if (global_dict.count(slot_desc->id()) && slot_desc->type().type == LowCardDictType) {
            SlotDescriptor* newSlot = runtime_state->global_obj_pool()->add(new SlotDescriptor(*slot_desc));
            newSlot->type().type = TYPE_VARCHAR;
            slot_desc = newSlot;
        }
    }
}

} // namespace starrocks
