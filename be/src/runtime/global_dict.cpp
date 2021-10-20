#include <algorithm>
#include <cstring>
#include <memory>
#include <utility>

#include "column/binary_column.h"
#include "column/column_viewer.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/column_ref.h"
#include "gutil/casts.h"
#include "runtime/global_dicts.h"
#include "runtime/runtime_state.h"

namespace starrocks::vectorized {
void DictOptimizeParser::check_could_apply_dict_optimize(ExprContext* expr_ctx, DictOptimizeContext* dict_opt_ctx) {
    // if root expr was a slot ref
    // we don't have to caculate dict
    if (expr_ctx->root()->is_slotref()) {
        dict_opt_ctx->could_apply_dict_optimize = false;
        return;
    }

    std::vector<Expr*> exprs;
    std::vector<Expr*> leaf_children;
    exprs.push_back(expr_ctx->root());
    while (!exprs.empty()) {
        Expr* expr = exprs.back();
        exprs.pop_back();
        auto& children = expr->children();
        if (!children.empty()) {
            for (auto child : children) {
                exprs.push_back(child);
            }
        } else {
            leaf_children.push_back(expr);
        }
    }

    // Count the number of SlotRef and Constant in leaf nodes
    ColumnRef* column_ref = nullptr;
    int col_ref_sz = 0;
    int const_expr_sz = 0;
    for (auto leaf_child : leaf_children) {
        if (leaf_child->is_slotref()) {
            col_ref_sz++;
            column_ref = down_cast<ColumnRef*>(leaf_child);
        } else if (leaf_child->is_constant()) {
            const_expr_sz++;
        }
    }

    // if leaf child has other expr or more than one col_ref_sz
    // we couldn't use global dict optimize
    if (col_ref_sz + const_expr_sz != leaf_children.size() || col_ref_sz != 1) {
        return;
    }

    DCHECK(column_ref != nullptr);
    bool could_apply = _mutable_dict_maps->count(column_ref->slot_id());
    dict_opt_ctx->could_apply_dict_optimize = could_apply;
    if (could_apply) {
        dict_opt_ctx->column_ref = column_ref;
    }
}

void DictOptimizeParser::eval_expr(RuntimeState* state, ExprContext* expr_ctx, DictOptimizeContext* dict_opt_ctx,
                                   int32_t targetSlotId) {
    DCHECK(dict_opt_ctx->could_apply_dict_optimize);
    SlotId need_decode_slot_id = dict_opt_ctx->column_ref->slot_id();
    // Slice -> dict-code
    auto& column_dict_map = _mutable_dict_maps->at(need_decode_slot_id).first;

    std::vector<Slice> slices;
    std::vector<int> codes;

    slices.reserve(column_dict_map.size());
    codes.reserve(column_dict_map.size());

    for (auto& [slice, code] : column_dict_map) {
        slices.emplace_back(slice);
        codes.emplace_back(code);
    }

    auto binary_column = BinaryColumn::create();
    binary_column->append_strings(slices);

    ChunkPtr temp_chunk = std::make_shared<Chunk>();
    temp_chunk->append_column(binary_column, need_decode_slot_id);

    auto result_column = expr_ctx->evaluate(temp_chunk.get());

    ColumnViewer<TYPE_VARCHAR> viewer(result_column);
    int row_sz = viewer.size();

    dict_opt_ctx->code_convert_map_holder.resize(DICT_DECODE_MAX_SIZE + 1);
    std::fill(dict_opt_ctx->code_convert_map_holder.begin(), dict_opt_ctx->code_convert_map_holder.end(), -1);
    dict_opt_ctx->code_convert_map = dict_opt_ctx->code_convert_map_holder.data() + 1;
    auto& code_convert_map = dict_opt_ctx->code_convert_map;

    GlobalDictMap result_map;
    RGlobalDictMap rresult_map;
    int id_allocator = 0;
    for (int i = 0; i < row_sz; ++i) {
        if (viewer.is_null(i)) {
            code_convert_map[codes[i]] = -1;
            dict_opt_ctx->result_nullable = true;
        } else {
            auto value = viewer.value(i);
            Slice slice(value.data, value.size);
            auto res = result_map.emplace(slice, id_allocator);
            if (res.second) {
                id_allocator++;
                auto node = result_map.extract(res.first);
                auto data = state->instance_mem_pool()->allocate(value.size);
                memcpy(data, value.data, value.size);
                slice = Slice(data, value.size);
                node.key() = slice;
                result_map.insert(res.first, std::move(node));
            } else {
                slice = res.first->first;
            }

            code_convert_map[codes[i]] = res.first->second;
            rresult_map.emplace(res.first->second, slice);
        }
    }
    LOG(INFO) << "result has key:0 " << rresult_map.count(0);
    DCHECK_EQ(_mutable_dict_maps->count(targetSlotId), 0);
    _mutable_dict_maps->emplace(targetSlotId, std::make_pair(std::move(result_map), std::move(rresult_map)));
}

void DictOptimizeParser::eval_code_convert(const DictOptimizeContext& opt_ctx, const ColumnPtr& input,
                                           ColumnPtr* output) {
    int row_size = input->size();

    auto res = Int32Column::create_mutable();
    auto& res_data = res->get_data();
    res_data.resize(row_size);

    if (input->is_nullable()) {
        const auto* nullable_column = down_cast<const NullableColumn*>(input.get());
        const auto* null_column = down_cast<const NullColumn*>(nullable_column->null_column().get());
        const auto* data_column = down_cast<const Int32Column*>(nullable_column->data_column().get());
        const auto& input_data = data_column->get_data();

        for (int i = 0; i < row_size; ++i) {
            DCHECK(input_data[i] >= -1 && input_data[i] <= DICT_DECODE_MAX_SIZE);
            res_data[i] = opt_ctx.code_convert_map[input_data[i]];
        }

        if (opt_ctx.result_nullable) {
            auto res_null_column = null_column->clone();
            auto& res_null_data = down_cast<NullColumn*>(res_null_column.get())->get_data();

            for (int i = 0; i < row_size; ++i) {
                res_null_data[i] |= (res_data[i] == -1);
            }

            *output = NullableColumn::create(std::move(res), std::move(res_null_column));
        } else {
            *output = NullableColumn::create(std::move(res), null_column->clone());
        }
    } else {
        const auto* data_column = down_cast<const Int32Column*>(input.get());
        const auto& input_data = data_column->get_data();

        for (int i = 0; i < row_size; ++i) {
            res_data[i] = opt_ctx.code_convert_map[input_data[i]];
        }

        if (opt_ctx.result_nullable) {
            auto res_null = NullColumn::create_mutable();
            auto& res_null_data = res_null->get_data();
            res_null_data.resize(row_size);

            for (int i = 0; i < row_size; ++i) {
                res_null_data[i] = (res_data[i] == -1);
            }

            *output = NullableColumn::create(std::move(res), std::unique_ptr<Column>(res_null.release()));

        } else {
            *output = std::move(res);
        }
    }
}
} // namespace starrocks::vectorized