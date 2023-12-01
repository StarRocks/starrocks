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

#pragma once

#include <cstdint>
#include <vector>

#include "column/column.h"
#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "runtime/global_dict/types.h"

namespace starrocks {

class RuntimeState;
class ExprContext;
class Expr;
class SlotDescriptor;

struct DictOptimizeContext {
    bool could_apply_dict_optimize = false;
    SlotId slot_id;
    // if input was not nullable but output was nullable this flag will set true
    bool result_nullable = false;
    // size: DICT_DECODE_MAX_SIZE + 1
    std::vector<int16_t> code_convert_map;
    Filter filter;
    // for no-string column convert map
    ColumnPtr convert_column;
};

class DictOptimizeParser {
public:
    DictOptimizeParser() = default;
    ~DictOptimizeParser() = default;
    void set_mutable_dict_maps(RuntimeState* state, GlobalDictMaps* dict_maps) {
        _runtime_state = state;
        _mutable_dict_maps = dict_maps;
    }

    [[nodiscard]] Status rewrite_exprs(std::vector<ExprContext*>* expr_ctxs, RuntimeState* state,
                                       const std::vector<SlotId>& target_slotids);

    [[nodiscard]] Status rewrite_conjuncts(std::vector<ExprContext*>* conjuncts_ctxs, RuntimeState* state);

    void close(RuntimeState* state) noexcept;

    [[nodiscard]] Status eval_expression(ExprContext* conjunct, DictOptimizeContext* dict_opt_ctx,
                                         int32_t targetSlotId);

    void check_could_apply_dict_optimize(ExprContext* expr_ctx, DictOptimizeContext* dict_opt_ctx);

    // For global dictionary optimized columns,
    // the type at the execution level is INT but at the storage level is TYPE_STRING/TYPE_CHAR,
    // so we need to pass the real type to the Table Scanner.
    static void rewrite_descriptor(RuntimeState* runtime_state, const std::vector<ExprContext*>& conjunct_ctxs,
                                   const std::map<int32_t, int32_t>& dict_slots_mapping,
                                   std::vector<SlotDescriptor*>* slot_descs);

private:
    void _check_could_apply_dict_optimize(Expr* expr, DictOptimizeContext* dict_opt_ctx);

    // use code mapping rewrite expr
    [[nodiscard]] Status _rewrite_expr_ctxs(std::vector<ExprContext*>* expr_ctxs, RuntimeState* state,
                                            const std::vector<SlotId>& slot_ids);
    [[nodiscard]] Status rewrite_expr(ExprContext* ctx, Expr* expr, SlotId slot_id);
    [[nodiscard]] Status _eval_and_rewrite(ExprContext* ctx, Expr* expr, DictOptimizeContext* dict_opt_ctx,
                                           int32_t targetSlotId);

    RuntimeState* _runtime_state = nullptr;
    GlobalDictMaps* _mutable_dict_maps = nullptr;
    ObjectPool _free_pool;
};

} // namespace starrocks
