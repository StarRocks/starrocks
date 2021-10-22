#pragma once

#include <array>

#include "column/column.h"
#include "column/column_hash.h"
#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "util/slice.h"

namespace starrocks {
class ExprContext;
class RuntimeState;
namespace vectorized {
class ColumnRef;
// slice -> global dict code
using GlobalDictMap = std::unordered_map<Slice, int, SliceHashWithSeed<PhmapSeed1>, SliceEqual>;
// global dict code -> slice
using RGlobalDictMap = std::unordered_map<int, Slice>;

using GlobalDictMapEntity = std::pair<GlobalDictMap, RGlobalDictMap>;
// column-id -> GlobalDictMap
using GlobalDictMaps = std::unordered_map<uint32_t, GlobalDictMapEntity>;

static inline std::unordered_map<uint32_t, GlobalDictMap*> EMPTY_GLOBAL_DICTMAPS;

constexpr int DICT_DECODE_MAX_SIZE = 256;

struct DictOptimizeContext {
    bool could_apply_dict_optimize = false;
    SlotId slot_id;
    // if input was not nullable but output was nullable this flag will set true
    bool result_nullable = false;
    // size: DICT_DECODE_MAX_SIZE + 1
    std::vector<int> code_convert_map_holder;
    // code_convert_map_holder.data() + 1
    // code_convert_map[-1] is accessable
    int* code_convert_map;
};

class DictOptimizeParser {
public:
    DictOptimizeParser() = default;
    ~DictOptimizeParser() = default;
    void set_mutable_dict_maps(GlobalDictMaps* dict_maps) { _mutable_dict_maps = dict_maps; }
    void check_could_apply_dict_optimize(ExprContext* expr_ctx, DictOptimizeContext* dict_opt_ctx);
    void eval_expr(RuntimeState* state, ExprContext* expr_ctx, DictOptimizeContext* dict_opt_ctx, int32_t targetSlotId);
    void eval_code_convert(const DictOptimizeContext& opt_ctx, const ColumnPtr& input, ColumnPtr* output);

private:
    GlobalDictMaps* _mutable_dict_maps;
};

} // namespace vectorized
} // namespace starrocks
