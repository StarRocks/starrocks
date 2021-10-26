#pragma once

#include <array>

#include "column/column.h"
#include "column/column_hash.h"
#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "util/phmap/phmap.h"
#include "util/slice.h"

namespace starrocks {
class ExprContext;
class RuntimeState;
namespace vectorized {
class ColumnRef;

// TODO: we need to change the dict type to int16 later
using DictId = int32_t;

using LowCardDictColumn = vectorized::Int32Column;
// slice -> global dict code
using GlobalDictMap = phmap::flat_hash_map<Slice, DictId, SliceHashWithSeed<PhmapSeed1>, SliceEqual>;
// global dict code -> slice
using RGlobalDictMap = phmap::flat_hash_map<DictId, Slice>;

using GlobalDictMapEntity = std::pair<GlobalDictMap, RGlobalDictMap>;
// column-id -> GlobalDictMap
using GlobalDictMaps = phmap::flat_hash_map<uint32_t, GlobalDictMapEntity>;

inline std::ostream& operator<<(std::ostream& stream, const RGlobalDictMap& map) {
    stream << "[";
    for (const auto& [k, v] : map) {
        stream << "(" << k << "," << v << "),";
    }
    stream << "]";
    return stream;
}

// column-name -> GlobalDictMap
using GlobalDictByNameMaps = phmap::flat_hash_map<std::string, GlobalDictMap>;

using InvalidDictColumnsSet = phmap::flat_hash_set<std::string, SliceHashWithSeed<PhmapSeed1>, SliceEqual>;

using ColumnIdToGlobalDictMap = phmap::flat_hash_map<uint32_t, GlobalDictMap*>;
static inline ColumnIdToGlobalDictMap EMPTY_GLOBAL_DICTMAPS;

constexpr int DICT_DECODE_MAX_SIZE = 256;

struct DictOptimizeContext {
    bool could_apply_dict_optimize = false;
    SlotId slot_id;
    // if input was not nullable but output was nullable this flag will set true
    bool result_nullable = false;
    // size: DICT_DECODE_MAX_SIZE + 1
    std::vector<int16_t> code_convert_map;
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
