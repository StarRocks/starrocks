// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <array>

#include "column/binary_column.h"
#include "column/column.h"
#include "column/column_hash.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/primitive_type.h"
#include "util/phmap/phmap.h"
#include "util/slice.h"

namespace starrocks {
class ExprContext;
class RuntimeState;
namespace vectorized {
class ColumnRef;

constexpr auto LowCardDictType = TYPE_INT;
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

inline std::ostream& operator<<(std::ostream& stream, const GlobalDictMap& map) {
    stream << "[";
    for (const auto& [k, v] : map) {
        stream << "(" << k << "," << v << "),";
    }
    stream << "]";
    return stream;
}

// column-name -> GlobalDictMap
using GlobalDictByNameMaps = phmap::flat_hash_map<std::string, GlobalDictMap>;

using DictColumnsValidMap = phmap::flat_hash_map<std::string, bool, SliceHashWithSeed<PhmapSeed1>, SliceEqual>;

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
    Column::Filter filter;
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

    Status rewrite_exprs(std::vector<ExprContext*>* expr_ctxs, RuntimeState* state,
                         const std::vector<SlotId>& target_slotids);
    template <bool close_original_expr>
    Status rewrite_conjuncts(std::vector<ExprContext*>* conjuncts_ctxs, RuntimeState* state);

    void close(RuntimeState* state) noexcept;

    Status eval_expression(ExprContext* conjunct, DictOptimizeContext* dict_opt_ctx, int32_t targetSlotId);

    Status check_could_apply_dict_optimize(ExprContext* expr_ctx, DictOptimizeContext* dict_opt_ctx);

    // For global dictionary optimized columns,
    // the type at the execution level is INT but at the storage level is TYPE_STRING/TYPE_CHAR,
    // so we need to pass the real type to the Table Scanner.
    static void rewrite_descriptor(RuntimeState* runtime_state, const std::vector<ExprContext*>& conjunct_ctxs,
                                   const std::map<int32_t, int32_t>& dict_slots_mapping,
                                   std::vector<SlotDescriptor*>* slot_descs);

private:
    Status _check_could_apply_dict_optimize(ExprContext* expr_ctx, DictOptimizeContext* dict_opt_ctx);

    // use code mapping rewrite expr
    template <bool close_original_expr>
    Status _rewrite_expr_ctxs(std::vector<ExprContext*>* expr_ctxs, RuntimeState* state,
                              const std::vector<SlotId>& slot_ids);

    RuntimeState* _runtime_state = nullptr;
    GlobalDictMaps* _mutable_dict_maps = nullptr;
    ObjectPool _free_pool;
    std::vector<ExprContext*> _expr_close_list;
};

std::pair<std::shared_ptr<NullableColumn>, std::vector<int32_t>> extract_column_with_codes(
        const GlobalDictMap& dict_map);

template <PrimitiveType primitive_type, typename Dict, PrimitiveType result_primitive_type>
struct DictDecoder {
    using FieldType = RunTimeCppType<primitive_type>;
    using ResultColumnType = RunTimeColumnType<result_primitive_type>;
    using ColumnType = RunTimeColumnType<primitive_type>;
    Dict dict;
    Status decode(vectorized::Column* in, vectorized::Column* out) {
        DCHECK(in != nullptr);
        DCHECK(out != nullptr);

        // handle const columns
        if (in->is_constant()) {
            if (in->only_null()) {
                bool res = out->append_nulls(in->size());
                DCHECK(res);
                return Status::OK();
            } else {
                out->append_datum(in->get(0));
                out->assign(in->size(), 0);
                return Status::OK();
            }
        }

        if (!in->is_nullable()) {
            auto res_column = down_cast<ResultColumnType*>(out);
            auto column = down_cast<ColumnType*>(in);
            for (size_t i = 0; i < in->size(); i++) {
                FieldType key = column->get_data()[i];
                auto iter = dict.find(key);
                if (iter == dict.end()) {
                    return Status::InternalError(
                            fmt::format("Dict Decode failed, Dict can't take cover all key :{}", key));
                }
                res_column->append(iter->second);
            }
            return Status::OK();
        }

        auto column = down_cast<NullableColumn*>(in);
        auto res_column = down_cast<NullableColumn*>(out);
        res_column->null_column_data().resize(in->size());

        auto res_data_column = down_cast<ResultColumnType*>(res_column->data_column().get());
        auto data_column = down_cast<ColumnType*>(column->data_column().get());

        for (size_t i = 0; i < in->size(); i++) {
            if (column->null_column_data()[i] == 0) {
                res_column->null_column_data()[i] = 0;
                FieldType key = data_column->get_data()[i];
                auto iter = dict.find(key);
                if (iter == dict.end()) {
                    return Status::InternalError(
                            fmt::format("Dict Decode failed, Dict can't take cover all key :{}", key));
                }
                res_data_column->append(iter->second);
            } else {
                res_data_column->append_default();
                res_column->set_null(i);
            }
        }
        return Status::OK();
    }
};

using DefaultDecoder = DictDecoder<LowCardDictType, RGlobalDictMap, TYPE_VARCHAR>;
using DefaultDecoderPtr = std::unique_ptr<DefaultDecoder>;

} // namespace vectorized
} // namespace starrocks
