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

#include "join_hash_table_descriptor.h"

#include "types/date_value.h"
#include "types/timestamp_value.h"

namespace starrocks {

// if the same hash values are clustered, after the first probe, all related hash buckets are cached, without too many
// misses. So check time locality of probe keys here.
void HashTableProbeState::consider_probe_time_locality() {
    if (active_coroutines > 0) {
        // redo decision
        if ((probe_chunks & (detect_step - 1)) == 0) {
            int window_size = std::min(active_coroutines * 4, 50);
            if (probe_row_count > window_size) {
                phmap::flat_hash_map<uint32_t, uint32_t, StdHash<uint32_t>> occurrence;
                occurrence.reserve(probe_row_count);
                uint32_t unique_size = 0;
                bool enable_interleaving = true;
                uint32_t target = probe_row_count >> 3;
                for (auto i = 0; i < probe_row_count; i++) {
                    if (occurrence[next[i]] == 0) {
                        ++unique_size;
                        if (unique_size >= target) {
                            break;
                        }
                    }
                    occurrence[next[i]]++;
                    if (i >= window_size) {
                        occurrence[next[i - window_size]]--;
                    }
                }
                if (unique_size < target) {
                    active_coroutines = 0;
                    enable_interleaving = false;
                }
                // enlarge step if the decision is the same, otherwise reduce it
                if (enable_interleaving == last_enable_interleaving) {
                    detect_step = detect_step >= 1024 ? detect_step : (detect_step << 1);
                } else {
                    last_enable_interleaving = enable_interleaving;
                    detect_step = 1;
                }
            } else {
                active_coroutines = 0;
            }
        } else if (!last_enable_interleaving) {
            active_coroutines = 0;
        }
    }
    ++probe_chunks;
}

#define INSTANTIATE_ASOF_VECTOR(CppType) \
    template class AsofLookupVector<CppType, TExprOpcode::LT>; \
    template class AsofLookupVector<CppType, TExprOpcode::LE>; \
    template class AsofLookupVector<CppType, TExprOpcode::GT>; \
    template class AsofLookupVector<CppType, TExprOpcode::GE>;

INSTANTIATE_ASOF_VECTOR(int64_t)
INSTANTIATE_ASOF_VECTOR(DateValue)
INSTANTIATE_ASOF_VECTOR(TimestampValue)

#undef INSTANTIATE_ASOF_VECTOR

AsofVectorVariant create_asof_lookup_vector_base(LogicalType logical_type, TExprOpcode::type opcode) {
#define MAKE_ASOF_VECTOR(T, OP) std::make_unique<AsofLookupVector<T, TExprOpcode::OP>>()
#define ASOF_OPCODE_SWITCH(CppType) \
    switch (opcode) { \
        case TExprOpcode::LT: return MAKE_ASOF_VECTOR(CppType, LT); \
        case TExprOpcode::LE: return MAKE_ASOF_VECTOR(CppType, LE); \
        case TExprOpcode::GT: return MAKE_ASOF_VECTOR(CppType, GT); \
        case TExprOpcode::GE: return MAKE_ASOF_VECTOR(CppType, GE); \
        default: CHECK(false) << "Unsupported opcode: " << opcode; __builtin_unreachable(); \
    }

    switch (logical_type) {
        case TYPE_BIGINT:   ASOF_OPCODE_SWITCH(int64_t)
        case TYPE_DATE:     ASOF_OPCODE_SWITCH(DateValue)  
        case TYPE_DATETIME: ASOF_OPCODE_SWITCH(TimestampValue)
        default: CHECK(false) << "Unsupported type: " << logical_type; __builtin_unreachable();
    }
#undef MAKE_ASOF_VECTOR
#undef ASOF_OPCODE_SWITCH
}

#define ASOF_VARIANT_CALL(index, method_call) \
    std::get<index>(asof_lookup_vectors[lookup_index])->method_call

#define ASOF_TYPE_DISPATCH(CppType, start_index, method_call, error_msg) \
    if constexpr (std::is_same_v<CppType, int64_t>) { \
        switch (type_index) { \
            case 0: return ASOF_VARIANT_CALL(0, method_call);   /* INT64_LT */ \
            case 1: return ASOF_VARIANT_CALL(1, method_call);   /* INT64_LE */ \
            case 2: return ASOF_VARIANT_CALL(2, method_call);   /* INT64_GT */ \
            case 3: return ASOF_VARIANT_CALL(3, method_call);   /* INT64_GE */ \
            default: error_msg; \
        } \
    } else if constexpr (std::is_same_v<CppType, DateValue>) { \
        switch (type_index) { \
            case 4: return ASOF_VARIANT_CALL(4, method_call);   /* DATE_LT */ \
            case 5: return ASOF_VARIANT_CALL(5, method_call);   /* DATE_LE */ \
            case 6: return ASOF_VARIANT_CALL(6, method_call);   /* DATE_GT */ \
            case 7: return ASOF_VARIANT_CALL(7, method_call);   /* DATE_GE */ \
            default: error_msg; \
        } \
    } else if constexpr (std::is_same_v<CppType, TimestampValue>) { \
        switch (type_index) { \
            case 8:  return ASOF_VARIANT_CALL(8, method_call);  /* DATETIME_LT */ \
            case 9:  return ASOF_VARIANT_CALL(9, method_call);  /* DATETIME_LE */ \
            case 10: return ASOF_VARIANT_CALL(10, method_call); /* DATETIME_GT */ \
            case 11: return ASOF_VARIANT_CALL(11, method_call); /* DATETIME_GE */ \
            default: error_msg; \
        } \
    } else { \
        static_assert(std::is_same_v<CppType, int64_t> || \
                     std::is_same_v<CppType, DateValue> || \
                     std::is_same_v<CppType, TimestampValue>, \
                     "Unsupported CppType for AsofVector"); \
        error_msg; \
    }

#define ASOF_VOID_DISPATCH(CppType, method_call) \
    if constexpr (std::is_same_v<CppType, int64_t>) { \
        switch (type_index) { \
            case 0: ASOF_VARIANT_CALL(0, method_call); break;   /* INT64_LT */ \
            case 1: ASOF_VARIANT_CALL(1, method_call); break;   /* INT64_LE */ \
            case 2: ASOF_VARIANT_CALL(2, method_call); break;   /* INT64_GT */ \
            case 3: ASOF_VARIANT_CALL(3, method_call); break;   /* INT64_GE */ \
            default: DCHECK(false) << "Type mismatch: expected int64_t variant index 0-3, got " << type_index; \
        } \
    } else if constexpr (std::is_same_v<CppType, DateValue>) { \
        switch (type_index) { \
            case 4: ASOF_VARIANT_CALL(4, method_call); break;   /* DATE_LT */ \
            case 5: ASOF_VARIANT_CALL(5, method_call); break;   /* DATE_LE */ \
            case 6: ASOF_VARIANT_CALL(6, method_call); break;   /* DATE_GT */ \
            case 7: ASOF_VARIANT_CALL(7, method_call); break;   /* DATE_GE */ \
            default: DCHECK(false) << "Type mismatch: expected DateValue variant index 4-7, got " << type_index; \
        } \
    } else if constexpr (std::is_same_v<CppType, TimestampValue>) { \
        switch (type_index) { \
            case 8:  ASOF_VARIANT_CALL(8, method_call); break;  /* DATETIME_LT */ \
            case 9:  ASOF_VARIANT_CALL(9, method_call); break;  /* DATETIME_LE */ \
            case 10: ASOF_VARIANT_CALL(10, method_call); break; /* DATETIME_GT */ \
            case 11: ASOF_VARIANT_CALL(11, method_call); break; /* DATETIME_GE */ \
            default: DCHECK(false) << "Type mismatch: expected TimestampValue variant index 8-11, got " << type_index; \
        } \
    } else { \
        static_assert(std::is_same_v<CppType, int64_t> || \
                     std::is_same_v<CppType, DateValue> || \
                     std::is_same_v<CppType, TimestampValue>, \
                     "Unsupported CppType for AsofVector"); \
    }

template <typename CppType>
void JoinHashTableItems::add_asof_row(uint32_t lookup_index, CppType asof_value, uint32_t row_index) {
    size_t type_index = asof_lookup_vectors[lookup_index].index();
    ASOF_VOID_DISPATCH(CppType, add_row(asof_value, row_index));
}

template <typename CppType>
uint32_t JoinHashTableItems::find_asof_match(uint32_t lookup_index, CppType probe_value) const {
    size_t type_index = asof_lookup_vectors[lookup_index].index();
    ASOF_TYPE_DISPATCH(CppType, 0, find_asof_match(probe_value), return 0);
}

// Clean up macros after template definitions
#undef ASOF_VARIANT_CALL
#undef ASOF_TYPE_DISPATCH
#undef ASOF_VOID_DISPATCH

// Explicit template instantiations - compact form
#define INSTANTIATE_ASOF_METHODS(CppType) \
    template void JoinHashTableItems::add_asof_row<CppType>(uint32_t, CppType, uint32_t); \
    template uint32_t JoinHashTableItems::find_asof_match<CppType>(uint32_t, CppType) const;

INSTANTIATE_ASOF_METHODS(int64_t)
INSTANTIATE_ASOF_METHODS(DateValue)
INSTANTIATE_ASOF_METHODS(TimestampValue)

#undef INSTANTIATE_ASOF_METHODS

} // namespace starrocks