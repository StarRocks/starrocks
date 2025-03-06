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
// This file is based on code available under the Apache license here:
//  https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionsStringSimilarity.cpp

#include "column/column_hash.h"
#include "exprs/string_functions.h"
#include "gutil/strings/fastmem.h"
namespace starrocks {
static constexpr size_t MAX_STRING_SIZE = 1 << 15;
// uint16[2^16] can almost fit into L2
static constexpr size_t MAP_SIZE = 1 << 16;
// we restrict needle's size smaller than 2^16, so even if every gram in needle is the same as each other
// we still only need one uint16 to store its frequency
using NgramHash = uint16;

struct Ngramstate {
    // use std::unique_ptr<std::vector<NgramHash>> instead  of vector as key
    // to prevent vector use after free when hash map resize
    using DriverMap = phmap::parallel_flat_hash_map<int32_t, std::unique_ptr<std::vector<NgramHash>>,
                                                    phmap::Hash<int32_t>, phmap::EqualTo<int32_t>,
                                                    phmap::Allocator<int32_t>, NUM_LOCK_SHARD_LOG, std::mutex>;
    Ngramstate(size_t hash_map_len) : publicHashMap(hash_map_len, 0){};
    // unmodified map, only used for driver to copy
    std::vector<NgramHash> publicHashMap;
    DriverMap driver_maps; // hashMap for each pipeline_driver, to make it driver-local

    size_t needle_gram_count = 0;

    float result = -1;

    std::vector<NgramHash>* get_or_create_driver_hashmap() {
        int32_t driver_id = CurrentThread::current().get_driver_id();

        std::vector<NgramHash>* result = nullptr;
        driver_maps.if_contains(driver_id, [&](const auto& value) { result = value.get(); });
        // create the dirver map when one driver first call this function
        if (UNLIKELY(result == nullptr)) {
            std::unique_ptr<std::vector<NgramHash>> result_ptr =
                    std::make_unique<std::vector<NgramHash>>(publicHashMap);
            result = result_ptr.get();
            driver_maps.lazy_emplace(driver_id, [&](auto build) { build(driver_id, std::move(result_ptr)); });
        }

        DCHECK(result != nullptr);

        return result;
    }
};

template <bool case_insensitive, bool use_utf_8, class Gram>
class NgramFunctionImpl {
public:
    StatusOr<ColumnPtr> static ngram_search_impl(FunctionContext* context, const Columns& columns) {
        RETURN_IF_COLUMNS_ONLY_NULL(columns);
        const auto& haystack_column = columns[0];

        const auto& needle_column = columns[1];
        const auto& gram_num_column = columns[2];

        if (!needle_column->is_constant()) {
            return Status::NotSupported("ngram search's second parameter must be const");
        }

        const Slice& needle = ColumnHelper::get_const_value<TYPE_VARCHAR>(needle_column);
        if (needle.get_size() > MAX_STRING_SIZE) {
            return Status::NotSupported("ngram function's second parameter is larger than 2^15");
        }

        int gram_num = ColumnHelper::get_const_value<TYPE_INT>(gram_num_column);

        if (gram_num <= 0) {
            return Status::NotSupported("ngram search's third parameter must be a positive number");
        }

        // needle is too small so we can not get even single Ngram, so they are not similar at all
        if (needle.get_size() < gram_num) {
            return ColumnHelper::create_const_column<TYPE_DOUBLE>(0, haystack_column->size());
        }

        auto state = reinterpret_cast<Ngramstate*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        std::vector<NgramHash>* map = state->get_or_create_driver_hashmap();
        if (haystack_column->is_constant()) {
            if (context->is_constant_column(0)) {
                // already calculated in prepare and cache result in state
                DCHECK(state->result != -1);
                return ColumnHelper::create_const_column<TYPE_DOUBLE>(state->result, haystack_column->size());
            } else {
                // haystack is const column but not constant
                float result = haystack_const_and_needle_const(
                        ColumnHelper::get_const_value<TYPE_VARCHAR>(haystack_column), *map, context, gram_num);
                return ColumnHelper::create_const_column<TYPE_DOUBLE>(result, haystack_column->size());
            }
        } else {
            return haystack_vector_and_needle_const(haystack_column, *map, context, gram_num);
        }
    }

    Status static ngram_search_prepare_impl(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        if (scope != FunctionContext::FRAGMENT_LOCAL) {
            return Status::OK();
        }

        // make sure this funtion is idempotent
        if (context->get_function_state(FunctionContext::FRAGMENT_LOCAL) != nullptr) {
            return Status::OK();
        }

        auto* state = new Ngramstate(MAP_SIZE);

        context->set_function_state(scope, state);

        if (!context->is_notnull_constant_column(1) || !context->is_notnull_constant_column(2)) {
            return Status::OK();
        }

        auto const& needle_column = context->get_constant_column(1);
        const Slice& needle = ColumnHelper::get_const_value<TYPE_VARCHAR>(needle_column);

        auto const& gram_num_column = context->get_constant_column(2);
        size_t gram_num = ColumnHelper::get_const_value<TYPE_INT>(gram_num_column);

        if (needle.get_size() > MAX_STRING_SIZE) {
            return Status::OK();
        }

        // only calculate needle's hashmap once
        state->needle_gram_count = calculateMapWithNeedle(state->publicHashMap, needle, gram_num);

        // all not-null const, so we just calculate the result once
        if (context->is_notnull_constant_column(0)) {
            const Slice& haystack = ColumnHelper::get_const_value<TYPE_VARCHAR>(context->get_constant_column(0));
            state->result = haystack_const_and_needle_const(haystack, state->publicHashMap, context, gram_num);
        }
        return Status::OK();
    }

private:
    // for every gram of needle, we calculate its' hash value and store its' frequency in map, and return the number of gram in needle
    size_t static calculateMapWithNeedle(std::vector<NgramHash>& map, const Slice& needle, size_t gram_num) {
        size_t needle_length = needle.get_size();
        NgramHash cur_hash;
        size_t i;
        Slice cur_needle(needle.get_data(), needle_length);
        const char* cur_char_ptr;
        std::string buf;
        if constexpr (case_insensitive) {
            tolower(needle, buf);
            cur_needle = Slice(buf.c_str(), buf.size());
        }
        cur_char_ptr = cur_needle.get_data();

        for (i = 0; i + gram_num <= needle_length; i++) {
            cur_hash = getAsciiHash(cur_char_ptr + i, gram_num);
            map[cur_hash]++;
        }

        return i;
    }

    ColumnPtr static haystack_vector_and_needle_const(const ColumnPtr& haystack_column, std::vector<NgramHash>& map,
                                                      FunctionContext* context, size_t gram_num) {
        std::vector<NgramHash> map_restore_helper(MAX_STRING_SIZE, 0);

        NullColumnPtr res_null = nullptr;
        ColumnPtr haystackPtr = nullptr;
        // used in case_insensitive
        StatusOr<ColumnPtr> lower;
        if (haystack_column->is_nullable()) {
            auto haystack_nullable = ColumnHelper::as_column<NullableColumn>(haystack_column);
            res_null = haystack_nullable->null_column();
            haystackPtr = haystack_nullable->data_column();
        } else {
            haystackPtr = haystack_column;
        }
        if constexpr (case_insensitive) {
            Columns temp;
            temp.emplace_back(haystackPtr);
            lower = StringFunctions::lower(nullptr, temp);
            haystackPtr = lower.value();
        }

        BinaryColumn* haystack = ColumnHelper::as_raw_column<BinaryColumn>(haystackPtr);
        size_t chunk_size = haystack->size();
        auto res = RunTimeColumnType<TYPE_DOUBLE>::create(chunk_size);

        auto state = reinterpret_cast<Ngramstate*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

        // needle_gram_count may be zero because needle is empty or N is too large for needle
        size_t needle_gram_count = state->needle_gram_count;
        for (size_t i = 0; i < chunk_size; i++) {
            const Slice& cur_haystack_str = haystack->get_slice(i);
            // if haystack is too large, we can say they are not similar at all
            if (cur_haystack_str.get_size() > MAX_STRING_SIZE) {
                res->get_data()[i] = 0;
                continue;
            }

            size_t needle_not_overlap_with_haystack = calculateDistanceWithHaystack<true>(
                    map, cur_haystack_str, map_restore_helper, needle_gram_count, gram_num);
            DCHECK(needle_not_overlap_with_haystack <= needle_gram_count);

            // now get the result
            double row_result = 1.0f - (needle_not_overlap_with_haystack)*1.0f / std::max(needle_gram_count, (size_t)1);

            res->get_data()[i] = row_result;
        }

        if (haystack_column->is_nullable()) {
            return NullableColumn::create(std::move(res), std::move(res_null));
        } else {
            return res;
        }
    }

    float static haystack_const_and_needle_const(const Slice& haystack, std::vector<NgramHash>& map,
                                                 FunctionContext* context, size_t gram_num) {
        std::vector<NgramHash> map_restore_helper{};
        // if haystack is too large, we can say they are not similar at all
        if (haystack.get_size() > MAX_STRING_SIZE) {
            return 0;
        }

        Slice cur_haystack(haystack.get_data(), haystack.get_size());

        std::string buf;
        if constexpr (case_insensitive) {
            tolower(haystack, buf);
            cur_haystack = Slice(buf.c_str(), buf.size());
        }

        auto state = reinterpret_cast<Ngramstate*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

        // needle_gram_count may be zero because needle is empty or N is too large for needle
        size_t needle_gram_count = state->needle_gram_count;
        size_t needle_not_overlap_with_haystack = calculateDistanceWithHaystack<false>(
                map, cur_haystack, map_restore_helper, needle_gram_count, gram_num);
        float result = 1.0f - (needle_not_overlap_with_haystack)*1.0f / std::max(needle_gram_count, (size_t)1);
        DCHECK(needle_not_overlap_with_haystack <= needle_gram_count);
        return result;
    }

    // traverse haystack‘s every gram, find whether this gram is in needle or not using gram's hash
    // 16bit hash value may cause hash collision, but because we just calculate the similarity of two string
    // so don't need to be so accurate.
    template <bool need_recovery_map>
    size_t static calculateDistanceWithHaystack(std::vector<NgramHash>& map, const Slice& haystack,
                                                [[maybe_unused]] std::vector<NgramHash>& map_restore_helper,
                                                size_t needle_gram_count, size_t gram_num) {
        size_t haystack_length = haystack.get_size();
        NgramHash cur_hash;
        size_t i;
        const char* ptr = haystack.get_data();

        for (i = 0; i + gram_num <= haystack_length; i++) {
            cur_hash = getAsciiHash(ptr + i, gram_num);
            // if this gram is in needle
            if (map[cur_hash] > 0) {
                needle_gram_count--;
                map[cur_hash]--;
                if constexpr (need_recovery_map) {
                    map_restore_helper[i] = cur_hash;
                }
            }
        }

        if constexpr (need_recovery_map) {
            for (int j = 0; j < i; j++) {
                if (map_restore_helper[j]) {
                    map[map_restore_helper[j]]++;
                    // reset map_restore_helper
                    map_restore_helper[j] = 0;
                }
            }
        }

        return needle_gram_count;
    }

    void inline static tolower(const Slice& str, std::string& buf) {
        buf.assign(str.get_data(), str.get_size());
        std::transform(buf.begin(), buf.end(), buf.begin(), [](unsigned char c) { return std::tolower(c); });
    }

    static NgramHash getAsciiHash(const Gram* ch, size_t gram_num) {
        return crc_hash_32(ch, gram_num, CRC_HASH_SEEDS::CRC_HASH_SEED1) & (0xffffu);
    }
};

StatusOr<ColumnPtr> StringFunctions::ngram_search(FunctionContext* context, const Columns& columns) {
    return NgramFunctionImpl<false, false, char>::ngram_search_impl(context, columns);
}

StatusOr<ColumnPtr> StringFunctions::ngram_search_case_insensitive(FunctionContext* context, const Columns& columns) {
    return NgramFunctionImpl<true, false, char>::ngram_search_impl(context, columns);
}

Status StringFunctions::ngram_search_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    return NgramFunctionImpl<false, false, char>::ngram_search_prepare_impl(context, scope);
}

Status StringFunctions::ngram_search_case_insensitive_prepare(FunctionContext* context,
                                                              FunctionContext::FunctionStateScope scope) {
    return NgramFunctionImpl<true, false, char>::ngram_search_prepare_impl(context, scope);
}

Status StringFunctions::ngram_search_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* state = reinterpret_cast<Ngramstate*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (state != nullptr) {
            delete state;
            state = nullptr;
        }
    }
    return Status::OK();
}

} // namespace starrocks