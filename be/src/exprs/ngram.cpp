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
#include "exprs/function_context.h"
#include "exprs/string_functions.h"
#include "gutil/strings/fastmem.h"
#include "runtime/runtime_state.h"
#include "util/utf8.h"

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
    using DriverMap = phmap::parallel_flat_hash_map<std::thread::id, std::unique_ptr<std::vector<NgramHash>>,
                                                    phmap::Hash<std::thread::id>, phmap::EqualTo<std::thread::id>,
                                                    phmap::Allocator<std::thread::id>, NUM_LOCK_SHARD_LOG, std::mutex>;
    Ngramstate(size_t hash_map_len) : publicHashMap(hash_map_len, 0){};
    // unmodified map, only used for driver to copy
    std::vector<NgramHash> publicHashMap;
    DriverMap driver_maps; // hashMap for each pipeline_driver, to make it driver-local

    size_t needle_gram_count = 0;

    float result = -1;

    // Flag to indicate whether UTF-8 mode is enabled (set in prepare from template parameter)
    bool use_utf8 = false;

    std::vector<NgramHash>* get_or_create_driver_hashmap() {
        std::thread::id current_thread_id = std::this_thread::get_id();

        std::vector<NgramHash>* result = nullptr;
        driver_maps.lazy_emplace_l(
                current_thread_id, [&](const auto& value) { result = value.get(); },
                [&](auto build) {
                    std::unique_ptr<std::vector<NgramHash>> result_ptr =
                            std::make_unique<std::vector<NgramHash>>(publicHashMap);
                    result = result_ptr.get();
                    build(current_thread_id, std::move(result_ptr));
                });

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

        const Slice needle = ColumnHelper::get_const_value<TYPE_VARCHAR>(needle_column);
        if (needle.get_size() > MAX_STRING_SIZE) {
            return Status::NotSupported("ngram function's second parameter is larger than 2^15");
        }

        int gram_num = ColumnHelper::get_const_value<TYPE_INT>(gram_num_column);

        if (gram_num <= 0) {
            return Status::NotSupported("ngram search's third parameter must be a positive number");
        }

        auto state = reinterpret_cast<Ngramstate*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

        // For UTF-8 mode, check character count instead of byte count
        size_t needle_char_count;
        if constexpr (use_utf_8) {
            needle_char_count = utf8_len(needle.get_data(), needle.get_data() + needle.get_size());
        } else {
            needle_char_count = needle.get_size();
        }

        // needle is too small so we can not get even single Ngram, so they are not similar at all
        if (needle_char_count < gram_num) {
            return ColumnHelper::create_const_column<TYPE_DOUBLE>(0, haystack_column->size());
        }

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
        state->use_utf8 = use_utf_8;

        context->set_function_state(scope, state);

        if (!context->is_notnull_constant_column(1) || !context->is_notnull_constant_column(2)) {
            return Status::OK();
        }

        auto const& needle_column = context->get_constant_column(1);
        const Slice needle = ColumnHelper::get_const_value<TYPE_VARCHAR>(needle_column);

        auto const& gram_num_column = context->get_constant_column(2);
        size_t gram_num = ColumnHelper::get_const_value<TYPE_INT>(gram_num_column);

        if (needle.get_size() > MAX_STRING_SIZE) {
            return Status::OK();
        }

        // only calculate needle's hashmap once
        state->needle_gram_count = calculateMapWithNeedle(state->publicHashMap, needle, gram_num);

        // all not-null const, so we just calculate the result once
        if (context->is_notnull_constant_column(0)) {
            const Slice haystack = ColumnHelper::get_const_value<TYPE_VARCHAR>(context->get_constant_column(0));
            state->result = haystack_const_and_needle_const(haystack, state->publicHashMap, context, gram_num);
        }
        return Status::OK();
    }

private:
    // Get UTF-8 character positions for a string
    static void get_utf8_positions(const char* data, size_t len, std::vector<size_t>& positions) {
        positions.clear();
        for (size_t i = 0; i < len;) {
            positions.push_back(i);
            i += UTF8_BYTE_LENGTH_TABLE[static_cast<uint8_t>(data[i])];
        }
    }

    // UTF-8 aware tolower - uses shared implementation from util/utf8.h
    static void tolower_utf8(const Slice& str, std::string& buf) {
        if (validate_ascii_fast(str.get_data(), str.get_size())) {
            Slice(str.get_data(), str.get_size()).tolower(buf);
        } else {
            utf8_tolower(str.get_data(), str.get_size(), buf);
        }
    }

    // for every gram of needle, we calculate its' hash value and store its' frequency in map, and return the number of gram in needle
    size_t static calculateMapWithNeedle(std::vector<NgramHash>& map, const Slice& needle, size_t gram_num) {
        Slice cur_needle(needle.get_data(), needle.get_size());
        std::string buf;
        if constexpr (case_insensitive) {
            if constexpr (use_utf_8) {
                tolower_utf8(needle, buf);
            } else {
                buf.assign(needle.get_data(), needle.get_size());
                std::transform(buf.begin(), buf.end(), buf.begin(), [](unsigned char c) { return std::tolower(c); });
            }
            cur_needle = Slice(buf.c_str(), buf.size());
        }

        const char* data = cur_needle.get_data();
        size_t len = cur_needle.get_size();

        if constexpr (use_utf_8) {
            // UTF-8 mode: iterate by characters
            std::vector<size_t> positions;
            get_utf8_positions(data, len, positions);

            size_t num_chars = positions.size();
            if (num_chars < gram_num) {
                return 0;
            }

            size_t gram_count = 0;
            for (size_t i = 0; i + gram_num <= num_chars; i++) {
                size_t start = positions[i];
                size_t end = (i + gram_num < num_chars) ? positions[i + gram_num] : len;
                size_t ngram_bytes = end - start;

                NgramHash cur_hash = crc_hash_32(data + start, ngram_bytes, CRC_HASH_SEEDS::CRC_HASH_SEED1) & (0xffffu);
                map[cur_hash]++;
                gram_count++;
            }
            return gram_count;
        } else {
            // ASCII mode: iterate by bytes (original behavior)
            size_t i;
            for (i = 0; i + gram_num <= len; i++) {
                NgramHash cur_hash = crc_hash_32(data + i, gram_num, CRC_HASH_SEEDS::CRC_HASH_SEED1) & (0xffffu);
                map[cur_hash]++;
            }
            return i;
        }
    }

    ColumnPtr static haystack_vector_and_needle_const(const ColumnPtr& haystack_column, std::vector<NgramHash>& map,
                                                      FunctionContext* context, size_t gram_num) {
        std::vector<NgramHash> map_restore_helper(MAX_STRING_SIZE, 0);

        NullColumnPtr res_null = nullptr;
        ColumnPtr haystackPtr = nullptr;
        if (haystack_column->is_nullable()) {
            auto haystack_nullable = ColumnHelper::as_column<NullableColumn>(haystack_column);
            res_null = haystack_nullable->null_column();
            haystackPtr = haystack_nullable->data_column();
        } else {
            haystackPtr = haystack_column;
        }

        // For case-insensitive ASCII mode, use the fast StringCaseToggleFunction
        // For UTF-8 mode, we handle case conversion per-string in calculateDistanceWithHaystack
        if constexpr (case_insensitive && !use_utf_8) {
            haystackPtr = StringCaseToggleFunction<false>::evaluate<TYPE_VARCHAR, TYPE_VARCHAR>(haystackPtr);
        }

        const BinaryColumn* haystack = ColumnHelper::as_raw_column<BinaryColumn>(haystackPtr);
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
            if constexpr (use_utf_8) {
                tolower_utf8(haystack, buf);
            } else {
                buf.assign(haystack.get_data(), haystack.get_size());
                std::transform(buf.begin(), buf.end(), buf.begin(), [](unsigned char c) { return std::tolower(c); });
            }
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

    // traverse haystack's every gram, find whether this gram is in needle or not using gram's hash
    // 16bit hash value may cause hash collision, but because we just calculate the similarity of two string
    // so don't need to be so accurate.
    template <bool need_recovery_map>
    size_t static calculateDistanceWithHaystack(std::vector<NgramHash>& map, const Slice& haystack,
                                                [[maybe_unused]] std::vector<NgramHash>& map_restore_helper,
                                                size_t needle_gram_count, size_t gram_num) {
        // For UTF-8 case-insensitive mode in vector processing, we need to convert here
        std::string lower_buf;
        Slice cur_haystack = haystack;
        if constexpr (case_insensitive && use_utf_8) {
            tolower_utf8(haystack, lower_buf);
            cur_haystack = Slice(lower_buf.c_str(), lower_buf.size());
        }

        const char* data = cur_haystack.get_data();
        size_t len = cur_haystack.get_size();

        if constexpr (use_utf_8) {
            // UTF-8 mode: iterate by characters
            std::vector<size_t> positions;
            get_utf8_positions(data, len, positions);

            size_t num_chars = positions.size();
            if (num_chars < gram_num) {
                return needle_gram_count;
            }

            // For UTF-8 mode, we use positions as indices in map_restore_helper
            size_t gram_idx = 0;
            for (size_t i = 0; i + gram_num <= num_chars; i++, gram_idx++) {
                size_t start = positions[i];
                size_t end = (i + gram_num < num_chars) ? positions[i + gram_num] : len;
                size_t ngram_bytes = end - start;

                NgramHash cur_hash = crc_hash_32(data + start, ngram_bytes, CRC_HASH_SEEDS::CRC_HASH_SEED1) & (0xffffu);

                if (map[cur_hash] > 0) {
                    needle_gram_count--;
                    map[cur_hash]--;
                    if constexpr (need_recovery_map) {
                        map_restore_helper[gram_idx] = cur_hash;
                    }
                }
            }

            if constexpr (need_recovery_map) {
                for (size_t j = 0; j < gram_idx; j++) {
                    if (map_restore_helper[j]) {
                        map[map_restore_helper[j]]++;
                        map_restore_helper[j] = 0;
                    }
                }
            }
        } else {
            // ASCII mode: iterate by bytes (original behavior)
            size_t i;
            for (i = 0; i + gram_num <= len; i++) {
                NgramHash cur_hash = crc_hash_32(data + i, gram_num, CRC_HASH_SEEDS::CRC_HASH_SEED1) & (0xffffu);
                if (map[cur_hash] > 0) {
                    needle_gram_count--;
                    map[cur_hash]--;
                    if constexpr (need_recovery_map) {
                        map_restore_helper[i] = cur_hash;
                    }
                }
            }

            if constexpr (need_recovery_map) {
                for (size_t j = 0; j < i; j++) {
                    if (map_restore_helper[j]) {
                        map[map_restore_helper[j]]++;
                        map_restore_helper[j] = 0;
                    }
                }
            }
        }

        return needle_gram_count;
    }
};

// Wrapper functions that check the UTF-8 flag at runtime and dispatch to the correct implementation
StatusOr<ColumnPtr> StringFunctions::ngram_search(FunctionContext* context, const Columns& columns) {
    auto state = reinterpret_cast<Ngramstate*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (state && state->use_utf8) {
        return NgramFunctionImpl<false, true, char>::ngram_search_impl(context, columns);
    }
    return NgramFunctionImpl<false, false, char>::ngram_search_impl(context, columns);
}

StatusOr<ColumnPtr> StringFunctions::ngram_search_case_insensitive(FunctionContext* context, const Columns& columns) {
    auto state = reinterpret_cast<Ngramstate*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (state && state->use_utf8) {
        return NgramFunctionImpl<true, true, char>::ngram_search_impl(context, columns);
    }
    return NgramFunctionImpl<true, false, char>::ngram_search_impl(context, columns);
}

Status StringFunctions::ngram_search_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (context->state() && context->state()->ngram_search_support_utf8()) {
        return NgramFunctionImpl<false, true, char>::ngram_search_prepare_impl(context, scope);
    }
    return NgramFunctionImpl<false, false, char>::ngram_search_prepare_impl(context, scope);
}

Status StringFunctions::ngram_search_case_insensitive_prepare(FunctionContext* context,
                                                              FunctionContext::FunctionStateScope scope) {
    if (context->state() && context->state()->ngram_search_support_utf8()) {
        return NgramFunctionImpl<true, true, char>::ngram_search_prepare_impl(context, scope);
    }
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
