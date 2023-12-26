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
//  https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/Volnitsky.h

#include "column/column_hash.h"
#include "exprs/string_functions.h"
namespace starrocks {
static constexpr size_t max_string_size = 1 << 15;

template <size_t N, bool case_insensitive, bool use_utf_8, class Gram>
class NgramFunctionImpl {
public:
    // we restrict needle's size smaller than 2^16, so even if every gram in needle is the same as each other
    // we still only need one uint16 to store its frequency
    using NgramHash = uint16;
    // uint16[2^16] can almost fit into L2
    static constexpr size_t map_size = 1 << 16;

    static NgramHash getAsciiHash(const Gram* ch) { return crc_hash_32(ch, N, CRC_HASH_SEED1) & (0xffffu); }

    // for every gram of needle, we calculate its' hash value and store its' frequency in map, and return the number of gram in needle
    size_t static calculateMapWithNeedle(NgramHash* map, const Slice& needle) {
        size_t needle_length = needle.get_size();
        NgramHash cur_hash;
        size_t i;
        Slice cur_needle(needle.get_data(), needle_length);
        const char* ptr;
        std::string buf;
        if constexpr (case_insensitive) {
            tolower(needle, buf);
            cur_needle = Slice(buf.c_str(), buf.size());
        }
        ptr = cur_needle.get_data();

        for (i = 0; i + N <= needle_length; i++) {
            cur_hash = getAsciiHash(ptr + i);
            map[cur_hash]++;
        }

        return i;
    }

    // traverse haystack‘s every gram, find whether this gram is in needle or not using gram's hash
    // 16bit hash value may cause hash collision, but because we just calculate the similarity of two string
    // so don't need to be so accurate.
    template <bool need_recovery_map>
    size_t static calculateDistanceWithHaystack(NgramHash* map, const Slice& haystack,
                                                [[maybe_unused]] NgramHash* map_memo, size_t needle_gram_count) {
        size_t haystack_length = haystack.get_size();
        NgramHash cur_hash;
        size_t i;
        const char* ptr = haystack.get_data();
        std::memset(map_memo, 0, map_size * sizeof(NgramHash));

        // only for test
        std::unique_ptr<NgramHash[]> checkMap(new NgramHash[map_size]);
        NgramHash* checkMapPtr;
        if constexpr (need_recovery_map) {
            checkMapPtr = checkMap.get();
            std::memcpy(checkMapPtr, map, map_size * sizeof(NgramHash));
        }

        for (i = 0; i + N <= haystack_length; i++) {
            cur_hash = getAsciiHash(ptr + i);
            // if this gram is in needle
            if (map[cur_hash] > 0) {
                needle_gram_count--;
                map[cur_hash]--;
                if constexpr (need_recovery_map) {
                    map_memo[i] = cur_hash;
                }
            }
        }

        if constexpr (need_recovery_map) {
            bool isTest = true;
            for (int j = 0; j < i; j++) {
                if (map_memo[j]) {
                    map[map_memo[j]]++;
                }
            }
            if (std::memcmp(map, checkMapPtr, map_size) != 0) {
                while (isTest) {
                }
            }
        }

        return needle_gram_count;
    }

    ColumnPtr static haystack_vector_and_needle_const(const ColumnPtr& haystack_ptr, const Slice& needle) {
        std::unique_ptr<NgramHash[]> map(new NgramHash[map_size]);
        std::unique_ptr<NgramHash[]> memo(new NgramHash[map_size]);

        NullColumnPtr res_null = nullptr;
        BinaryColumn* haystack = nullptr;
        if (haystack_ptr->is_nullable()) {
            auto haystack_nullable = ColumnHelper::as_column<NullableColumn>(haystack_ptr);
            res_null = haystack_nullable->null_column();
            haystack = ColumnHelper::as_raw_column<BinaryColumn>(haystack_nullable->data_column());
        } else {
            haystack = ColumnHelper::as_raw_column<BinaryColumn>(haystack_ptr);
        }
        if constexpr (case_insensitive) {
            Columns temp;
            temp.emplace_back(haystack_ptr);
            StatusOr<ColumnPtr> lower = StringFunctions::lower(nullptr, temp);
            haystack = ColumnHelper::as_raw_column<BinaryColumn>(lower.value());
        }

        auto res = RunTimeColumnType<TYPE_FLOAT>::create(haystack->size());
        // needle_gram_count may be zero because needle is empty or N is too large for needle
        size_t needle_gram_count = calculateMapWithNeedle(map.get(), needle);
        for (int i = 0; i < haystack->size(); i++) {
            const Slice& cur_haystack_str = haystack->get_slice(i);
            // if haystack is too large, we can say they are not similar at all
            if (cur_haystack_str.get_size() >= max_string_size) {
                res->get_data()[i] = 0;
                continue;
            }
            size_t needle_not_overlap_with_haystack =
                    calculateDistanceWithHaystack<true>(map.get(), cur_haystack_str, memo.get(), needle_gram_count);
            float row_result = 1.0f - (needle_not_overlap_with_haystack)*1.0f / std::max(needle_gram_count, (size_t)1);

            LOG(INFO) << "needle_not_overlap_with_haystack: " << needle_not_overlap_with_haystack
                      << "\nneedle_gram_count:" << needle_gram_count;
            res->get_data()[i] = row_result;
        }

        if (haystack_ptr->is_nullable()) {
            return NullableColumn::create(res, res_null);
        } else {
            return res;
        }
    }

    ColumnPtr static haystack_const_and_needle_const(const ColumnPtr& haystack_ptr, const Slice& needle) {
        std::unique_ptr<NgramHash[]> hash(new NgramHash[map_size]);
        memset(hash.get(), 0, sizeof(NgramHash) * map_size);

        const Slice& haystack = ColumnHelper::get_const_value<TYPE_VARCHAR>(haystack_ptr);
        // if haystack is too large, we can say they are not similar at all
        if (haystack.get_size() >= max_string_size) {
            return ColumnHelper::create_const_column<TYPE_FLOAT>(0, haystack_ptr->size());
        }

        Slice cur_haystack(haystack.get_data(), haystack.get_size());

        std::string buf;
        if constexpr (case_insensitive) {
            tolower(haystack, buf);
            cur_haystack = Slice(buf.c_str(), buf.size());
        }
        // needle_gram_count may be zero because needle is empty or N is too large for needle
        size_t needle_gram_count = calculateMapWithNeedle(hash.get(), needle);
        size_t needle_not_overlap_with_haystack =
                calculateDistanceWithHaystack<false>(hash.get(), cur_haystack, nullptr, needle_gram_count);
        float result = 1.0f - (needle_not_overlap_with_haystack)*1.0f / std::max(needle_gram_count, (size_t)1);
        LOG(INFO) << "needle_not_overlap_with_haystack: " << needle_not_overlap_with_haystack
                  << "\nneedle_gram_count:" << needle_gram_count;
        DCHECK(needle_not_overlap_with_haystack <= needle_gram_count);
        return ColumnHelper::create_const_column<TYPE_FLOAT>(result, haystack_ptr->size());
    }

private:
    void inline static tolower(const Slice& str, std::string& buf) {
        buf = str.to_string();
        std::transform(buf.begin(), buf.end(), buf.begin(), [](unsigned char c) { return std::tolower(c); });
    }
};

template <size_t N, bool case_insensitive, bool use_utf_8, class Gram>
StatusOr<ColumnPtr> static ngram_search_impl(const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    const ColumnPtr& haystack_ptr = columns[0];

    const ColumnPtr& needle_ptr = columns[1];

    if (!needle_ptr->is_constant()) {
        return Status::NotSupported("ngram function's second parameter must be const");
    }

    const Slice& needle = ColumnHelper::get_const_value<TYPE_VARCHAR>(needle_ptr);
    if (needle.get_size() >= max_string_size) {
        return Status::NotSupported("ngram function's second parameter is larger than 2^15");
    }

    // needle is too small so we can not get even single Ngram, so they are not similar at all
    if (needle.get_size() < N) {
        return ColumnHelper::create_const_column<TYPE_FLOAT>(0, haystack_ptr->size());
    }

    if (haystack_ptr->is_constant()) {
        return NgramFunctionImpl<N, case_insensitive, use_utf_8, Gram>::haystack_const_and_needle_const(haystack_ptr,
                                                                                                        needle);
    } else {
        return NgramFunctionImpl<N, case_insensitive, use_utf_8, Gram>::haystack_vector_and_needle_const(haystack_ptr,
                                                                                                         needle);
    }
}

StatusOr<ColumnPtr> StringFunctions::ngram_search(FunctionContext* context, const Columns& columns) {
    return ngram_search_impl<4, false, false, char>(columns);
}

StatusOr<ColumnPtr> StringFunctions::ngram_search_case_insensitive(FunctionContext* context, const Columns& columns) {
    return ngram_search_impl<4, true, false, char>(columns);
}

} // namespace starrocks