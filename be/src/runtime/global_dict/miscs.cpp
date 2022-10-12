// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "runtime/global_dict/miscs.h"

#include <vector>

#include "column/binary_column.h"
#include "column/nullable_column.h"
#include "util/slice.h"

namespace starrocks::vectorized {

std::pair<std::shared_ptr<NullableColumn>, std::vector<int32_t>> extract_column_with_codes(
        const GlobalDictMap& dict_map) {
    auto res = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    res->reserve(dict_map.size() + 1);

    std::vector<Slice> slices;
    std::vector<int> codes;

    slices.reserve(dict_map.size() + 1);
    codes.reserve(dict_map.size() + 1);

    slices.emplace_back(Slice());
    codes.emplace_back(0);

    for (auto& [slice, code] : dict_map) {
        slices.emplace_back(slice);
        codes.emplace_back(code);
    }
    res->append_strings(slices);
    res->set_null(0);
    return std::make_pair(std::move(res), std::move(codes));
}

} // namespace starrocks::vectorized
