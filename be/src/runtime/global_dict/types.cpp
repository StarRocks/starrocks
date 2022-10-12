// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "runtime/global_dict/types.h"

namespace starrocks::vectorized {

ColumnIdToGlobalDictMap EMPTY_GLOBAL_DICTMAPS;

std::ostream& operator<<(std::ostream& stream, const RGlobalDictMap& map) {
    stream << "[";
    for (const auto& [k, v] : map) {
        stream << "(" << k << "," << v << "),";
    }
    stream << "]";
    return stream;
}

std::ostream& operator<<(std::ostream& stream, const GlobalDictMap& map) {
    stream << "[";
    for (const auto& [k, v] : map) {
        stream << "(" << k << "," << v << "),";
    }
    stream << "]";
    return stream;
}

} // namespace starrocks::vectorized
