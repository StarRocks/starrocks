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

#include <utility>
#include <vector>

#include "base/hash/hash_util.hpp"
#include "boost/container_hash/hash.hpp"
#include "gen_cpp/Types_types.h"
#include "storage/decimal12.h"
#include "storage/uint24.h"
#include "util/int96.h"

namespace std {

template <>
struct hash<starrocks::uint24_t> {
    size_t operator()(const starrocks::uint24_t& val) const { return starrocks::HashUtil::hash(&val, sizeof(val), 0); }
};

template <>
struct hash<starrocks::int96_t> {
    size_t operator()(const starrocks::int96_t& val) const { return starrocks::HashUtil::hash(&val, sizeof(val), 0); }
};

template <>
struct hash<starrocks::decimal12_t> {
    size_t operator()(const starrocks::decimal12_t& val) const {
        return starrocks::HashUtil::hash(&val, sizeof(val), 0);
    }
};

template <>
struct hash<starrocks::TUniqueId> {
    std::size_t operator()(const starrocks::TUniqueId& id) const {
        std::size_t seed = 0;
        seed = starrocks::HashUtil::hash(&id.lo, sizeof(id.lo), seed);
        seed = starrocks::HashUtil::hash(&id.hi, sizeof(id.hi), seed);
        return seed;
    }
};

template <>
struct hash<starrocks::TNetworkAddress> {
    size_t operator()(const starrocks::TNetworkAddress& address) const {
        std::size_t seed = 0;
        seed = starrocks::HashUtil::hash(address.hostname.data(), address.hostname.size(), seed);
        seed = starrocks::HashUtil::hash(&address.port, 4, seed);
        return seed;
    }
};

#if !__clang__ && __GNUC__ < 6
// Cause this is builtin function
template <>
struct hash<__int128> {
    std::size_t operator()(const __int128& val) const { return starrocks::HashUtil::hash(&val, sizeof(val), 0); }
};
#endif

template <>
struct hash<std::pair<starrocks::TUniqueId, int64_t>> {
    size_t operator()(const std::pair<starrocks::TUniqueId, int64_t>& pair) const {
        size_t seed = 0;
        seed = starrocks::HashUtil::hash(&pair.first.lo, sizeof(pair.first.lo), seed);
        seed = starrocks::HashUtil::hash(&pair.first.hi, sizeof(pair.first.hi), seed);
        seed = starrocks::HashUtil::hash(&pair.second, sizeof(pair.second), seed);
        return seed;
    }
};

template <typename T>
struct hash<std::vector<T>> {
    size_t operator()(const std::vector<T>& ts) const noexcept {
        std::size_t hash_value = 0; // seed
        for (auto& t : ts) {
            boost::hash_combine<T>(hash_value, t);
        }
        return hash_value;
    }
};

} // namespace std
