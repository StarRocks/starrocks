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

#include "column/hash_set.h"
#include "runtime/decimalv3.h"
#include "storage/type_traits.h"
#include "storage/types.h"
#include "util/string_parser.hpp"

namespace starrocks {

namespace in_pred_utils_detail {
template <typename T>
struct PHashSetTraits {
    using SetType = HashSet<T>;
};

template <>
struct PHashSetTraits<Slice> {
    using SetType = SliceNormalHashSet;
};

template <typename T>
using PHashSet = typename PHashSetTraits<T>::SetType;
} // namespace in_pred_utils_detail

template <typename T>
struct ItemHashSet : public in_pred_utils_detail::PHashSet<T> {
    bool contains(const T& v) const { return in_pred_utils_detail::PHashSet<T>::contains(v); }
};

template <typename T, size_t N>
struct ArraySet : public std::array<T, N> {
    static_assert(N <= 4 && N >= 1);

    bool contains(const T& v) const noexcept {
        const T* const ptr = this->data();
        if constexpr (N == 1) {
            return v == ptr[0];
        }
        if constexpr (N == 2) {
            return (v == ptr[0]) | (v == ptr[1]);
        }
        if constexpr (N == 3) {
            return (v == ptr[0]) | (v == ptr[1]) | (v == ptr[2]);
        }
        if constexpr (N == 4) {
            return (v == ptr[0]) | (v == ptr[1]) | (v == ptr[2]) | (v == ptr[3]);
        }
        CHECK(false) << "unreachable path";
        return false;
    }
};

namespace predicate_internal {

template <typename T>
class Converter {
public:
    void push_back(const T& v) { _elements.push_back(v); }

    template <typename Container,
              typename = typename std::enable_if_t<std::is_same_v<T, typename Container::value_type>>>
    operator Container() const {
        return convert_to_container<Container>()(_elements);
    }

private:
    template <typename Container>
    struct convert_to_container {
        Container operator()(const std::vector<T>& elems) {
            Container c;
            for (const T& v : elems) {
                c.push_back(v);
            }
            return c;
        }
    };

    template <typename U>
    struct convert_to_container<ItemHashSet<U>> {
        ItemHashSet<U> operator()(const std::vector<T>& elems) {
            ItemHashSet<U> c;
            for (const T& v : elems) {
                c.emplace(v);
            }
            return c;
        }
    };

    template <size_t N>
    struct convert_to_container<ArraySet<T, N>> {
        ArraySet<T, N> operator()(const std::vector<T>& elems) {
            ArraySet<T, N> c;
            for (size_t i = 0; i < N; i++) {
                c[i] = elems[i];
            }
            return c;
        }
    };

    std::vector<T> _elements;
};

template <LogicalType field_type>
inline Converter<typename CppTypeTraits<field_type>::CppType> strings_to_set(const std::vector<std::string>& strings) {
    using CppType = typename CppTypeTraits<field_type>::CppType;

    TypeInfoPtr type_info = get_type_info(field_type);
    Converter<CppType> result;
    for (const auto& s : strings) {
        CppType v;
        auto st = type_info->from_string(&v, s);
        DCHECK(st.ok());
        result.push_back(v);
    }
    return result;
}

template <LogicalType field_type>
inline Converter<typename CppTypeTraits<field_type>::CppType> strings_to_decimal_set(
        int scale, const std::vector<std::string>& strings) {
    using CppType = typename CppTypeTraits<field_type>::CppType;
    Converter<CppType> result;
    for (const auto& s : strings) {
        CppType v;
        auto st = DecimalV3Cast::from_string_with_overflow_allowed<CppType>(&v, scale, s.data(), s.size());
        CHECK_EQ(false, st);
        result.push_back(v);
    }
    return result;
}

template <LogicalType field_type>
inline ItemHashSet<typename CppTypeTraits<field_type>::CppType> strings_to_hashset(
        const std::vector<std::string>& strings) {
    using CppType = typename CppTypeTraits<field_type>::CppType;

    static TypeInfoPtr type_info = get_type_info(field_type);
    ItemHashSet<CppType> result;
    for (const auto& s : strings) {
        CppType v;
        auto st = type_info->from_string(&v, s);
        result.emplace(v);
    }
    return result;
}

} // namespace predicate_internal

} // namespace starrocks
