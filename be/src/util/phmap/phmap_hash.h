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
//   https://github.com/greg7mdp/parallel-hashmap/blob/master/parallel_hashmap/phmap.h
//
// ---------------------------------------------------------------------------
// Copyright (c) 2019, Gregory Popovitch - greg7mdp@gmail.com
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
//
// Includes work from abseil-cpp (https://github.com/abseil/abseil-cpp)
// with modifications.
//
// Copyright 2018 The Abseil Authors.
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
// ---------------------------------------------------------------------------

#pragma once

#include "phmap_config.h"
#include "phmap_fwd_decl.h"

#if PHMAP_HAVE_STD_STRING_VIEW
#include <string_view>
#endif

namespace phmap {
namespace priv {
// --------------------------------------------------------------------------
//  hash_default
// --------------------------------------------------------------------------

#if PHMAP_HAVE_STD_STRING_VIEW

// support char16_t wchar_t ....
template <class CharT>
struct StringHashT {
    using is_transparent = void;

    size_t operator()(std::basic_string_view<CharT> v) const {
        std::string_view bv{reinterpret_cast<const char*>(v.data()), v.size() * sizeof(CharT)};
        return std::hash<std::string_view>()(bv);
    }
};

// Supports heterogeneous lookup for basic_string<T>-like elements.
template <class CharT>
struct StringHashEqT {
    using Hash = StringHashT<CharT>;

    struct Eq {
        using is_transparent = void;

        bool operator()(std::basic_string_view<CharT> lhs, std::basic_string_view<CharT> rhs) const {
            return lhs == rhs;
        }
    };
};

template <>
struct HashEq<std::string> : StringHashEqT<char> {};

template <>
struct HashEq<std::string_view> : StringHashEqT<char> {};

// char16_t
template <>
struct HashEq<std::u16string> : StringHashEqT<char16_t> {};

template <>
struct HashEq<std::u16string_view> : StringHashEqT<char16_t> {};

// wchar_t
template <>
struct HashEq<std::wstring> : StringHashEqT<wchar_t> {};

template <>
struct HashEq<std::wstring_view> : StringHashEqT<wchar_t> {};

#endif

// Supports heterogeneous lookup for pointers and smart pointers.
// -------------------------------------------------------------
template <class T>
struct HashEq<T*> {
    struct Hash {
        using is_transparent = void;
        template <class U>
        size_t operator()(const U& ptr) const {
            return phmap::Hash<const T*>{}(HashEq::ToPtr(ptr));
        }
    };

    struct Eq {
        using is_transparent = void;
        template <class A, class B>
        bool operator()(const A& a, const B& b) const {
            return HashEq::ToPtr(a) == HashEq::ToPtr(b);
        }
    };

private:
    static const T* ToPtr(const T* ptr) { return ptr; }

    template <class U, class D>
    static const T* ToPtr(const std::unique_ptr<U, D>& ptr) {
        return ptr.get();
    }

    template <class U>
    static const T* ToPtr(const std::shared_ptr<U>& ptr) {
        return ptr.get();
    }
};

template <class T, class D>
struct HashEq<std::unique_ptr<T, D>> : HashEq<T*> {};

template <class T>
struct HashEq<std::shared_ptr<T>> : HashEq<T*> {};

} // namespace priv
} // namespace phmap
