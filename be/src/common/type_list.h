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

namespace starrocks {

// A typelist is a type that represents a list of types and can be manipulated by a
// template metapro-gram. It provides the operations typically associated with a list:
// iterating over the elements (types) in the list, adding elements, or removing elements.
// However, typelists differ from most run-time data structures, such as std::list,
// in that they don't allow mutation.
//
// reference: <<C++ Templates The Complete Guide (2nd edition)>>
template <typename... Elements>
class TypeList {};

/// IsEmpty
template <typename List>
class IsEmpty {
public:
    static constexpr bool value = false;
};

template <>
class IsEmpty<TypeList<>> {
public:
    static constexpr bool value = true;
};

/// Front
template <typename List>
class FrontT;

template <typename Head, typename... Tail>
class FrontT<TypeList<Head, Tail...>> {
public:
    using Type = Head;
};

template <typename List>
using Front = typename FrontT<List>::Type;

/// PopFront
template <typename List>
class PopFrontT;

template <typename Head, typename... Tail>
class PopFrontT<TypeList<Head, Tail...>> {
public:
    using Type = TypeList<Tail...>;
};

template <typename List>
using PopFront = typename PopFrontT<List>::Type;

/// PushFront
template <typename List, typename NewElement>
class PushFrontT;

template <typename... Elements, typename NewElement>
class PushFrontT<TypeList<Elements...>, NewElement> {
public:
    using Type = TypeList<NewElement, Elements...>;
};

template <typename List, typename NewElement>
using PushFront = typename PushFrontT<List, NewElement>::Type;

/// PushBack
template <typename List, typename NewElement>
class PushBackT;

template <typename... Elements, typename NewElement>
class PushBackT<TypeList<Elements...>, NewElement> {
public:
    using Type = TypeList<Elements..., NewElement>;
};

template <typename List, typename NewElement>
using PushBack = typename PushBackT<List, NewElement>::Type;

/// InList
template <typename Element, typename List>
class InList : public std::bool_constant<
                       std::disjunction<std::is_same<Front<List>, Element>, InList<Element, PopFront<List>>>::value> {};

template <typename Element>
class InList<Element, TypeList<>> : public std::false_type {};

/// ForEach
template <typename List, typename Func>
void ForEach(Func&& func) {
    if constexpr (!IsEmpty<List>::value) {
        func.template operator()<Front<List>>();
        ForEach<PopFront<List>>(std::forward<Func>(func));
    }
}

} // namespace starrocks
