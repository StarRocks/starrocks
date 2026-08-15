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

#include <variant>

namespace starrocks {

template <typename Derived>
class PredicateNodeFactory;

enum class CompoundNodeType : uint8_t { AND, OR };

class PredicateBaseNode;
class PredicateColumnNode;

template <CompoundNodeType Type>
class PredicateCompoundNode;
using PredicateAndNode = PredicateCompoundNode<CompoundNodeType::AND>;
using PredicateOrNode = PredicateCompoundNode<CompoundNodeType::OR>;

struct CompoundNodeContext;
using CompoundNodeContexts = std::vector<CompoundNodeContext>;

struct PredicateNodePtr;
struct ConstPredicateNodePtr;

template <CompoundNodeType Type, bool Constant>
class PredicateNodeIterator;
template <CompoundNodeType Type>
using MutablePredicateNodeIterator = PredicateNodeIterator<Type, false>;
template <CompoundNodeType Type>
using ConstPredicateNodeIterator = PredicateNodeIterator<Type, true>;

class PredicateTree;

struct PredicateTreeParams;

} // namespace starrocks
