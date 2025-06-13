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

#include "storage/predicate_tree/predicate_tree.h"

namespace starrocks {

template <CompoundNodeType Type>
template <typename Vistor>
void PredicateCompoundNode<Type>::partition_copy(Vistor&& cond, PredicateAndNode* true_pred_tree,
                                                 PredicateAndNode* false_pred_tree) const {
    for (const auto& child_var : children()) {
        if (cond(child_var)) {
            child_var.visit([&](const auto& child) { true_pred_tree->add_child(child); });
        } else {
            child_var.visit([&](const auto& child) { false_pred_tree->add_child(child); });
        }
    }
}

template <CompoundNodeType Type>
template <typename Vistor>
void PredicateCompoundNode<Type>::partition_move(Vistor&& cond, PredicateAndNode* true_pred_tree,
                                                 PredicateAndNode* false_pred_tree) {
    for (auto child_var : children()) {
        if (cond(child_var)) {
            child_var.visit([&](auto& child) { true_pred_tree->add_child(std::move(child)); });
        } else {
            child_var.visit([&](auto& child) { false_pred_tree->add_child(std::move(child)); });
        }
    }
}

} // namespace starrocks
