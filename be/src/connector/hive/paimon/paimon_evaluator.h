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

#include <memory>
#include <vector>

namespace paimon {
class Predicate;
}

namespace starrocks {

class Expr;
class SlotDescriptor;

// Converts the supported subset of StarRocks conjuncts into Paimon predicates. The slot order
// must match the field order passed to ReadContextBuilder::SetReadFieldNames because Paimon
// predicates address fields by their position in that read schema.
class PaimonEvaluator {
public:
    explicit PaimonEvaluator(const std::vector<SlotDescriptor*>& read_slots);

    // Returns nullptr when no safe predicate can be produced. The caller must retain the original
    // StarRocks conjuncts as residual predicates regardless of whether pushdown succeeds.
    std::shared_ptr<paimon::Predicate> evaluate(const std::vector<Expr*>* conjuncts) const;

private:
    std::vector<SlotDescriptor*> _read_slots;
};

} // namespace starrocks
