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

#include <vector>

#include "storage/conjunctive_predicates.h"
#include "storage/disjunctive_predicates.h"

namespace starrocks {

// DeletePredicates is a set of delete predicates of different versions.
// version is rowset index if tablet is lake tablet.
class DeletePredicates {
public:
    // Add a new version of delete predicates.
    void add(int32_t version, ConjunctivePredicates preds);

    // Return all the predicates with version greater than or equal to |min_version|.
    DisjunctivePredicates get_predicates(int32_t min_version) const;

private:
    struct VersionAndPredicate {
        VersionAndPredicate(int32_t v, ConjunctivePredicates preds) : _version(v), _preds(std::move(preds)) {}

        bool operator<(const VersionAndPredicate& rhs) { return _version < rhs._version; }

        int32_t _version; // filter version
        ConjunctivePredicates _preds;
    };

    // sorted by version in ascending order.
    std::vector<VersionAndPredicate> _version_predicates;
};

} // namespace starrocks
