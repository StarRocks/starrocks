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

#include "storage/delete_predicates.h"

#include <algorithm>

#include "column/chunk.h"

namespace starrocks {

void DeletePredicates::add(int32_t version, ConjunctivePredicates preds) {
    // fast path.
    if (_version_predicates.empty() || version > _version_predicates.back()._version) {
        _version_predicates.emplace_back(version, std::move(preds));
        return;
    }
    VersionAndPredicate vp(version, std::move(preds));
    auto iter = std::lower_bound(_version_predicates.begin(), _version_predicates.end(), vp);
    if (iter->_version == version) {
        iter->_preds.add(vp._preds);
    } else {
        _version_predicates.emplace(iter, std::move(vp));
    }
}

DisjunctivePredicates DeletePredicates::get_predicates(int32_t min_version) const {
    DisjunctivePredicates ret;
    size_t i = 0;
    for (; i < _version_predicates.size() && _version_predicates[i]._version < min_version; i++) {
    }
    for (; i < _version_predicates.size(); i++) {
        ret.add(_version_predicates[i]._preds);
    }
    return ret;
}

} // namespace starrocks
