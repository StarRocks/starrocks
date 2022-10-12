// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/delete_predicates.h"

#include <algorithm>

#include "column/chunk.h"

namespace starrocks::vectorized {

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

} // namespace starrocks::vectorized
