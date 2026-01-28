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

#include "storage/variant_tuple.h"

namespace starrocks {

class TabletRange {
public:
    TabletRange() = default;

    TabletRange(const VariantTuple& lower_bound, const VariantTuple& upper_bound, bool lower_bound_included,
                bool upper_bound_included)
            : _lower_bound(lower_bound),
              _upper_bound(upper_bound),
              _lower_bound_included(lower_bound_included),
              _upper_bound_included(upper_bound_included) {}

    const VariantTuple& lower_bound() const { return _lower_bound; }

    const VariantTuple& upper_bound() const { return _upper_bound; }

    bool lower_bound_included() const { return _lower_bound_included; }

    bool upper_bound_included() const { return _upper_bound_included; }

    bool lower_bound_excluded() const { return !lower_bound_included(); }

    bool upper_bound_excluded() const { return !upper_bound_included(); }

    bool is_minimum() const { return _lower_bound.empty(); }

    bool is_maximum() const { return _upper_bound.empty(); }

    bool is_all() const { return is_minimum() && is_maximum(); }

    bool less_than(const VariantTuple& key) const;

    bool greater_than(const VariantTuple& key) const;

    bool contains(const VariantTuple& key) const { return !(less_than(key) || greater_than(key)); }

    void to_proto(TabletRangePB* tablet_range_pb) const;

    Status from_proto(const TabletRangePB& tablet_range_pb);

private:
    VariantTuple _lower_bound;
    VariantTuple _upper_bound;
    bool _lower_bound_included = false;
    bool _upper_bound_included = false;
};

inline bool TabletRange::less_than(const VariantTuple& key) const {
    if (is_maximum()) {
        return false;
    }

    int result = _upper_bound.compare(key);
    return result < 0 || (result == 0 && upper_bound_excluded());
}

inline bool TabletRange::greater_than(const VariantTuple& key) const {
    if (is_minimum()) {
        return false;
    }

    int result = _lower_bound.compare(key);
    return result > 0 || (result == 0 && lower_bound_excluded());
}

inline void TabletRange::to_proto(TabletRangePB* tablet_range_pb) const {
    _lower_bound.to_proto(tablet_range_pb->mutable_lower_bound());
    _upper_bound.to_proto(tablet_range_pb->mutable_upper_bound());
    tablet_range_pb->set_lower_bound_included(_lower_bound_included);
    tablet_range_pb->set_upper_bound_included(_upper_bound_included);
}

inline Status TabletRange::from_proto(const TabletRangePB& tablet_range_pb) {
    if (tablet_range_pb.has_lower_bound()) {
        RETURN_IF_ERROR(_lower_bound.from_proto(tablet_range_pb.lower_bound()));
    } else {
        _lower_bound.clear();
    }
    if (tablet_range_pb.has_upper_bound()) {
        RETURN_IF_ERROR(_upper_bound.from_proto(tablet_range_pb.upper_bound()));
    } else {
        _upper_bound.clear();
    }
    _lower_bound_included = tablet_range_pb.lower_bound_included();
    _upper_bound_included = tablet_range_pb.upper_bound_included();
    return Status::OK();
}

} // namespace starrocks
