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

#include "common/statusor.h"
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
    // Strictly inside the open interval of this range.
    bool strictly_contains(const VariantTuple& key) const;

    // Return the intersection between this range and rhs.
    StatusOr<TabletRange> intersect(const TabletRange& rhs) const;

    // Empty range such as [x, x) or any range where lower bound is larger than upper bound.
    bool is_empty() const;

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

inline bool TabletRange::strictly_contains(const VariantTuple& key) const {
    if (!is_minimum() && key.compare(_lower_bound) <= 0) {
        return false;
    }
    if (!is_maximum() && key.compare(_upper_bound) >= 0) {
        return false;
    }
    return true;
}

inline void TabletRange::to_proto(TabletRangePB* tablet_range_pb) const {
    tablet_range_pb->clear_lower_bound();
    tablet_range_pb->clear_upper_bound();
    tablet_range_pb->clear_lower_bound_included();
    tablet_range_pb->clear_upper_bound_included();
    if (!is_minimum()) {
        _lower_bound.to_proto(tablet_range_pb->mutable_lower_bound());
        tablet_range_pb->set_lower_bound_included(_lower_bound_included);
    }
    if (!is_maximum()) {
        _upper_bound.to_proto(tablet_range_pb->mutable_upper_bound());
        tablet_range_pb->set_upper_bound_included(_upper_bound_included);
    }
}

inline bool TabletRange::is_empty() const {
    if (is_minimum() || is_maximum()) {
        return false;
    }
    const int cmp = _lower_bound.compare(_upper_bound);
    return cmp > 0 || (cmp == 0 && (!_lower_bound_included || !_upper_bound_included));
}

inline StatusOr<TabletRange> TabletRange::intersect(const TabletRange& rhs) const {
    TabletRange result;

    if (is_minimum()) {
        result._lower_bound = rhs._lower_bound;
        result._lower_bound_included = rhs._lower_bound_included;
    } else if (rhs.is_minimum()) {
        result._lower_bound = _lower_bound;
        result._lower_bound_included = _lower_bound_included;
    } else {
        const int cmp = _lower_bound.compare(rhs._lower_bound);
        if (cmp > 0) {
            result._lower_bound = _lower_bound;
            result._lower_bound_included = _lower_bound_included;
        } else if (cmp < 0) {
            result._lower_bound = rhs._lower_bound;
            result._lower_bound_included = rhs._lower_bound_included;
        } else {
            result._lower_bound = _lower_bound;
            result._lower_bound_included = _lower_bound_included && rhs._lower_bound_included;
        }
    }

    if (is_maximum()) {
        result._upper_bound = rhs._upper_bound;
        result._upper_bound_included = rhs._upper_bound_included;
    } else if (rhs.is_maximum()) {
        result._upper_bound = _upper_bound;
        result._upper_bound_included = _upper_bound_included;
    } else {
        const int cmp = _upper_bound.compare(rhs._upper_bound);
        if (cmp > 0) {
            result._upper_bound = rhs._upper_bound;
            result._upper_bound_included = rhs._upper_bound_included;
        } else if (cmp < 0) {
            result._upper_bound = _upper_bound;
            result._upper_bound_included = _upper_bound_included;
        } else {
            result._upper_bound = _upper_bound;
            result._upper_bound_included = _upper_bound_included && rhs._upper_bound_included;
        }
    }

    if (!result.is_minimum() && !result.is_maximum()) {
        const int cmp = result._lower_bound.compare(result._upper_bound);
        if (cmp > 0 || (cmp == 0 && (!result._lower_bound_included || !result._upper_bound_included))) {
            // Canonicalize empty range to [x, x).
            result._upper_bound = result._lower_bound;
            result._lower_bound_included = true;
            result._upper_bound_included = false;
        }
    }

    return result;
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
