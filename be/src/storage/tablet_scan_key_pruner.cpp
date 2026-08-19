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

#include "storage/tablet_scan_key_pruner.h"

#include "column/chunk_factory.h"
#include "column/column.h"
#include "column/datum_convert.h"
#include "storage/types.h"
#include "types/datum.h"

namespace starrocks {

bool TabletScanKeyPruner::is_routable_type(LogicalType type) {
    switch (type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_DATE:
    case TYPE_DATETIME:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        return true;
    default:
        return false;
    }
}

namespace {

// Turns the scan-key tuple position into the tablet schema column id, mirroring how
// TabletReader::_to_seek_tuple() indexes the tuple.
bool tuple_position_to_cid(const TabletSchema& tablet_schema, int32_t position, ColumnId* cid) {
    if (position < 0) {
        return false;
    }
    const auto& sort_key_idxes = tablet_schema.sort_key_idxes();
    if (sort_key_idxes.empty()) {
        if (static_cast<size_t>(position) >= tablet_schema.num_columns()) {
            return false;
        }
        *cid = static_cast<ColumnId>(position);
        return true;
    }
    if (static_cast<size_t>(position) >= sort_key_idxes.size()) {
        return false;
    }
    ColumnId candidate = sort_key_idxes[position];
    if (static_cast<size_t>(candidate) >= tablet_schema.num_columns()) {
        return false;
    }
    *cid = candidate;
    return true;
}

// True when both bounds pin this position to the same non-null, non-sentinel value, i.e. the scan key
// fixes the column and the bucket can be derived from it.
bool position_is_fixed_value(const OlapScanRange& range, int32_t position, std::string* value) {
    const auto& begin = range.begin_scan_range;
    const auto& end = range.end_scan_range;
    auto pos = static_cast<size_t>(position);
    if (pos >= begin.size() || pos >= end.size()) {
        return false;
    }
    // A NULL bound carries no hashable value here. The load path routes NULL through a nullable
    // column, which this pruner deliberately does not reproduce -- keep the range instead.
    if (begin.is_null(pos) || end.is_null(pos)) {
        return false;
    }
    const std::string& low = begin.get_value(pos);
    const std::string& high = end.get_value(pos);
    if (low != high) {
        return false;
    }
    if (low == NEGATIVE_INFINITY || low == POSITIVE_INFINITY) {
        return false;
    }
    // begin==end with an exclusive bound describes an empty range; leave such a range alone rather
    // than reasoning about it here.
    if (!range.begin_include || !range.end_include) {
        return false;
    }
    *value = low;
    return true;
}

// Reproduces OlapTablePartitionParam::_compute_hashes(): seed starts at zero and each distribution
// column accumulates into it via Column::crc32_hash(), in DDL declaration order.
//
// CHAR is hashed as VARCHAR because the load path computes the hash before padding
// (see the "must padding char column after find_tablets" note in olap_table_sink.cpp), so the bytes
// fed to crc32_hash() are the unpadded value in both paths.
bool compute_bucket_hash(const TabletSchema& tablet_schema, const std::vector<int32_t>& positions,
                         const OlapScanRange& range, uint32_t* hash) {
    uint32_t seed = 0;
    for (int32_t position : positions) {
        ColumnId cid = 0;
        if (!tuple_position_to_cid(tablet_schema, position, &cid)) {
            return false;
        }
        const TabletColumn& column = tablet_schema.column(cid);
        LogicalType type = column.type();
        if (!TabletScanKeyPruner::is_routable_type(type)) {
            return false;
        }

        std::string value;
        if (!position_is_fixed_value(range, position, &value)) {
            return false;
        }

        LogicalType hash_type = (type == TYPE_CHAR) ? TYPE_VARCHAR : type;
        TypeInfoPtr type_info = get_type_info(hash_type);
        if (type_info == nullptr) {
            return false;
        }
        Datum datum;
        // No allocator: the parsed value may borrow from `value`, which outlives the append below,
        // and append_datum() copies string bytes into the column.
        if (!datum_from_string(type_info.get(), &datum, value, nullptr).ok()) {
            return false;
        }
        MutableColumnPtr hash_column = ChunkFactory::column_from_field_type(hash_type, false);
        if (hash_column == nullptr) {
            return false;
        }
        hash_column->append_datum(datum);
        hash_column->crc32_hash(&seed, 0, 1);
    }
    *hash = seed;
    return true;
}

TabletScanKeyPruneResult keep_all(const std::vector<OlapScanRange*>& ranges, bool fallback) {
    TabletScanKeyPruneResult result;
    result.ranges = ranges;
    result.fallback = fallback;
    return result;
}

} // namespace

TabletScanKeyPruneResult TabletScanKeyPruner::prune_hash(const TabletHashBucketConstraint& constraint,
                                                         const TabletSchema& tablet_schema,
                                                         const std::vector<OlapScanRange*>& ranges) {
    if (constraint.hash_version != kSupportedHashVersion || constraint.bucket_num <= 0 ||
        constraint.bucket_id < 0 || constraint.bucket_id >= constraint.bucket_num ||
        constraint.distribution_key_positions.empty()) {
        return keep_all(ranges, /*fallback=*/true);
    }
    // Nothing to prune, and an empty input must never be reported as exact_empty: no scan keys means
    // no scan-key predicate, which is not the same as "no key belongs here".
    if (ranges.empty()) {
        return keep_all(ranges, /*fallback=*/false);
    }
    // Reject a topology whose positions cannot be resolved against this tablet's schema before
    // touching any range, so a schema mismatch degrades the whole tablet rather than half of it.
    for (int32_t position : constraint.distribution_key_positions) {
        ColumnId cid = 0;
        if (!tuple_position_to_cid(tablet_schema, position, &cid) ||
            !is_routable_type(tablet_schema.column(cid).type())) {
            return keep_all(ranges, /*fallback=*/true);
        }
    }

    TabletScanKeyPruneResult result;
    result.ranges.reserve(ranges.size());
    for (OlapScanRange* range : ranges) {
        uint32_t hash = 0;
        if (range == nullptr || !compute_bucket_hash(tablet_schema, constraint.distribution_key_positions,
                                                    *range, &hash)) {
            // Unroutable range: keep it. Not a tablet-level fallback -- other ranges may still route.
            result.ranges.emplace_back(range);
            continue;
        }
        if (static_cast<int32_t>(hash % static_cast<uint32_t>(constraint.bucket_num)) == constraint.bucket_id) {
            result.ranges.emplace_back(range);
        } else {
            ++result.pruned;
        }
    }
    result.exact_empty = result.ranges.empty();
    return result;
}

TabletScanKeyPruneResult TabletScanKeyPruner::prune_hash(const TabletHashBucketConstraint& constraint,
                                                         const TabletSchema& tablet_schema,
                                                         const std::vector<std::unique_ptr<OlapScanRange>>& ranges) {
    std::vector<OlapScanRange*> borrowed;
    borrowed.reserve(ranges.size());
    for (const auto& range : ranges) {
        borrowed.emplace_back(range.get());
    }
    return prune_hash(constraint, tablet_schema, borrowed);
}

} // namespace starrocks
