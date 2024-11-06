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

#include "storage/rowset/data_sample.h"

#include <fmt/format.h>

#include <random>
#include <stdexcept>

#include "storage/types.h"
#include "storage/zone_map_detail.h"

namespace starrocks {

StatusOr<RowIdSparseRange> BlockDataSample::sample() {
    std::mt19937 mt(_random_seed);
    std::bernoulli_distribution dist(_probability_percent / 100.0);

    // Directly use member variables for rows_per_block and total_rows
    size_t sampled_blocks = 0;
    size_t total_blocks = _total_rows / _rows_per_block;
    RowIdSparseRange sampled_ranges;
    for (size_t i = 0; i < _total_rows; i += _rows_per_block) {
        if (dist(mt)) {
            sampled_blocks++;
            sampled_ranges.add(RowIdRange(i, std::min(i + _rows_per_block, _total_rows)));
        }
    }

    VLOG(2) << fmt::format("sample {}/{} blocks in segment", sampled_blocks, total_blocks);

    return sampled_ranges;
}

static int compare_datum(const TypeInfoPtr& type_info, const Datum& lhs, const Datum& rhs) {
    if (lhs.is_null() && rhs.is_null()) {
        return 0;
    }
    if (lhs.is_null() != rhs.is_null()) {
        return lhs.is_null() < rhs.is_null();
    }
    return type_info->cmp(lhs, rhs);
}

void SortableZoneMap::sort() {
    if (zonemap.empty()) {
        return;
    }
    auto type_info = get_type_info(type);
    auto compare_zonemap = [type_info](const ZoneMapDetail& a, const ZoneMapDetail& b) {
        int cmp = compare_datum(type_info, a.min_value(), b.min_value());
        if (cmp != 0) {
            return cmp;
        }
        return compare_datum(type_info, a.max_value(), b.max_value());
    };

    // first sort the page_indices,  then sort the zonemap, this way can keep the relative ordering of them
    page_indices.resize(zonemap.size());
    std::iota(page_indices.begin(), page_indices.end(), 0);
    std::sort(page_indices.begin(), page_indices.end(),
              [&](size_t lhs, size_t rhs) { return compare_zonemap(zonemap[lhs], zonemap[rhs]); });
    // std::vector<ZoneMapDetail> sorted;
    // sorted.reserve(zonemap.size());
    // for (size_t page_index : page_indices) {
    //     sorted.push_back(zonemap[page_index]);
    // }
    // std::swap(sorted, zonemap);
}

double SortableZoneMap::width(const ZoneMapDetail& zone) {
    return width(zone.min_value(), zone.max_value());
}

double SortableZoneMap::width(const Datum& lhs, const Datum& rhs) {
    if (lhs.is_null() || rhs.is_null()) {
        return 0;
    }
    switch (type) {
    case TYPE_TINYINT:
        return rhs.get_int8() - lhs.get_int8();
    case TYPE_SMALLINT:
        return rhs.get_int16() - lhs.get_int16();
    case TYPE_INT:
        return rhs.get_int32() - lhs.get_int32();
    case TYPE_BIGINT:
        return rhs.get_int64() - lhs.get_int64();
    case TYPE_LARGEINT:
        return rhs.get_int128() - lhs.get_int128();
    case TYPE_UNSIGNED_TINYINT:
        return rhs.get_uint8() - lhs.get_uint8();
    case TYPE_UNSIGNED_SMALLINT:
        return rhs.get_uint16() - lhs.get_uint16();
    case TYPE_UNSIGNED_INT:
        return rhs.get_uint32() - lhs.get_uint32();
    case TYPE_UNSIGNED_BIGINT:
        return rhs.get_uint64() - lhs.get_uint64();
    case TYPE_FLOAT:
        return rhs.get_float() - lhs.get_float();
    case TYPE_DOUBLE:
        return rhs.get_double() - lhs.get_double();
    case TYPE_DECIMAL:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128:
        return rhs.get_decimal() - lhs.get_decimal();
    case TYPE_DATE:
        return rhs.get_date().julian() - lhs.get_date().julian();
    case TYPE_DATETIME:
        return rhs.get_timestamp().timestamp() - lhs.get_timestamp().timestamp();
    default:
        throw std::runtime_error(fmt::format("unsupported SortableZoneMap type: {}", type));
    }
}

double SortableZoneMap::overlap(const ZoneMapDetail& lhs, const ZoneMapDetail& rhs) {
    auto type_info = get_type_info(type);
    Datum upper = type_info->cmp(lhs.max_value(), rhs.max_value()) <= 0 ? lhs.max_value() : rhs.max_value();
    Datum lower = type_info->cmp(lhs.min_value(), rhs.min_value()) >= 0 ? lhs.min_value() : rhs.min_value();
    if (type_info->cmp(lower, upper) <= 0) {
        return 0;
    }
    return width(lower, upper);
}

// Check the diversity of zonemap
bool SortableZoneMap::is_diverse() {
    double total_area = 0.0;
    double overlap_area = 0.0;
    for (size_t i = 1; i < page_indices.size(); i++) {
        auto& prev = zonemap[ith_zone(i)];
        auto& curr = zonemap[ith_zone(i - 1)];
        total_area += width(curr);
        overlap_area += overlap(prev, curr);
    }
    if (total_area == 0) {
        return false;
    }
    return (overlap_area / total_area) < diversity_threshold;
}

void SortableZoneMap::build_histogram(size_t buckets) {
    DCHECK_GT(buckets, 0);
    size_t total_rows = 0;
    for (auto& x : zonemap) {
        total_rows += x.num_rows();
    }
    size_t bucket_depth = std::max<size_t>(1, total_rows / buckets);

    size_t current_row = 0;
    Datum min_value;
    Datum max_value;
    bool has_null = false;
    std::vector<size_t> current_pages;
    for (size_t i = 0; i < page_indices.size(); i++) {
        size_t idx = ith_zone(i);
        auto& zone = zonemap[idx];
        current_row += zone.num_rows();
        max_value = zone.max_value();
        has_null = has_null || zone.has_null();
        if (min_value.is_null()) {
            min_value = zone.min_value();
        }
        current_pages.push_back(idx);
        if (current_row >= bucket_depth || i == zonemap.size() - 1) {
            auto bucket = std::make_pair(ZoneMapDetail(min_value, max_value), std::move(current_pages));
            bucket.first.set_num_rows(current_row);
            bucket.first.set_has_null(has_null);
            histogram.emplace_back(std::move(bucket));

            // reset
            min_value.set_null();
            current_row = 0;
            has_null = false;
            current_pages.clear();
        }
    }
}

StatusOr<RowIdSparseRange> PageDataSample::sample() {
    if (_zonemap) {
        _prepare_histogram();
        if (_has_histogram()) {
            return _histogram_sample();
        }
    }

    return _bernoulli_sample();
}

StatusOr<RowIdSparseRange> PageDataSample::_histogram_sample() {
    auto& hist = _zonemap->histogram;
    std::mt19937 mt(_random_seed);
    std::vector<size_t> sample_pages(hist.size());
    for (auto& bucket : hist) {
        auto& pages = bucket.second;

        std::uniform_int_distribution<> dist(0, pages.size() - 1);
        int rnd = dist(mt);
        sample_pages.push_back(rnd);
    }

    std::ranges::sort(sample_pages);

    RowIdSparseRange sampled_ranges;
    for (size_t page_index : sample_pages) {
        auto [first_ordinal, last_ordinal] = _page_indexer(page_index);
        sampled_ranges.add(RowIdRange(first_ordinal, last_ordinal));
    }

    VLOG(2) << fmt::format("histogram sample pages {}/{}", sample_pages.size(), _num_pages);

    return sampled_ranges;
}

bool PageDataSample::_has_histogram() const {
    return !_zonemap->histogram.empty();
}

// Build a histogram based on zonemap to make the data sampling more even
// Without histogram, page sampling may fall into local optimal but not global uniform
void PageDataSample::_prepare_histogram() {
    size_t expected_pages = _probability_percent / 100 * _num_pages;
    if (expected_pages <= 1) {
        return;
    }
    _zonemap->sort();
    if (!_zonemap->is_diverse()) {
        return;
    }
    _zonemap->build_histogram(expected_pages);
}

StatusOr<RowIdSparseRange> PageDataSample::_bernoulli_sample() {
    size_t num_data_pages = _num_pages;
    size_t sampled_pages = 0;
    std::mt19937 mt(_random_seed);
    std::bernoulli_distribution dist(_probability_percent / 100.0);
    RowIdSparseRange sampled_ranges;
    for (size_t i = 0; i < num_data_pages; i++) {
        if (dist(mt)) {
            // FIXME(murphy) use rowid_t rather than ordinal_t
            auto [first_ordinal, last_ordinal] = _page_indexer(i);
            sampled_ranges.add(RowIdRange(first_ordinal, last_ordinal));
            sampled_pages++;
        }
    }

    VLOG(2) << fmt::format("bernoulli sample pages {}/{}", sampled_pages, num_data_pages);

    return sampled_ranges;
}

} // namespace starrocks