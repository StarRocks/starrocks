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

#include "storage/olap_common.h"
#include "storage/types.h"
#include "storage/zone_map_detail.h"
#include "types/logical_type.h"
#include "util/runtime_profile.h"

namespace starrocks {

StatusOr<RowIdSparseRange> BlockDataSample::sample(OlapReaderStatistics* stats) {
    RETURN_IF(_probability_percent == 0 || _probability_percent == 100,
              Status::InvalidArgument("percent should be in (0, 100)"));
    std::mt19937 mt(_random_seed);
    std::bernoulli_distribution dist(_probability_percent / 100.0);

    size_t sampled_blocks = 0;
    size_t total_blocks = _total_rows / _rows_per_block;
    RowIdSparseRange sampled_ranges;
    for (size_t i = 0; i < _total_rows; i += _rows_per_block) {
        if (dist(mt)) {
            sampled_blocks++;
            sampled_ranges.add(RowIdRange(i, std::min(i + _rows_per_block, _total_rows)));
        }
    }

    if (sampled_blocks == 0) {
        std::uniform_int_distribution<size_t> uniform(0, total_blocks);
        size_t block = uniform(mt);
        sampled_blocks++;
        sampled_ranges.add(RowIdRange(block, std::min(block + _rows_per_block, _total_rows)));
    }

    stats->sample_size += sampled_blocks;
    stats->sample_population_size += total_blocks;

    return sampled_ranges;
}

static int compare_datum(const TypeInfoPtr& type_info, const Datum& lhs, const Datum& rhs) {
    if (lhs.is_null() && rhs.is_null()) {
        return 0;
    }
    if (lhs.is_null() != rhs.is_null()) {
        if (lhs.is_null())
            return 1;
        else
            return -1;
    }
    return type_info->cmp(lhs, rhs);
}

static int compare_zonemap(const TypeInfoPtr& type_info, const ZoneMapDetail& a, const ZoneMapDetail& b) {
    int cmp = compare_datum(type_info, a.min_value(), b.min_value());
    if (cmp != 0) {
        return cmp;
    }
    return compare_datum(type_info, a.max_value(), b.max_value());
}

void SortableZoneMap::sort() {
    if (zonemap.empty()) {
        return;
    }
    auto type_info = get_type_info(type);

    // only sort the page_indices but not zonemap
    page_indices.resize(zonemap.size());
    std::iota(page_indices.begin(), page_indices.end(), 0);
    std::sort(page_indices.begin(), page_indices.end(),
              [&](size_t lhs, size_t rhs) { return compare_zonemap(type_info, zonemap[lhs], zonemap[rhs]) < 0; });
}

double SortableZoneMap::width(const ZoneMapDetail& zone) {
    return width(zone.min_value(), zone.max_value());
}

bool SortableZoneMap::is_support_data_type(LogicalType type) {
    // TODO: support decimal type
    switch (type) {
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_UNSIGNED_TINYINT:
    case TYPE_UNSIGNED_SMALLINT:
    case TYPE_UNSIGNED_INT:
    case TYPE_UNSIGNED_BIGINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_DATE:
    case TYPE_DATETIME:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128:
        return true;
    default:
        return false;
    }
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
    case TYPE_DATE:
        return rhs.get_date().julian() - lhs.get_date().julian();
    case TYPE_DATETIME:
        return rhs.get_timestamp().timestamp() - lhs.get_timestamp().timestamp();
    case TYPE_DECIMAL32:
        return rhs.get_int32() - lhs.get_int32();
    case TYPE_DECIMAL64:
        return rhs.get_int64() - lhs.get_int64();
    case TYPE_DECIMAL128:
        return rhs.get_int128() - lhs.get_int128();
    default:
        throw std::runtime_error(fmt::format("unsupported SortableZoneMap type: {}", type));
    }
}

std::string SortableZoneMap::zonemap_string() const {
    std::ostringstream oss;
    oss << "zonemap: ";
    auto type_info = get_type_info(type);
    for (const auto& index : page_indices) {
        auto& zone = zonemap[index];
        oss << fmt::format("[{},{}]", type_info->to_string(&zone.min_value()), type_info->to_string(&zone.max_value()))
            << ",";
    }
    return oss.str();
}

std::string SortableZoneMap::histogram_string() const {
    std::ostringstream oss;
    oss << "histogram: ";
    auto type_info = get_type_info(type);
    for (auto& bucket : histogram) {
        oss << fmt::format("[{},{}]", type_info->to_string(&bucket.first.min_value()),
                           type_info->to_string(&bucket.first.max_value()));
        oss << fmt::format(": ({})\n", fmt::join(bucket.second, ","));
    }
    return oss.str();
}

double SortableZoneMap::overlap(const ZoneMapDetail& lhs, const ZoneMapDetail& rhs) {
    auto type_info = get_type_info(type);
    Datum upper = type_info->cmp(lhs.max_value(), rhs.max_value()) <= 0 ? lhs.max_value() : rhs.max_value();
    Datum lower = type_info->cmp(lhs.min_value(), rhs.min_value()) >= 0 ? lhs.min_value() : rhs.min_value();
    if (type_info->cmp(lower, upper) >= 0) {
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

// Build a histogram to improve the quality of data sampling
// It's more like a equi-width histogram to make the data sampling more uniform, but it's not a strict one.
void SortableZoneMap::build_histogram(size_t buckets) {
    DCHECK_GT(buckets, 0);

    // consider the bucket width but put at most `bucket_max_depth` elements in a bucket
    size_t total_width = width(zonemap.front().min_value(), zonemap.back().max_value());
    size_t bucket_width = total_width / buckets;
    size_t bucket_max_depth = std::max<size_t>(zonemap.size() / buckets, zonemap.size() * 0.25);

    size_t bucket_count = 0;
    Datum min_value, max_value;
    bool has_null = false;
    std::vector<size_t> current_pages;
    for (size_t i = 0; i < page_indices.size(); i++) {
        size_t idx = ith_zone(i);
        auto& zone = zonemap[idx];
        bucket_count++;
        max_value = zone.max_value();
        has_null = has_null || zone.has_null();
        if (min_value.is_null()) {
            min_value = zone.min_value();
        }
        current_pages.push_back(idx);

        double current_width = width(min_value, zone.min_value());
        if (bucket_count >= bucket_max_depth || i == zonemap.size() - 1 || current_width > bucket_width) {
            auto bucket = std::make_pair(ZoneMapDetail(min_value, max_value), std::move(current_pages));
            bucket.first.set_num_rows(bucket_count);
            bucket.first.set_has_null(has_null);
            histogram.emplace_back(std::move(bucket));

            // reset
            min_value.set_null();
            bucket_count = 0;
            has_null = false;
            current_pages.clear();
        }
    }
}

StatusOr<RowIdSparseRange> PageDataSample::sample(OlapReaderStatistics* stats) {
    RETURN_IF(_probability_percent == 0 || _probability_percent == 100,
              Status::InvalidArgument("percent should be in (0, 100)"));
    if (_zonemap) {
        _prepare_histogram(stats);
        if (_has_histogram()) {
            return _histogram_sample(stats);
        }
    }

    return _bernoulli_sample(stats);
}

StatusOr<RowIdSparseRange> PageDataSample::_histogram_sample(OlapReaderStatistics* stats) {
    auto& hist = _zonemap->histogram;
    std::mt19937 mt(_random_seed);
    std::vector<size_t> sample_pages;
    for (auto& bucket : hist) {
        auto& pages = bucket.second;

        std::uniform_int_distribution<> dist(0, pages.size() - 1);
        int rnd = dist(mt);
        sample_pages.push_back(pages[rnd]);
    }

    std::ranges::sort(sample_pages);

    RowIdSparseRange sampled_ranges;
    for (size_t page_index : sample_pages) {
        auto [first_ordinal, last_ordinal] = _page_indexer(page_index);
        sampled_ranges.add(RowIdRange(first_ordinal, last_ordinal));
    }

    stats->sample_size += sample_pages.size();
    stats->sample_population_size += _num_pages;

    return sampled_ranges;
}

bool PageDataSample::_has_histogram() const {
    return !_zonemap->histogram.empty();
}

// Build a histogram based on zonemap to make the data sampling more even
// Without histogram, page sampling may fall into local optimal but not global uniform
void PageDataSample::_prepare_histogram(OlapReaderStatistics* stats) {
    size_t expected_pages = _probability_percent * _num_pages / 100;
    if (expected_pages <= 1) {
        return;
    }
    SCOPED_RAW_TIMER(&stats->sample_build_histogram_time_ns);
    _zonemap->sort();
    if (!_zonemap->is_diverse()) {
        return;
    }
    stats->sample_build_histogram_count++;
    _zonemap->build_histogram(expected_pages);
}

StatusOr<RowIdSparseRange> PageDataSample::_bernoulli_sample(OlapReaderStatistics* stats) {
    size_t sampled_pages = 0;
    std::mt19937 mt(_random_seed);
    std::bernoulli_distribution dist(_probability_percent / 100.0);
    RowIdSparseRange sampled_ranges;
    for (size_t i = 0; i < _num_pages; i++) {
        if (dist(mt)) {
            // FIXME(murphy) use rowid_t rather than ordinal_t
            auto [first_ordinal, last_ordinal] = _page_indexer(i);
            sampled_ranges.add(RowIdRange(first_ordinal, last_ordinal));
            sampled_pages++;
        }
    }

    if (sampled_pages == 0) {
        // provide at least one page
        std::uniform_int_distribution<size_t> uniform(0, _num_pages);
        auto [first_ordinal, last_ordinal] = _page_indexer(uniform(mt));
        sampled_ranges.add(RowIdRange(first_ordinal, last_ordinal));
        sampled_pages++;
    }

    stats->sample_size += sampled_pages;
    stats->sample_population_size += _num_pages;

    return sampled_ranges;
}

} // namespace starrocks