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

#include "storage/lake/rowset.h"

#include "storage/chunk_helper.h"
#include "storage/delete_predicates.h"
#include "storage/lake/tablet.h"
#include "storage/lake/update_manager.h"
#include "storage/projection_iterator.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/union_iterator.h"

namespace starrocks::lake {

Rowset::Rowset(Tablet tablet, RowsetMetadataPtr rowset_metadata, int index)
        : _tablet(std::move(tablet)), _rowset_metadata(std::move(rowset_metadata)), _index(index) {}

Rowset::Rowset(Tablet tablet, RowsetMetadataPtr rowset_metadata)
        : _tablet(std::move(tablet)), _rowset_metadata(std::move(rowset_metadata)) {}

Rowset::~Rowset() = default;

// TODO: support
//  1. rowid range and short key range
StatusOr<std::vector<ChunkIteratorPtr>> Rowset::read(const Schema& schema, const RowsetReadOptions& options) {
    SegmentReadOptions seg_options;
    ASSIGN_OR_RETURN(seg_options.fs, FileSystem::CreateSharedFromString(_tablet.root_location()));
    seg_options.stats = options.stats;
    seg_options.ranges = options.ranges;
    seg_options.predicates = options.predicates;
    seg_options.predicates_for_zone_map = options.predicates_for_zone_map;
    seg_options.use_page_cache = options.use_page_cache;
    seg_options.profile = options.profile;
    seg_options.reader_type = options.reader_type;
    seg_options.chunk_size = options.chunk_size;
    seg_options.global_dictmaps = options.global_dictmaps;
    seg_options.unused_output_column_ids = options.unused_output_column_ids;
    seg_options.runtime_range_pruner = options.runtime_range_pruner;
    seg_options.tablet_schema = options.tablet_schema;
    seg_options.fill_data_cache = options.fill_data_cache;
    if (options.is_primary_keys) {
        seg_options.is_primary_keys = true;
        seg_options.delvec_loader = std::make_shared<LakeDelvecLoader>(_tablet.update_mgr(), nullptr);
        seg_options.version = options.version;
        seg_options.tablet_id = _tablet.id();
        seg_options.rowset_id = _rowset_metadata->id();
    }
    if (options.delete_predicates != nullptr) {
        seg_options.delete_predicates = options.delete_predicates->get_predicates(_index);
    }

    std::unique_ptr<Schema> segment_schema_guard;
    auto* segment_schema = const_cast<Schema*>(&schema);
    // Append the columns with delete condition to segment schema.
    std::set<ColumnId> delete_columns;
    seg_options.delete_predicates.get_column_ids(&delete_columns);
    for (ColumnId cid : delete_columns) {
        const TabletColumn& col = options.tablet_schema->column(cid);
        if (segment_schema->get_field_by_name(std::string(col.name())) != nullptr) {
            continue;
        }
        // copy on write
        if (segment_schema == &schema) {
            segment_schema = new Schema(schema);
            segment_schema_guard.reset(segment_schema);
        }
        auto f = ChunkHelper::convert_field(cid, col);
        segment_schema->append(std::make_shared<Field>(std::move(f)));
    }

    std::vector<ChunkIteratorPtr> segment_iterators;
    segment_iterators.reserve(num_segments());
    if (options.stats) {
        options.stats->segments_read_count += num_segments();
    }

    std::vector<SegmentPtr> segments;
    RETURN_IF_ERROR(load_segments(&segments, options.fill_data_cache));
    for (auto& seg_ptr : segments) {
        if (seg_ptr->num_rows() == 0) {
            continue;
        }

        auto res = seg_ptr->new_iterator(*segment_schema, seg_options);
        if (res.status().is_end_of_file()) {
            continue;
        }
        if (!res.ok()) {
            return res.status();
        }
        if (segment_schema != &schema) {
            segment_iterators.emplace_back(new_projection_iterator(schema, std::move(res).value()));
        } else {
            segment_iterators.emplace_back(std::move(res).value());
        }
    }
    if (segment_iterators.size() > 1 && !is_overlapped()) {
        // union non-overlapped segment iterators
        auto iter = new_union_iterator(std::move(segment_iterators));
        return std::vector<ChunkIteratorPtr>{iter};
    } else {
        return segment_iterators;
    }
}

StatusOr<size_t> Rowset::get_read_iterator_num() {
    std::vector<SegmentPtr> segments;
    RETURN_IF_ERROR(load_segments(&segments, false));

    size_t segment_num = 0;
    for (auto& seg_ptr : segments) {
        if (seg_ptr->num_rows() == 0) {
            continue;
        }
        ++segment_num;
    }

    if (segment_num > 1 && !is_overlapped()) {
        return 1;
    } else {
        return segment_num;
    }
}

StatusOr<std::vector<ChunkIteratorPtr>> Rowset::get_each_segment_iterator(const Schema& schema,
                                                                          OlapReaderStatistics* stats) {
    std::vector<SegmentPtr> segments;
    RETURN_IF_ERROR(load_segments(&segments, false));
    std::vector<ChunkIteratorPtr> seg_iterators;
    seg_iterators.reserve(segments.size());
    SegmentReadOptions seg_options;
    ASSIGN_OR_RETURN(seg_options.fs, FileSystem::CreateSharedFromString(_tablet.root_location()));
    seg_options.stats = stats;
    for (auto& seg_ptr : segments) {
        auto res = seg_ptr->new_iterator(schema, seg_options);
        if (res.status().is_end_of_file()) {
            continue;
        }
        if (!res.ok()) {
            return res.status();
        }
        seg_iterators.push_back(std::move(res).value());
    }
    return seg_iterators;
}

StatusOr<std::vector<ChunkIteratorPtr>> Rowset::get_each_segment_iterator_with_delvec(const Schema& schema,
                                                                                      int64_t version,
                                                                                      const MetaFileBuilder* builder,
                                                                                      OlapReaderStatistics* stats) {
    std::vector<SegmentPtr> segments;
    RETURN_IF_ERROR(load_segments(&segments, false));
    std::vector<ChunkIteratorPtr> seg_iterators;
    seg_iterators.reserve(segments.size());
    SegmentReadOptions seg_options;
    ASSIGN_OR_RETURN(seg_options.fs, FileSystem::CreateSharedFromString(_tablet.root_location()));
    seg_options.stats = stats;
    seg_options.is_primary_keys = true;
    seg_options.delvec_loader = std::make_shared<LakeDelvecLoader>(_tablet.update_mgr(), builder);
    seg_options.version = version;
    seg_options.tablet_id = _tablet.id();
    seg_options.rowset_id = _rowset_metadata->id();
    for (auto& seg_ptr : segments) {
        auto res = seg_ptr->new_iterator(schema, seg_options);
        if (res.status().is_end_of_file()) {
            continue;
        }
        if (!res.ok()) {
            return res.status();
        }
        seg_iterators.push_back(std::move(res).value());
    }
    return seg_iterators;
}

StatusOr<std::vector<SegmentPtr>> Rowset::segments(bool fill_cache) {
    return segments(fill_cache, fill_cache);
}

StatusOr<std::vector<SegmentPtr>> Rowset::segments(bool fill_data_cache, bool fill_metadata_cache) {
    std::vector<SegmentPtr> segments;
    RETURN_IF_ERROR(load_segments(&segments, fill_data_cache, fill_metadata_cache));
    return segments;
}

Status Rowset::load_segments(std::vector<SegmentPtr>* segments, bool fill_cache) {
    return load_segments(segments, fill_cache, fill_cache);
}

Status Rowset::load_segments(std::vector<SegmentPtr>* segments, bool fill_data_cache, bool fill_metadata_cache) {
    size_t footer_size_hint = 16 * 1024;
    uint32_t seg_id = 0;
    bool ignore_lost_segment = config::experimental_lake_ignore_lost_segment;
    segments->reserve(_rowset_metadata->segments().size());
    for (const auto& seg_name : _rowset_metadata->segments()) {
        auto segment_or =
                _tablet.load_segment(seg_name, seg_id++, &footer_size_hint, fill_data_cache, fill_metadata_cache);
        if (segment_or.ok()) {
            segments->emplace_back(std::move(segment_or.value()));
        } else if (segment_or.status().is_not_found() && ignore_lost_segment) {
            LOG(WARNING) << "Ignored lost segment " << seg_name;
            continue;
        } else {
            return segment_or.status();
        }
    }
    return Status::OK();
}

} // namespace starrocks::lake
