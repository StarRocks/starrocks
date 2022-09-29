// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/lake/rowset.h"

#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/delete_predicates.h"
#include "storage/empty_iterator.h"
#include "storage/lake/tablet.h"
#include "storage/merge_iterator.h"
#include "storage/projection_iterator.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/union_iterator.h"

namespace starrocks::lake {

Rowset::Rowset(Tablet* tablet, RowsetMetadataPtr rowset_metadata, int index)
        : _tablet(tablet), _rowset_metadata(std::move(rowset_metadata)), _index(index) {}

Rowset::~Rowset() = default;

// TODO: support
//  1. primary key table
//  2. rowid range and short key range
StatusOr<ChunkIteratorPtr> Rowset::read(const vectorized::Schema& schema, const RowsetReadOptions& options) {
    vectorized::SegmentReadOptions seg_options;
    ASSIGN_OR_RETURN(seg_options.fs, FileSystem::CreateSharedFromString(_tablet->root_location()));
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
    if (options.delete_predicates != nullptr) {
        seg_options.delete_predicates = options.delete_predicates->get_predicates(_index);
    }

    std::unique_ptr<vectorized::Schema> segment_schema_guard;
    auto* segment_schema = const_cast<vectorized::Schema*>(&schema);
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
            segment_schema = new vectorized::Schema(schema);
            segment_schema_guard.reset(segment_schema);
        }
        auto f = ChunkHelper::convert_field_to_format_v2(cid, col);
        segment_schema->append(std::make_shared<vectorized::Field>(std::move(f)));
    }

    std::vector<vectorized::ChunkIteratorPtr> segment_iterators;
    segment_iterators.reserve(num_segments());
    if (options.stats) {
        options.stats->segments_read_count += num_segments();
    }

    std::vector<SegmentPtr> segments;
    RETURN_IF_ERROR(load_segments(&segments, /*fill_cache=*/seg_options.reader_type == READER_QUERY));
    for (auto& seg_ptr : segments) {
        if (seg_ptr->num_rows() == 0) {
            continue;
        }

        if (options.rowid_range_option != nullptr && !options.rowid_range_option->match_segment(seg_ptr.get())) {
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
            segment_iterators.emplace_back(vectorized::new_projection_iterator(schema, std::move(res).value()));
        } else {
            segment_iterators.emplace_back(std::move(res).value());
        }
    }
    if (segment_iterators.empty()) {
        return vectorized::new_empty_iterator(schema, options.chunk_size);
    } else if (segment_iterators.size() == 1) {
        return segment_iterators[0];
    } else if (options.sorted) {
        return vectorized::new_heap_merge_iterator(segment_iterators);
    } else {
        return vectorized::new_union_iterator(segment_iterators);
    }
}

Status Rowset::load_segments(std::vector<SegmentPtr>* segments, bool fill_cache) {
    size_t footer_size_hint = 16 * 1024;
    uint32_t seg_id = 0;
    segments->reserve(_rowset_metadata->segments().size());
    for (const auto& seg_name : _rowset_metadata->segments()) {
        ASSIGN_OR_RETURN(auto segment, _tablet->load_segment(seg_name, seg_id++, &footer_size_hint, fill_cache));
        segments->emplace_back(std::move(segment));
    }
    return Status::OK();
}

} // namespace starrocks::lake
