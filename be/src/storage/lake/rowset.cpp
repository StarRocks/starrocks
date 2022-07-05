// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/rowset.h"

#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/delete_predicates.h"
#include "storage/lake/tablet.h"
#include "storage/projection_iterator.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/union_iterator.h"

namespace starrocks::lake {

class SegmentIteratorWrapper : public vectorized::ChunkIterator {
public:
    SegmentIteratorWrapper(vectorized::ChunkIteratorPtr iter)
            : ChunkIterator(iter->schema(), iter->chunk_size()), _iter(std::move(iter)) {}

    void close() override {
        _iter->close();
        _iter.reset();
    }

    Status init_encoded_schema(vectorized::ColumnIdToGlobalDictMap& dict_maps) override {
        RETURN_IF_ERROR(vectorized::ChunkIterator::init_encoded_schema(dict_maps));
        return _iter->init_encoded_schema(dict_maps);
    }

    Status init_output_schema(const std::unordered_set<uint32_t>& unused_output_column_ids) override {
        ChunkIterator::init_output_schema(unused_output_column_ids);
        return _iter->init_output_schema(unused_output_column_ids);
    }

protected:
    Status do_get_next(vectorized::Chunk* chunk) override { return _iter->get_next(chunk); }
    Status do_get_next(vectorized::Chunk* chunk, vector<uint32_t>* rowid) override {
        return _iter->get_next(chunk, rowid);
    }

private:
    vectorized::ChunkIteratorPtr _iter;
};

Rowset::Rowset(Tablet* tablet, RowsetMetadataPtr rowset_metadata)
        : _tablet(tablet), _rowset_metadata(std::move(rowset_metadata)) {}

Rowset::~Rowset() {
    _segments.clear();
}

Status Rowset::init() {
    ASSIGN_OR_RETURN(_tablet_schema, _tablet->get_schema());
    return load_segments();
}

// TODO: support
//  1. delete predicates
//  2. primary key table
//  3. rowid range and short key range
StatusOr<std::vector<ChunkIteratorPtr>> Rowset::get_segment_iterators(const vectorized::Schema& schema,
                                                                      const vectorized::RowsetReadOptions& options) {
    vectorized::SegmentReadOptions seg_options;
    ASSIGN_OR_RETURN(seg_options.fs, FileSystem::CreateSharedFromString(_tablet->group_assemble()));
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

    auto segment_schema = schema;
    // Append the columns with delete condition to segment schema.
    std::set<ColumnId> delete_columns;
    seg_options.delete_predicates.get_column_ids(&delete_columns);
    for (ColumnId cid : delete_columns) {
        const TabletColumn& col = options.tablet_schema->column(cid);
        if (segment_schema.get_field_by_name(std::string(col.name())) == nullptr) {
            auto f = vectorized::ChunkHelper::convert_field_to_format_v2(cid, col);
            segment_schema.append(std::make_shared<vectorized::Field>(std::move(f)));
        }
    }

    std::vector<vectorized::ChunkIteratorPtr> tmp_seg_iters;
    tmp_seg_iters.reserve(num_segments());
    if (options.stats) {
        options.stats->segments_read_count += num_segments();
    }
    for (auto& seg_ptr : _segments) {
        if (seg_ptr->num_rows() == 0) {
            continue;
        }

        if (options.rowid_range_option != nullptr && !options.rowid_range_option->match_segment(seg_ptr.get())) {
            continue;
        }

        auto res = seg_ptr->new_iterator(segment_schema, seg_options);
        if (res.status().is_end_of_file()) {
            continue;
        }
        if (!res.ok()) {
            return res.status();
        }
        if (segment_schema.num_fields() > schema.num_fields()) {
            tmp_seg_iters.emplace_back(vectorized::new_projection_iterator(schema, std::move(res).value()));
        } else {
            tmp_seg_iters.emplace_back(std::move(res).value());
        }
    }

    std::vector<ChunkIteratorPtr> segment_iterators;
    if (tmp_seg_iters.empty()) {
        // nothing to do
    } else if (is_overlapped()) {
        for (auto& iter : tmp_seg_iters) {
            auto wrapper = std::make_shared<SegmentIteratorWrapper>(std::move(iter));
            segment_iterators.emplace_back(std::move(wrapper));
        }
    } else {
        auto iter = vectorized::new_union_iterator(std::move(tmp_seg_iters));
        auto wrapper = std::make_shared<SegmentIteratorWrapper>(std::move(iter));
        segment_iterators.emplace_back(std::move(wrapper));
    }
    return segment_iterators;
}

// TODO: load from segment cache
Status Rowset::load_segments() {
    _segments.clear();

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_tablet->group_assemble()));
    size_t footer_size_hint = 16 * 1024;
    uint32_t seg_id = 0;
    for (const auto& seg_name : _rowset_metadata->segments()) {
        auto seg_path = _tablet->segment_path_assemble(seg_name);
        auto res = Segment::open(ExecEnv::GetInstance()->tablet_meta_mem_tracker(), fs, seg_path, seg_id++,
                                 _tablet_schema.get(), &footer_size_hint);
        if (!res.ok()) {
            LOG(WARNING) << "Fail to open " << seg_path << ": " << res.status();
            _segments.clear();
            return res.status();
        }
        _segments.push_back(std::move(res).value());
    }
    return Status::OK();
}

} // namespace starrocks::lake
