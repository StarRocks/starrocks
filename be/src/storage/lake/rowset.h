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
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/tablet.h"
#include "storage/lake/types_fwd.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/rowset/base_rowset.h"

namespace starrocks::lake {

class MetaFileBuilder;
class TabletManager;
class TabletWriter;

class Rowset : public BaseRowset {
public:
    static std::vector<RowsetPtr> get_rowsets(TabletManager* tablet_mgr, const TabletMetadataPtr& tablet_metadata);

    // Does NOT take the ownership of |tablet_mgr| and |metadata|.
    explicit Rowset(TabletManager* tablet_mgr, int64_t tablet_id, const RowsetMetadataPB* metadata, int index,
                    TabletSchemaPtr tablet_schema);

    // Create a Rowset based on the rowset metadata of index |rowset_index| saved in the tablet metadata
    // pointed by |tablet_metadata|.
    //
    // Note: the caller must ensure that the object pointed by |tablet_metadata| will not be updated during
    // the lifecycle of this Rowset, otherwise the behavior is undefined.
    //
    // Requires:
    //  - |tablet_mgr| and |tablet_metadata| is not nullptr
    //  - 0 <= |rowset_index| && |rowset_index| < tablet_metadata->rowsets_size()
    explicit Rowset(TabletManager* tablet_mgr, TabletMetadataPtr tablet_metadata, int rowset_index,
                    size_t compaction_segment_limit);

    virtual ~Rowset();

    DISALLOW_COPY_AND_MOVE(Rowset);

    [[nodiscard]] StatusOr<std::vector<ChunkIteratorPtr>> read(const Schema& schema, const RowsetReadOptions& options);

    [[nodiscard]] StatusOr<size_t> get_read_iterator_num();

    // only used for updatable tablets' rowset, for update state load, it wouldn't load delvec
    // simply get iterators to iterate all rows without complex options like predicates
    // |schema| read schema
    // |stats| used for iterator read stats
    // return iterator list, an iterator for each segment,
    // if the segment is empty, it wouln't add this iterator to iterator list
    [[nodiscard]] StatusOr<std::vector<ChunkIteratorPtr>> get_each_segment_iterator(const Schema& schema,
                                                                                    bool file_data_cache,
                                                                                    OlapReaderStatistics* stats);

    // used for primary index load, it will get segment iterator by specifice version and it's delvec,
    // without complex options like predicates
    // |schema| read schema
    // |version| read version, use for get delvec
    // |stats| used for iterator read stats
    // return iterator list, an iterator for each segment,
    // if the segment is empty, it wouln't add this iterator to iterator list
    [[nodiscard]] StatusOr<std::vector<ChunkIteratorPtr>> get_each_segment_iterator_with_delvec(
            const Schema& schema, int64_t version, const MetaFileBuilder* builder, OlapReaderStatistics* stats);

    [[nodiscard]] bool is_overlapped() const override { return metadata().overlapped(); }

    // if _compaction_segment_limit is set > 0, it means only partial segments will be used
    [[nodiscard]] int64_t num_segments() const {
        return _compaction_segment_limit > 0 ? _compaction_segment_limit : metadata().segments_size();
    }

    // only used in compaction
    [[nodiscard]] bool partial_segments_compaction() const { return _compaction_segment_limit > 0; }
    // only used in compaction
    [[nodiscard]] Status add_partial_compaction_segments_info(TxnLogPB_OpCompaction* op_compaction,
                                                              TabletWriter* writer, uint64_t& uncompacted_num_rows,
                                                              uint64_t& uncompacted_data_size);

    [[nodiscard]] int64_t num_rows() const override { return metadata().num_rows(); }

    [[nodiscard]] int64_t num_dels() const { return metadata().num_dels(); }

    [[nodiscard]] int64_t data_size() const { return metadata().data_size(); }

    [[nodiscard]] uint32_t id() const { return metadata().id(); }

    [[nodiscard]] RowsetId rowset_id() const override;

    [[nodiscard]] int index() const { return _index; }

    [[nodiscard]] const RowsetMetadataPB& metadata() const { return *_metadata; }

    [[nodiscard]] std::vector<SegmentSharedPtr> get_segments() override;

    [[nodiscard]] StatusOr<std::vector<SegmentPtr>> segments(bool fill_cache);

    [[nodiscard]] StatusOr<std::vector<SegmentPtr>> segments(const LakeIOOptions& lake_io_opts,
                                                             bool fill_metadata_cache);

    // `fill_cache` controls `fill_data_cache` and `fill_meta_cache`
    [[nodiscard]] Status load_segments(std::vector<SegmentPtr>* segments, bool fill_cache, int64_t buffer_size = -1);

    [[nodiscard]] Status load_segments(std::vector<SegmentPtr>* segments, const LakeIOOptions& lake_io_opts,
                                       bool fill_metadata_cache,
                                       std::pair<std::vector<SegmentPtr>, std::vector<SegmentPtr>>* not_used_segments);

    int64_t tablet_id() const { return _tablet_id; }

    [[nodiscard]] int64_t version() const { return metadata().version(); }

private:
    TabletManager* _tablet_mgr;
    int64_t _tablet_id;
    const RowsetMetadataPB* _metadata;
    int _index;
    TabletSchemaPtr _tablet_schema;
    TabletMetadataPtr _tablet_metadata;
    std::vector<SegmentSharedPtr> _segments;
    bool _parallel_load;
    // only takes effect when rowset is overlapped, tells how many segments will be used in compaction,
    // default is 0 means every segment will be used.
    // only used for compaction
    size_t _compaction_segment_limit;
};

inline std::vector<RowsetPtr> Rowset::get_rowsets(TabletManager* tablet_mgr, const TabletMetadataPtr& tablet_metadata) {
    std::vector<RowsetPtr> rowsets;
    rowsets.reserve(tablet_metadata->rowsets_size());
    for (int i = 0, size = tablet_metadata->rowsets_size(); i < size; ++i) {
        auto rowset = std::make_shared<Rowset>(tablet_mgr, tablet_metadata, i, 0 /* compaction_segment_limit */);
        rowsets.emplace_back(std::move(rowset));
    }
    return rowsets;
}

} // namespace starrocks::lake
