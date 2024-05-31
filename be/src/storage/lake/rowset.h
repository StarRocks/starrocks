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

#include "common/ownership.h"
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

class Rowset : public BaseRowset {
public:
    static std::vector<RowsetPtr> get_rowsets(TabletManager* tablet_mgr, const TabletMetadataPtr& tablet_metadata);

    // Does NOT take the ownership of |tablet_mgr|
    explicit Rowset(TabletManager* tablet_mgr, int64_t tablet_id, const RowsetMetadataPB* metadata, int32_t index,
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
    explicit Rowset(TabletManager* tablet_mgr, TabletMetadataPtr tablet_metadata, int rowset_index);

    ~Rowset() override;

    DISALLOW_COPY_AND_MOVE(Rowset);

    StatusOr<std::vector<ChunkIteratorPtr>> read(const Schema& schema, const RowsetReadOptions& options);

    StatusOr<size_t> get_read_iterator_num();

    // only used for updatable tablets' rowset, for update state load, it wouldn't load delvec
    // simply get iterators to iterate all rows without complex options like predicates
    // |schema| read schema
    // |stats| used for iterator read stats
    // return iterator list, an iterator for each segment,
    // if the segment is empty, it wouln't add this iterator to iterator list
    StatusOr<std::vector<ChunkIteratorPtr>> get_each_segment_iterator(const Schema& schema,
                                                                      OlapReaderStatistics* stats);

    // used for primary index load, it will get segment iterator by specifice version and it's delvec,
    // without complex options like predicates
    // |schema| read schema
    // |version| read version, use for get delvec
    // |stats| used for iterator read stats
    // return iterator list, an iterator for each segment,
    // if the segment is empty, it wouln't add this iterator to iterator list
    StatusOr<std::vector<ChunkIteratorPtr>> get_each_segment_iterator_with_delvec(const Schema& schema, int64_t version,
                                                                                  const MetaFileBuilder* builder,
                                                                                  OlapReaderStatistics* stats);

    [[nodiscard]] bool is_overlapped() const override { return metadata().overlapped(); }

    [[nodiscard]] int64_t num_segments() const { return metadata().segments_size(); }

    [[nodiscard]] int64_t num_rows() const override { return metadata().num_rows(); }

    [[nodiscard]] int64_t num_dels() const { return metadata().num_dels(); }

    [[nodiscard]] int64_t data_size() const { return metadata().data_size(); }

    [[nodiscard]] uint32_t id() const { return metadata().id(); }

    [[nodiscard]] RowsetId rowset_id() const override;

    [[nodiscard]] int32_t index() const { return _index; }

    [[nodiscard]] const RowsetMetadataPB& metadata() const { return *_metadata; }

    [[nodiscard]] std::vector<SegmentSharedPtr> get_segments() override;

    StatusOr<std::vector<SegmentPtr>> segments(const LakeIOOptions& lake_io_opts, bool fill_metadata_cache);

    Status load_segments(const LakeIOOptions& lake_io_opts, bool fill_metadata_cache);

    int64_t tablet_id() const { return _tablet_id; }

    [[nodiscard]] int64_t version() const { return metadata().version(); }

    TabletSchemaPtr tablet_schema() const { return _tablet_schema; }

    void take_metadata_ownership() { _metadata_ownership = kTakesOwnership; }

    void set_first_segment_id(uint32_t first_segment_id) { _first_segment_id = first_segment_id; }

private:
    Ownership _metadata_ownership{kDontTakeOwnership};
    TabletManager* _tablet_mgr;
    int64_t _tablet_id;
    const RowsetMetadataPB* _metadata;
    int32_t _index;
    TabletSchemaPtr _tablet_schema;
    TabletMetadataPtr _tablet_metadata;
    std::vector<SegmentSharedPtr> _segments;
    uint32_t _first_segment_id{0};
};

inline std::vector<RowsetPtr> Rowset::get_rowsets(TabletManager* tablet_mgr, const TabletMetadataPtr& tablet_metadata) {
    std::vector<RowsetPtr> rowsets;
    rowsets.reserve(tablet_metadata->rowsets_size());
    for (int i = 0, size = tablet_metadata->rowsets_size(); i < size; ++i) {
        auto rowset = std::make_shared<Rowset>(tablet_mgr, tablet_metadata, i);
        rowsets.emplace_back(std::move(rowset));
    }
    return rowsets;
}

} // namespace starrocks::lake
