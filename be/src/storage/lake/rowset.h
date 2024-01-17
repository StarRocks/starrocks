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
#include "storage/lake/types_fwd.h"
#include "storage/olap_common.h"
#include "storage/options.h"

namespace starrocks::lake {
class MetaFileBuilder;

static const int kInvalidRowsetIndex = -1;

class Rowset {
public:
    explicit Rowset(Tablet* tablet, RowsetMetadataPtr rowset_metadata, int index);

    explicit Rowset(Tablet* tablet, RowsetMetadataPtr rowset_metadata);

    ~Rowset();

    [[nodiscard]] StatusOr<std::vector<ChunkIteratorPtr>> read(const Schema& schema, const RowsetReadOptions& options);

    [[nodiscard]] StatusOr<size_t> get_read_iterator_num();

    // only used for updatable tablets' rowset, for update state load, it wouldn't load delvec
    // simply get iterators to iterate all rows without complex options like predicates
    // |schema| read schema
    // |stats| used for iterator read stats
    // return iterator list, an iterator for each segment,
    // if the segment is empty, it wouln't add this iterator to iterator list
    [[nodiscard]] StatusOr<std::vector<ChunkIteratorPtr>> get_each_segment_iterator(const Schema& schema,
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

    [[nodiscard]] bool is_overlapped() const { return metadata().overlapped(); }

    [[nodiscard]] int64_t num_segments() const { return metadata().segments_size(); }

    [[nodiscard]] int64_t num_rows() const { return metadata().num_rows(); }

    [[nodiscard]] int64_t num_dels() const { return metadata().num_dels(); }

    [[nodiscard]] int64_t data_size() const { return metadata().data_size(); }

    [[nodiscard]] uint32_t id() const { return metadata().id(); }

    [[nodiscard]] int index() const { return _index; }

    [[nodiscard]] const RowsetMetadata& metadata() const { return *_rowset_metadata; }

    [[nodiscard]] StatusOr<std::vector<SegmentPtr>> segments(bool fill_cache);

    [[nodiscard]] StatusOr<std::vector<SegmentPtr>> segments(const LakeIOOptions& lake_io_opts,
                                                             bool fill_metadata_cache);

    // `fill_cache` controls `fill_data_cache` and `fill_meta_cache`
    [[nodiscard]] Status load_segments(std::vector<SegmentPtr>* segments, bool fill_cache, int64_t buffer_size = -1);

    [[nodiscard]] Status load_segments(std::vector<SegmentPtr>* segments, const LakeIOOptions& lake_io_opts,
                                       bool fill_metadata_cache);

    int64_t tablet_id() const;

private:
    // _tablet is owned by TabletReader
    Tablet* _tablet;
    RowsetMetadataPtr _rowset_metadata;
    int _index{kInvalidRowsetIndex};
};

} // namespace starrocks::lake
