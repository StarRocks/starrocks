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

namespace starrocks::lake {
class Rowset {
public:
    explicit Rowset(Tablet* tablet, RowsetMetadataPtr rowset_metadata, int index);

    ~Rowset();

    [[nodiscard]] StatusOr<ChunkIteratorPtr> read(const VectorizedSchema& schema, const RowsetReadOptions& options);

    [[nodiscard]] StatusOr<std::vector<ChunkIteratorPtr>> get_each_segment_iterator(const VectorizedSchema& schema,
                                                                                    OlapReaderStatistics* stats);

    [[nodiscard]] StatusOr<std::vector<ChunkIteratorPtr>> get_each_segment_iterator_with_delvec(
            const VectorizedSchema& schema, const int64_t version, OlapReaderStatistics* stats);

    [[nodiscard]] bool is_overlapped() const { return metadata().overlapped(); }

    [[nodiscard]] int64_t num_segments() const { return metadata().segments_size(); }

    [[nodiscard]] int64_t num_rows() const { return metadata().num_rows(); }

    [[nodiscard]] int64_t data_size() const { return metadata().data_size(); }

    [[nodiscard]] uint32_t id() const { return metadata().id(); }

    [[nodiscard]] int index() const { return _index; }

    [[nodiscard]] const RowsetMetadata& metadata() const { return *_rowset_metadata; }

private:
    [[nodiscard]] Status load_segments(std::vector<SegmentPtr>* segments, bool fill_cache);

    // _tablet is owned by TabletReader
    Tablet* _tablet;
    RowsetMetadataPtr _rowset_metadata;
    int _index;
};

} // namespace starrocks::lake
