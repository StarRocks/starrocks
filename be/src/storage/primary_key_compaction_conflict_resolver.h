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

#include <cstdint>
#include <string>

#include "common/status.h"
#include "fs/fs.h"
#include "storage/chunk_iterator.h"
#include "storage/del_vector.h"
#include "storage/primary_key_encoding_types.h"

namespace starrocks {

class DelvecLoader;
class Schema;
class PrimaryIndex;
class Segment;

// Sentinel returned by output_segment_num_rows() for an output segment whose row count is unavailable
// (e.g. a cross-version rowset whose metadata has no num_rows). Lets execute_without_update_index() fail
// clearly and fall back to loading the segment for that count instead of silently under-advancing the
// rows-mapper and failing later with a confusing row-count mismatch.
inline constexpr uint32_t kUnknownSegmentNumRows = UINT32_MAX;

struct CompactConflictResolveParams {
    int64_t tablet_id = 0;
    uint32_t rowset_id = 0;
    int64_t base_version = 0;
    int64_t new_version = 0;
    DelvecLoader* delvec_loader = 0;
    PrimaryIndex* index = 0;
};

class PrimaryKeyCompactionConflictResolver {
public:
    virtual ~PrimaryKeyCompactionConflictResolver() = default;
    // Returns FileInfo instead of string to support both local and remote storage
    // WHY: Changed from string to FileInfo to include optional file size.
    // BENEFIT: For remote storage (S3/HDFS), having size upfront avoids expensive
    // get_size() metadata calls (~10-50ms each), significantly improving performance
    // during parallel pk index execution where hundreds of mapper files are accessed.
    virtual StatusOr<FileInfo> filename() const = 0;
    virtual StatusOr<PrimaryKeyEncodingType> primary_key_encoding_type() const = 0;
    virtual Schema generate_pkey_schema() = 0;
    virtual Status breakpoint_check() { return Status::OK(); }
    virtual Status segment_iterator(
            const std::function<Status(const CompactConflictResolveParams&, const std::vector<ChunkIteratorPtr>&,
                                       const std::function<void(uint32_t, const DelVectorPtr&, uint32_t)>&)>&
                    handler) = 0;
    // This function won't read data from each segment files. Only need to get segment's row count.
    // The handler's `segments` argument may be EMPTY: a backend that gets row counts from metadata
    // (see output_segment_num_rows()) leaves it empty to avoid opening segment footers. Callers must
    // derive the segment count from output_segment_num_rows() when `segments` is empty.
    virtual Status segment_iterator(
            const std::function<
                    Status(const CompactConflictResolveParams&, const std::vector<std::shared_ptr<Segment>>&,
                           const std::function<void(uint32_t, const DelVectorPtr&, uint32_t)>&)>& handler) = 0;

    Status execute();

    Status execute_without_update_index();

protected:
    // Per-output-segment row counts, in segment_metas order, so execute_without_update_index() can
    // advance the rows-mapper without opening segment footers. The default empty vector means the
    // backend has no metadata source and always materialises `segments`; only the lake resolver
    // overrides it.
    virtual std::vector<uint32_t> output_segment_num_rows() const { return {}; }
};

} // namespace starrocks