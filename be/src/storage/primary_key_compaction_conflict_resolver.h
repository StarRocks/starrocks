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
#include <set>
#include <string>

#include "common/status.h"
#include "fs/fs.h"
#include "storage/del_vector.h"
#include "storage_primitive/chunk_iterator.h"
#include "storage_primitive/primary_key_encoding_types.h"

namespace starrocks {

class DelvecLoader;
class Schema;
class PrimaryIndex;
class Segment;

// Sentinel returned by output_segment_num_rows() for a segment whose row count is unavailable (e.g. an
// old cross-version rowset whose metadata has no num_rows). Lets the resolver fail with a clear error
// if it ever needs that count to advance the rows-mapper past a lost segment, instead of silently
// under-advancing and failing later with a confusing row-count mismatch.
inline constexpr uint32_t kUnknownSegmentNumRows = UINT32_MAX;

struct CompactConflictResolveParams {
    int64_t tablet_id = 0;
    uint32_t rowset_id = 0;
    int64_t base_version = 0;
    int64_t new_version = 0;
    DelvecLoader* delvec_loader = nullptr;
    PrimaryIndex* index = nullptr;
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
    // (see output_segment_num_rows()) leaves it empty to avoid opening segment footers, and only
    // materialises it when it must detect physically-lost segments (null slots). Callers must derive
    // the segment count from output_segment_num_rows() when `segments` is empty.
    virtual Status segment_iterator(
            const std::function<
                    Status(const CompactConflictResolveParams&, const std::vector<std::shared_ptr<Segment>>&,
                           const std::function<void(uint32_t, const DelVectorPtr&, uint32_t)>&)>& handler) = 0;

    Status execute();

    Status execute_without_update_index();

    // Output segment positions that were skipped because their segment file was lost
    // (experimental_lake_ignore_lost_segment). Populated by execute_without_update_index(); the caller
    // must skip the SST ingest and delvec for these positions so the PK index does not reference a
    // lost rssid. Positions index the output rowset's segments/ssts (which are 1:1).
    const std::set<uint32_t>& lost_segment_positions() const { return _lost_segment_positions; }

protected:
    // Per-output-segment row counts, in segment_metas order. Used to advance the rows-mapper past a
    // lost segment (a nullptr placeholder produced when experimental_lake_ignore_lost_segment drops a
    // missing segment) so the mapper stays aligned for the remaining segments -- the segment object
    // itself is only needed for its row count here. The default empty vector means "no lost segment is
    // possible" (local engine, whose segments are never null); only the lake resolver overrides it.
    virtual std::vector<uint32_t> output_segment_num_rows() const { return {}; }

    // Output segment positions skipped due to a lost segment; see lost_segment_positions().
    std::set<uint32_t> _lost_segment_positions;
};

} // namespace starrocks
