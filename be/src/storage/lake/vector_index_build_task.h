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

#include <algorithm>
#include <string>
#include <vector>

#include "common/status.h"
#include "fs/fs.h"
#include "gen_cpp/lake_service.pb.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

class TabletManager;

// Builds vector index (.vi) files for a tablet's segments.
//
// Supports two modes:
//   1. Phased API for parallel execution:
//        prepare() -> build_one_segment() x N (concurrent) -> compute_built_version()
//   2. Legacy synchronous API:
//        execute()
class VectorIndexBuildTask {
public:
    struct SegmentWork {
        int64_t rowset_version;
        FileInfo segment_file_info;
        std::vector<int64_t> index_ids;
    };

    explicit VectorIndexBuildTask(TabletManager* tablet_mgr);
    ~VectorIndexBuildTask() = default;

    // Phase 1: Read metadata and collect segment work items.
    Status prepare(const BuildVectorIndexRequest& request);

    // Phase 2: Build one segment. Thread-safe for concurrent calls with different indices.
    Status build_one_segment(size_t work_index);

    // Phase 3: Compute max_built_version from in-memory build results.
    // |segment_results| is parallel to work_items(): Status of each build_one_segment() call.
    int64_t compute_built_version(const std::vector<Status>& segment_results) const;

    size_t work_count() const { return _work_items.size(); }
    int64_t tablet_id() const { return _tablet_id; }

    // Number of OpenMP threads each per-segment TenANN builder should use.
    // Caller (e.g. LakeServiceImpl::build_vector_index) derives this from the CPU budget
    // and configures it before submitting segment tasks. Defaults to 1 so unit tests that
    // never call the setter still behave deterministically.
    void set_omp_threads(int n) { _omp_threads = std::max(1, n); }

    // Legacy synchronous interface (builds all segments sequentially).
    Status execute(const BuildVectorIndexRequest& request, BuildVectorIndexResponse* response);

private:
    Status build_segment(int64_t tablet_id, const FileInfo& segment_file_info, const std::vector<int64_t>& index_ids,
                         const TabletSchemaCSPtr& tablet_schema);

    TabletManager* _tablet_mgr;
    int64_t _tablet_id = 0;
    int64_t _built_version = 0;
    // Target version from the request. Used as the watermark fallback when there is
    // nothing to build (e.g. the metadata version was bumped by an EMPTY_TXNLOG with
    // no new rowsets), so the watermark advances and FE stops re-scheduling.
    int64_t _target_version = 0;
    int _omp_threads = 1;
    TabletSchemaCSPtr _tablet_schema;

    std::vector<SegmentWork> _work_items;

    // All rowset versions > built_version, sorted ascending. Used by compute_built_version().
    struct RowsetVersionInfo {
        int64_t version;
        bool has_vi;    // true if rowset has vector_index_ids in segment_metas
        bool processed; // true if work items were created for this version (within batch_limit)
    };
    std::vector<RowsetVersionInfo> _rowset_versions;
};

} // namespace starrocks::lake
