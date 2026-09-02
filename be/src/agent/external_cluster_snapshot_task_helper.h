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

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "base/phmap/phmap.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/lake_service.pb.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/tablet_metadata.h"

namespace starrocks::lake {

class LocationProvider;
using FileSet = phmap::flat_hash_set<std::string>;
using RowsetIndex = phmap::flat_hash_map<uint32_t, const RowsetMetadataPB*>;

struct TabletFileCollections {
    RowsetIndex pre_rowsets;
    RowsetIndex new_rowsets;
    FileSet pre_sstable_files;
    FileSet new_sstable_files;
    FileSet pre_dcg_files;
    FileSet new_dcg_files;
    FileSet pre_delvec_files;
    FileSet new_delvec_files;
    FileSet pre_del_files;
    FileSet new_del_files;
    // Segment index sidecars: vector index (.vi) + inverted/IDG index (.idx). Merged into one class
    // because both are flat files in the same data dir and are uploaded/deleted identically.
    FileSet pre_index_files;
    FileSet new_index_files;

    TabletFileCollections() = default;

    static TabletFileCollections collect(const TabletMetadataPtr& pre_metadata, const TabletMetadataPtr& new_metadata);
};

// File collection and diff functions
RowsetIndex build_rowset_index(const TabletMetadataPtr& metadata);
FileSet collect_sstable_files(const TabletMetadataPtr& metadata);
FileSet collect_dcg_files(const TabletMetadataPtr& metadata);
FileSet collect_delvec_files(const TabletMetadataPtr& metadata);
FileSet collect_del_files(const TabletMetadataPtr& metadata);
// Per-segment vector index (.vi) sidecars, one per (segment, vector index id) recorded in
// SegmentMetadataPB.vector_index_ids. Enumerated with no async-build-watermark gate; a .vi that is
// referenced but not yet built (async deferred build) is tolerated at upload time via
// skip_if_not_exists rather than filtered here (the watermark is not a reliable "file exists" signal).
FileSet collect_vector_index_files(const TabletMetadataPtr& metadata);
// Inverted / Index-Delta-Group index files: the flat .idx file named by each IndexDeltaGroupEntryPB.
// A GIN entry is skipped -- GIN is stored as a directory the file-based syncer cannot transport (and
// lake does not produce GIN on shared-data today); every other, flat, index type is collected.
FileSet collect_inverted_index_files(const TabletMetadataPtr& metadata);
// Union of the vector (.vi) and inverted/IDG (.idx) index sidecars for a tablet -- the merged
// index_files class the enumerator tracks.
FileSet collect_index_files(const TabletMetadataPtr& metadata);
// All current live data filenames from |collections.new_*| (segments + del + sstable + dcg + delvec
// + index).
// Used to build the partition-wide live set that guards deletion: a data file is deleted from external
// only when no current tablet in the partition references it. Derives from the already-built
// collections to avoid re-walking the metadata.
FileSet collect_live_data_files(const TabletFileCollections& collections);
void collect_schema_ids(const TabletMetadataPtr& metadata, phmap::flat_hash_set<int64_t>& schema_ids);
void collect_unused_files(const TabletFileCollections& collections, FileSet& unused_data_files,
                          FileSet& pre_bundle_data_files);

TabletDataSnapshotPB* populate_tablet_snapshot(int64_t tablet_id, const TabletFileCollections& collections,
                                               FileSet& pre_bundle_data_files,
                                               phmap::flat_hash_set<std::string>& globally_bound_files,
                                               UploadSnapshotFilesRequestPB& node_req);

void populate_meta_schema_files(bool is_filebundling, bool meta_added, int64_t tablet_id, int64_t pre_version,
                                int64_t new_version, const TabletMetadataPtr& pre_tablet_metadata,
                                const TabletMetadataPtr& new_tablet_metadata,
                                phmap::flat_hash_set<int64_t>& pre_schema_ids,
                                phmap::flat_hash_set<int64_t>& new_schema_ids, FileSet& unused_meta_files,
                                TabletDataSnapshotPB* tablet_pb);

void prepare_unused_files_for_log(int64_t pre_version, const FileSet& pre_bundle_data_files, FileSet& unused_data_files,
                                  const FileSet& unused_meta_files, const phmap::flat_hash_set<int64_t>& pre_schema_ids,
                                  const phmap::flat_hash_set<int64_t>& new_schema_ids, FileSet& unused_schema_files,
                                  const FileSet& partition_live_files);

// Ordered delete-log roots: the destination tablet for new requests, followed by sorted source-tablet
// fallbacks for requests/logs written by older versions.
std::optional<int64_t> get_snapshot_log_tablet_id(const TExternalClusterSnapshotRequest& request);
std::vector<int64_t> get_snapshot_log_tablet_ids(const TExternalClusterSnapshotRequest& request);

} // namespace starrocks::lake
