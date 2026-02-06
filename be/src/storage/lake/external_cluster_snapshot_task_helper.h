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
#include <string>
#include <vector>

#include "base/phmap/phmap.h"
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

    TabletFileCollections() = default;

    static TabletFileCollections collect(const TabletMetadataPtr& pre_metadata, const TabletMetadataPtr& new_metadata);
};

// File collection and diff functions
RowsetIndex build_rowset_index(const TabletMetadataPtr& metadata);
FileSet collect_sstable_files(const TabletMetadataPtr& metadata);
FileSet collect_dcg_files(const TabletMetadataPtr& metadata);
FileSet collect_delvec_files(const TabletMetadataPtr& metadata);
void collect_schema_ids(const TabletMetadataPtr& metadata, phmap::flat_hash_set<int64_t>& schema_ids);
void collect_unused_files(const TabletFileCollections& collections, FileSet& unused_data_files,
                          FileSet& pre_bundle_data_files);

TabletDataSnapshotPB* populate_tablet_snapshot(int64_t tablet_id, const TabletFileCollections& collections,
                                               FileSet& pre_bundle_data_files,
                                               phmap::flat_hash_set<std::string>& globally_bound_segments,
                                               UploadSnapshotFilesRequestPB& node_req);

void populate_meta_schema_files(bool is_filebundling, bool meta_added, int64_t tablet_id, int64_t pre_version,
                                int64_t new_version, const TabletMetadataPtr& pre_tablet_metadata,
                                const TabletMetadataPtr& new_tablet_metadata,
                                phmap::flat_hash_set<int64_t>& pre_schema_ids,
                                phmap::flat_hash_set<int64_t>& new_schema_ids, FileSet& unused_meta_files,
                                TabletDataSnapshotPB* tablet_pb);

void prepare_unused_files_for_log(int64_t pre_version, const FileSet& pre_bundle_data_files, FileSet& unused_data_files,
                                  const FileSet& unused_meta_files, const phmap::flat_hash_set<int64_t>& pre_schema_ids,
                                  const phmap::flat_hash_set<int64_t>& new_schema_ids, FileSet& unused_schema_files);

} // namespace starrocks::lake
