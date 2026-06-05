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

#include "storage/lake/external_cluster_snapshot_task_helper.h"

#include <fmt/format.h>

#include "storage/lake/filenames.h"
#include "storage/lake/join_path.h"

namespace starrocks::lake {

RowsetIndex build_rowset_index(const TabletMetadataPtr& metadata) {
    RowsetIndex index;
    if (metadata == nullptr) {
        return index;
    }
    index.reserve(metadata->rowsets_size());
    for (const RowsetMetadataPB& rowset : metadata->rowsets()) {
        index.emplace(rowset.id(), &rowset);
    }
    return index;
}

FileSet collect_sstable_files(const TabletMetadataPtr& metadata) {
    FileSet files;
    if (metadata == nullptr || !metadata->has_sstable_meta()) {
        return files;
    }
    for (const auto& sstable : metadata->sstable_meta().sstables()) {
        if (!sstable.filename().empty()) {
            files.emplace(sstable.filename());
        }
    }
    return files;
}

FileSet collect_dcg_files(const TabletMetadataPtr& metadata) {
    FileSet files;
    if (metadata == nullptr || !metadata->has_dcg_meta()) {
        return files;
    }
    for (const auto& entry : metadata->dcg_meta().dcgs()) {
        const auto& dcg = entry.second;
        for (const std::string& column_file : dcg.column_files()) {
            if (!column_file.empty()) {
                files.emplace(column_file);
            }
        }
    }
    return files;
}

FileSet collect_delvec_files(const TabletMetadataPtr& metadata) {
    FileSet files;
    if (metadata == nullptr || !metadata->has_delvec_meta()) {
        return files;
    }
    for (const auto& entry : metadata->delvec_meta().version_to_file()) {
        const auto& meta = entry.second;
        if (!meta.name().empty()) {
            files.emplace(meta.name());
        }
    }
    return files;
}

void collect_schema_ids(const TabletMetadataPtr& metadata, phmap::flat_hash_set<int64_t>& schema_ids) {
    if (metadata == nullptr) {
        return;
    }

    if (metadata->has_schema()) {
        schema_ids.emplace(metadata->schema().id());
    }
    const auto& historical_schemas = metadata->historical_schemas();
    schema_ids.reserve(schema_ids.size() + historical_schemas.size());
    for (const auto& [_, schema] : historical_schemas) {
        schema_ids.emplace(schema.id());
    }
    return;
}

TabletFileCollections TabletFileCollections::collect(const TabletMetadataPtr& pre_metadata,
                                                     const TabletMetadataPtr& new_metadata) {
    TabletFileCollections collections;

    if (pre_metadata) {
        collections.pre_rowsets = build_rowset_index(pre_metadata);
        collections.pre_sstable_files = collect_sstable_files(pre_metadata);
        collections.pre_dcg_files = collect_dcg_files(pre_metadata);
        collections.pre_delvec_files = collect_delvec_files(pre_metadata);
    }

    collections.new_rowsets = build_rowset_index(new_metadata);
    collections.new_sstable_files = collect_sstable_files(new_metadata);
    collections.new_dcg_files = collect_dcg_files(new_metadata);
    collections.new_delvec_files = collect_delvec_files(new_metadata);

    return collections;
}

void collect_unused_files(const TabletFileCollections& collections, FileSet& unused_data_files,
                          FileSet& pre_bundle_data_files) {
    // Collect unused DCG files
    for (const auto& dcg_file : collections.pre_dcg_files) {
        if (!collections.new_dcg_files.contains(dcg_file)) {
            unused_data_files.emplace(dcg_file);
        }
    }

    // Collect unused SSTable files
    for (const auto& sstable_file : collections.pre_sstable_files) {
        if (!collections.new_sstable_files.contains(sstable_file)) {
            unused_data_files.emplace(sstable_file);
        }
    }

    // Collect unused delvec files
    for (const auto& delvec_file : collections.pre_delvec_files) {
        if (!collections.new_delvec_files.contains(delvec_file)) {
            unused_data_files.emplace(delvec_file);
        }
    }

    // Process rowsets for bundle/non-bundle files
    for (const auto& [rowset_id, rowset] : collections.pre_rowsets) {
        if (collections.new_rowsets.contains(rowset_id)) {
            continue;
        }

        for (const auto& segment_meta : rowset->segment_metas()) {
            if (segment_meta.has_bundle_file_offset()) {
                pre_bundle_data_files.insert(segment_meta.filename());
            } else {
                unused_data_files.insert(segment_meta.filename());
            }
        }
    }
}

TabletDataSnapshotPB* populate_tablet_snapshot(int64_t tablet_id, const TabletFileCollections& collections,
                                               FileSet& pre_bundle_data_files,
                                               phmap::flat_hash_set<std::string>& globally_bound_segments,
                                               UploadSnapshotFilesRequestPB& node_req) {
    auto* tablet_pb = node_req.add_tablet_snapshots();
    tablet_pb->set_tablet_id(tablet_id);

    // Add new SSTable files
    for (const auto& file : collections.new_sstable_files) {
        if (!collections.pre_sstable_files.contains(file)) {
            tablet_pb->add_new_data_files(file);
        }
    }

    // Add new DCG files
    for (const auto& file : collections.new_dcg_files) {
        if (!collections.pre_dcg_files.contains(file)) {
            tablet_pb->add_new_data_files(file);
        }
    }

    // Add new delvec files
    for (const auto& file : collections.new_delvec_files) {
        if (!collections.pre_delvec_files.contains(file)) {
            tablet_pb->add_new_data_files(file);
        }
    }

    // Process new rowsets for segments
    for (const auto& [rowset_id, rowset] : collections.new_rowsets) {
        if (collections.pre_rowsets.contains(rowset_id)) {
            continue;
        }
        for (const auto& segment_meta : rowset->segment_metas()) {
            const auto& segment_filename = segment_meta.filename();
            pre_bundle_data_files.erase(segment_filename);
            auto [it, inserted] = globally_bound_segments.emplace(segment_filename);
            if (inserted) {
                tablet_pb->add_new_data_files(segment_filename);
            }
        }
    }

    // Log new data files for debugging
    for (const auto& file : tablet_pb->new_data_files()) {
        VLOG(3) << "tablet_id: " << tablet_id << ", new_data_file: " << file;
    }
    return tablet_pb;
}

void populate_meta_schema_files(bool is_filebundling, bool meta_added, int64_t tablet_id, int64_t pre_version,
                                int64_t new_version, const TabletMetadataPtr& pre_tablet_metadata,
                                const TabletMetadataPtr& new_tablet_metadata,
                                phmap::flat_hash_set<int64_t>& pre_schema_ids,
                                phmap::flat_hash_set<int64_t>& new_schema_ids, FileSet& unused_meta_files,
                                TabletDataSnapshotPB* tablet_pb) {
    DCHECK(tablet_pb != nullptr);
    // Handle metadata files
    if (is_filebundling) {
        if (!meta_added) {
            tablet_pb->add_new_metadata_files(tablet_metadata_filename(0, new_version));
            unused_meta_files.emplace(tablet_metadata_filename(0, pre_version));
        }
    } else {
        unused_meta_files.emplace(tablet_metadata_filename(tablet_id, pre_version));
        tablet_pb->add_new_metadata_files(tablet_metadata_filename(tablet_id, new_version));
    }

    // Collect and process schema files
    if (pre_tablet_metadata) {
        collect_schema_ids(pre_tablet_metadata, pre_schema_ids);
    }
    collect_schema_ids(new_tablet_metadata, new_schema_ids);

    for (auto schema_id : new_schema_ids) {
        if (!pre_schema_ids.contains(schema_id)) {
            tablet_pb->add_new_schema_files(schema_filename(schema_id));
            pre_schema_ids.insert(schema_id);
        }
    }
}

void prepare_unused_files_for_log(int64_t pre_version, const FileSet& pre_bundle_data_files, FileSet& unused_data_files,
                                  const FileSet& unused_meta_files, const phmap::flat_hash_set<int64_t>& pre_schema_ids,
                                  const phmap::flat_hash_set<int64_t>& new_schema_ids, FileSet& unused_schema_files) {
    if (pre_version < 0) {
        return;
    }

    // Add bundle files to unused data files
    for (const auto& file : pre_bundle_data_files) {
        unused_data_files.emplace(file);
    }

    // Collect unused schema files
    for (auto schema_id : pre_schema_ids) {
        if (!new_schema_ids.contains(schema_id)) {
            unused_schema_files.emplace(schema_filename(schema_id));
        }
    }
}

} // namespace starrocks::lake