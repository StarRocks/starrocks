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

#include "agent/external_cluster_snapshot_task_helper.h"

#include <fmt/format.h>

#include <algorithm>

#include "storage/lake/filenames.h"
#include "storage/lake/join_path.h"

namespace starrocks::lake {

std::optional<int64_t> get_snapshot_log_tablet_id(const TExternalClusterSnapshotRequest& request) {
    auto tablet_ids = get_snapshot_log_tablet_ids(request);
    return tablet_ids.empty() ? std::nullopt : std::optional<int64_t>(tablet_ids.front());
}

std::vector<int64_t> get_snapshot_log_tablet_ids(const TExternalClusterSnapshotRequest& request) {
    std::vector<int64_t> tablet_ids;
    for (const auto& compute_node_tablets : request.compute_node_tablets) {
        for (int64_t candidate : compute_node_tablets.tablets) {
            tablet_ids.emplace_back(candidate);
        }
    }
    std::sort(tablet_ids.begin(), tablet_ids.end());
    tablet_ids.erase(std::unique(tablet_ids.begin(), tablet_ids.end()), tablet_ids.end());
    if (request.__isset.dest_tablet_id) {
        tablet_ids.erase(std::remove(tablet_ids.begin(), tablet_ids.end(), request.dest_tablet_id), tablet_ids.end());
        tablet_ids.insert(tablet_ids.begin(), request.dest_tablet_id);
    }
    return tablet_ids;
}

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

FileSet collect_del_files(const TabletMetadataPtr& metadata) {
    FileSet files;
    if (metadata == nullptr) {
        return files;
    }
    for (const auto& rowset : metadata->rowsets()) {
        for (const auto& del_file : rowset.del_files()) {
            if (!del_file.name().empty()) {
                files.emplace(del_file.name());
            }
        }
    }
    return files;
}

FileSet collect_vector_index_files(const TabletMetadataPtr& metadata) {
    FileSet files;
    if (metadata == nullptr) {
        return files;
    }
    // Every .vi referenced by a current segment, with NO async-build watermark gate. The watermark
    // (vector_index_built_version) is not a reliable "the file exists" oracle -- a .vi can exist above
    // it (compaction can carry a built .vi into a higher-versioned rowset) and can be missing below it
    // (a reshard merge inherits only the first source tablet's watermark, so a lagging source's
    // not-yet-built rowset can sit under an inflated watermark). So enumeration stays complete
    // (upload every referenced .vi) and the upload path tolerates a not-yet-built file via
    // skip_if_not_exists (see populate_tablet_snapshot / SnapshotFileSyncer).
    for (const auto& rowset : metadata->rowsets()) {
        for (const auto& segment_meta : rowset.segment_metas()) {
            if (segment_meta.vector_index_ids().empty()) {
                continue;
            }
            // segment_vector_index_uid (the id embedded in the .vi name) was added ~3 months after
            // vector_index_ids, so a segment written in that window and not since rewritten carries
            // vector_index_ids but no uid. gen_vector_index_filename_for_segment DCHECKs the uid, and
            // a release build would otherwise name the file by uid 0 -- a name that never existed on
            // disk. The read path (Rowset::read -> _init_ann_reader) can't resolve such a legacy
            // segment's .vi either, so the DR copy loses nothing the source could use. Skip it rather
            // than crash a debug build or enumerate a bogus name.
            if (!segment_meta.has_segment_vector_index_uid()) {
                VLOG(3) << "external snapshot: skip legacy vector-indexed segment without a recorded "
                           "vector index uid: "
                        << segment_meta.filename();
                continue;
            }
            for (int64_t index_id : segment_meta.vector_index_ids()) {
                files.emplace(gen_vector_index_filename_for_segment(segment_meta, index_id));
            }
        }
    }
    return files;
}

FileSet collect_inverted_index_files(const TabletMetadataPtr& metadata) {
    FileSet files;
    if (metadata == nullptr || !metadata->has_idg_meta()) {
        return files;
    }
    // Each IndexDeltaGroupEntryPB references one index artifact added by ADD INDEX (bitmap / ngram
    // bloom / bloom filter today -- all flat .idx files). Skip a GIN entry: GIN is stored as a
    // directory the file-based syncer cannot transport. Lake does not produce GIN on shared-data
    // today (AddIndexSchemaChange returns NotSupported), so this is a defensive skip keyed on the
    // entry's own index_type rather than a filename suffix -- so a future flat index type is
    // collected, never silently dropped.
    for (const auto& [_, idg_ver] : metadata->idg_meta().idgs()) {
        for (const auto& entry : idg_ver.entries()) {
            if (!entry.has_index_file() || entry.index_file().empty()) {
                continue;
            }
            bool is_gin = false;
            for (const auto& key : entry.keys()) {
                if (key.index_type() == IndexType::GIN) {
                    is_gin = true;
                    break;
                }
            }
            if (is_gin) {
                VLOG(3) << "external snapshot: skip GIN index artifact (directory not transportable by "
                           "the file syncer): "
                        << entry.index_file();
                continue;
            }
            files.emplace(entry.index_file());
        }
    }
    return files;
}

FileSet collect_index_files(const TabletMetadataPtr& metadata) {
    FileSet files = collect_vector_index_files(metadata);
    auto inverted = collect_inverted_index_files(metadata);
    files.insert(inverted.begin(), inverted.end());
    return files;
}

FileSet collect_live_data_files(const TabletFileCollections& collections) {
    FileSet files;
    // Segments live only in the rowsets; the other classes are already materialized as new_* sets by
    // TabletFileCollections::collect, so reuse them instead of re-walking the metadata.
    for (const auto& [rowset_id, rowset] : collections.new_rowsets) {
        for (const auto& segment_meta : rowset->segment_metas()) {
            if (!segment_meta.filename().empty()) {
                files.emplace(segment_meta.filename());
            }
        }
    }
    for (const auto* set : {&collections.new_sstable_files, &collections.new_dcg_files, &collections.new_delvec_files,
                            &collections.new_del_files, &collections.new_index_files}) {
        files.insert(set->begin(), set->end());
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
        collections.pre_del_files = collect_del_files(pre_metadata);
        collections.pre_index_files = collect_index_files(pre_metadata);
    }

    collections.new_rowsets = build_rowset_index(new_metadata);
    collections.new_sstable_files = collect_sstable_files(new_metadata);
    collections.new_dcg_files = collect_dcg_files(new_metadata);
    collections.new_delvec_files = collect_delvec_files(new_metadata);
    collections.new_del_files = collect_del_files(new_metadata);
    collections.new_index_files = collect_index_files(new_metadata);

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

    // Collect unused del files (kept safe later by the partition-wide live-file subtraction, so a
    // .del still referenced by a sibling child is not deleted).
    for (const auto& del_file : collections.pre_del_files) {
        if (!collections.new_del_files.contains(del_file)) {
            unused_data_files.emplace(del_file);
        }
    }

    // Collect unused index files (.vi / .idx). Also kept safe by the partition-wide live-file
    // subtraction, so an index file still referenced by a split/merge sibling is not deleted.
    for (const auto& index_file : collections.pre_index_files) {
        if (!collections.new_index_files.contains(index_file)) {
            unused_data_files.emplace(index_file);
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
                                               phmap::flat_hash_set<std::string>& globally_bound_files,
                                               UploadSnapshotFilesRequestPB& node_req) {
    auto* tablet_pb = node_req.add_tablet_snapshots();
    tablet_pb->set_tablet_id(tablet_id);

    // Emit a data file at most once per partition cycle: split siblings can reference the same shared
    // sstable/dcg/delvec/del/segment file, and the receiver's path_exists->create is not atomic, so a
    // partition-wide dedup across all classes and all tablets prevents redundant concurrent uploads.
    auto add_data_file = [&](const std::string& name) {
        if (globally_bound_files.emplace(name).second) {
            tablet_pb->add_new_data_files(name);
        }
    };
    // Index sidecars go to a separate list the receiver copies tolerantly (skip_if_not_exists): an
    // async-built .vi may be referenced by metadata before its file exists. Shares the same dedup set
    // so a file is emitted to exactly one list per partition cycle (names never collide across
    // classes -- .vi/.idx suffixes differ from segments).
    auto add_index_file = [&](const std::string& name) {
        if (globally_bound_files.emplace(name).second) {
            tablet_pb->add_new_index_data_files(name);
        }
    };

    // New SSTable / DCG / delvec files: incremental (not already on external from the previous
    // snapshot), then partition-wide dedup.
    for (const auto& file : collections.new_sstable_files) {
        if (!collections.pre_sstable_files.contains(file)) {
            add_data_file(file);
        }
    }
    for (const auto& file : collections.new_dcg_files) {
        if (!collections.pre_dcg_files.contains(file)) {
            add_data_file(file);
        }
    }
    for (const auto& file : collections.new_delvec_files) {
        if (!collections.pre_delvec_files.contains(file)) {
            add_data_file(file);
        }
    }

    // Del files: uploaded in FULL (all current), not incrementally. Del files were previously untracked
    // by external snapshot, so an incremental new-minus-pre diff would never backfill a del file present
    // in both pre and new, leaving existing snapshots permanently incomplete. skip_if_exists on the
    // receiver makes re-requesting already-uploaded files cheap.
    for (const auto& file : collections.new_del_files) {
        add_data_file(file);
    }

    // Index files (.vi / .idx): uploaded in FULL (all current), for the same reason as .del files --
    // index sidecars were previously untracked by external snapshot, so an incremental new-minus-pre
    // diff would never backfill an index file present in both pre and new, leaving existing snapshots
    // permanently incomplete. skip_if_exists on the receiver makes re-requesting already-uploaded
    // files cheap. Emitted to the tolerant list: a not-yet-built async .vi is skipped rather than
    // failing the snapshot (an .idx is always present when referenced, so tolerance is a no-op for it).
    for (const auto& file : collections.new_index_files) {
        add_index_file(file);
    }

    // Process new rowsets for segments
    for (const auto& [rowset_id, rowset] : collections.new_rowsets) {
        if (collections.pre_rowsets.contains(rowset_id)) {
            continue;
        }
        for (const auto& segment_meta : rowset->segment_metas()) {
            const auto& segment_filename = segment_meta.filename();
            pre_bundle_data_files.erase(segment_filename);
            add_data_file(segment_filename);
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
    // Handle metadata files. Only queue the pre-version meta for deletion when there actually was a
    // pre-version metadata (same predicate the schema-file half uses below): on the reshard boundary a
    // split/merge child has no {tablet}_{pre_version}.meta, so it must not be deleted; the new metadata
    // is still uploaded.
    if (is_filebundling) {
        if (!meta_added) {
            tablet_pb->add_new_metadata_files(tablet_metadata_filename(0, new_version));
            if (pre_tablet_metadata != nullptr) {
                unused_meta_files.emplace(tablet_metadata_filename(0, pre_version));
            }
        }
    } else {
        if (pre_tablet_metadata != nullptr) {
            unused_meta_files.emplace(tablet_metadata_filename(tablet_id, pre_version));
        }
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
                                  const phmap::flat_hash_set<int64_t>& new_schema_ids, FileSet& unused_schema_files,
                                  const FileSet& partition_live_files) {
    if (pre_version < 0) {
        return;
    }

    // Add bundle files to unused data files
    for (const auto& file : pre_bundle_data_files) {
        unused_data_files.emplace(file);
    }

    // Never delete a data file that some current tablet in the partition still references. After a
    // split/merge, siblings share files, so a per-tablet pre-minus-new diff can flag a file a sibling
    // still uses. Subtract the partition-wide live set here -- AFTER the bundle expansion above, since a
    // bundle file can also be live in a sibling -- so both mutations of unused_data_files stay together.
    for (const auto& file : partition_live_files) {
        unused_data_files.erase(file);
    }

    // Collect unused schema files
    for (auto schema_id : pre_schema_ids) {
        if (!new_schema_ids.contains(schema_id)) {
            unused_schema_files.emplace(schema_filename(schema_id));
        }
    }
}

} // namespace starrocks::lake
