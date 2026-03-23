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

#include "storage/lake/tablet_merger.h"

#include <algorithm>
#include <limits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "storage/lake/meta_file.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "testutil/sync_point.h"

namespace starrocks::lake {

namespace {

struct TabletMergeInfo {
    TabletMetadataPtr metadata;
    int64_t rssid_offset = 0;
};

struct RowsetGroup {
    std::vector<size_t> data_rowset_indices;
    size_t predicate_rowset_index = 0;
    int64_t predicate_rowset_version = 0;

    bool has_predicate() const { return predicate_rowset_version > 0; }
};

struct TabletGroupState {
    int64_t tablet_id = 0;
    std::vector<RowsetGroup> rowset_groups;
    std::vector<size_t> predicate_rowset_group_indices;
    std::vector<int64_t> predicate_rowset_versions;
    size_t next_predicate_index = 0;
};

int64_t compute_rssid_offset(const TabletMetadataPB& base_metadata, const TabletMetadataPB& append_metadata) {
    uint32_t min_id = std::numeric_limits<uint32_t>::max();
    for (const auto& rowset : append_metadata.rowsets()) {
        min_id = std::min(min_id, rowset.id());
    }
    if (min_id == std::numeric_limits<uint32_t>::max()) {
        return 0;
    }
    return static_cast<int64_t>(base_metadata.next_rowset_id()) - min_id;
}

Status merge_rowsets(const TabletMetadataPB& old_metadata, int64_t rssid_offset, TabletMetadataPB* new_metadata) {
    for (const auto& rowset : old_metadata.rowsets()) {
        auto* new_rowset = new_metadata->add_rowsets();
        new_rowset->CopyFrom(rowset);
        RETURN_IF_ERROR(tablet_reshard_helper::update_rowset_range(new_rowset, old_metadata.range()));
        new_rowset->set_id(static_cast<uint32_t>(static_cast<int64_t>(rowset.id()) + rssid_offset));
        if (rowset.has_max_compact_input_rowset_id()) {
            new_rowset->set_max_compact_input_rowset_id(
                    static_cast<uint32_t>(static_cast<int64_t>(rowset.max_compact_input_rowset_id()) + rssid_offset));
        }
        for (auto& del : *new_rowset->mutable_del_files()) {
            del.set_origin_rowset_id(
                    static_cast<uint32_t>(static_cast<int64_t>(del.origin_rowset_id()) + rssid_offset));
        }

        const auto& rowset_to_schema = old_metadata.rowset_to_schema();
        const auto rowset_schema_it = rowset_to_schema.find(rowset.id());
        if (rowset_schema_it != rowset_to_schema.end()) {
            (*new_metadata->mutable_rowset_to_schema())[new_rowset->id()] = rowset_schema_it->second;
        }
    }
    return Status::OK();
}

Status merge_sstables(const TabletMetadataPB& old_metadata, int64_t rssid_offset, TabletMetadataPB* new_metadata) {
    if (!old_metadata.has_sstable_meta()) {
        return Status::OK();
    }
    auto* dest_sstables = new_metadata->mutable_sstable_meta()->mutable_sstables();
    for (const auto& sstable : old_metadata.sstable_meta().sstables()) {
        auto* new_sstable = dest_sstables->Add();
        new_sstable->CopyFrom(sstable);
        new_sstable->set_rssid_offset(static_cast<int32_t>(rssid_offset));
        const uint64_t max_rss_rowid = sstable.max_rss_rowid();
        const uint64_t low = max_rss_rowid & 0xffffffffULL;
        const int64_t high = static_cast<int64_t>(max_rss_rowid >> 32);
        new_sstable->set_max_rss_rowid((static_cast<uint64_t>(high + rssid_offset) << 32) | low);
        new_sstable->clear_delvec();
    }
    return Status::OK();
}

Status build_rowset_groups(const TabletMetadataPB& metadata, size_t start_rowset_index, size_t end_rowset_index,
                           TabletGroupState* state) {
    RowsetGroup current_group;
    int64_t last_predicate_version = -1;
    for (size_t rowset_index = start_rowset_index; rowset_index < end_rowset_index; ++rowset_index) {
        const auto& rowset = metadata.rowsets(static_cast<int>(rowset_index));
        if (!rowset.has_delete_predicate()) {
            current_group.data_rowset_indices.push_back(rowset_index);
            continue;
        }

        const int64_t predicate_version = rowset.version();
        if (predicate_version <= 0) {
            return Status::InvalidArgument("Invalid delete predicate version");
        }
        if (last_predicate_version >= 0 && predicate_version <= last_predicate_version) {
            return Status::InvalidArgument("Delete predicate version not increasing in one tablet");
        }

        current_group.predicate_rowset_index = rowset_index;
        current_group.predicate_rowset_version = predicate_version;
        state->rowset_groups.emplace_back(std::move(current_group));
        state->predicate_rowset_group_indices.push_back(state->rowset_groups.size() - 1);
        state->predicate_rowset_versions.push_back(predicate_version);
        current_group = RowsetGroup();
        last_predicate_version = predicate_version;
    }

    if (!current_group.data_rowset_indices.empty()) {
        state->rowset_groups.emplace_back(std::move(current_group));
    }

    return Status::OK();
}

Status build_rowset_index_offsets(const std::vector<TabletMergeInfo>& merge_infos, const TabletMetadataPB& new_metadata,
                                  std::vector<size_t>* rowset_index_offsets) {
    rowset_index_offsets->reserve(merge_infos.size() + 1);
    rowset_index_offsets->push_back(0);
    for (const auto& info : merge_infos) {
        rowset_index_offsets->push_back(rowset_index_offsets->back() + info.metadata->rowsets_size());
    }
    if (rowset_index_offsets->back() != static_cast<size_t>(new_metadata.rowsets_size())) {
        return Status::Corruption("Rowset count mismatch during merge reorder");
    }
    return Status::OK();
}

Status build_tablet_group_states(const std::vector<TabletMergeInfo>& merge_infos, const TabletMetadataPB& new_metadata,
                                 const std::vector<size_t>& rowset_index_offsets,
                                 std::vector<TabletGroupState>* tablet_states,
                                 std::vector<int64_t>* all_predicate_versions) {
    tablet_states->reserve(merge_infos.size());
    for (size_t i = 0; i < merge_infos.size(); ++i) {
        TabletGroupState state;
        state.tablet_id = merge_infos[i].metadata->id();
        RETURN_IF_ERROR(
                build_rowset_groups(new_metadata, rowset_index_offsets[i], rowset_index_offsets[i + 1], &state));

        for (const auto& rowset_group : state.rowset_groups) {
            if (rowset_group.has_predicate()) {
                all_predicate_versions->push_back(rowset_group.predicate_rowset_version);
            }
        }
        tablet_states->emplace_back(std::move(state));
    }
    return Status::OK();
}

Status build_reordered_rowsets(const std::vector<int64_t>& all_predicate_versions, const TabletMetadataPB& new_metadata,
                               std::vector<TabletGroupState>* tablet_states,
                               std::vector<RowsetMetadataPB>* reordered_rowsets) {
    reordered_rowsets->reserve(new_metadata.rowsets_size());
    for (int64_t version : all_predicate_versions) {
        std::vector<size_t> predicate_rowset_indices;
        for (auto& state : *tablet_states) {
            if (state.next_predicate_index >= state.predicate_rowset_versions.size()) {
                continue;
            }

            const int64_t next_version = state.predicate_rowset_versions[state.next_predicate_index];
            if (next_version < version) {
                return Status::Corruption("Delete predicate version order mismatch across tablets");
            }
            if (next_version == version) {
                const size_t rowset_group_index = state.predicate_rowset_group_indices[state.next_predicate_index];
                const auto& rowset_group = state.rowset_groups[rowset_group_index];
                for (auto rowset_index : rowset_group.data_rowset_indices) {
                    reordered_rowsets->emplace_back(new_metadata.rowsets(static_cast<int>(rowset_index)));
                }
                predicate_rowset_indices.push_back(rowset_group.predicate_rowset_index);
                ++state.next_predicate_index;
            }
        }

        if (predicate_rowset_indices.empty()) {
            return Status::Corruption("Delete predicate version not found during merge reorder");
        }
        reordered_rowsets->emplace_back(new_metadata.rowsets(static_cast<int>(predicate_rowset_indices.front())));
    }

    for (auto& state : *tablet_states) {
        size_t start_rowset_group_index = 0;
        if (state.next_predicate_index > 0) {
            start_rowset_group_index = state.predicate_rowset_group_indices[state.next_predicate_index - 1] + 1;
        }
        for (size_t rowset_group_index = start_rowset_group_index; rowset_group_index < state.rowset_groups.size();
             ++rowset_group_index) {
            const auto& rowset_group = state.rowset_groups[rowset_group_index];
            if (rowset_group.has_predicate()) {
                return Status::Corruption("Unprocessed delete predicate group during merge reorder");
            }
            for (auto rowset_index : rowset_group.data_rowset_indices) {
                reordered_rowsets->emplace_back(new_metadata.rowsets(static_cast<int>(rowset_index)));
            }
        }
    }

    return Status::OK();
}

void cleanup_orphan_schema_mappings(TabletMetadataPB* new_metadata) {
    if (new_metadata->rowset_to_schema().empty()) {
        return;
    }

    std::unordered_set<uint32_t> rowset_ids;
    rowset_ids.reserve(new_metadata->rowsets_size());
    for (const auto& rowset : new_metadata->rowsets()) {
        rowset_ids.insert(rowset.id());
    }

    auto* rowset_to_schema = new_metadata->mutable_rowset_to_schema();
    for (auto it = rowset_to_schema->begin(); it != rowset_to_schema->end();) {
        if (rowset_ids.count(it->first) == 0) {
            it = rowset_to_schema->erase(it);
        } else {
            ++it;
        }
    }

    std::unordered_set<int64_t> schema_ids;
    for (const auto& pair : *rowset_to_schema) {
        schema_ids.insert(pair.second);
    }

    for (auto it = new_metadata->mutable_historical_schemas()->begin();
         it != new_metadata->mutable_historical_schemas()->end();) {
        if (schema_ids.count(it->first) == 0) {
            it = new_metadata->mutable_historical_schemas()->erase(it);
        } else {
            ++it;
        }
    }

    if (new_metadata->historical_schemas().count(new_metadata->schema().id()) == 0) {
        auto& item = (*new_metadata->mutable_historical_schemas())[new_metadata->schema().id()];
        item.CopyFrom(new_metadata->schema());
    }
}

Status reorder_rowsets_by_delete_predicate(const std::vector<TabletMergeInfo>& merge_infos,
                                           TabletMetadataPB* new_metadata) {
    std::vector<size_t> rowset_index_offsets;
    RETURN_IF_ERROR(build_rowset_index_offsets(merge_infos, *new_metadata, &rowset_index_offsets));

    std::vector<TabletGroupState> tablet_states;
    std::vector<int64_t> all_predicate_versions;
    RETURN_IF_ERROR(build_tablet_group_states(merge_infos, *new_metadata, rowset_index_offsets, &tablet_states,
                                              &all_predicate_versions));

    if (all_predicate_versions.empty()) {
        return Status::OK();
    }

    std::sort(all_predicate_versions.begin(), all_predicate_versions.end());
    all_predicate_versions.erase(std::unique(all_predicate_versions.begin(), all_predicate_versions.end()),
                                 all_predicate_versions.end());

    std::vector<RowsetMetadataPB> reordered_rowsets;
    RETURN_IF_ERROR(build_reordered_rowsets(all_predicate_versions, *new_metadata, &tablet_states, &reordered_rowsets));

    new_metadata->mutable_rowsets()->Clear();
    for (auto& rowset : reordered_rowsets) {
        new_metadata->add_rowsets()->Swap(&rowset);
    }

    cleanup_orphan_schema_mappings(new_metadata);
    return Status::OK();
}

Status merge_delvecs(TabletManager* tablet_manager, const std::vector<TabletMergeInfo>& merge_infos,
                     int64_t new_version, int64_t txn_id, TabletMetadataPB* new_metadata) {
    std::vector<DelvecFileInfo> old_delvec_files;
    old_delvec_files.reserve(merge_infos.size());
    std::unordered_map<int64_t, std::unordered_map<std::string, size_t>> file_index_by_tablet;

    for (const auto& info : merge_infos) {
        const auto& metadata = info.metadata;
        if (!metadata->has_delvec_meta()) {
            continue;
        }
        for (const auto& [ver, file] : metadata->delvec_meta().version_to_file()) {
            (void)ver;
            auto& file_index = file_index_by_tablet[metadata->id()];
            if (file_index.emplace(file.name(), old_delvec_files.size()).second) {
                DelvecFileInfo file_info;
                file_info.tablet_id = metadata->id();
                file_info.delvec_file = file;
                old_delvec_files.emplace_back(std::move(file_info));
            }
        }
    }

    if (old_delvec_files.empty()) {
        return Status::OK();
    }

    FileMetaPB new_delvec_file;
    std::vector<uint64_t> offsets;
    RETURN_IF_ERROR(merge_delvec_files(tablet_manager, old_delvec_files, new_metadata->id(), txn_id, &new_delvec_file,
                                       &offsets));

    std::unordered_map<int64_t, std::unordered_map<std::string, uint64_t>> base_offset_by_tablet;
    for (size_t i = 0; i < old_delvec_files.size(); ++i) {
        base_offset_by_tablet[old_delvec_files[i].tablet_id][old_delvec_files[i].delvec_file.name()] = offsets[i];
    }
    TEST_SYNC_POINT_CALLBACK("merge_delvecs:before_apply_offsets", &base_offset_by_tablet);

    auto* delvec_meta = new_metadata->mutable_delvec_meta();
    delvec_meta->Clear();
    for (const auto& info : merge_infos) {
        const auto& metadata = info.metadata;
        if (!metadata->has_delvec_meta()) {
            continue;
        }
        for (const auto& [segment_id, page] : metadata->delvec_meta().delvecs()) {
            auto file_it = metadata->delvec_meta().version_to_file().find(page.version());
            if (file_it == metadata->delvec_meta().version_to_file().end()) {
                return Status::InvalidArgument("Delvec file not found for page version");
            }
            auto tablet_it = base_offset_by_tablet.find(metadata->id());
            if (tablet_it == base_offset_by_tablet.end()) {
                return Status::InvalidArgument("Delvec file not merged for page version");
            }
            auto offset_it = tablet_it->second.find(file_it->second.name());
            if (offset_it == tablet_it->second.end()) {
                return Status::InvalidArgument("Delvec file not merged for page version");
            }
            const int64_t new_segment_id = static_cast<int64_t>(segment_id) + info.rssid_offset;
            if (new_segment_id < 0 || new_segment_id > static_cast<int64_t>(std::numeric_limits<uint32_t>::max())) {
                return Status::InvalidArgument("Segment id overflow during delvec merge");
            }
            DelvecPagePB new_page = page;
            new_page.set_version(new_version);
            new_page.set_crc32c_gen_version(new_version);
            new_page.set_offset(offset_it->second + page.offset());
            (*delvec_meta->mutable_delvecs())[static_cast<uint32_t>(new_segment_id)] = std::move(new_page);
        }
    }

    (*delvec_meta->mutable_version_to_file())[new_version] = std::move(new_delvec_file);
    return Status::OK();
}

} // namespace

StatusOr<MutableTabletMetadataPtr> merge_tablet(TabletManager* tablet_manager,
                                                const std::vector<TabletMetadataPtr>& old_tablet_metadatas,
                                                const MergingTabletInfoPB& merging_tablet, int64_t new_version,
                                                const TxnInfoPB& txn_info) {
    if (old_tablet_metadatas.empty()) {
        return Status::InvalidArgument("No old tablet metadata to merge");
    }

    std::vector<TabletMergeInfo> merge_infos;
    merge_infos.reserve(old_tablet_metadatas.size());
    for (const auto& old_tablet_metadata : old_tablet_metadatas) {
        if (old_tablet_metadata == nullptr) {
            return Status::InvalidArgument("old tablet metadata is null");
        }
        merge_infos.push_back({old_tablet_metadata, 0});
    }

    auto new_tablet_metadata = std::make_shared<TabletMetadataPB>(*merge_infos.front().metadata);
    new_tablet_metadata->set_id(merging_tablet.new_tablet_id());
    new_tablet_metadata->set_version(new_version);
    new_tablet_metadata->set_commit_time(txn_info.commit_time());
    new_tablet_metadata->set_gtid(txn_info.gtid());
    new_tablet_metadata->clear_rowsets();
    new_tablet_metadata->clear_delvec_meta();
    new_tablet_metadata->clear_sstable_meta();
    new_tablet_metadata->clear_dcg_meta();
    new_tablet_metadata->clear_rowset_to_schema();
    new_tablet_metadata->clear_compaction_inputs();
    new_tablet_metadata->clear_orphan_files();
    new_tablet_metadata->clear_prev_garbage_version();
    new_tablet_metadata->set_cumulative_point(0);

    auto* merged_historical_schemas = new_tablet_metadata->mutable_historical_schemas();
    merged_historical_schemas->clear();
    for (const auto& info : merge_infos) {
        for (const auto& [schema_id, schema] : info.metadata->historical_schemas()) {
            (*merged_historical_schemas)[schema_id] = schema;
        }
    }
    if (new_tablet_metadata->schema().has_id()) {
        (*merged_historical_schemas)[new_tablet_metadata->schema().id()] = new_tablet_metadata->schema();
    }

    auto* merged_range = new_tablet_metadata->mutable_range();
    const auto& first_range = merge_infos.front().metadata->range();
    const auto& last_range = merge_infos.back().metadata->range();
    merged_range->clear_lower_bound();
    merged_range->clear_upper_bound();
    if (first_range.has_lower_bound()) {
        merged_range->mutable_lower_bound()->CopyFrom(first_range.lower_bound());
    }
    merged_range->set_lower_bound_included(first_range.lower_bound_included());
    if (last_range.has_upper_bound()) {
        merged_range->mutable_upper_bound()->CopyFrom(last_range.upper_bound());
    }
    merged_range->set_upper_bound_included(last_range.upper_bound_included());

    uint32_t next_rowset_id = merge_infos.front().metadata->next_rowset_id();
    merge_infos.front().rssid_offset = 0;
    for (size_t i = 1; i < merge_infos.size(); ++i) {
        new_tablet_metadata->set_next_rowset_id(next_rowset_id);
        const int64_t offset = compute_rssid_offset(*new_tablet_metadata, *merge_infos[i].metadata);
        merge_infos[i].rssid_offset = offset;

        uint32_t max_end = 0;
        for (const auto& rowset : merge_infos[i].metadata->rowsets()) {
            max_end = std::max(max_end, rowset.id() + get_rowset_id_step(rowset));
        }
        if (max_end > 0) {
            next_rowset_id = std::max(next_rowset_id, static_cast<uint32_t>(static_cast<int64_t>(max_end) + offset));
        }
    }

    for (const auto& info : merge_infos) {
        RETURN_IF_ERROR(merge_rowsets(*info.metadata, info.rssid_offset, new_tablet_metadata.get()));
        if (info.metadata->has_dcg_meta()) {
            for (const auto& [segment_id, dcg_ver] : info.metadata->dcg_meta().dcgs()) {
                const int64_t new_segment_id = static_cast<int64_t>(segment_id) + info.rssid_offset;
                if (new_segment_id < 0 || new_segment_id > static_cast<int64_t>(std::numeric_limits<uint32_t>::max())) {
                    return Status::InvalidArgument("Segment id overflow during tablet merge");
                }
                (*new_tablet_metadata->mutable_dcg_meta()->mutable_dcgs())[static_cast<uint32_t>(new_segment_id)] =
                        dcg_ver;
            }
        }
        RETURN_IF_ERROR(merge_sstables(*info.metadata, info.rssid_offset, new_tablet_metadata.get()));
    }

    new_tablet_metadata->set_next_rowset_id(next_rowset_id);

    if (is_primary_key(*new_tablet_metadata)) {
        RETURN_IF_ERROR(
                merge_delvecs(tablet_manager, merge_infos, new_version, txn_info.txn_id(), new_tablet_metadata.get()));
    }

    RETURN_IF_ERROR(reorder_rowsets_by_delete_predicate(merge_infos, new_tablet_metadata.get()));
    return new_tablet_metadata;
}

} // namespace starrocks::lake
