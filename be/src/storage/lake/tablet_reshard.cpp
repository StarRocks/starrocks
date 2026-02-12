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

#include "storage/lake/tablet_reshard.h"

#include <algorithm>
#include <limits>
#include <set>
#include <span>
#include <unordered_map>

#include "base/testutil/sync_point.h"
#include "storage/del_vector.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/tablet_range.h"
#include "storage/variant_tuple.h"

namespace starrocks::lake {

static std::ostream& operator<<(std::ostream& out, const std::vector<int64_t>& tablet_ids) {
    out << '[';
    for (size_t i = 0; i < tablet_ids.size(); ++i) {
        if (i > 0) {
            out << ", ";
        }
        out << tablet_ids[i];
    }
    out << ']';
    return out;
}

std::ostream& operator<<(std::ostream& out, const PublishTabletInfo& tablet_info) {
    if (tablet_info.get_publish_tablet_type() == PublishTabletInfo::PUBLISH_NORMAL) {
        return out << "{tablet_id: " << tablet_info.get_tablet_id_in_metadata() << '}';
    }

    return out << "{publish_tablet_type: " << (int)tablet_info.get_publish_tablet_type()
               << ", tablet_id_in_metadata: " << tablet_info.get_tablet_id_in_metadata()
               << ", tablet_id_in_txn_log: " << tablet_info.get_tablet_ids_in_txn_logs() << '}';
}

static void set_all_data_files_shared(RowsetMetadataPB* rowset_metadata) {
    // Set dat files shared
    auto* shared_segments = rowset_metadata->mutable_shared_segments();
    shared_segments->Clear();
    shared_segments->Resize(rowset_metadata->segments_size(), true);

    // Set del files shared
    for (auto& del : *rowset_metadata->mutable_del_files()) {
        del.set_shared(true);
    }
}

static void set_all_data_files_shared(TxnLogPB* txn_log) {
    if (txn_log->has_op_write()) {
        auto* op_write = txn_log->mutable_op_write();
        if (op_write->has_rowset()) {
            set_all_data_files_shared(op_write->mutable_rowset());
        }
    }

    if (txn_log->has_op_compaction()) {
        auto* op_compaction = txn_log->mutable_op_compaction();
        if (op_compaction->has_output_rowset()) {
            set_all_data_files_shared(op_compaction->mutable_output_rowset());
        }
        if (op_compaction->has_output_sstable()) {
            auto* sstable = op_compaction->mutable_output_sstable();
            sstable->set_shared(true);
        }
    }

    if (txn_log->has_op_schema_change()) {
        auto* op_schema_change = txn_log->mutable_op_schema_change();
        for (auto& rowset : *op_schema_change->mutable_rowsets()) {
            set_all_data_files_shared(&rowset);
        }
        if (op_schema_change->has_delvec_meta()) {
            for (auto& pair : *op_schema_change->mutable_delvec_meta()->mutable_version_to_file()) {
                pair.second.set_shared(true);
            }
        }
    }

    if (txn_log->has_op_replication()) {
        for (auto& op_write : *txn_log->mutable_op_replication()->mutable_op_writes()) {
            if (op_write.has_rowset()) {
                set_all_data_files_shared(op_write.mutable_rowset());
            }
        }
    }
}

static void set_all_data_files_shared(TabletMetadataPB* tablet_metadata, bool skip_delvecs = false) {
    // rowset (dat and del)
    for (auto& rowset_metadata : *tablet_metadata->mutable_rowsets()) {
        set_all_data_files_shared(&rowset_metadata);
    }

    // delvec
    if (!skip_delvecs && tablet_metadata->has_delvec_meta()) {
        for (auto& pair : *tablet_metadata->mutable_delvec_meta()->mutable_version_to_file()) {
            pair.second.set_shared(true);
        }
    }

    // dcg
    if (tablet_metadata->has_dcg_meta()) {
        for (auto& dcg : *tablet_metadata->mutable_dcg_meta()->mutable_dcgs()) {
            auto* shared_files = dcg.second.mutable_shared_files();
            shared_files->Clear();
            shared_files->Resize(dcg.second.column_files_size(), true);
        }
    }

    // sst
    if (tablet_metadata->has_sstable_meta()) {
        for (auto& sstable : *tablet_metadata->mutable_sstable_meta()->mutable_sstables()) {
            sstable.set_shared(true);
        }
    }
}

static int64_t compute_rssid_offset(const TabletMetadataPB& base_metadata, const TabletMetadataPB& append_metadata) {
    uint32_t min_id = std::numeric_limits<uint32_t>::max();
    for (const auto& rowset : append_metadata.rowsets()) {
        min_id = std::min(min_id, rowset.id());
    }
    if (min_id == std::numeric_limits<uint32_t>::max()) {
        return 0;
    }
    return static_cast<int64_t>(base_metadata.next_rowset_id()) - min_id;
}

static Status merge_rowsets(const TabletMetadataPB& old_metadata, int64_t rssid_offset,
                            TabletMetadataPB* new_metadata) {
    for (const auto& rowset : old_metadata.rowsets()) {
        auto* new_rowset = new_metadata->add_rowsets();
        new_rowset->CopyFrom(rowset);
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

static Status merge_sstables(const TabletMetadataPB& old_metadata, int64_t rssid_offset,
                             TabletMetadataPB* new_metadata) {
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

struct TabletMergeInfo {
    TabletMetadataPtr metadata;
    int64_t rssid_offset = 0;
};

static Status merge_delvecs(TabletManager* tablet_manager, const std::vector<TabletMergeInfo>& merge_infos,
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

struct Statistic {
    int64_t num_rows = 0;
    int64_t data_size = 0;
};

struct TabletRangeInfo {
    TabletRangePB range;
    // rowset_id -> rowset stat
    std::unordered_map<uint32_t, Statistic> rowset_stats;
};

static Status get_tablet_split_ranges(TabletManager* tablet_manager, const TabletMetadataPtr& tablet_metadata,
                                      int32_t split_count, std::vector<TabletRangeInfo>& split_ranges) {
    if (split_count < 2) {
        return Status::InvalidArgument("Invalid split count, it is less than 2");
    }

    struct SegmentInfo {
        uint32_t rowset_id;
        VariantTuple min;
        VariantTuple max;
        Statistic stat;
    };

    const bool use_delvec = is_primary_key(*tablet_metadata) && tablet_metadata->has_delvec_meta();
    // Collect all segment infos
    std::vector<SegmentInfo> segment_infos;
    for (const auto& rowset : tablet_metadata->rowsets()) {
        if (rowset.segments_size() != rowset.segment_size_size() ||
            rowset.segments_size() != rowset.segment_metas_size()) {
            return Status::InvalidArgument("Segment metadata is inconsistent with segment list");
        }
        for (int32_t i = 0; i < rowset.segments_size(); ++i) {
            auto& segment_info = segment_infos.emplace_back();
            segment_info.rowset_id = rowset.id();
            const auto& segment_meta = rowset.segment_metas(i);
            RETURN_IF_ERROR(segment_info.min.from_proto(segment_meta.sort_key_min()));
            RETURN_IF_ERROR(segment_info.max.from_proto(segment_meta.sort_key_max()));
            segment_info.stat.num_rows = segment_meta.num_rows();
            segment_info.stat.data_size = rowset.segment_size(i);
            if (use_delvec) {
                const uint32_t segment_id = rowset.id() + i;
                DelVector delvec;
                LakeIOOptions lake_io_opts{.fill_data_cache = false};
                auto st = lake::get_del_vec(tablet_manager, *tablet_metadata, segment_id, false, lake_io_opts, &delvec);
                if (!st.ok()) {
                    LOG(WARNING) << "Failed to get delvec for tablet " << tablet_metadata->id() << ", segment_id "
                                 << segment_id << ", status: " << st;
                    continue;
                }
                const int64_t total_rows = segment_info.stat.num_rows;
                const int64_t deleted_rows = delvec.cardinality();
                const int64_t live_rows = std::max<int64_t>(0, total_rows - deleted_rows);
                const int64_t live_size = total_rows > 0 ? segment_info.stat.data_size * live_rows / total_rows : 0;
                segment_info.stat.num_rows = live_rows;
                segment_info.stat.data_size = live_size;
            }
        }
    }

    if (segment_infos.empty()) {
        return Status::InvalidArgument("No segments found in tablet metadata");
    }

    // Collect all segment range boundaries in order
    auto comparator = [](const VariantTuple* key1, const VariantTuple* key2) { return key1->compare(*key2) < 0; };
    std::set<const VariantTuple*, decltype(comparator)> ordered_range_boundaries;
    for (const auto& segment_info : segment_infos) {
        ordered_range_boundaries.insert(&segment_info.min);
        ordered_range_boundaries.insert(&segment_info.max);
    }

    struct RangeInfo {
        VariantTuple min;
        VariantTuple max;
        Statistic stat;
        // rowset_id -> rowset stat
        std::unordered_map<uint32_t, Statistic> rowset_stats;
    };

    // Build ordered ranges
    std::vector<RangeInfo> ordered_ranges;
    ordered_ranges.reserve(ordered_range_boundaries.size());
    const VariantTuple* last_boundary = nullptr;
    for (const auto* range_boundary : ordered_range_boundaries) {
        if (last_boundary != nullptr) {
            auto& range_info = ordered_ranges.emplace_back();
            range_info.min = *last_boundary;
            range_info.max = *range_boundary;
            range_info.stat.num_rows = 0;
            range_info.stat.data_size = 0;
        }
        last_boundary = range_boundary;
    }

    if (ordered_ranges.empty()) {
        return Status::InvalidArgument("No split ranges available");
    }

    // Estimate num_rows and data_size in each ordered ranges
    for (const auto& segment_info : segment_infos) {
        std::vector<RangeInfo*> overlapping_ranges;
        for (auto& range_info : ordered_ranges) {
            if (&range_info != &ordered_ranges.back()) {
                // Non-last ranges, treat range as [min, max) to avoid double counting on boundaries
                if (!(range_info.max.compare(segment_info.min) <= 0 || range_info.min.compare(segment_info.max) >= 0)) {
                    overlapping_ranges.push_back(&range_info);
                }
            } else {
                // The last range, treat range as [min, max]
                if (!(range_info.max.compare(segment_info.min) < 0 || range_info.min.compare(segment_info.max) > 0)) {
                    overlapping_ranges.push_back(&range_info);
                }
            }
        }

        if (overlapping_ranges.empty()) {
            continue;
        }

        // Divide num rows and data size equally among all overlapping ranges,
        // we can add more samples to improve the accuracy of estimation in future
        const auto split_num_rows = segment_info.stat.num_rows / overlapping_ranges.size();
        const auto remain_num_rows = segment_info.stat.num_rows % overlapping_ranges.size();
        const auto split_data_size = segment_info.stat.data_size / overlapping_ranges.size();
        const auto remain_data_size = segment_info.stat.data_size % overlapping_ranges.size();
        for (size_t i = 0; i < overlapping_ranges.size(); ++i) {
            auto delta_num_rows = split_num_rows;
            auto delta_data_size = split_data_size;
            if (i < remain_num_rows) {
                ++delta_num_rows;
            }
            if (i < remain_data_size) {
                ++delta_data_size;
            }

            auto* range_info = overlapping_ranges[i];
            range_info->stat.num_rows += delta_num_rows;
            range_info->stat.data_size += delta_data_size;

            auto& rowset_stat = range_info->rowset_stats[segment_info.rowset_id];
            rowset_stat.num_rows += delta_num_rows;
            rowset_stat.data_size += delta_data_size;
        }
    }

    // Pick tablet overlapped ranges
    TabletRange tablet_range;
    RETURN_IF_ERROR(tablet_range.from_proto(tablet_metadata->range()));
    std::vector<const RangeInfo*> tablet_overlapped_ranges;
    tablet_overlapped_ranges.reserve(ordered_ranges.size());
    int64_t total_num_rows = 0;
    size_t non_empty_ranges = 0;
    for (const auto& range_info : ordered_ranges) {
        if (tablet_range.less_than(range_info.min) || tablet_range.greater_than(range_info.max)) {
            continue;
        }
        tablet_overlapped_ranges.push_back(&range_info);
        total_num_rows += range_info.stat.num_rows;
        if (range_info.stat.num_rows > 0) {
            ++non_empty_ranges;
        }
    }

    // Need enough non-empty ranges to form split_count ranges; otherwise fallback.
    if (non_empty_ranges < split_count) {
        return Status::InvalidArgument("Not enough split ranges available");
    }

    // Calculate split ranges
    DCHECK(split_ranges.empty());
    split_ranges.reserve(split_count);
    const int64_t avg_num_rows = std::max<int64_t>(1, total_num_rows / split_count);
    int64_t acc_num_rows = 0;
    std::unordered_map<uint32_t, starrocks::lake::Statistic> acc_rowset_stats;
    last_boundary = nullptr;
    size_t remaining_non_empty_ranges = non_empty_ranges;
    for (size_t i = 0; i < tablet_overlapped_ranges.size(); ++i) {
        const auto* range_info = tablet_overlapped_ranges[i];
        const bool is_non_empty = range_info->stat.num_rows > 0;
        acc_num_rows += range_info->stat.num_rows;
        for (const auto& [rowset_id, stat] : range_info->rowset_stats) {
            auto& rowset_stat = acc_rowset_stats[rowset_id];
            rowset_stat.num_rows += stat.num_rows;
            rowset_stat.data_size += stat.data_size;
        }

        const auto remaining_splits = static_cast<size_t>(split_count) - split_ranges.size();
        if (is_non_empty && remaining_splits > 0 &&
            (acc_num_rows >= avg_num_rows || remaining_non_empty_ranges <= remaining_splits)) {
            const VariantTuple* boundary = &range_info->max;
            // Advance boundary across empty ranges within tablet range to reach the gap end.
            for (size_t j = i + 1; j < tablet_overlapped_ranges.size(); ++j) {
                const auto* next_range = tablet_overlapped_ranges[j];
                if (next_range->stat.num_rows > 0 || !tablet_range.strictly_contains(next_range->max)) {
                    break;
                }
                boundary = &next_range->max;
            }

            // Skip invalid boundary to avoid generating empty or overlapping ranges.
            if ((last_boundary == nullptr || boundary->compare(*last_boundary) > 0) &&
                tablet_range.strictly_contains(*boundary)) {
                auto& split_range = split_ranges.emplace_back();
                if (last_boundary == nullptr) {
                    // Use lower bound in tablet range
                    split_range.range = tablet_metadata->range();
                } else {
                    last_boundary->to_proto(split_range.range.mutable_lower_bound());
                    split_range.range.set_lower_bound_included(true);
                }

                boundary->to_proto(split_range.range.mutable_upper_bound());
                split_range.range.set_upper_bound_included(false);

                for (const auto& [rowset_id, stat] : acc_rowset_stats) {
                    auto& rowset_stat = split_range.rowset_stats[rowset_id];
                    rowset_stat.num_rows = stat.num_rows;
                    rowset_stat.data_size = stat.data_size;
                }

                acc_num_rows = 0;
                acc_rowset_stats.clear();
                last_boundary = boundary;
            }
        }

        if (is_non_empty) {
            --remaining_non_empty_ranges;
        }
    }

    if (split_ranges.size() == split_count) {
        auto& split_range = split_ranges.back();
        if (tablet_metadata->range().has_upper_bound()) {
            split_range.range.mutable_upper_bound()->CopyFrom(tablet_metadata->range().upper_bound());
            split_range.range.set_upper_bound_included(tablet_metadata->range().upper_bound_included());
        } else {
            split_range.range.clear_upper_bound();
            split_range.range.clear_upper_bound_included();
        }
        for (const auto& [rowset_id, stat] : acc_rowset_stats) {
            auto& rowset_stat = split_range.rowset_stats[rowset_id];
            rowset_stat.num_rows += stat.num_rows;
            rowset_stat.data_size += stat.data_size;
        }
    } else if (split_ranges.size() + 1 == split_count && acc_num_rows > 0) {
        auto& split_range = split_ranges.emplace_back();
        // Use upper bound in tablet range
        split_range.range = tablet_metadata->range();
        // Lower bound use the upper bound of previous range
        split_range.range.mutable_lower_bound()->CopyFrom(split_ranges[split_count - 2].range.upper_bound());
        split_range.range.set_lower_bound_included(true);
        for (const auto& [rowset_id, stat] : acc_rowset_stats) {
            auto& rowset_stat = split_range.rowset_stats[rowset_id];
            rowset_stat.num_rows = stat.num_rows;
            rowset_stat.data_size = stat.data_size;
        }
    } else {
        return Status::InvalidArgument("Not enough split ranges available");
    }

    return Status::OK();
}

static Status handle_splitting_tablet(TabletManager* tablet_manager, const SplittingTabletInfoPB& splitting_tablet,
                                      int64_t base_version, int64_t new_version, const TxnInfoPB& txn_info,
                                      std::unordered_map<int64_t, TabletMetadataPtr>& new_metadatas,
                                      std::unordered_map<int64_t, TabletRangePB>& tablet_ranges) {
    // Check tablet metadata cache
    {
        auto old_tablet_new_metadata_location =
                tablet_manager->tablet_metadata_location(splitting_tablet.old_tablet_id(), new_version);
        auto cached_old_tablet_new_metadata =
                tablet_manager->metacache()->lookup_tablet_metadata(old_tablet_new_metadata_location);
        if (cached_old_tablet_new_metadata == nullptr) {
            goto CONTINUE_HANDLE_SPLITTING_TABLET;
        }

        new_metadatas.emplace(splitting_tablet.old_tablet_id(), std::move(cached_old_tablet_new_metadata));

        for (auto new_tablet_id : splitting_tablet.new_tablet_ids()) {
            auto new_tablet_new_metadata_location =
                    tablet_manager->tablet_metadata_location(new_tablet_id, new_version);
            auto cached_new_tablet_new_metadata =
                    tablet_manager->metacache()->lookup_tablet_metadata(new_tablet_new_metadata_location);
            if (cached_new_tablet_new_metadata == nullptr) {
                new_metadatas.clear();
                tablet_ranges.clear();
                goto CONTINUE_HANDLE_SPLITTING_TABLET;
            }

            tablet_ranges.emplace(new_tablet_id, cached_new_tablet_new_metadata->range());
            new_metadatas.emplace(new_tablet_id, std::move(cached_new_tablet_new_metadata));
        }

        // All new metadatas found in cache, return ok
        return Status::OK();
    }

CONTINUE_HANDLE_SPLITTING_TABLET:

    auto old_tablet_old_metadata_or =
            tablet_manager->get_tablet_metadata(splitting_tablet.old_tablet_id(), base_version, false);
    if (old_tablet_old_metadata_or.status().is_not_found()) {
        // Check new metadata
        auto old_tablet_new_metadata_or =
                tablet_manager->get_tablet_metadata(splitting_tablet.old_tablet_id(), new_version, txn_info.gtid());
        if (!old_tablet_new_metadata_or.ok()) {
            // Return old metadata not found error
            return old_tablet_old_metadata_or.status();
        }

        new_metadatas.emplace(splitting_tablet.old_tablet_id(), std::move(old_tablet_new_metadata_or.value()));

        for (auto new_tablet_id : splitting_tablet.new_tablet_ids()) {
            auto new_tablet_new_metadata_or =
                    tablet_manager->get_tablet_metadata(new_tablet_id, new_version, txn_info.gtid());
            if (!new_tablet_new_metadata_or.ok()) {
                // Return old metadata not found error
                return old_tablet_old_metadata_or.status();
            }

            auto& new_tablet_new_metadata = new_tablet_new_metadata_or.value();
            tablet_ranges.emplace(new_tablet_id, new_tablet_new_metadata->range());
            new_metadatas.emplace(new_tablet_id, std::move(new_tablet_new_metadata));
        }

        // All new metadatas found, return ok
        return Status::OK();
    }

    if (!old_tablet_old_metadata_or.ok()) {
        LOG(WARNING) << "Failed to get tablet: " << splitting_tablet.old_tablet_id() << ", version: " << base_version
                     << ", txn_id: " << txn_info.txn_id() << ", status: " << old_tablet_old_metadata_or.status();
        return old_tablet_old_metadata_or.status();
    }

    const auto& old_tablet_old_metadata = old_tablet_old_metadata_or.value();

    // Old tablet
    {
        auto old_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_old_metadata);
        old_tablet_new_metadata->set_version(new_version);
        old_tablet_new_metadata->set_commit_time(txn_info.commit_time());
        old_tablet_new_metadata->set_gtid(txn_info.gtid());
        set_all_data_files_shared(old_tablet_new_metadata.get());

        new_metadatas.emplace(splitting_tablet.old_tablet_id(), std::move(old_tablet_new_metadata));
    }

    // Get tablet split ranges
    std::vector<TabletRangeInfo> split_ranges;
    Status status = get_tablet_split_ranges(tablet_manager, old_tablet_old_metadata,
                                            splitting_tablet.new_tablet_ids_size(), split_ranges);
    if (!status.ok()) {
        LOG(WARNING) << "Failed to get tablet split ranges, will not split this tablet: "
                     << splitting_tablet.old_tablet_id() << ", version: " << base_version
                     << ", txn_id: " << txn_info.txn_id() << ", status: " << status;

        auto new_tablet_id = splitting_tablet.new_tablet_ids(0);
        auto new_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_old_metadata);
        new_tablet_new_metadata->set_id(new_tablet_id);
        new_tablet_new_metadata->set_version(new_version);
        new_tablet_new_metadata->set_commit_time(txn_info.commit_time());
        new_tablet_new_metadata->set_gtid(txn_info.gtid());
        // New tablet in identical tablet need not to share data files

        new_metadatas.emplace(new_tablet_id, std::move(new_tablet_new_metadata));
        tablet_ranges.emplace(new_tablet_id, old_tablet_old_metadata->range());
        return Status::OK();
    }

    DCHECK(split_ranges.size() == splitting_tablet.new_tablet_ids_size());

    // New tablets
    for (int32_t i = 0; i < splitting_tablet.new_tablet_ids_size(); ++i) {
        auto new_tablet_id = splitting_tablet.new_tablet_ids(i);
        const auto& new_tablet_range = split_ranges[i];

        auto new_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_old_metadata);
        new_tablet_new_metadata->set_id(new_tablet_id);
        new_tablet_new_metadata->set_version(new_version);
        new_tablet_new_metadata->set_commit_time(txn_info.commit_time());
        new_tablet_new_metadata->set_gtid(txn_info.gtid());
        new_tablet_new_metadata->mutable_range()->CopyFrom(new_tablet_range.range);
        set_all_data_files_shared(new_tablet_new_metadata.get());

        // Update num rows and data size for rowsets
        for (auto& rowset_metadata : *new_tablet_new_metadata->mutable_rowsets()) {
            const auto it = new_tablet_range.rowset_stats.find(rowset_metadata.id());
            if (it != new_tablet_range.rowset_stats.end()) {
                rowset_metadata.set_num_rows(it->second.num_rows);
                rowset_metadata.set_data_size(it->second.data_size);
            } else {
                rowset_metadata.set_num_rows(0);
                rowset_metadata.set_data_size(0);
            }
        }

        new_metadatas.emplace(new_tablet_id, std::move(new_tablet_new_metadata));
        tablet_ranges.emplace(new_tablet_id, new_tablet_range.range);
    }

    return Status::OK();
}

static Status handle_merging_tablet(TabletManager* tablet_manager, const MergingTabletInfoPB& merging_tablet,
                                    int64_t base_version, int64_t new_version, const TxnInfoPB& txn_info,
                                    std::unordered_map<int64_t, TabletMetadataPtr>& new_metadatas,
                                    std::unordered_map<int64_t, TabletRangePB>& tablet_ranges) {
    // Check tablet metadata cache
    {
        for (auto old_tablet_id : merging_tablet.old_tablet_ids()) {
            auto old_tablet_new_metadata_location =
                    tablet_manager->tablet_metadata_location(old_tablet_id, new_version);
            auto cached_old_tablet_new_metadata =
                    tablet_manager->metacache()->lookup_tablet_metadata(old_tablet_new_metadata_location);
            if (cached_old_tablet_new_metadata == nullptr) {
                goto CONTINUE_HANDLE_MERGING_TABLET;
            }
            new_metadatas.emplace(old_tablet_id, std::move(cached_old_tablet_new_metadata));
        }

        auto new_tablet_new_metadata_location =
                tablet_manager->tablet_metadata_location(merging_tablet.new_tablet_id(), new_version);
        auto cached_new_tablet_new_metadata =
                tablet_manager->metacache()->lookup_tablet_metadata(new_tablet_new_metadata_location);
        if (cached_new_tablet_new_metadata == nullptr) {
            new_metadatas.clear();
            goto CONTINUE_HANDLE_MERGING_TABLET;
        }
        tablet_ranges.emplace(merging_tablet.new_tablet_id(), cached_new_tablet_new_metadata->range());
        new_metadatas.emplace(merging_tablet.new_tablet_id(), std::move(cached_new_tablet_new_metadata));

        // All new metadatas found in cache, return ok
        return Status::OK();
    }

CONTINUE_HANDLE_MERGING_TABLET:

    std::vector<TabletMetadataPtr> old_tablet_metadatas;
    old_tablet_metadatas.reserve(merging_tablet.old_tablet_ids_size());
    for (auto old_tablet_id : merging_tablet.old_tablet_ids()) {
        auto old_tablet_old_metadata_or = tablet_manager->get_tablet_metadata(old_tablet_id, base_version, false);
        if (old_tablet_old_metadata_or.status().is_not_found()) {
            new_metadatas.clear();
            for (auto retry_tablet_id : merging_tablet.old_tablet_ids()) {
                auto old_tablet_new_metadata_or =
                        tablet_manager->get_tablet_metadata(retry_tablet_id, new_version, txn_info.gtid());
                if (!old_tablet_new_metadata_or.ok()) {
                    return old_tablet_old_metadata_or.status();
                }
                new_metadatas.emplace(retry_tablet_id, std::move(old_tablet_new_metadata_or.value()));
            }
            auto new_tablet_new_metadata_or =
                    tablet_manager->get_tablet_metadata(merging_tablet.new_tablet_id(), new_version, txn_info.gtid());
            if (!new_tablet_new_metadata_or.ok()) {
                return old_tablet_old_metadata_or.status();
            }
            auto& new_tablet_new_metadata = new_tablet_new_metadata_or.value();
            tablet_ranges.emplace(merging_tablet.new_tablet_id(), new_tablet_new_metadata->range());
            new_metadatas.emplace(merging_tablet.new_tablet_id(), std::move(new_tablet_new_metadata));
            return Status::OK();
        }
        if (!old_tablet_old_metadata_or.ok()) {
            LOG(WARNING) << "Failed to get tablet: " << old_tablet_id << ", version: " << base_version
                         << ", txn_id: " << txn_info.txn_id() << ", status: " << old_tablet_old_metadata_or.status();
            return old_tablet_old_metadata_or.status();
        }
        old_tablet_metadatas.emplace_back(std::move(old_tablet_old_metadata_or.value()));
    }

    // Update old tablets to new version and mark shared data files.
    for (const auto& old_tablet_old_metadata : old_tablet_metadatas) {
        auto old_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_old_metadata);
        old_tablet_new_metadata->set_version(new_version);
        old_tablet_new_metadata->set_commit_time(txn_info.commit_time());
        old_tablet_new_metadata->set_gtid(txn_info.gtid());
        set_all_data_files_shared(old_tablet_new_metadata.get(), true);
        new_metadatas.emplace(old_tablet_old_metadata->id(), std::move(old_tablet_new_metadata));
    }

    // Collect tablet metadata in order.
    std::vector<TabletMergeInfo> merge_infos;
    merge_infos.reserve(old_tablet_metadatas.size());
    for (const auto& old_tablet_old_metadata : old_tablet_metadatas) {
        merge_infos.push_back({old_tablet_old_metadata, 0});
    }

    // Build new merged tablet metadata.
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

    // Merge historical schemas.
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

    // Merge ranges.
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
            max_end = std::max(max_end, rowset.id() + std::max(1, rowset.segments_size()));
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

    tablet_ranges.emplace(merging_tablet.new_tablet_id(), new_tablet_metadata->range());
    new_metadatas.emplace(merging_tablet.new_tablet_id(), std::move(new_tablet_metadata));
    return Status::OK();
}

static Status handle_identical_tablet(TabletManager* tablet_manager, const IdenticalTabletInfoPB& identical_tablet,
                                      int64_t base_version, int64_t new_version, const TxnInfoPB& txn_info,
                                      std::unordered_map<int64_t, TabletMetadataPtr>& new_metadatas) {
    // Check tablet metadata cache
    {
        auto old_tablet_new_metadata_location =
                tablet_manager->tablet_metadata_location(identical_tablet.old_tablet_id(), new_version);
        auto cached_old_tablet_new_metadata =
                tablet_manager->metacache()->lookup_tablet_metadata(old_tablet_new_metadata_location);
        if (cached_old_tablet_new_metadata == nullptr) {
            goto CONTINUE_HANDLE_IDENTICAL_TABLET;
        }

        new_metadatas.emplace(identical_tablet.old_tablet_id(), std::move(cached_old_tablet_new_metadata));

        auto new_tablet_new_metadata_location =
                tablet_manager->tablet_metadata_location(identical_tablet.new_tablet_id(), new_version);
        auto cached_new_tablet_new_metadata =
                tablet_manager->metacache()->lookup_tablet_metadata(new_tablet_new_metadata_location);
        if (cached_new_tablet_new_metadata == nullptr) {
            new_metadatas.clear();
            goto CONTINUE_HANDLE_IDENTICAL_TABLET;
        }

        new_metadatas.emplace(identical_tablet.new_tablet_id(), std::move(cached_new_tablet_new_metadata));

        // All new metadatas found in cache, return ok
        return Status::OK();
    }

CONTINUE_HANDLE_IDENTICAL_TABLET:

    auto old_tablet_old_metadata_or =
            tablet_manager->get_tablet_metadata(identical_tablet.old_tablet_id(), base_version, false);
    if (old_tablet_old_metadata_or.status().is_not_found()) {
        // Check new metadata
        auto old_tablet_new_metadata_or =
                tablet_manager->get_tablet_metadata(identical_tablet.old_tablet_id(), new_version, txn_info.gtid());
        if (!old_tablet_new_metadata_or.ok()) {
            // Return old metadata not found error
            return old_tablet_old_metadata_or.status();
        }

        new_metadatas.emplace(identical_tablet.old_tablet_id(), std::move(old_tablet_new_metadata_or.value()));

        auto new_tablet_new_metadata_or =
                tablet_manager->get_tablet_metadata(identical_tablet.new_tablet_id(), new_version, txn_info.gtid());
        if (!new_tablet_new_metadata_or.ok()) {
            // Return old metadata not found error
            return old_tablet_old_metadata_or.status();
        }

        new_metadatas.emplace(identical_tablet.new_tablet_id(), std::move(new_tablet_new_metadata_or.value()));

        // All new metadatas found, return ok
        return Status::OK();
    }

    if (!old_tablet_old_metadata_or.ok()) {
        LOG(WARNING) << "Failed to get tablet: " << identical_tablet.old_tablet_id() << ", version: " << base_version
                     << ", txn_id: " << txn_info.txn_id() << ", status: " << old_tablet_old_metadata_or.status();
        return old_tablet_old_metadata_or.status();
    }

    const auto& old_tablet_old_metadata = old_tablet_old_metadata_or.value();

    // Old tablet
    {
        auto old_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_old_metadata);
        old_tablet_new_metadata->set_version(new_version);
        old_tablet_new_metadata->set_commit_time(txn_info.commit_time());
        old_tablet_new_metadata->set_gtid(txn_info.gtid());
        set_all_data_files_shared(old_tablet_new_metadata.get());

        new_metadatas.emplace(identical_tablet.old_tablet_id(), std::move(old_tablet_new_metadata));
    }

    // New tablet
    {
        auto new_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_old_metadata);
        new_tablet_new_metadata->set_id(identical_tablet.new_tablet_id());
        new_tablet_new_metadata->set_version(new_version);
        new_tablet_new_metadata->set_commit_time(txn_info.commit_time());
        new_tablet_new_metadata->set_gtid(txn_info.gtid());
        // New tablet in identical tablet need not to share data files

        new_metadatas.emplace(identical_tablet.new_tablet_id(), std::move(new_tablet_new_metadata));
    }

    return Status::OK();
}

TxnLogPtr convert_txn_log(const TxnLogPtr& txn_log, const TabletMetadataPtr& base_tablet_metadata,
                          const PublishTabletInfo& publish_tablet_info) {
    if (publish_tablet_info.get_publish_tablet_type() == PublishTabletInfo::PUBLISH_NORMAL) {
        return txn_log;
    }

    // Copy a new txn log from original txn log
    auto new_txn_log = std::make_shared<TxnLogPB>(*txn_log);
    new_txn_log->set_tablet_id(publish_tablet_info.get_tablet_id_in_metadata());

    // For tablet splitting, set all data files shared in new txn log
    if (publish_tablet_info.get_publish_tablet_type() == PublishTabletInfo::SPLITTING_TABLET) {
        set_all_data_files_shared(new_txn_log.get());
    }

    return new_txn_log;
}

Status publish_resharding_tablet(TabletManager* tablet_manager, const ReshardingTabletInfoPB& resharding_tablet,
                                 int64_t base_version, int64_t new_version, const TxnInfoPB& txn_info,
                                 bool skip_write_tablet_metadata,
                                 std::unordered_map<int64_t, TabletMetadataPtr>& tablet_metadatas,
                                 std::unordered_map<int64_t, TabletRangePB>& tablet_ranges) {
    if (resharding_tablet.has_splitting_tablet_info()) {
        RETURN_IF_ERROR(handle_splitting_tablet(tablet_manager, resharding_tablet.splitting_tablet_info(), base_version,
                                                new_version, txn_info, tablet_metadatas, tablet_ranges));
    } else if (resharding_tablet.has_merging_tablet_info()) {
        RETURN_IF_ERROR(handle_merging_tablet(tablet_manager, resharding_tablet.merging_tablet_info(), base_version,
                                              new_version, txn_info, tablet_metadatas, tablet_ranges));
    } else if (resharding_tablet.has_identical_tablet_info()) {
        RETURN_IF_ERROR(handle_identical_tablet(tablet_manager, resharding_tablet.identical_tablet_info(), base_version,
                                                new_version, txn_info, tablet_metadatas));
    }

    for (const auto& [tablet_id, new_metadata] : tablet_metadatas) {
        if (!skip_write_tablet_metadata) {
            RETURN_IF_ERROR(tablet_manager->put_tablet_metadata(new_metadata));
        } else {
            RETURN_IF_ERROR(tablet_manager->cache_tablet_metadata(new_metadata));
            tablet_manager->metacache()->cache_aggregation_partition(
                    tablet_manager->tablet_metadata_root_location(tablet_id), true);
        }
    }

    return Status::OK();
}

} // namespace starrocks::lake
