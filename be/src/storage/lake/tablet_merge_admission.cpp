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

#include "storage/lake/tablet_merge_admission.h"

#include <bvar/bvar.h>
#include <fmt/format.h>

#include <map>
#include <set>
#include <string>
#include <string_view>
#include <utility>

#include "common/logging.h"
#include "gutil/strings/join.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet_range_helper.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/sstable/comparator.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

namespace {

bvar::Adder<int64_t> g_tablet_reshard_merge_rejected_total("tablet_reshard_merge_rejected_total");

// A file belongs to one source at most. Segments are identified by their slice of a file, since file bundling
// puts the segments of several tablets into one file at different offsets. Delete-vector files are left out: the
// merge copies the pages it keeps into a file of its own and never references a source's delete-vector file.
Status check_file_ownership(const std::vector<TabletMetadataPtr>& sources) {
    std::map<std::string, int64_t> owners;
    auto claim = [&owners](std::string_view kind, const std::string& filename, int64_t tablet_id) -> Status {
        auto [iter, inserted] = owners.try_emplace(std::string(kind) + '/' + filename, tablet_id);
        if (inserted || iter->second == tablet_id) return Status::OK();
        return Status::Corruption(fmt::format("source tablets {} and {} both declare {} file {}", iter->second,
                                              tablet_id, kind, filename));
    };
    std::set<std::string> sstables;
    for (const auto& source : sources) {
        const int64_t tablet_id = source->id();
        for (const auto& rowset : source->rowsets()) {
            for (const auto& segment : rowset.segment_metas()) {
                const int64_t offset = segment.has_bundle_file_offset() ? segment.bundle_file_offset() : 0;
                RETURN_IF_ERROR(claim("segment", fmt::format("{}@{}", segment.filename(), offset), tablet_id));
            }
            for (const auto& del : rowset.del_files()) {
                RETURN_IF_ERROR(claim("del", del.name(), tablet_id));
            }
        }
        for (const auto& [rssid, dcg] : source->dcg_meta().dcgs()) {
            (void)rssid;
            for (const auto& file : dcg.column_files()) {
                if (!file.empty()) RETURN_IF_ERROR(claim("DCG", file, tablet_id));
            }
        }
        for (const auto& [rssid, idg] : source->idg_meta().idgs()) {
            (void)rssid;
            for (const auto& entry : idg.entries()) {
                if (!entry.index_file().empty()) RETURN_IF_ERROR(claim("IDG", entry.index_file(), tablet_id));
            }
        }
        for (const auto& sstable : source->sstable_meta().sstables()) {
            RETURN_IF_ERROR(claim("SST", sstable.filename(), tablet_id));
            if (!sstables.insert(sstable.filename()).second) {
                return Status::Corruption(
                        fmt::format("source tablet {} lists SST file {} twice", tablet_id, sstable.filename()));
            }
        }
    }
    return Status::OK();
}

// Rowsets sharing a uid are pieces of one logical rowset: a split hands each child the segments it owns, and a
// merged tablet keeps the pieces as separate rowsets. The pieces must not claim the same segment index, or two of them
// would stand for the same rows. Checked between pieces only; a duplicate inside one rowset is the per-rowset
// validation's to report. Delete files are not compared: their coordinates are absolute RSSIDs of each source's own
// numbering, which an earlier merge may have repacked, and a delete file shared by two pieces is already refused as a
// file two sources declare.
Status check_same_uid_coordinates(const std::vector<TabletMetadataPtr>& sources) {
    struct Piece {
        int64_t tablet_id;
        uint32_t rowset_id;
    };
    using Uid = std::pair<int64_t, int64_t>;
    std::map<Uid, std::map<uint32_t, Piece>> segment_claims;
    for (const auto& source : sources) {
        for (const auto& rowset : source->rowsets()) {
            if (!tablet_reshard_helper::has_valid_uid(rowset)) continue;
            const Uid uid{rowset.uid().hi(), rowset.uid().lo()};
            const Piece here{source->id(), rowset.id()};
            std::set<uint32_t> segment_indices;
            for (int i = 0; i < rowset.segment_metas_size(); ++i) segment_indices.insert(get_segment_idx(rowset, i));
            auto& segments = segment_claims[uid];
            for (uint32_t index : segment_indices) {
                auto [iter, inserted] = segments.try_emplace(index, here);
                if (!inserted) {
                    return Status::Corruption(fmt::format(
                            "source tablet {} rowset {} and source tablet {} rowset {} are the same logical rowset and "
                            "both claim segment index {}",
                            iter->second.tablet_id, iter->second.rowset_id, here.tablet_id, here.rowset_id, index));
                }
            }
        }
    }
    return Status::OK();
}

// The merged tablet starts as a copy of one source, so a table-level setting is right for all of them only if
// they agree on it.
Status check_configuration(const std::vector<TabletMetadataPtr>& sources) {
    const auto& first = *sources.front();
    const std::string first_flat_json_config = first.flat_json_config().SerializeAsString();
    const std::string first_source_schema = first.source_schema().SerializeAsString();
    for (size_t i = 1; i < sources.size(); ++i) {
        const auto& other = *sources[i];
        auto disagree = [&](std::string_view what) {
            return Status::Corruption(
                    fmt::format("source tablets {} and {} disagree on {}", first.id(), other.id(), what));
        };
        if (first.enable_persistent_index() != other.enable_persistent_index()) {
            return disagree("enable_persistent_index");
        }
        if (first.persistent_index_type() != other.persistent_index_type()) return disagree("persistent_index_type");
        if (first.compaction_strategy() != other.compaction_strategy()) return disagree("compaction_strategy");
        if (first_flat_json_config != other.flat_json_config().SerializeAsString()) return disagree("flat_json_config");
        if (first_source_schema != other.source_schema().SerializeAsString()) return disagree("source_schema");
    }
    return Status::OK();
}

// The bounds |source|'s range puts on primary-key index keys, computed once per source rather than once per SST.
StatusOr<SstSeekRange> source_key_range(const TabletMetadataPB& source) {
    auto schema_pb = source.schema();
    schema_pb.set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    auto schema = TabletSchema::create(schema_pb);
    RETURN_IF_ERROR(TabletRangeHelper::validate_range_structural(source.range(), *schema));
    return TabletRangeHelper::create_sst_seek_range_from(source.range(), schema);
}

Status check_sstable_range_within(const TabletMetadataPB& source, const PersistentIndexSstablePB& sstable,
                                  const StatusOr<SstSeekRange>& source_range) {
    if (!sstable.has_range() || !sstable.range().has_start_key() || !sstable.range().has_end_key()) {
        return Status::Corruption(
                fmt::format("source tablet {} SST {} declares no key range", source.id(), sstable.filename()));
    }
    if (!source_range.ok()) {
        return Status::Corruption(fmt::format("source tablet {} range cannot be compared with SST {}: {}", source.id(),
                                              sstable.filename(), source_range.status().message()));
    }
    const auto* comparator = sstable::BytewiseComparator();
    const bool below = source.range().has_lower_bound() &&
                       comparator->Compare(Slice(sstable.range().start_key()), Slice(source_range->seek_key)) < 0;
    const bool above = source.range().has_upper_bound() &&
                       comparator->Compare(Slice(sstable.range().end_key()), Slice(source_range->stop_key)) >= 0;
    if (below || above) {
        return Status::Corruption(fmt::format("source tablet {} SST {} has a key range outside the tablet range",
                                              source.id(), sstable.filename()));
    }
    return Status::OK();
}

} // namespace

Status admit_merge_sources(const std::vector<TabletMetadataPtr>& sources) {
    for (const auto& source : sources) {
        if (!source->has_range()) {
            return Status::Corruption(fmt::format("source tablet {} declares no range", source->id()));
        }
    }
    for (const auto& source : sources) {
        if (tablet_reshard_helper::has_shared_files(*source)) {
            return Status::Corruption(fmt::format("source tablet {} still holds shared data files", source->id()));
        }
    }
    RETURN_IF_ERROR(check_file_ownership(sources));
    for (const auto& source : sources) {
        if (source->sstable_meta().sstables().empty()) continue;
        const auto source_range = source_key_range(*source);
        for (const auto& sstable : source->sstable_meta().sstables()) {
            RETURN_IF_ERROR(check_sstable_range_within(*source, sstable, source_range));
        }
    }
    RETURN_IF_ERROR(check_same_uid_coordinates(sources));
    return check_configuration(sources);
}

// The persistent-index sstables of a source are carried into the merged tablet with one translation per source,
// which is correct only if every key they hold belongs to that source. Every producer of an sstable records its
// key range, so an sstable without one, or with one reaching outside its source, cannot be attributed.
Status check_sstable_range(const TabletMetadataPB& source, const PersistentIndexSstablePB& sstable) {
    return check_sstable_range_within(source, sstable, source_key_range(source));
}

Status merge_rejection(const Status& status, const std::vector<TabletMetadataPtr>& sources, int64_t target_tablet_id) {
    g_tablet_reshard_merge_rejected_total << 1;
    Status rejected = status.clone_and_prepend("MERGE_REJECTED");
    LOG(WARNING) << rejected.message() << "; source tablets ["
                 << JoinMapped(
                            sources, [](const TabletMetadataPtr& source) { return std::to_string(source->id()); }, ",")
                 << "], target tablet " << target_tablet_id;
    return rejected;
}

} // namespace starrocks::lake
