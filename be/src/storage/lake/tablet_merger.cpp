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

#include <fmt/format.h>
#include <google/protobuf/unknown_field_set.h>

#include <algorithm>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "base/failpoint/fail_point.h"
#include "base/testutil/sync_point.h"
#include "base/uid_util.h"
#include "common/logging.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/primary_key_manager.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_merge_admission.h"
#include "storage/lake/tablet_range_helper.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/sstable/comparator.h"
#include "storage/tablet_range.h"

namespace starrocks::lake {

DEFINE_FAIL_POINT(skip_lake_pk_index_merge_source_flush);
DEFINE_FAIL_POINT(tablet_merge_after_write_delvec);

namespace {

// PersistentIndexSstablePB::max_rss_rowid encodes (rssid << 32) | rowid.
constexpr int kRssRowidHighShift = 32;
constexpr uint64_t kRssRowidLowMask = 0xFFFFFFFFULL;

uint32_t extract_rss_rowid_high(uint64_t encoded) {
    return static_cast<uint32_t>(encoded >> kRssRowidHighShift);
}

uint64_t encode_rss_rowid(uint32_t rssid_high, uint64_t rowid_low) {
    return (static_cast<uint64_t>(rssid_high) << kRssRowidHighShift) | (rowid_low & kRssRowidLowMask);
}

constexpr uint64_t kSourceRssidExclusiveLimit = uint64_t{std::numeric_limits<uint32_t>::max()} + 1;
constexpr uint64_t kTargetRssidExclusiveLimit = static_cast<uint64_t>(std::numeric_limits<int32_t>::max());
// A primary-key index records its watermark (rssid << 32 | rowid) as a signed 64-bit value, so it covers RSSIDs below
// 2^31 only.
constexpr uint64_t kWatermarkRssidExclusiveLimit = uint64_t{1} << 31;

// The RSSIDs a rowset occupies: [id, id + span), where span is its highest segment index plus one, or one for a
// rowset without segments.
uint64_t rowset_extent_end(const RowsetMetadataPB& rowset) {
    return uint64_t{rowset.id()} + (rowset.segment_metas_size() == 0 ? 1 : uint64_t{get_max_segment_idx(rowset)} + 1);
}

// The replay point of a delete file: op_offset when present, else the containing rowset's highest segment index,
// which is how the primary-key index replays a delete file without one.
uint32_t effective_op_offset(const RowsetMetadataPB& rowset, const DelfileWithRowsetId& del) {
    return del.has_op_offset() ? del.op_offset() : get_max_segment_idx(rowset);
}

bool uses_cloud_native_pk_index(const TabletMetadataPB& metadata) {
    return is_primary_key(metadata) && metadata.enable_persistent_index() &&
           metadata.persistent_index_type() == PersistentIndexTypePB::CLOUD_NATIVE;
}

// ---------------------------------------------------------------------------------------------------------------
// Translation of one source's RSSIDs into the merged tablet. The RSSIDs a source can reference are grouped into
// runs (overlapping or adjacent intervals are joined); the runs of all sources are packed one after another, so
// every run moves by a single delta and runs of different sources never meet.
// ---------------------------------------------------------------------------------------------------------------

struct RssidRun {
    uint64_t source_begin;
    uint64_t source_end;
    uint32_t target_begin;
};

class RssidProjection {
public:
    Status build(std::vector<std::pair<uint64_t, uint64_t>> atoms, uint32_t target_begin) {
        std::sort(atoms.begin(), atoms.end());
        std::vector<std::pair<uint64_t, uint64_t>> merged;
        for (const auto& [begin, end] : atoms) {
            if (begin >= end || end > kSourceRssidExclusiveLimit) {
                return Status::InvalidArgument("tablet merge source RSSID interval is outside the supported domain");
            }
            if (merged.empty() || begin > merged.back().second) {
                merged.emplace_back(begin, end);
            } else {
                merged.back().second = std::max(merged.back().second, end);
            }
        }
        uint64_t cursor = target_begin;
        for (const auto& [begin, end] : merged) {
            const uint64_t length = end - begin;
            if (cursor + length > kTargetRssidExclusiveLimit) {
                return Status::InvalidArgument("tablet merge exhausts the supported rssid allocation domain");
            }
            _runs.push_back(RssidRun{begin, end, static_cast<uint32_t>(cursor)});
            cursor += length;
        }
        _target_end = static_cast<uint32_t>(cursor);
        return Status::OK();
    }

    StatusOr<uint32_t> map(uint64_t source) const {
        const RssidRun* run = find(source);
        if (run == nullptr) {
            return Status::Corruption(fmt::format("tablet merge RSSID {} is not planned", source));
        }
        return static_cast<uint32_t>(run->target_begin + (source - run->source_begin));
    }

    // The delta every RSSID of this source moves by, when the source is a single run.
    std::optional<int64_t> single_delta() const {
        if (_runs.size() != 1) return std::nullopt;
        return static_cast<int64_t>(_runs.front().target_begin) - static_cast<int64_t>(_runs.front().source_begin);
    }

    uint32_t target_end() const { return _target_end; }

    // One past the highest source RSSID this source references.
    uint64_t source_end() const { return _runs.empty() ? 0 : _runs.back().source_end; }

private:
    const RssidRun* find(uint64_t source) const {
        auto iter = std::upper_bound(_runs.begin(), _runs.end(), source,
                                     [](uint64_t value, const RssidRun& run) { return value < run.source_begin; });
        if (iter == _runs.begin()) return nullptr;
        --iter;
        return source < iter->source_end ? &*iter : nullptr;
    }

    std::vector<RssidRun> _runs;
    uint32_t _target_end = 1;
};

// ---------------------------------------------------------------------------------------------------------------
// Validation of each source on its own.
// ---------------------------------------------------------------------------------------------------------------

Status validate_source_rssid_domain(const TabletMetadataPB& metadata) {
    for (const auto& rowset : metadata.rowsets()) {
        if (rowset_extent_end(rowset) > kSourceRssidExclusiveLimit) {
            return Status::InvalidArgument("tablet merge source rowset extent exceeds the uint32 RSSID domain");
        }
        for (const auto& del : rowset.del_files()) {
            if (uint64_t{del.origin_rowset_id()} + effective_op_offset(rowset, del) + 1 > kSourceRssidExclusiveLimit) {
                return Status::InvalidArgument("tablet merge source delete span exceeds the uint32 RSSID domain");
            }
        }
    }
    if (is_primary_key(metadata)) {
        for (const auto& rowset : metadata.rowsets()) {
            if (rowset.id() == 0) {
                return Status::InvalidArgument(
                        fmt::format("Writable primary-key tablet merge source tablet {} has invalid rowset {}",
                                    metadata.id(), rowset.id()));
            }
        }
    }
    return Status::OK();
}

// The replay span itself was checked against the RSSID domain by validate_source_rssid_domain.
Status validate_del_coordinate(const RowsetMetadataPB& rowset, const DelfileWithRowsetId& del) {
    if (del.origin_rowset_id() != rowset.id()) return Status::OK();
    const uint32_t offset = effective_op_offset(rowset, del);
    if (rowset.segment_metas_size() == 0) {
        return offset == 0 ? Status::OK() : Status::Corruption("segmentless self-origin del offset is not zero");
    }
    return offset <= get_max_segment_idx(rowset)
                   ? Status::OK()
                   : Status::Corruption("self-origin del offset exceeds rowset replay span");
}

bool same_range(const TabletRange& a, const TabletRange& b) {
    return a.lower_bound() == b.lower_bound() && a.upper_bound() == b.upper_bound() &&
           (a.is_minimum() || a.lower_bound_included() == b.lower_bound_included()) &&
           (a.is_maximum() || a.upper_bound_included() == b.upper_bound_included());
}

Status validate_source_rowset(const RowsetMetadataPB& rowset, const TabletMetadataPB& source) {
    if (!tablet_reshard_helper::has_valid_uid(rowset)) {
        return Status::Corruption("tablet merge source rowset has no valid uid");
    }
    if (rowset.num_rows() < 0 || rowset.data_size() < 0 || rowset.num_dels() < 0) {
        return Status::Corruption("tablet merge source rowset has invalid statistics");
    }
    if (rowset.has_delete_predicate() &&
        (rowset.segment_metas_size() != 0 || rowset.del_files_size() != 0 || rowset.num_rows() != 0 ||
         rowset.data_size() != 0 || rowset.num_dels() != 0)) {
        return Status::Corruption("tablet merge predicate rowset has a noncanonical payload");
    }
    if (!rowset.has_delete_predicate() && rowset.segment_metas_size() == 0 && rowset.del_files_size() == 0) {
        return Status::Corruption("tablet merge source has an empty ordinary rowset");
    }
    TabletRange effective;
    TabletRange parent;
    const auto& range = tablet_reshard_helper::effective_old_tablet_local_range(rowset, source);
    RETURN_IF_ERROR(TabletRangeHelper::validate_tablet_range(range));
    RETURN_IF_ERROR(effective.from_proto(range));
    RETURN_IF_ERROR(parent.from_proto(source.range()));
    ASSIGN_OR_RETURN(auto intersection, effective.intersect(parent));
    if (effective.is_empty() || !same_range(effective, intersection)) {
        return Status::Corruption("tablet merge rowset range is empty or outside its source tablet");
    }
    for (int i = 0; i < rowset.segment_metas_size(); ++i) {
        const auto& segment = rowset.segment_metas(i);
        if (!segment.has_num_rows()) {
            return Status::Corruption("tablet merge source segment has no explicit row count");
        }
        if (segment.num_rows() < 0 || segment.size() < 0) {
            return Status::Corruption("tablet merge source segment has negative statistics");
        }
        if (!segment.has_segment_idx()) {
            return Status::Corruption("tablet merge source segment has no explicit segment_idx");
        }
        if (i > 0 && segment.segment_idx() <= rowset.segment_metas(i - 1).segment_idx()) {
            return Status::Corruption("tablet merge source segment indices are not strictly increasing");
        }
    }
    for (const auto& del : rowset.del_files()) RETURN_IF_ERROR(validate_del_coordinate(rowset, del));
    return Status::OK();
}

// The source ranges must be non-empty, pairwise disjoint and contiguous; the target range is their union.
Status validate_merge_inputs(const std::vector<TabletMetadataPtr>& sources) {
    std::vector<TabletRange> ranges(sources.size());
    for (size_t i = 0; i < sources.size(); ++i) {
        const auto& source = *sources[i];
        RETURN_IF_ERROR(TabletRangeHelper::validate_tablet_range(source.range()));
        RETURN_IF_ERROR(ranges[i].from_proto(source.range()));
        if (ranges[i].is_empty()) return Status::Corruption("tablet merge source tablet range is empty");
        for (const auto& rowset : source.rowsets()) RETURN_IF_ERROR(validate_source_rowset(rowset, source));
    }
    std::sort(ranges.begin(), ranges.end(), [](const auto& a, const auto& b) {
        if (a.is_minimum() != b.is_minimum()) return a.is_minimum();
        return a.lower_bound().compare(b.lower_bound()) < 0;
    });
    for (size_t i = 1; i < ranges.size(); ++i) {
        const auto& left = ranges[i - 1];
        const auto& right = ranges[i];
        if (left.is_maximum() || right.is_minimum() || left.upper_bound() != right.lower_bound() ||
            left.upper_bound_included() == right.lower_bound_included()) {
            return Status::Corruption("tablet merge source ranges are not a nonoverlapping contiguous cover");
        }
    }
    return Status::OK();
}

// ---------------------------------------------------------------------------------------------------------------
// Physical segment declarations.
// ---------------------------------------------------------------------------------------------------------------

using PhysicalSlice = std::pair<std::string, int64_t>;

PhysicalSlice physical_slice(const SegmentMetadataPB& segment) {
    return {segment.filename(), segment.has_bundle_file_offset() ? segment.bundle_file_offset() : int64_t{0}};
}

// Everything that describes the bytes of a segment, without its position in a rowset or its ownership flag.
std::string physical_base_key(const SegmentMetadataPB& segment) {
    SegmentMetadataPB normalized(segment);
    normalized.clear_segment_idx();
    normalized.clear_shared();
    return normalized.SerializeAsString();
}

Status validate_physical_segment_shape(const SegmentMetadataPB& segment) {
    constexpr int64_t kSegmentFooterTrailerSize = 12;
    if (!segment.has_filename() || segment.filename().empty()) {
        return Status::Corruption("tablet merge segment has a missing or empty filename");
    }
    if (segment.has_bundle_file_offset() && segment.bundle_file_offset() < 0) {
        return Status::Corruption(fmt::format("tablet merge segment {} has negative bundle_file_offset {}",
                                              segment.filename(), segment.bundle_file_offset()));
    }
    if (segment.has_size() && segment.size() < 0) {
        return Status::Corruption(
                fmt::format("tablet merge segment {} has negative size {}", segment.filename(), segment.size()));
    }
    if (segment.has_bundle_file_offset() && !segment.has_size()) {
        return Status::Corruption(fmt::format("tablet merge segment {} has bundle_file_offset {} but no size",
                                              segment.filename(), segment.bundle_file_offset()));
    }
    if (segment.has_bundle_file_offset()) {
        if (segment.size() < kSegmentFooterTrailerSize) {
            return Status::Corruption(
                    fmt::format("tablet merge bundled segment {} size {} is smaller than the {}-byte footer trailer",
                                segment.filename(), segment.size(), kSegmentFooterTrailerSize));
        }
        const uint64_t bundle_end =
                static_cast<uint64_t>(segment.bundle_file_offset()) + static_cast<uint64_t>(segment.size());
        if (bundle_end > std::numeric_limits<int64_t>::max()) {
            return Status::Corruption(
                    fmt::format("tablet merge segment {} bundle_file_offset {} plus size {} exceeds int64 range",
                                segment.filename(), segment.bundle_file_offset(), segment.size()));
        }
    }
    return Status::OK();
}

// Segment indices are strictly increasing by now, so a slice listed twice in one rowset is listed under two indices.
Status validate_physical_rowset_shape(const RowsetMetadataPB& rowset) {
    std::optional<bool> bundled;
    std::set<PhysicalSlice> slices;
    for (int position = 0; position < rowset.segment_metas_size(); ++position) {
        const auto& segment = rowset.segment_metas(position);
        RETURN_IF_ERROR(validate_physical_segment_shape(segment));
        if (!bundled.has_value()) {
            bundled = segment.has_bundle_file_offset();
        } else if (*bundled != segment.has_bundle_file_offset()) {
            return Status::Corruption(
                    fmt::format("tablet merge rowset {} mixes bundled and standalone segments", rowset.id()));
        }
        const auto slice = physical_slice(segment);
        if (!slices.insert(slice).second) {
            return Status::Corruption(fmt::format(
                    "tablet merge rowset {} references physical segment slice {} at bundle offset {} from multiple "
                    "segment indices",
                    rowset.id(), slice.first, slice.second));
        }
    }
    return Status::OK();
}

// Across every rowset of every source: one file is either bundled or standalone, the bundled slices of one file do
// not overlap, and one slice is described the same way wherever it is declared. A slice declared twice by one
// source is refused here; one declared by two sources is the admission check's to refuse.
Status validate_physical_slices(const std::vector<TabletMetadataPtr>& sources) {
    struct FileDeclaration {
        bool bundled = false;
        std::map<int64_t, int64_t> bundled_intervals;
    };
    struct SliceDeclaration {
        const SegmentMetadataPB* segment;
        int64_t tablet_id;
    };
    std::map<std::string, FileDeclaration> files;
    std::map<PhysicalSlice, SliceDeclaration> slices;
    for (const auto& source : sources) {
        for (const auto& rowset : source->rowsets()) {
            RETURN_IF_ERROR(validate_physical_rowset_shape(rowset));
            for (const auto& segment : rowset.segment_metas()) {
                auto [file, file_inserted] = files.try_emplace(
                        segment.filename(), FileDeclaration{.bundled = segment.has_bundle_file_offset()});
                if (!file_inserted && file->second.bundled != segment.has_bundle_file_offset()) {
                    return Status::Corruption(
                            fmt::format("tablet merge physical segment file {} mixes bundled and standalone forms",
                                        segment.filename()));
                }
                if (segment.has_bundle_file_offset()) {
                    const int64_t bundle_end =
                            static_cast<int64_t>(static_cast<uint64_t>(segment.bundle_file_offset()) +
                                                 static_cast<uint64_t>(segment.size()));
                    file->second.bundled_intervals.try_emplace(segment.bundle_file_offset(), bundle_end);
                }
                const auto slice = physical_slice(segment);
                auto [declared, inserted] = slices.try_emplace(slice, SliceDeclaration{&segment, source->id()});
                if (inserted) continue;
                if (physical_base_key(*declared->second.segment) != physical_base_key(segment)) {
                    return Status::Corruption(fmt::format(
                            "tablet merge physical segment slice {} at bundle offset {} has conflicting declarations",
                            slice.first, slice.second));
                }
                // A rowset never lists a slice twice (checked above), so an earlier declaration by the same source
                // comes from another of its rowsets.
                if (declared->second.tablet_id == source->id()) {
                    return Status::Corruption(fmt::format(
                            "tablet merge source tablet {} declares physical segment slice {} at bundle offset {} in "
                            "more than one rowset",
                            source->id(), slice.first, slice.second));
                }
            }
        }
    }
    for (const auto& [filename, declaration] : files) {
        std::optional<std::pair<int64_t, int64_t>> previous;
        for (const auto& [begin, end] : declaration.bundled_intervals) {
            if (previous.has_value() && begin < previous->second) {
                return Status::Corruption(fmt::format(
                        "tablet merge physical segment file {} has overlapping bundled slices [{}, {}) and [{}, {})",
                        filename, previous->first, previous->second, begin, end));
            }
            previous = std::pair{begin, end};
        }
    }
    return Status::OK();
}

// ---------------------------------------------------------------------------------------------------------------
// Sidecar declarations: DCG (.cols), IDG (.idx), delete vectors, primary-key index sstables.
// ---------------------------------------------------------------------------------------------------------------

Status validate_dcg_shape(const DeltaColumnGroupVerPB& dcg) {
    if (dcg.GetReflection()->GetUnknownFields(dcg).field_count() != 0) {
        return Status::Corruption("DCG message has unknown fields that cannot be attributed to an entry");
    }
    if (dcg.unique_column_ids_size() != dcg.column_files_size() || dcg.versions_size() != dcg.column_files_size()) {
        return Status::Corruption("DCG shape invalid: column_files/unique_column_ids/versions size mismatch");
    }
    if (dcg.encryption_metas_size() > dcg.column_files_size() || dcg.shared_files_size() > dcg.column_files_size() ||
        dcg.column_file_sizes_size() > dcg.column_files_size()) {
        return Status::Corruption("DCG shape invalid: optional fields exceed column_files size");
    }
    std::unordered_set<uint32_t> all_cids;
    for (const auto& unique_column_ids : dcg.unique_column_ids()) {
        for (auto cid : unique_column_ids.column_ids()) {
            if (!all_cids.insert(cid).second) {
                return Status::Corruption("DCG contains duplicate column UID across entries");
            }
        }
    }
    return Status::OK();
}

void normalize_dcg_optional_fields(DeltaColumnGroupVerPB* dcg) {
    while (dcg->encryption_metas_size() < dcg->column_files_size()) dcg->add_encryption_metas("");
    while (dcg->shared_files_size() < dcg->column_files_size()) dcg->add_shared_files(false);
    // 0 means "size unknown": readers fall back to stat/HeadObject.
    while (dcg->column_file_sizes_size() < dcg->column_files_size()) dcg->add_column_file_sizes(0);
}

DeltaColumnGroupVerPB make_single_entry_dcg(const DeltaColumnGroupVerPB& source, int entry_index) {
    DeltaColumnGroupVerPB out;
    out.add_column_files(source.column_files(entry_index));
    out.add_unique_column_ids()->CopyFrom(source.unique_column_ids(entry_index));
    out.add_versions(source.versions(entry_index));
    out.add_encryption_metas(source.encryption_metas(entry_index));
    out.add_shared_files(source.shared_files(entry_index));
    out.add_column_file_sizes(source.column_file_sizes(entry_index));
    return out;
}

uint64_t idg_pack_key(int32_t col_uid, IndexType type) {
    return (static_cast<uint64_t>(static_cast<uint32_t>(col_uid)) << 32) | static_cast<uint32_t>(type);
}

// An entry is alive while some key it declares is not dropped. The read path loads the same set.
bool idg_entry_has_active_key(const IndexDeltaGroupEntryPB& entry) {
    std::unordered_set<uint64_t> dropped;
    for (const auto& key : entry.dropped_keys()) dropped.insert(idg_pack_key(key.col_unique_id(), key.index_type()));
    for (const auto& key : entry.keys()) {
        if (!dropped.contains(idg_pack_key(key.col_unique_id(), key.index_type()))) return true;
    }
    return false;
}

Status validate_idg_entry_shape(const IndexDeltaGroupEntryPB& entry) {
    if (!entry.has_index_file() || entry.index_file().empty()) {
        return Status::Corruption("tablet merge IDG entry has a missing or empty filename");
    }
    if (entry.has_file_size() && entry.file_size() < 0) {
        return Status::Corruption(fmt::format("tablet merge IDG file {} has a negative size", entry.index_file()));
    }
    std::unordered_set<uint64_t> declared_keys;
    for (const auto& key : entry.keys()) {
        if (!key.has_col_unique_id() || !key.has_index_type()) {
            return Status::Corruption(
                    fmt::format("tablet merge IDG file {} has an incomplete declared key", entry.index_file()));
        }
        if (!declared_keys.insert(idg_pack_key(key.col_unique_id(), key.index_type())).second) {
            return Status::Corruption(
                    fmt::format("tablet merge IDG file {} has a duplicate declared key", entry.index_file()));
        }
    }
    for (const auto& key : entry.dropped_keys()) {
        if (!key.has_col_unique_id() || !key.has_index_type()) {
            return Status::Corruption(
                    fmt::format("tablet merge IDG file {} has an incomplete dropped key", entry.index_file()));
        }
    }
    return Status::OK();
}

// One sidecar file is described the same way, and attached to the same segment bytes, wherever it is declared.
class SidecarDeclarations {
public:
    Status add(std::string_view kind, const std::string& filename, const std::string& declaration,
               const SegmentMetadataPB* segment) {
        auto [iter, inserted] = _declarations.try_emplace(filename, Declaration{declaration, segment});
        if (inserted) return Status::OK();
        if (iter->second.segment != segment &&
            physical_base_key(*iter->second.segment) != physical_base_key(*segment)) {
            return Status::Corruption(
                    fmt::format("tablet merge {} file {} is attached to a different physical base", kind, filename));
        }
        if (iter->second.declaration != declaration) {
            return Status::Corruption(fmt::format(
                    "tablet merge {} file {} has conflicting physical declarations (unique_column_ids/keys, version, "
                    "size, encryption, presence, or unknown fields differ)",
                    kind, filename));
        }
        return Status::OK();
    }

private:
    struct Declaration {
        std::string declaration;
        const SegmentMetadataPB* segment;
    };
    std::map<std::string, Declaration> _declarations;
};

bool delvec_file_metadata_matches(const FileMetaPB& left, const FileMetaPB& right) {
    return left.has_size() == right.has_size() && (!left.has_size() || left.size() == right.size()) &&
           left.has_shared() == right.has_shared() && (!left.has_shared() || left.shared() == right.shared());
}

StatusOr<uint32_t> effective_shared_rssid(const PersistentIndexSstablePB& sst) {
    const int64_t effective = static_cast<int64_t>(sst.shared_rssid()) + sst.rssid_offset();
    if (effective < 0 || effective > std::numeric_limits<uint32_t>::max()) {
        return Status::Corruption(
                fmt::format("Shared sstable {} effective rssid is out of uint32 range: shared_rssid={} "
                            "rssid_offset={} effective={}",
                            sst.filename(), sst.shared_rssid(), sst.rssid_offset(), effective));
    }
    return static_cast<uint32_t>(effective);
}

bool has_valid_modern_sstable_shared_version(const PersistentIndexSstablePB& sst) {
    return sst.has_shared_version() && sst.shared_version() > 0;
}

Status validate_non_shared_legacy_sstable_form(const PersistentIndexSstablePB& sst) {
    if (sst.has_shared_version() && sst.shared_version() > 0 && !sst.has_shared_rssid()) {
        return Status::Corruption("non-shared legacy sstable has shared_version without shared_rssid");
    }
    if (sst.has_delvec() && sst.delvec().size() > 0) {
        return Status::Corruption("non-shared sstable has delvec but no shared_rssid");
    }
    return Status::OK();
}

// The two forms a primary-key index sstable can take. Modern: shared_rssid and a positive shared_version, every
// value read back is owned by shared_rssid + rssid_offset. Legacy: neither, and no embedded delete vector, every
// value carries its own RSSID, shifted by rssid_offset. Anything in between is refused.
Status validate_sstable_declaration(const PersistentIndexSstablePB& sst) {
    if (!sst.has_filename() || sst.filename().empty()) {
        return Status::Corruption("tablet merge source SST has a missing or empty filename");
    }
    if (sst.has_filesize() && sst.filesize() < 0) {
        return Status::Corruption(fmt::format("tablet merge source SST {} has a negative size", sst.filename()));
    }
    if (sst.has_shared_rssid()) {
        if (!has_valid_modern_sstable_shared_version(sst)) {
            const std::string version = sst.has_shared_version() ? std::to_string(sst.shared_version()) : "missing";
            return Status::Corruption(fmt::format(
                    "tablet merge source SST {} has invalid shared_version {}; modern shared form requires a "
                    "positive shared_version",
                    sst.filename(), version));
        }
        auto effective = effective_shared_rssid(sst);
        if (!effective.ok()) {
            return Status::Corruption(fmt::format("tablet merge source SST {} has an invalid shared form: {}",
                                                  sst.filename(), effective.status().message()));
        }
    } else {
        auto legacy_status = validate_non_shared_legacy_sstable_form(sst);
        if (!legacy_status.ok()) {
            return Status::Corruption(fmt::format("tablet merge source SST {} has an invalid legacy form: {}",
                                                  sst.filename(), legacy_status.message()));
        }
    }
    if (sst.has_range()) {
        if (!sst.range().has_start_key() || !sst.range().has_end_key()) {
            return Status::Corruption(
                    fmt::format("tablet merge source SST {} has an incomplete range", sst.filename()));
        }
        if (sstable::BytewiseComparator()->Compare(Slice(sst.range().start_key()), Slice(sst.range().end_key())) > 0) {
            return Status::Corruption(fmt::format("tablet merge source SST {} has a reversed range", sst.filename()));
        }
    }
    return Status::OK();
}

PersistentIndexSstablePB normalized_sstable_form(const PersistentIndexSstablePB& sst) {
    PersistentIndexSstablePB normalized(sst);
    normalized.clear_version();
    normalized.clear_filename();
    normalized.clear_filesize();
    normalized.clear_max_rss_rowid();
    normalized.clear_encryption_meta();
    normalized.clear_shared();
    normalized.clear_shared_rssid();
    normalized.clear_shared_version();
    normalized.clear_delvec();
    normalized.clear_fileset_id();
    normalized.clear_rssid_offset();
    normalized.clear_generation_version();
    return normalized;
}

// Each source's sstables must be well formed, and listed in non-decreasing watermark order inside the signed
// domain: the source flush commits the index with exactly these checks, so they are made here, before anything is
// written. Two declarations of one file name must describe the same file.
Status validate_sstable_declarations(const std::vector<TabletMetadataPtr>& sources) {
    std::map<std::string, const PersistentIndexSstablePB*> physical_sstables;
    for (const auto& source : sources) {
        uint64_t previous_watermark = 0;
        for (const auto& sst : source->sstable_meta().sstables()) {
            RETURN_IF_ERROR(validate_sstable_declaration(sst));
            if (sst.max_rss_rowid() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                return Status::InvalidArgument("tablet merge SST watermark exceeds the supported signed domain");
            }
            if (sst.max_rss_rowid() < previous_watermark) {
                return Status::Corruption(
                        fmt::format("tablet merge source tablet {} lists SST {} below the watermark of an earlier one",
                                    source->id(), sst.filename()));
            }
            previous_watermark = sst.max_rss_rowid();
            auto [iter, inserted] = physical_sstables.try_emplace(sst.filename(), &sst);
            if (inserted) continue;
            const auto& existing = *iter->second;
            const bool size_matches = existing.has_filesize() == sst.has_filesize() &&
                                      (!existing.has_filesize() || existing.filesize() == sst.filesize());
            const bool encryption_matches =
                    existing.has_encryption_meta() == sst.has_encryption_meta() &&
                    (!existing.has_encryption_meta() || existing.encryption_meta() == sst.encryption_meta());
            if (!size_matches || !encryption_matches) {
                return Status::Corruption(
                        fmt::format("tablet merge source SST {} has conflicting physical metadata", sst.filename()));
            }
            if (normalized_sstable_form(existing).SerializeAsString() !=
                normalized_sstable_form(sst).SerializeAsString()) {
                return Status::Corruption(
                        fmt::format("tablet merge source SST {} has conflicting form or range", sst.filename()));
            }
        }
    }
    return Status::OK();
}

// ---------------------------------------------------------------------------------------------------------------
// The merge plan: everything decided before the first write.
// ---------------------------------------------------------------------------------------------------------------

struct RowsetRef {
    size_t source;
    int rowset;
};

// A rowset of the merged tablet. A delete predicate published to the whole partition at one version reaches every
// source as its own rowset; those copies are one rowset of the merged tablet, emitted from one of them.
struct RowsetNode {
    std::vector<RowsetRef> refs;
    using OrderKey = std::tuple<int64_t, int, int64_t, int64_t, std::string, int64_t, int>;
    OrderKey key;
};

struct MergePlan {
    std::vector<TabletMetadataPtr> sources; // in tablet id order
    std::vector<RssidProjection> projections;
    MutableTabletMetadataPtr target;
    std::map<uint32_t, DelvecPageInfo> delvec_pages; // by target RSSID
    bool rebuild_index = false; // the merged tablet drops the source sstables and rebuilds its index
};

// Delete predicates of one version are the same delete published to every tablet of the partition, so their copies
// must agree byte for byte and map to the same schema.
StatusOr<std::vector<RowsetNode>> group_rowsets(const std::vector<TabletMetadataPtr>& sources) {
    std::vector<RowsetNode> nodes;
    std::map<int64_t, size_t> predicate_node_by_version;
    for (size_t s = 0; s < sources.size(); ++s) {
        const auto& source = *sources[s];
        for (int r = 0; r < source.rowsets_size(); ++r) {
            const auto& rowset = source.rowsets(r);
            if (!rowset.has_delete_predicate()) {
                nodes.push_back(RowsetNode{{RowsetRef{s, r}}, {}});
                continue;
            }
            auto [iter, inserted] = predicate_node_by_version.try_emplace(rowset.version(), nodes.size());
            if (inserted) {
                nodes.push_back(RowsetNode{{RowsetRef{s, r}}, {}});
                continue;
            }
            const auto& first_ref = nodes[iter->second].refs.front();
            const auto& first_source = *sources[first_ref.source];
            const auto& first = first_source.rowsets(first_ref.rowset);
            auto schema_of = [](const TabletMetadataPB& metadata, uint32_t rowset_id) -> std::optional<int64_t> {
                auto found = metadata.rowset_to_schema().find(rowset_id);
                if (found == metadata.rowset_to_schema().end()) return std::nullopt;
                return found->second;
            };
            if (schema_of(first_source, first.id()) != schema_of(source, rowset.id())) {
                return Status::Corruption("tablet merge duplicate rowset schema mapping conflict");
            }
            if (first.delete_predicate().SerializeAsString() != rowset.delete_predicate().SerializeAsString()) {
                return Status::Corruption("tablet merge duplicate delete-predicate declaration conflict");
            }
            nodes[iter->second].refs.push_back(RowsetRef{s, r});
        }
    }
    for (auto& node : nodes) {
        const auto& ref = node.refs.front();
        const auto& source = *sources[ref.source];
        const auto& rowset = source.rowsets(ref.rowset);
        const auto& range = tablet_reshard_helper::effective_old_tablet_local_range(rowset, source);
        // A merged delete predicate is identified by its version; a data rowset by its uid.
        const bool predicate = rowset.has_delete_predicate();
        node.key = RowsetNode::OrderKey{rowset.version(),
                                        predicate ? 1 : 0,
                                        predicate ? rowset.version() : rowset.uid().hi(),
                                        predicate ? 0 : rowset.uid().lo(),
                                        range.lower_bound().SerializeAsString(),
                                        source.id(),
                                        ref.rowset};
    }
    return nodes;
}

// The order of a lake tablet's rowsets array is its recency order: a delete predicate covers the rowsets before it,
// and merge-on-read and index rebuild let a later rowset win. RowsetMetadataPB.version is not that order (a
// compaction output keeps its first input's slot but carries the newest version, and a batch publish gives several
// rowsets one version). So the merged order is a linear extension of every source's own array order; among rowsets
// left unconstrained the smallest key goes first, version first. Sources that disagree have no linear extension.
StatusOr<std::vector<size_t>> order_rowsets(const std::vector<TabletMetadataPtr>& sources,
                                            const std::vector<RowsetNode>& nodes) {
    std::vector<std::vector<int>> node_of(sources.size());
    for (size_t s = 0; s < sources.size(); ++s) node_of[s].assign(sources[s]->rowsets_size(), -1);
    for (size_t n = 0; n < nodes.size(); ++n) {
        for (const auto& ref : nodes[n].refs) node_of[ref.source][ref.rowset] = static_cast<int>(n);
    }
    std::vector<std::vector<size_t>> successors(nodes.size());
    for (size_t s = 0; s < sources.size(); ++s) {
        // A source can visit one predicate node twice (DELETE, INSERT, DELETE in one batch publish). Its later
        // position is the one that holds: that delete covers the first one's rows plus the write between them.
        // Keeping only each node's last visit removes the false cycle and can create none.
        const auto& walk = node_of[s];
        std::unordered_map<int, size_t> last_visit;
        for (size_t i = 0; i < walk.size(); ++i) last_visit[walk[i]] = i;
        int previous = -1;
        for (size_t i = 0; i < walk.size(); ++i) {
            if (last_visit[walk[i]] != i) continue;
            if (previous >= 0) successors[previous].push_back(walk[i]);
            previous = walk[i];
        }
    }
    std::vector<size_t> indegree(nodes.size(), 0);
    for (auto& edges : successors) {
        std::sort(edges.begin(), edges.end());
        edges.erase(std::unique(edges.begin(), edges.end()), edges.end());
        for (size_t next : edges) ++indegree[next];
    }
    std::set<std::pair<RowsetNode::OrderKey, size_t>> ready;
    for (size_t n = 0; n < nodes.size(); ++n) {
        if (indegree[n] == 0) ready.emplace(nodes[n].key, n);
    }
    std::vector<size_t> ordered;
    ordered.reserve(nodes.size());
    while (!ready.empty()) {
        const size_t node = ready.begin()->second;
        ready.erase(ready.begin());
        ordered.push_back(node);
        for (size_t next : successors[node]) {
            if (--indegree[next] == 0) ready.emplace(nodes[next].key, next);
        }
    }
    if (ordered.size() != nodes.size()) {
        std::string witness;
        for (size_t n = 0; n < nodes.size() && witness.empty(); ++n) {
            if (indegree[n] == 0) continue;
            const auto& ref = nodes[n].refs.front();
            const auto& rowset = sources[ref.source]->rowsets(ref.rowset);
            witness = fmt::format("rowset uid {}-{} at index {} of source tablet {}", rowset.uid().hi(),
                                  rowset.uid().lo(), ref.rowset, sources[ref.source]->id());
        }
        return Status::Corruption(fmt::format(
                "tablet merge source tablets disagree on rowset order: {} of {} rowsets unordered, first blocked {}",
                nodes.size() - ordered.size(), nodes.size(), witness));
    }
    return ordered;
}

// The RSSIDs each source references, packed source by source in tablet id order. Runs of a source are packed
// densely, except that a cloud-native primary-key source is one run from the lowest to the highest RSSID it
// references: a legacy index sstable carries one rssid_offset for all its entries, so every RSSID those entries can
// name must move by the same delta, including ones written to the sstable before and after the source's flush.
Status allocate_rssids(MergePlan* plan, const std::vector<RowsetNode>& nodes) {
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> atoms(plan->sources.size());
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> extents(plan->sources.size());
    for (const auto& node : nodes) {
        // Only the emitted copy of a merged delete predicate needs an RSSID.
        const auto& ref = node.refs.front();
        const auto& rowset = plan->sources[ref.source]->rowsets(ref.rowset);
        auto& source_atoms = atoms[ref.source];
        source_atoms.emplace_back(rowset.id(), rowset_extent_end(rowset));
        extents[ref.source].push_back(source_atoms.back());
        if (rowset.has_max_compact_input_rowset_id()) {
            source_atoms.emplace_back(rowset.max_compact_input_rowset_id(),
                                      uint64_t{rowset.max_compact_input_rowset_id()} + 1);
        }
        for (const auto& del : rowset.del_files()) {
            source_atoms.emplace_back(del.origin_rowset_id(),
                                      uint64_t{del.origin_rowset_id()} + effective_op_offset(rowset, del) + 1);
        }
    }
    for (size_t s = 0; s < plan->sources.size(); ++s) {
        // Rowsets of one source never share an RSSID.
        auto& source_extents = extents[s];
        std::sort(source_extents.begin(), source_extents.end());
        for (size_t i = 1; i < source_extents.size(); ++i) {
            if (source_extents[i].first < source_extents[i - 1].second) {
                return Status::Corruption("tablet merge emitted rowset extents overlap");
            }
        }
        if (uses_cloud_native_pk_index(*plan->sources[s]) && !atoms[s].empty()) {
            uint64_t begin = kSourceRssidExclusiveLimit;
            uint64_t end = 0;
            for (const auto& atom : atoms[s]) {
                begin = std::min(begin, atom.first);
                end = std::max(end, atom.second);
            }
            atoms[s].emplace_back(begin, end);
        }
    }
    plan->projections.resize(plan->sources.size());
    uint32_t cursor = 1;
    for (size_t s = 0; s < plan->sources.size(); ++s) {
        RETURN_IF_ERROR(plan->projections[s].build(std::move(atoms[s]), cursor));
        cursor = plan->projections[s].target_end();
    }
    plan->target->set_next_rowset_id(cursor);
    return Status::OK();
}

// Rowsets in merged order with their absolute coordinates translated. segment_idx and op_offset are relative to
// their rowset and stay as they are, as does a missing op_offset. Each rowset keeps the range it covered in its
// source; a merged delete predicate covers the union of its copies' ranges, so that a later split hands it to
// every child whose range it covers.
Status materialize_rowsets(MergePlan* plan, const std::vector<RowsetNode>& nodes, const std::vector<size_t>& order) {
    auto* target = plan->target.get();
    for (size_t n : order) {
        const auto& node = nodes[n];
        const auto& ref = node.refs.front();
        const auto& source = *plan->sources[ref.source];
        const auto& projection = plan->projections[ref.source];
        RowsetMetadataPB output(source.rowsets(ref.rowset));
        RETURN_IF_ERROR(tablet_reshard_helper::update_rowset_range(&output, source.range()));
        for (size_t i = 1; i < node.refs.size(); ++i) {
            const auto& other_source = *plan->sources[node.refs[i].source];
            const auto& other = other_source.rowsets(node.refs[i].rowset);
            ASSIGN_OR_RETURN(auto united,
                             tablet_reshard_helper::union_range(
                                     output.range(),
                                     tablet_reshard_helper::effective_old_tablet_local_range(other, other_source)));
            output.mutable_range()->CopyFrom(united);
        }
        const uint32_t source_id = output.id();
        ASSIGN_OR_RETURN(auto target_id, projection.map(source_id));
        output.set_id(target_id);
        if (output.has_max_compact_input_rowset_id()) {
            ASSIGN_OR_RETURN(auto recovery, projection.map(output.max_compact_input_rowset_id()));
            output.set_max_compact_input_rowset_id(recovery);
        }
        for (auto& del : *output.mutable_del_files()) {
            ASSIGN_OR_RETURN(auto origin, projection.map(del.origin_rowset_id()));
            del.set_origin_rowset_id(origin);
        }
        auto schema = source.rowset_to_schema().find(source_id);
        if (schema != source.rowset_to_schema().end()) {
            (*target->mutable_rowset_to_schema())[target_id] = schema->second;
        }
        target->add_rowsets()->Swap(&output);
    }
    return Status::OK();
}

// The segments of the merged tablet by RSSID (pointing into the plan's source metadata), and for each source the
// RSSID each of its segments now has.
Status map_segments(const MergePlan& plan, std::map<uint32_t, const SegmentMetadataPB*>* target_segments,
                    std::vector<std::map<uint32_t, uint32_t>>* source_to_target) {
    source_to_target->assign(plan.sources.size(), {});
    for (size_t s = 0; s < plan.sources.size(); ++s) {
        for (const auto& rowset : plan.sources[s]->rowsets()) {
            for (int i = 0; i < rowset.segment_metas_size(); ++i) {
                const uint32_t source_rssid = get_rssid(rowset, i);
                ASSIGN_OR_RETURN(auto target_rssid, plan.projections[s].map(source_rssid));
                auto [iter, inserted] = target_segments->try_emplace(target_rssid, &rowset.segment_metas(i));
                if (!inserted) {
                    return Status::Corruption(
                            fmt::format("tablet merge target RSSID {} has duplicate physical bases", target_rssid));
                }
                (*source_to_target)[s][source_rssid] = target_rssid;
            }
        }
    }
    return Status::OK();
}

// Test hook: drops the target of one projected sidecar key, so a test can show the closure check is what refuses
// such a plan.
bool omit_target_for_test(const char* sync_point) {
    bool omit = false;
    TEST_SYNC_POINT_CALLBACK(sync_point, &omit);
    return omit;
}

// DCG entries move to their segment's new RSSID unchanged. A DCG on a segment its source no longer has is refused:
// unlike an index or delete-vector entry, a column file overrides values, and nothing proves which segment it was
// meant for.
Status project_dcgs(MergePlan* plan, const std::vector<std::map<uint32_t, uint32_t>>& source_to_target,
                    const std::map<uint32_t, const SegmentMetadataPB*>& target_segments,
                    SidecarDeclarations* declarations) {
    auto* dcgs = plan->target->mutable_dcg_meta()->mutable_dcgs();
    for (size_t s = 0; s < plan->sources.size(); ++s) {
        for (const auto& [source_rssid, source_dcg] : plan->sources[s]->dcg_meta().dcgs()) {
            auto segment = source_to_target[s].find(source_rssid);
            if (segment == source_to_target[s].end()) {
                return Status::Corruption(
                        fmt::format("tablet merge stale DCG RSSID {} has no live source segment", source_rssid));
            }
            auto target_segment = target_segments.find(segment->second);
            if (omit_target_for_test("tablet_merge_test:force_dcg_target_omission") ||
                target_segment == target_segments.end()) {
                return Status::Corruption(
                        fmt::format("tablet merge source-live DCG RSSID {} target was omitted", source_rssid));
            }
            DeltaColumnGroupVerPB normalized(source_dcg);
            RETURN_IF_ERROR(validate_dcg_shape(normalized));
            normalize_dcg_optional_fields(&normalized);
            for (int i = 0; i < normalized.column_files_size(); ++i) {
                if (normalized.column_files(i).empty()) {
                    return Status::Corruption("tablet merge DCG entry has an empty filename");
                }
                if (normalized.column_file_sizes(i) < 0) {
                    return Status::Corruption(
                            fmt::format("tablet merge DCG file {} has a negative size", normalized.column_files(i)));
                }
                RETURN_IF_ERROR(declarations->add("DCG", normalized.column_files(i),
                                                  make_single_entry_dcg(normalized, i).SerializeAsString(),
                                                  target_segment->second));
            }
            (*dcgs)[segment->second] = std::move(normalized);
        }
    }
    return Status::OK();
}

// IDG entries move to their segment's new RSSID. An entry on a segment its source no longer has is left out: its file
// stays in the source's metadata, which the merge marks shared, so the shared-file cleanup reclaims it once the
// source is dropped. An entry whose keys are all dropped is not installed, and its file is handed
// to vacuum through orphan_files: vacuum deletes an orphan without looking at live entries, so a file may be
// referenced by one live entry at most.
Status project_idgs(MergePlan* plan, const std::vector<std::map<uint32_t, uint32_t>>& source_to_target,
                    const std::map<uint32_t, const SegmentMetadataPB*>& target_segments,
                    SidecarDeclarations* declarations) {
    std::map<uint32_t, std::vector<IndexDeltaGroupEntryPB>> entries_by_target;
    std::set<std::string> files;
    for (size_t s = 0; s < plan->sources.size(); ++s) {
        for (const auto& [source_rssid, source_idg] : plan->sources[s]->idg_meta().idgs()) {
            auto segment = source_to_target[s].find(source_rssid);
            if (segment == source_to_target[s].end()) continue;
            auto target_segment = target_segments.find(segment->second);
            if (omit_target_for_test("tablet_merge_test:force_idg_target_omission") ||
                target_segment == target_segments.end()) {
                return Status::Corruption(
                        fmt::format("tablet merge source-live IDG RSSID {} target was omitted", source_rssid));
            }
            for (const auto& entry : source_idg.entries()) {
                RETURN_IF_ERROR(validate_idg_entry_shape(entry));
                IndexDeltaGroupEntryPB stable(entry);
                stable.clear_dropped_keys();
                stable.clear_shared_file();
                RETURN_IF_ERROR(declarations->add("IDG", entry.index_file(), stable.SerializeAsString(),
                                                  target_segment->second));
                if (!files.insert(entry.index_file()).second) {
                    return Status::Corruption(fmt::format(
                            "tablet merge IDG file {} is referenced by more than one index entry", entry.index_file()));
                }
                entries_by_target[segment->second].push_back(entry);
            }
        }
    }
    if (entries_by_target.empty()) return Status::OK();
    auto* idgs = plan->target->mutable_idg_meta()->mutable_idgs();
    for (auto& [target_rssid, entries] : entries_by_target) {
        std::stable_sort(entries.begin(), entries.end(),
                         [](const auto& a, const auto& b) { return a.version() > b.version(); });
        IndexDeltaGroupVerPB version;
        for (auto& entry : entries) {
            if (idg_entry_has_active_key(entry)) {
                *version.add_entries() = std::move(entry);
                continue;
            }
            auto* orphan = plan->target->add_orphan_files();
            orphan->set_name(entry.index_file());
            if (entry.has_file_size()) orphan->set_size(entry.file_size());
            orphan->set_version(entry.version());
        }
        if (version.entries_size() > 0) (*idgs)[target_rssid] = std::move(version);
    }
    return Status::OK();
}

// Every live delete-vector page has exactly one target RSSID, so the merged file is the pages copied in target
// RSSID order. Pages of segments a source no longer has are left out. Once every declaration is known to be
// consistent, the checks the page writer makes before it creates its file are made too, so a refusal comes before
// anything is written.
Status plan_delvec_pages(MergePlan* plan, const std::vector<std::map<uint32_t, uint32_t>>& source_to_target) {
    std::map<std::string, FileMetaPB> files;
    for (size_t s = 0; s < plan->sources.size(); ++s) {
        const auto& source = *plan->sources[s];
        for (const auto& [source_rssid, page] : source.delvec_meta().delvecs()) {
            auto segment = source_to_target[s].find(source_rssid);
            if (segment == source_to_target[s].end()) continue;
            if (omit_target_for_test("tablet_merge_test:force_delvec_target_omission")) {
                return Status::Corruption(
                        fmt::format("tablet merge source-live delvec RSSID {} target was omitted", source_rssid));
            }
            auto file = source.delvec_meta().version_to_file().find(page.version());
            if (file == source.delvec_meta().version_to_file().end()) {
                return Status::Corruption(
                        fmt::format("tablet merge live delvec page has no file for version {}", page.version()));
            }
            const auto& declaration = file->second;
            if (!declaration.has_name() || declaration.name().empty()) {
                return Status::Corruption("tablet merge live delvec page has a missing or empty filename");
            }
            if (!declaration.encryption_meta().empty()) {
                return Status::NotSupported(fmt::format(
                        "encrypted delvec input is unsupported; delvec must be plaintext: {}", declaration.name()));
            }
            if (declaration.has_size()) {
                if (declaration.size() < 0) {
                    return Status::Corruption(
                            fmt::format("tablet merge delvec file {} has a negative size", declaration.name()));
                }
                const uint64_t file_size = declaration.size();
                if (page.offset() > file_size || page.size() > file_size - page.offset()) {
                    return Status::Corruption(
                            fmt::format("tablet merge delvec page exceeds file {} bounds", declaration.name()));
                }
            }
            auto [known, inserted] = files.try_emplace(declaration.name(), declaration);
            if (!inserted && !delvec_file_metadata_matches(known->second, declaration)) {
                return Status::Corruption(fmt::format(
                        "Delvec actual page source metadata mismatch for file {} during tablet merge preflight",
                        declaration.name()));
            }
            plan->delvec_pages[segment->second] = DelvecPageInfo{source.id(), declaration, page};
        }
    }
    constexpr uint64_t kInt64Max = std::numeric_limits<int64_t>::max();
    std::map<std::tuple<int64_t, std::string, uint64_t>, uint64_t> sizes;
    uint64_t total_size = 0;
    for (const auto& [target_rssid, info] : plan->delvec_pages) {
        (void)target_rssid;
        const auto& page = info.page;
        if (page.size() == 0) {
            return Status::InvalidArgument("compacted delvec raw page size must be positive");
        }
        uint64_t page_end = 0;
        if (page.offset() > kInt64Max || page.size() > kInt64Max ||
            __builtin_add_overflow(page.offset(), page.size(), &page_end) || page_end > kInt64Max) {
            return Status::InvalidArgument("compacted delvec raw page end is outside signed int64 domain");
        }
        auto [seen, first] = sizes.try_emplace({info.tablet_id, info.delvec_file.name(), page.offset()}, page.size());
        if (!first && seen->second != page.size()) {
            return Status::Corruption("compacted delvec duplicate page declarations disagree on size");
        }
        if (__builtin_add_overflow(total_size, page.size(), &total_size) || total_size > kInt64Max) {
            return Status::InvalidArgument("compacted delvec output size is outside signed int64 domain");
        }
    }
    return Status::OK();
}

// historical_schemas is the union over the sources, kept to the schemas some rowset still maps to plus the current
// one.
void merge_schemas(MergePlan* plan) {
    auto* target = plan->target.get();
    std::unordered_set<int64_t> referenced;
    for (const auto& [rowset_id, schema_id] : target->rowset_to_schema()) referenced.insert(schema_id);
    auto* schemas = target->mutable_historical_schemas();
    schemas->clear();
    for (const auto& source : plan->sources) {
        for (const auto& [schema_id, schema] : source->historical_schemas()) {
            // FE assigns schema ids, so one id is one schema; copies written by different BE versions may still
            // differ in bytes. The lowest-id source's copy is kept.
            if (referenced.contains(schema_id)) schemas->insert({schema_id, schema});
        }
    }
    if (target->schema().has_id() && !schemas->contains(target->schema().id())) {
        (*schemas)[target->schema().id()] = target->schema();
    }
}

// A rowset is built in the merged tablet only if it was built in its own source, so the watermark is the lowest
// over the sources; a source without one has built nothing and counts as 0.
void merge_vector_index_built_version(MergePlan* plan) {
    bool any = false;
    int64_t lowest = std::numeric_limits<int64_t>::max();
    for (const auto& source : plan->sources) {
        any |= source->has_vector_index_built_version();
        lowest = std::min(lowest, source->has_vector_index_built_version() ? source->vector_index_built_version() : 0);
    }
    if (any) {
        plan->target->set_vector_index_built_version(lowest);
    } else {
        plan->target->clear_vector_index_built_version();
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Primary-key index sstables.
// ---------------------------------------------------------------------------------------------------------------

// Whether |sst| of source |s| can be carried into the merged tablet by translating its RSSIDs. When it cannot, the
// merged tablet drops every source sstable and rebuilds its index from the rowsets the next time it is loaded.
std::optional<std::string> sstable_projection_problem(const MergePlan& plan, size_t s,
                                                      const PersistentIndexSstablePB& sst) {
    const auto& projection = plan.projections[s];
    const auto delta = projection.single_delta();
    if (!delta.has_value()) return "the source is not a single translation run";
    if (!projection.map(extract_rss_rowid_high(sst.max_rss_rowid())).ok()) return "its watermark is outside the source";
    if (sst.has_shared_rssid()) {
        auto owner = effective_shared_rssid(sst);
        if (!owner.ok()) return "its owner is outside the source";
        auto target_owner = projection.map(owner.value());
        if (!target_owner.ok()) return "its owner is outside the source";
        if (sst.has_delvec() && sst.delvec().size() > 0) {
            // Loading the sstable reads this page, so the source flush would fail on a page it cannot read.
            const auto& page = sst.delvec();
            const auto& version_to_file = plan.sources[s]->delvec_meta().version_to_file();
            auto file = version_to_file.find(page.version());
            if (file == version_to_file.end() || file->second.name().empty())
                return "its delete vector page has no file";
            constexpr uint64_t kInt64Max = std::numeric_limits<int64_t>::max();
            if (page.size() > kInt64Max || page.offset() > kInt64Max - page.size() ||
                (file->second.has_size() &&
                 (file->second.size() < 0 ||
                  page.offset() + page.size() > static_cast<uint64_t>(file->second.size())))) {
                return "its delete vector page lies outside its file";
            }
            if (!plan.delvec_pages.contains(target_owner.value())) {
                return "its owner has no delete vector page in the merged tablet";
            }
        }
        return std::nullopt;
    }
    const int64_t offset = static_cast<int64_t>(sst.rssid_offset()) + *delta;
    if (offset < std::numeric_limits<int32_t>::min() || offset > std::numeric_limits<int32_t>::max()) {
        return "its translated rssid_offset exceeds int32";
    }
    return std::nullopt;
}

std::optional<std::string> sstable_attribution_problem(const TabletMetadataPB& source,
                                                       const PersistentIndexSstablePB& sst) {
    if (!validate_sstable_declaration(sst).ok()) return "it is malformed";
    if (sst.has_shared_rssid()) return "it is not the legacy form a flush writes";
    if (auto status = check_sstable_range(source, sst); !status.ok()) return std::string(status.message());
    return std::nullopt;
}

// Modern: every value belongs to shared_rssid + rssid_offset, which becomes the translated owner with no offset.
// Legacy: every value carries its own RSSID, shifted by rssid_offset, so the offset absorbs the source's delta.
// Either way the watermark moves with its RSSID. A modern sstable's embedded delete vector page is the owner's
// page as of when the sstable was written; the merged tablet's page for the same segment is that page or a
// superset of it (a segment's delete vector only ever grows), so pointing at the merged page loses no delete.
// An sstable that had no page gets none.
PersistentIndexSstablePB project_sstable(const MergePlan& plan, size_t s, const PersistentIndexSstablePB& sst) {
    const auto& projection = plan.projections[s];
    PersistentIndexSstablePB out(sst);
    const uint32_t high = projection.map(extract_rss_rowid_high(sst.max_rss_rowid())).value();
    out.set_max_rss_rowid(encode_rss_rowid(high, sst.max_rss_rowid()));
    if (sst.has_shared_rssid()) {
        const uint32_t owner = projection.map(effective_shared_rssid(sst).value()).value();
        out.set_shared_rssid(owner);
        out.set_rssid_offset(0);
        if (sst.has_delvec() && sst.delvec().size() > 0) {
            out.mutable_delvec()->CopyFrom(plan.target->delvec_meta().delvecs().at(owner));
        }
    } else {
        out.set_rssid_offset(static_cast<int32_t>(sst.rssid_offset() + *projection.single_delta()));
        out.clear_delvec();
    }
    out.mutable_fileset_id()->CopyFrom(UniqueId::gen_uid().to_proto());
    return out;
}

// The fallback: no source sstable is carried over, and each one is recorded once as a shared orphan of the merged
// tablet. The sstables a source already had are still referenced by the source's new metadata, so vacuum leaves them
// to the shared-file cleanup, which also reclaims any sstable the merge's own flush wrote: nothing references those.
void drop_source_sstables(const std::vector<TabletMetadataPtr>& sources, TabletMetadataPB* target) {
    target->clear_sstable_meta();
    for (const auto& source : sources) {
        for (const auto& sst : source->sstable_meta().sstables()) {
            auto* orphan = target->add_orphan_files();
            orphan->set_name(sst.filename());
            orphan->set_size(sst.filesize());
            orphan->set_encryption_meta(sst.encryption_meta());
            orphan->set_shared(true);
            const int64_t generation = sst.generation_version();
            orphan->set_version(generation > 0 && generation <= target->version() ? generation : 0);
        }
    }
}

// Everything the merge decides from metadata alone. A failure here is a refusal: nothing has been written.
StatusOr<MergePlan> plan_merge(const std::vector<TabletMetadataPtr>& requested, const MergingTabletInfoPB& merging,
                               int64_t new_version, const TxnInfoPB& txn_info) {
    MergePlan plan;
    plan.sources = requested;
    std::stable_sort(plan.sources.begin(), plan.sources.end(),
                     [](const auto& a, const auto& b) { return a->id() < b->id(); });
    for (const auto& source : plan.sources) RETURN_IF_ERROR(validate_source_rssid_domain(*source));

    // The merged tablet is a copy of a source with every data, garbage and per-tablet field cleared, so table-level
    // settings and fields this code does not know about are carried over.
    plan.target = std::make_shared<TabletMetadataPB>(*plan.sources.front());
    auto* target = plan.target.get();
    target->set_id(merging.new_tablet_id());
    target->set_version(new_version);
    target->set_commit_time(txn_info.commit_time());
    target->set_gtid(txn_info.gtid());
    target->clear_rowsets();
    target->clear_delvec_meta();
    target->clear_sstable_meta();
    target->clear_dcg_meta();
    target->clear_idg_meta();
    target->clear_rowset_to_schema();
    target->clear_compaction_inputs();
    target->clear_orphan_files();
    target->clear_prev_garbage_version();
    target->set_cumulative_point(0);
    merge_vector_index_built_version(&plan);

    TabletRangePB range = plan.sources.front()->range();
    for (size_t i = 1; i < plan.sources.size(); ++i) {
        ASSIGN_OR_RETURN(range, tablet_reshard_helper::union_range(range, plan.sources[i]->range()));
    }
    target->mutable_range()->CopyFrom(range);

    RETURN_IF_ERROR(validate_merge_inputs(plan.sources));
    RETURN_IF_ERROR(validate_physical_slices(plan.sources));
    ASSIGN_OR_RETURN(auto nodes, group_rowsets(plan.sources));
    RETURN_IF_ERROR(allocate_rssids(&plan, nodes));

    std::map<uint32_t, const SegmentMetadataPB*> target_segments;
    std::vector<std::map<uint32_t, uint32_t>> source_to_target;
    RETURN_IF_ERROR(map_segments(plan, &target_segments, &source_to_target));
    SidecarDeclarations declarations;
    RETURN_IF_ERROR(project_dcgs(&plan, source_to_target, target_segments, &declarations));
    RETURN_IF_ERROR(project_idgs(&plan, source_to_target, target_segments, &declarations));
    if (is_primary_key(*target)) RETURN_IF_ERROR(plan_delvec_pages(&plan, source_to_target));
    RETURN_IF_ERROR(validate_sstable_declarations(plan.sources));
    // The admission check runs last, so the more specific checks above report first.
    RETURN_IF_ERROR(admit_merge_sources(plan.sources));

    ASSIGN_OR_RETURN(auto order, order_rowsets(plan.sources, nodes));
    RETURN_IF_ERROR(materialize_rowsets(&plan, nodes, order));
    merge_schemas(&plan);

    if (uses_cloud_native_pk_index(*target)) {
        for (size_t s = 0; s < plan.sources.size() && !plan.rebuild_index; ++s) {
            // The source flush could not record a watermark for such a source; a rebuild does not flush.
            if (plan.projections[s].source_end() > kWatermarkRssidExclusiveLimit) {
                LOG(INFO) << "tablet merge into " << target->id() << " rebuilds the primary key index: source tablet "
                          << plan.sources[s]->id() << " references RSSIDs beyond the index watermark domain";
                plan.rebuild_index = true;
                break;
            }
            for (const auto& sst : plan.sources[s]->sstable_meta().sstables()) {
                if (auto problem = sstable_projection_problem(plan, s, sst); problem.has_value()) {
                    LOG(INFO) << "tablet merge into " << target->id() << " rebuilds the primary key index: SST "
                              << sst.filename() << " of source tablet " << plan.sources[s]->id() << " " << *problem;
                    plan.rebuild_index = true;
                    break;
                }
            }
        }
    }
    return plan;
}

// Writes the merged delete-vector file. No live page means no file and an empty delvec_meta.
Status write_delvecs(TabletManager* tablet_manager, MergePlan* plan, int64_t txn_id) {
    if (plan->delvec_pages.empty()) return Status::OK();
    std::vector<DelvecOutputPage> pages;
    pages.reserve(plan->delvec_pages.size());
    for (const auto& [target_rssid, page] : plan->delvec_pages) {
        (void)target_rssid;
        pages.push_back(DelvecOutputPage{.raw_page = page});
    }
    FileMetaPB file;
    std::vector<uint64_t> offsets;
    RETURN_IF_ERROR(write_compacted_delvec_pages(tablet_manager, pages, plan->target->id(), txn_id, &file, &offsets));
    // Until the metadata below is published the file is an ordinary orphan, reclaimed by vacuum.
    FAIL_POINT_TRIGGER_RETURN_ERROR(tablet_merge_after_write_delvec);

    const int64_t version = plan->target->version();
    DelvecMetadataPB delvec_meta;
    size_t i = 0;
    for (const auto& [target_rssid, source] : plan->delvec_pages) {
        DelvecPagePB page;
        page.set_version(version);
        page.set_crc32c_gen_version(version);
        page.set_offset(offsets[i++]);
        page.set_size(source.page.size());
        // A source checksum is trusted only when it was generated for the page's own version; the read path checks
        // a page exactly when the two agree, so carrying an untrusted one forward would turn it into a failure.
        if (source.page.has_crc32c() && source.page.crc32c_gen_version() == source.page.version()) {
            page.set_crc32c(source.page.crc32c());
        }
        (*delvec_meta.mutable_delvecs())[target_rssid] = std::move(page);
    }
    file.clear_encryption_meta();
    (*delvec_meta.mutable_version_to_file())[version] = std::move(file);
    plan->target->mutable_delvec_meta()->Swap(&delvec_meta);
    return Status::OK();
}

// Flushes each source's primary-key memtable so its sstables cover all its rows (the merged tablet's cold load
// skips rowsets below the last sstable's watermark), then carries the sstables over, or falls back to a rebuild.
Status merge_sstables(TabletManager* tablet_manager, MergePlan* plan) {
    auto* target = plan->target.get();
    if (!uses_cloud_native_pk_index(*target)) return Status::OK();
    if (plan->rebuild_index) {
        drop_source_sstables(plan->sources, target);
        return Status::OK();
    }
    std::vector<TabletMetadataPtr> flushed = plan->sources;
    for (auto& source : flushed) {
        bool skip_source_flush = false;
        FAIL_POINT_TRIGGER_EXECUTE(skip_lake_pk_index_merge_source_flush, { skip_source_flush = true; });
        if (skip_source_flush) continue;
        ASSIGN_OR_RETURN(source, tablet_manager->primary_key_mgr()->flush_pk_memtable(source, target->version()));
    }
    for (size_t s = 0; s < flushed.size(); ++s) {
        std::set<std::string> existing;
        for (const auto& sst : plan->sources[s]->sstable_meta().sstables()) existing.insert(sst.filename());
        // Every sstable the flush returns is checked again before it is projected, the ones the source already had
        // included; only the ones the flush wrote must also be in the form a flush writes.
        for (const auto& sst : flushed[s]->sstable_meta().sstables()) {
            auto problem = existing.contains(sst.filename()) ? std::optional<std::string>()
                                                             : sstable_attribution_problem(*plan->sources[s], sst);
            if (!problem.has_value()) problem = sstable_projection_problem(*plan, s, sst);
            if (problem.has_value()) {
                LOG(INFO) << "tablet merge into " << target->id() << " rebuilds the primary key index: flushed SST "
                          << sst.filename() << " of source tablet " << flushed[s]->id() << " " << *problem;
                drop_source_sstables(flushed, target);
                return Status::OK();
            }
        }
    }
    auto* sstables = target->mutable_sstable_meta()->mutable_sstables();
    for (size_t s = 0; s < flushed.size(); ++s) {
        for (const auto& sst : flushed[s]->sstable_meta().sstables()) *sstables->Add() = project_sstable(*plan, s, sst);
    }
    std::stable_sort(sstables->begin(), sstables->end(), [](const auto& a, const auto& b) {
        return static_cast<int64_t>(a.max_rss_rowid()) < static_cast<int64_t>(b.max_rss_rowid());
    });
    if (sstables->empty()) target->clear_sstable_meta();
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
    for (const auto& metadata : old_tablet_metadatas) {
        if (metadata == nullptr) return Status::InvalidArgument("old tablet metadata is null");
    }
    auto plan_or = plan_merge(old_tablet_metadatas, merging_tablet, new_version, txn_info);
    if (!plan_or.ok()) {
        return merge_rejection(plan_or.status(), old_tablet_metadatas, merging_tablet.new_tablet_id());
    }
    auto plan = std::move(plan_or).value();
    RETURN_IF_ERROR(write_delvecs(tablet_manager, &plan, txn_info.txn_id()));
    RETURN_IF_ERROR(merge_sstables(tablet_manager, &plan));
    return plan.target;
}

} // namespace starrocks::lake
