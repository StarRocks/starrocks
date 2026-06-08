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

#include "storage/delta_column_group.h"

#include <cstring>
#include <memory>

#include "storage/protobuf_file.h"
#include "storage/rowset/rowset.h"
#include "storage/utils.h"

namespace starrocks {

void DeltaColumnGroup::init(int64_t version, const std::vector<std::vector<ColumnUID>>& column_ids,
                            const std::vector<std::string>& column_files,
                            const std::vector<std::string>& encryption_metas, int64_t file_size) {
    _version = version;
    _column_uids = column_ids;
    _column_files = column_files;
    _encryption_metas = encryption_metas;
    _file_size = file_size;
    _calc_memory_usage();
}

void DeltaColumnGroup::_calc_memory_usage() {
    size_t total_ids = 0;
    size_t total_column_name_size = 0;

    for (int i = 0; i < _column_uids.size(); ++i) {
        total_ids += _column_uids[i].size();
        total_column_name_size += _column_files[i].length();
    }

    size_t total_encryption_meta_size = 0;
    for (const auto& encryption_meta : _encryption_metas) {
        total_encryption_meta_size += encryption_meta.length();
    }

    // Inline patches live in meta (resident); account for their value blobs, source rowids and uids.
    size_t total_inline_bytes = 0;
    for (const auto& patch : _inline_patches) {
        total_inline_bytes += patch.source_rowids.size() * sizeof(uint32_t);
        total_inline_bytes += patch.column_uids.size() * sizeof(ColumnUID);
        for (const auto& blob : patch.column_values) {
            total_inline_bytes += blob.size();
        }
    }

    // Per-column presence lists are resident (loaded from the lake VerPB); account for the
    // roaring blobs (the dominant term) plus the per-entry fixed fields.
    size_t total_column_presence_bytes = 0;
    for (const auto& list : _column_presence_lists) {
        for (const auto& e : list.entries) {
            total_column_presence_bytes += e.roaring.size() + sizeof(ColumnSparsePresence);
        }
    }

    _memory_usage = sizeof(size_t) + sizeof(int64_t) + sizeof(uint32_t) * total_ids + total_column_name_size +
                    total_encryption_meta_size + total_inline_bytes + total_column_presence_bytes;
}

int DeltaColumnGroup::merge_into_by_version(DeltaColumnGroupList& dcgs, const std::string& dir,
                                            const RowsetId& rowset_id, int segment_id) {
    int merged = 0;
    for (auto& dcg : dcgs) {
        if (dcg->merge_by_version(*this, dir, rowset_id, segment_id)) {
            ++merged;
        }
    }
    return merged;
}

bool DeltaColumnGroup::merge_by_version(DeltaColumnGroup& dcg, const std::string& dir, const RowsetId& rowset_id,
                                        int segment_id) {
    if (_version != dcg.version()) {
        return false;
    }

    size_t orig_size = _column_files.size();

    _column_uids.insert(_column_uids.end(), dcg.column_ids().begin(), dcg.column_ids().end());

    _column_files.resize(_column_files.size() + dcg.relative_column_files().size());
    // update the file name suffix to finish merge
    for (size_t suffix = orig_size; suffix < _column_files.size(); ++suffix) {
        _column_files[suffix] =
                file_name(Rowset::delta_column_group_path(dir, rowset_id, segment_id, _version, suffix));
        if (dcg.encryption_metas().size() > 0) {
            _encryption_metas.push_back(dcg.encryption_metas()[suffix - orig_size]);
        }
    }

    _calc_memory_usage();
    return true;
}

Status DeltaColumnGroup::load(int64_t version, const char* data, size_t length) {
    _version = version;
    // It must be compatible with the old dcg metadata format, the basic idea
    // is following:
    // 1. remain the dcg metadata format definition called OldDeltaColumnGroupPB
    // 2. parse the meta data using OldDeltaColumnGroupPB if it fail in DeltaColumnGroupPB
    // 3. assign DeltaColumnGroupPB using OldDeltaColumnGroupPB

    OldDeltaColumnGroupPB old_dcg_pb;
    DeltaColumnGroupPB dcg_pb;

    bool parsed = dcg_pb.ParseFromArray(data, length);
    bool old_parsed = false;

    if (!parsed && !old_dcg_pb.ParseFromArray(data, length)) {
        return Status::Corruption("DeltaColumnGroup load failed");
    } else if (!parsed) {
        old_parsed = true;
    }

    RETURN_ERROR_IF_FALSE((parsed && !old_parsed) || (!parsed && old_parsed));

    if (!parsed && old_parsed) {
        auto column_ids = dcg_pb.add_column_ids();
        for (const auto& id : old_dcg_pb.column_ids()) {
            column_ids->add_column_ids(id);
        }

        dcg_pb.add_column_files(old_dcg_pb.column_file());
    }

    for (const auto& column_file : dcg_pb.column_files()) {
        _column_files.push_back(column_file);
    }
    for (const auto& encryption_meta : dcg_pb.encryption_metas()) {
        _encryption_metas.push_back(encryption_meta);
    }
    for (const auto& cids : dcg_pb.column_ids()) {
        _column_uids.emplace_back();
        for (const auto& cid : cids.column_ids()) {
            _column_uids.back().push_back(cid);
        }
    }
    _file_size = dcg_pb.file_size();
    _calc_memory_usage();
    return Status::OK();
}

Status DeltaColumnGroup::load(int64_t version, const DeltaColumnGroupVerPB& dcg_ver_pb) {
    _version = version;
    for (const auto& column_file : dcg_ver_pb.column_files()) {
        _column_files.push_back(column_file);
    }
    for (const auto& encryption_meta : dcg_ver_pb.encryption_metas()) {
        _encryption_metas.push_back(encryption_meta);
    }
    for (const auto& column_file_size : dcg_ver_pb.column_file_sizes()) {
        _column_file_sizes.push_back(column_file_size);
    }
    for (const auto& ucids : dcg_ver_pb.unique_column_ids()) {
        _column_uids.emplace_back();
        for (const auto& cid : ucids.column_ids()) {
            _column_uids.back().push_back(cid);
        }
    }
    // === SDCG === Fill kinds/counts in lockstep with column_files, normalizing an
    // absent (legacy) array to all-DENSE / 0 so downstream readers see a strictly
    // 1:1 view. We only materialize the vectors when the PB actually carries sparse
    // metadata; otherwise file_kind()/sparse_row_count() fall back to DENSE/0 and
    // local-engine behavior stays byte-identical.
    if (dcg_ver_pb.file_kinds_size() > 0 || dcg_ver_pb.sparse_row_counts_size() > 0 ||
        dcg_ver_pb.presences_size() > 0 || dcg_ver_pb.column_presence_lists_size() > 0 ||
        dcg_ver_pb.has_source_segment_num_rows()) {
        std::vector<DeltaColumnFileKind> kinds;
        std::vector<int64_t> counts;
        std::vector<SparsePresence> presences;
        // Per-column presence lists (packed files). Materialized only when the PB carries them;
        // an absent slot becomes an empty ColumnPresenceList (reader falls back to file-level).
        std::vector<ColumnPresenceList> column_presence_lists;
        kinds.reserve(_column_files.size());
        counts.reserve(_column_files.size());
        presences.reserve(_column_files.size());
        const bool has_column_presence = dcg_ver_pb.column_presence_lists_size() > 0;
        if (has_column_presence) {
            column_presence_lists.reserve(_column_files.size());
        }
        for (size_t i = 0; i < _column_files.size(); ++i) {
            kinds.push_back(i < static_cast<size_t>(dcg_ver_pb.file_kinds_size()) &&
                                            dcg_ver_pb.file_kinds(static_cast<int>(i)) == SPARSE_PERCOL
                                    ? DeltaColumnFileKind::SPARSE_PERCOL
                                    : DeltaColumnFileKind::DENSE_COLS);
            counts.push_back(i < static_cast<size_t>(dcg_ver_pb.sparse_row_counts_size())
                                     ? dcg_ver_pb.sparse_row_counts(static_cast<int>(i))
                                     : 0);
            // Normalize presences to 1:1 with column_files. An absent slot (legacy / dense /
            // a writer that predates the presences field) becomes an all-unknown SparsePresence
            // (kSDCGPresenceUnknown), which readers treat as "no skip". A present-but-empty
            // SparsePresencePB (a padded dense slot) also resolves to unknown because its
            // unset fields default to 0 yet has_* is false -- guard each field with has_*.
            SparsePresence p;
            if (i < static_cast<size_t>(dcg_ver_pb.presences_size())) {
                const auto& pb = dcg_ver_pb.presences(static_cast<int>(i));
                if (pb.has_min_source_rowid()) p.min_source_rowid = pb.min_source_rowid();
                if (pb.has_max_source_rowid()) p.max_source_rowid = pb.max_source_rowid();
                if (pb.has_row_count()) p.row_count = pb.row_count();
            }
            presences.push_back(p);

            // Per-column presence (packed files). Decode one ColumnSparsePresence per entry; the
            // roaring blob is kept opaque (the reader deserializes it via roaring::Roaring::readSafe
            // when it needs the exact apply gate). An absent / empty list slot stays empty == the
            // reader gates on the file-level presence above (dense / legacy / homogeneous file).
            if (has_column_presence) {
                ColumnPresenceList list;
                if (i < static_cast<size_t>(dcg_ver_pb.column_presence_lists_size())) {
                    const auto& list_pb = dcg_ver_pb.column_presence_lists(static_cast<int>(i));
                    list.entries.reserve(list_pb.entries_size());
                    for (const auto& entry_pb : list_pb.entries()) {
                        ColumnSparsePresence e;
                        e.column_uid = static_cast<ColumnUID>(entry_pb.column_uid());
                        if (entry_pb.has_min_source_rowid()) e.min_source_rowid = entry_pb.min_source_rowid();
                        if (entry_pb.has_max_source_rowid()) e.max_source_rowid = entry_pb.max_source_rowid();
                        if (entry_pb.has_count()) e.count = entry_pb.count();
                        if (entry_pb.has_roaring()) e.roaring = entry_pb.roaring();
                        list.entries.push_back(std::move(e));
                    }
                }
                column_presence_lists.push_back(std::move(list));
            }
        }
        if (has_column_presence) {
            set_sdcg_meta(std::move(kinds), std::move(counts), std::move(presences), std::move(column_presence_lists),
                          dcg_ver_pb.source_segment_num_rows());
        } else {
            set_sdcg_meta(std::move(kinds), std::move(counts), std::move(presences),
                          dcg_ver_pb.source_segment_num_rows());
        }
    }
    // === SDCG inline patches === a SEPARATE axis from the file lists. Decode each
    // InlineSparsePatchPB into an InlinePatch, eagerly converting the LE source_rowids bytes into
    // a uint32 vector (ascending) so the reader consumes them directly. column_values blobs are
    // kept opaque; the reader deserializes them via ColumnArraySerde into the read-schema type.
    _inline_patches.reserve(dcg_ver_pb.inline_patches_size());
    for (const auto& pb : dcg_ver_pb.inline_patches()) {
        InlinePatch patch;
        patch.version = pb.version();
        patch.row_count = pb.row_count();
        patch.column_uids.reserve(pb.column_uids_size());
        for (uint32_t uid : pb.column_uids()) {
            patch.column_uids.push_back(static_cast<ColumnUID>(uid));
        }
        patch.column_values.reserve(pb.column_values_size());
        for (const auto& blob : pb.column_values()) {
            patch.column_values.push_back(blob);
        }
        // source_rowids are K x uint32 little-endian, ascending. memcpy is safe on LE hosts
        // (StarRocks BE is x86_64/aarch64 LE); decode element-wise to stay byte-order explicit.
        const std::string& rowid_bytes = pb.source_rowids();
        const size_t n = rowid_bytes.size() / sizeof(uint32_t);
        patch.source_rowids.resize(n);
        for (size_t i = 0; i < n; ++i) {
            uint32_t v;
            std::memcpy(&v, rowid_bytes.data() + i * sizeof(uint32_t), sizeof(uint32_t));
            patch.source_rowids[i] = v;
        }
        if (pb.has_min_source_rowid()) patch.min_source_rowid = pb.min_source_rowid();
        if (pb.has_max_source_rowid()) patch.max_source_rowid = pb.max_source_rowid();
        _inline_patches.push_back(std::move(patch));
    }
    _calc_memory_usage();
    return Status::OK();
}

std::string DeltaColumnGroup::save() const {
    DeltaColumnGroupPB dcg_pb;
    for (const auto& column_file : _column_files) {
        dcg_pb.add_column_files(column_file);
    }
    for (const auto& encryption_meta : _encryption_metas) {
        dcg_pb.add_encryption_metas(encryption_meta);
    }
    for (const auto& cids : _column_uids) {
        auto* dcg_col_pb = dcg_pb.add_column_ids();
        for (const auto& cid : cids) {
            dcg_col_pb->add_column_ids(cid);
        }
    }
    dcg_pb.set_file_size(_file_size);
    std::string result;
    dcg_pb.SerializeToString(&result);
    return result;
}

std::string DeltaColumnGroupListSerializer::serialize_delta_column_group_list(const DeltaColumnGroupList& dcgs,
                                                                              DeltaColumnGroupListPB* dst) {
    DeltaColumnGroupListPB dcgs_pb;
    for (const auto& dcg : dcgs) {
        dcgs_pb.add_versions(dcg->version());
        DeltaColumnGroupPB dcg_pb;
        for (const auto& relative_column_file : dcg->relative_column_files()) {
            dcg_pb.add_column_files(relative_column_file);
        }
        for (const auto& encryption_meta : dcg->encryption_metas()) {
            dcg_pb.add_encryption_metas(encryption_meta);
        }
        for (const auto& cids : dcg->column_ids()) {
            auto* dcg_col_pb = dcg_pb.add_column_ids();
            for (const auto& cid : cids) {
                dcg_col_pb->add_column_ids(cid);
            }
        }
        dcg_pb.set_file_size(dcg->file_size());
        dcgs_pb.add_dcgs()->CopyFrom(dcg_pb);
    }

    if (dst != nullptr) {
        dst->CopyFrom(dcgs_pb);
    }

    std::string result;
    dcgs_pb.SerializeToString(&result);
    return result;
}

Status DeltaColumnGroupListSerializer::deserialize_delta_column_group_list(const char* data, size_t length,
                                                                           DeltaColumnGroupList* dcgs) {
    DeltaColumnGroupListPB dcgs_pb;
    if (!dcgs_pb.ParseFromArray(data, length)) {
        return Status::Corruption("parse delta column group failed");
    }
    return _deserialize_delta_column_group_list(dcgs_pb, dcgs);
}

Status DeltaColumnGroupListSerializer::deserialize_delta_column_group_list(const DeltaColumnGroupListPB& dcgs_pb,
                                                                           DeltaColumnGroupList* dcgs) {
    return _deserialize_delta_column_group_list(dcgs_pb, dcgs);
}

Status DeltaColumnGroupListSerializer::_deserialize_delta_column_group_list(const DeltaColumnGroupListPB& dcgs_pb,
                                                                            DeltaColumnGroupList* dcgs) {
    DCHECK(dcgs_pb.versions_size() == dcgs_pb.dcgs_size());
    for (int i = 0; i < dcgs_pb.versions_size(); i++) {
        auto dcg = std::make_shared<DeltaColumnGroup>();
        std::vector<std::vector<ColumnUID>> column_ids;
        std::vector<std::string> column_files;
        std::vector<std::string> encryption_metas;
        DCHECK(dcgs_pb.dcgs(i).column_ids().size() == dcgs_pb.dcgs(i).column_files().size());
        for (int j = 0; j < dcgs_pb.dcgs(i).column_ids().size(); ++j) {
            column_ids.emplace_back();
            for (const auto& cid : dcgs_pb.dcgs(i).column_ids(j).column_ids()) {
                column_ids.back().push_back(cid);
            }
            column_files.push_back(dcgs_pb.dcgs(i).column_files(j));
            if (j < dcgs_pb.dcgs(i).encryption_metas_size()) {
                encryption_metas.push_back(dcgs_pb.dcgs(i).encryption_metas(j));
            }
        }
        dcg->init(dcgs_pb.versions(i), column_ids, column_files, encryption_metas, dcgs_pb.dcgs(i).file_size());
        dcgs->push_back(dcg);
    }
    return Status::OK();
}

void DeltaColumnGroupListHelper::garbage_collection(DeltaColumnGroupList& dcg_list, const TabletSegmentId& tsid,
                                                    int64_t min_readable_version, const std::string& tablet_path,
                                                    std::vector<std::pair<TabletSegmentId, int64_t>>* garbage_dcgs,
                                                    std::vector<std::string>* garbage_files) {
    auto dcg_itr = dcg_list.begin();
    // The delta column group that need to be gc, should satisfy two point:
    // 1. It's version is not larger than min_readable_version
    // 2. It's all column are covered by other delta column groups which are also not larger than min_readable_version
    std::unordered_set<uint32_t> column_set;
    while (dcg_itr != dcg_list.end()) {
        if ((*dcg_itr)->version() > min_readable_version) {
            // skip the delta column group that isn't expired.
            dcg_itr++;
        } else {
            bool need_free = true;
            auto all_cids = (*dcg_itr)->column_ids();
            for (const auto& cids : all_cids) {
                for (uint32_t cid : cids) {
                    // We can't free the dcg that it's columns not in column_set yet.
                    if (column_set.count(cid) == 0) {
                        need_free = false;
                        break;
                    }
                }
            }
            if (need_free) {
                garbage_dcgs->emplace_back(tsid, (*dcg_itr)->version());
                std::vector<std::string> dcg_files = (*dcg_itr)->column_files(tablet_path);
                garbage_files->insert(garbage_files->end(), dcg_files.begin(), dcg_files.end());
                dcg_itr = dcg_list.erase(dcg_itr);
            } else {
                for (const auto& cids : all_cids) {
                    for (uint32_t cid : cids) {
                        column_set.insert(cid);
                    }
                }
                dcg_itr++;
            }
        }
    }
}

Status DeltaColumnGroupListHelper::save_snapshot(const std::string& file_path,
                                                 DeltaColumnGroupSnapshotPB& dcg_snapshot_pb) {
    DCHECK(!file_path.empty());
    ProtobufFileWithHeader file(file_path);
    return file.save(dcg_snapshot_pb, true);
}

Status DeltaColumnGroupListHelper::parse_snapshot(const std::string& file_path,
                                                  DeltaColumnGroupSnapshotPB& dcg_snapshot_pb) {
    ProtobufFileWithHeader file(file_path);
    Status st = file.load(&dcg_snapshot_pb);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to load dcg meta file: " << st;
        return st;
    }
    return Status::OK();
}

} // namespace starrocks