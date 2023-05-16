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

#include "storage/snapshot_meta.h"

#include "fmt/format.h"
#include "fs/output_stream_wrapper.h"
#include "gutil/endian.h"
#include "util/coding.h"
#include "util/raw_container.h"

namespace starrocks {

Status SnapshotMeta::serialize_to_file(const std::string& file_path) {
    std::unique_ptr<WritableFile> f;
    WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(f, FileSystem::Default()->new_writable_file(opts, file_path));
    RETURN_IF_ERROR(serialize_to_file(f.get()));
    RETURN_IF_ERROR(f->close());
    return Status::OK();
}

//
// File format of snapshot meta.
//
// +-------------------------------------+
// |      Serialized rowset meta         |  variant length
// +-------------------------------------+
// |             ......                  |
// +-------------------------------------+
// |      Serialized delete vector       |
// +-------------------------------------+
// |      Serialized delta column group  |
// +-------------------------------------+
// |             ......                  |
// +-------------------------------------+
// |      Serialized tablet meta         |  variant length
// +-------------------------------------+
// |        SnapshotMetaFooterPB         |  variant length
// +-------------------------------------+
// |  Offset of SnapshotMetaFooterPB     |  8 bytes
// +-------------------------------------+
// |             File length             |  8 bytes
// +-------------------------------------+
//
Status SnapshotMeta::serialize_to_file(WritableFile* file) {
    OutputStreamWrapper stream(file, kDontTakeOwnership);

    // Defined in `gensrc/proto/snapshot.proto`
    SnapshotMetaFooterPB footer;
    footer.set_format_version(_format_version);
    footer.set_snapshot_type(_snapshot_type);
    footer.set_snapshot_version(_snapshot_version);
    for (const auto& rowset_meta : _rowset_metas) {
        footer.add_rowset_meta_offsets(static_cast<int64_t>(stream.size()));
        if (!rowset_meta.SerializeToOstream(&stream)) {
            return Status::IOError("fail to serialize rowset meta to file");
        }
    }
    footer.add_rowset_meta_offsets(static_cast<int64_t>(stream.size()));

    for (const auto& [segment_id, del_vec] : _delete_vectors) {
        footer.add_delvec_segids(segment_id);
        footer.add_delvec_offsets(static_cast<int64_t>(stream.size()));
        footer.add_delvec_versions(del_vec.version());
        auto st = stream.append(del_vec.save());
        LOG_IF(WARNING, !st.ok()) << "Fail to save delete vector: " << st;
        RETURN_IF_ERROR(st);
    }
    footer.add_delvec_offsets(static_cast<int64_t>(stream.size()));
    footer.add_delvec_segids(-1);
    footer.add_delvec_versions(-1);

    for (const auto& [segment_id, dcg] : _dcgs) {
        footer.add_dcg_segids(segment_id);
        footer.add_dcg_offsets(static_cast<int64_t>(stream.size()));
        auto st = stream.append(DeltaColumnGroupListSerializer::serialize_delta_column_group_list(dcg));
        LOG_IF(WARNING, !st.ok()) << "Fail to save delta column group: " << st;
        RETURN_IF_ERROR(st);
    }
    footer.add_dcg_offsets(static_cast<int64_t>(stream.size()));
    footer.add_dcg_segids(-1);

    footer.set_tablet_meta_offset(static_cast<int64_t>(stream.size()));
    if (!_tablet_meta.SerializeToOstream(&stream)) {
        return Status::IOError("fail to serialize tablet meta to file");
    }

    auto footer_offset = static_cast<int64_t>(stream.size());
    if (!footer.SerializeToOstream(&stream)) {
        return Status::IOError("fail to serialize footer to file");
    }
    std::string s;
    s.reserve(16);
    put_fixed64_le(&s, BigEndian::FromHost64(footer_offset));
    put_fixed64_le(&s, BigEndian::FromHost64(stream.size() + 16));
    DCHECK_EQ(16, s.size());
    RETURN_IF_ERROR(stream.append(s));
    return Status::OK();
}

Status SnapshotMeta::_parse_delvec(SnapshotMetaFooterPB& footer, RandomAccessFile* file, int num_segments) {
    if (footer.delvec_offsets_size() <= 0) {
        return Status::InternalError(fmt::format("empty delete vector list, file: {}", file->filename()));
    }
    if (footer.delvec_offsets_size() != footer.delvec_segids_size()) {
        return Status::InternalError(
                fmt::format("mismatched delete vector size and segment id size, file: {}", file->filename()));
    }
    if (footer.delvec_offsets_size() != footer.delvec_versions_size()) {
        return Status::InternalError(
                fmt::format("mismatched delete vector size and version size, file: {}", file->filename()));
    }
    std::string buff;
    const int num_delvecs = footer.delvec_offsets_size() - 1;
    for (int i = 0; i < num_delvecs; i++) {
        auto segment_id = footer.delvec_segids(i);
        auto version = footer.delvec_versions(i);
        auto start = footer.delvec_offsets(i);
        auto end = footer.delvec_offsets(i + 1);
        raw::stl_string_resize_uninitialized(&buff, end - start);
        RETURN_IF_ERROR(file->read_at_fully(start, buff.data(), buff.size()));
        DelVector delvec;
        RETURN_IF_ERROR(delvec.load(version, buff.data(), buff.size()));
        (void)_delete_vectors.emplace(static_cast<uint32_t>(segment_id), std::move(delvec));
    }
    if (_delete_vectors.size() != num_delvecs) {
        return Status::InternalError(
                fmt::format("has duplicate segment id of delete vector, file: {}", file->filename()));
    }
    if (_snapshot_type == SNAPSHOT_TYPE_FULL && num_segments != num_delvecs) {
        return Status::InternalError(fmt::format("#segment mismatch #delvec, file: {}", file->filename()));
    }
    return Status::OK();
}

Status SnapshotMeta::_parse_delta_column_group(SnapshotMetaFooterPB& footer, RandomAccessFile* file) {
    if (footer.dcg_offsets_size() != footer.dcg_segids_size()) {
        return Status::InternalError(
                fmt::format("mismatched delta column group size and segment id size, file: {}", file->filename()));
    }
    if (footer.dcg_offsets_size() == 0) {
        // this snapshot meta is generated by low version BE.
        return Status::OK();
    }
    // Parse delta column group
    std::string buff;
    const int num_dcglists = footer.dcg_offsets_size() - 1;
    for (int i = 0; i < num_dcglists; i++) {
        auto segment_id = footer.dcg_segids(i);
        auto start = footer.dcg_offsets(i);
        auto end = footer.dcg_offsets(i + 1);
        raw::stl_string_resize_uninitialized(&buff, end - start);
        RETURN_IF_ERROR(file->read_at_fully(start, buff.data(), buff.size()));
        RETURN_IF_ERROR(DeltaColumnGroupListSerializer::deserialize_delta_column_group_list(
                buff.data(), buff.size(), &_dcgs[static_cast<uint32_t>(segment_id)]));
    }
    if (_dcgs.size() != num_dcglists) {
        return Status::InternalError(
                fmt::format("has duplicate segment id of delta column group, file: {}", file->filename()));
    }
    return Status::OK();
}

Status SnapshotMeta::parse_from_file(RandomAccessFile* file) {
    ASSIGN_OR_RETURN(const uint64_t file_length, file->get_size());
    if (file_length < 16) {
        return Status::InvalidArgument("snapshot meta file too short");
    }
    std::string buff;
    raw::stl_string_resize_uninitialized(&buff, 16);

    RETURN_IF_ERROR(file->read_at_fully(file_length - 16, buff.data(), buff.size()));
    // Parse SnapshotMetaFooterPB
    auto footer_limit = static_cast<int64_t>(file_length) - 16;
    auto footer_offset = static_cast<int64_t>(BigEndian::ToHost64(UNALIGNED_LOAD64(buff.data())));
    auto saved_length = static_cast<int64_t>(BigEndian::ToHost64(UNALIGNED_LOAD64(buff.data() + 8)));
    if (saved_length != file_length) {
        return Status::Corruption("invalid saved file length");
    }
    if (footer_offset < 0 || footer_limit <= footer_offset) {
        return Status::Corruption("invalid footer offset");
    }
    raw::stl_string_resize_uninitialized(&buff, footer_limit - footer_offset);
    RETURN_IF_ERROR(file->read_at_fully(footer_offset, buff.data(), buff.size()));
    SnapshotMetaFooterPB footer;
    if (!footer.ParseFromString(buff)) {
        return Status::Corruption("parse snapshot meta footer failed");
    }
    if (footer.rowset_meta_offsets_size() <= 0) {
        return Status::InternalError("empty rowset meta list");
    }
    if (!footer.has_tablet_meta_offset()) {
        return Status::InternalError("no tablet meta");
    }
    if (!footer.has_snapshot_type()) {
        return Status::InternalError("missing snapshot type");
    }
    if (!footer.has_format_version()) {
        return Status::InternalError("missing snapshot format");
    }
    if (footer.snapshot_type() == SNAPSHOT_TYPE_FULL && !footer.has_snapshot_version()) {
        return Status::InternalError("missing snapshot version");
    }
    _snapshot_type = footer.snapshot_type();
    _format_version = footer.format_version();
    _snapshot_version = footer.snapshot_version();

    // Parse RowsetMetaPB
    int num_rowset_metas = footer.rowset_meta_offsets_size() - 1;
    int num_segments = 0;
    _rowset_metas.resize(num_rowset_metas);
    for (int i = 0; i < num_rowset_metas; i++) {
        auto start = footer.rowset_meta_offsets(i);
        auto end = footer.rowset_meta_offsets(i + 1);
        raw::stl_string_resize_uninitialized(&buff, end - start);
        RETURN_IF_ERROR(file->read_at_fully(start, buff.data(), buff.size()));
        if (!_rowset_metas[i].ParseFromString(buff)) {
            return Status::InternalError("parse rowset meta failed");
        }
        num_segments += static_cast<int>(_rowset_metas[i].num_segments());
    }
    // Parse delete vector
    RETURN_IF_ERROR(_parse_delvec(footer, file, num_segments));
    // Parse delta column group
    RETURN_IF_ERROR(_parse_delta_column_group(footer, file));
    // Tablet meta
    auto tablet_meta_offset = footer.tablet_meta_offset();
    raw::stl_string_resize_uninitialized(&buff, footer_offset - tablet_meta_offset);
    RETURN_IF_ERROR(file->read_at_fully(tablet_meta_offset, buff.data(), buff.size()));
    if (!_tablet_meta.ParseFromString(buff)) {
        return Status::InternalError("parse tablet meta failed");
    }
    if (_snapshot_type == SNAPSHOT_TYPE_FULL && _tablet_meta.updates().versions_size() != 1) {
        return Status::InternalError("incorrect version list size");
    }
    if (_snapshot_type == SNAPSHOT_TYPE_FULL && _tablet_meta.updates().versions(0).rowsets_add_size() != 0) {
        return Status::InternalError("snapshot should not have delta rowset");
    }
    if (_snapshot_type == SNAPSHOT_TYPE_FULL &&
        _tablet_meta.updates().versions(0).rowsets_size() != _rowset_metas.size()) {
        return Status::InternalError("mismatched rowset meta size");
    }
    return Status::OK();
}

} // namespace starrocks
