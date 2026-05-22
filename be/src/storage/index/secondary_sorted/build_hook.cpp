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

#include "storage/index/secondary_sorted/build_hook.h"

#include "common/config.h"
#include "common/logging.h"
#include "fs/fs.h"
#include "storage/index/secondary_sorted/index_registry.h"
#include "storage/index/secondary_sorted/secondary_index_writer.h"
#include "storage/lake/tablet_manager.h"
#include "storage/olap_common.h"
#include "storage/rowset/segment.h"

namespace starrocks::secondary_sorted {

Status maybe_build_secondary_indexes(int64_t tablet_id, int64_t txn_id, const TabletSchemaCSPtr& source_schema,
                                     const std::vector<SegmentFileInfo>& seg_file_infos, std::shared_ptr<FileSystem> fs,
                                     lake::TabletManager* tablet_mgr, std::vector<SecondaryIndexFilePB>* out_pbs) {
    if (out_pbs == nullptr) return Status::InvalidArgument("out_pbs is null");
    if (!config::enable_secondary_index_write) return Status::OK();
    if (tablet_mgr == nullptr || fs == nullptr || source_schema == nullptr) return Status::OK();

    auto idx_defs = SecondaryIndexRegistry::get_for_tablet(tablet_id);
    if (idx_defs.empty()) return Status::OK();

    if (seg_file_infos.empty()) {
        LOG(INFO) << "secondary_index: tablet=" << tablet_id << " txn=" << txn_id
                  << " skipping build, no segments written";
        return Status::OK();
    }

    // Open each segment once and reuse across all index builds for this rowset.
    // Reading the segment footer + ordinal index is the per-segment fixed cost
    // we want amortised across N indexes; the per-index work is just an extra
    // column scan + sort + write.
    //
    // Skip segments that participate in a bundle file: their on-disk name
    // refers to a shared file owned by multiple tablets, which we cannot
    // re-open as a single Segment.
    //
    // If any individual Segment::open() fails (most commonly an OSS 404 from
    // a freshly-flushed segment that the bundle uploader hasn't materialised
    // yet), log a warning and skip index construction for this rowset
    // rather than failing the whole commit. This keeps the PoC's write path
    // non-fatal while production work picks a sturdier hook point.
    std::vector<std::shared_ptr<Segment>> segments;
    segments.reserve(seg_file_infos.size());
    for (size_t i = 0; i < seg_file_infos.size(); ++i) {
        const auto& info = seg_file_infos[i];
        if (info.bundle_file_offset.has_value() && info.bundle_file_offset.value() >= 0) {
            LOG(INFO) << "secondary_index: tablet=" << tablet_id << " txn=" << txn_id << " skipping index for bundled segment '"
                      << info.path << "'";
            return Status::OK();
        }
        FileInfo open_info;
        open_info.path = tablet_mgr->segment_location(tablet_id, info.path);
        open_info.size = info.size;
        open_info.encryption_meta = info.encryption_meta;
        auto seg_or = Segment::open(fs, open_info, /*segment_id=*/static_cast<uint32_t>(i), source_schema,
                                     /*footer_length_hint=*/nullptr, /*partial_rowset_footer=*/nullptr,
                                     LakeIOOptions{}, tablet_mgr);
        if (!seg_or.ok()) {
            LOG(WARNING) << "secondary_index: tablet=" << tablet_id << " txn=" << txn_id
                         << " skipping index, cannot open segment '" << info.path << "': " << seg_or.status();
            return Status::OK();
        }
        segments.push_back(std::move(seg_or).value());
    }

    for (const auto& idx_def : idx_defs) {
        if (idx_def.index_col_names.empty()) continue;
        SecondaryIndexWriter::BuildInput input;
        input.segments = segments; // shared_ptr copy, reuses footer cache
        input.source_schema = source_schema;
        input.index_name = idx_def.index_name;
        input.index_col_names = idx_def.index_col_names;
        input.tablet_mgr = tablet_mgr;
        input.fs = fs;
        input.tablet_id = tablet_id;
        input.txn_id = txn_id;

        ASSIGN_OR_RETURN(auto pb, SecondaryIndexWriter::build(input));
        if (!pb.file_name().empty()) {
            out_pbs->push_back(std::move(pb));
        }
    }
    return Status::OK();
}

} // namespace starrocks::secondary_sorted
