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

#include "storage/rowset/segment_file_info.h"

#include "gen_cpp/lake_types.pb.h"

namespace starrocks {

void to_file_meta_pb(const FileInfo& file, FileMetaPB* file_meta) {
    file_meta->set_name(file.path);
    if (file.size.has_value()) {
        file_meta->set_size(file.size.value());
    }
    if (!file.encryption_meta.empty()) {
        file_meta->set_encryption_meta(file.encryption_meta);
    }
}

void SegmentFileInfo::to_proto(uint32_t segment_idx, SegmentMetadataPB* segment_meta) const {
    // File attributes. An empty encryption_meta means "unencrypted", same as absent, so don't store it.
    segment_meta->set_filename(path);
    if (size.has_value()) {
        segment_meta->set_size(size.value());
    }
    if (!encryption_meta.empty()) {
        segment_meta->set_encryption_meta(encryption_meta);
    }
    if (bundle_file_offset.has_value() && bundle_file_offset.value() >= 0) {
        segment_meta->set_bundle_file_offset(bundle_file_offset.value());
    }
    // Sort-key fields.
    sort_key_min.to_proto(segment_meta->mutable_sort_key_min());
    sort_key_max.to_proto(segment_meta->mutable_sort_key_max());
    for (const auto& sample : sort_key_samples) {
        sample.to_proto(segment_meta->add_sort_key_samples());
    }
    if (!sort_key_samples.empty() && sort_key_sample_row_interval > 0) {
        segment_meta->set_sort_key_sample_row_interval(sort_key_sample_row_interval);
    }
    // Other per-segment metadata.
    segment_meta->set_num_rows(num_rows);
    segment_meta->set_segment_idx(segment_idx);
    for (int64_t vi_id : vector_index_ids) {
        segment_meta->add_vector_index_ids(vi_id);
    }
    // Record the segment's vector index uid for .vi naming (see SegmentMetadataPB.segment_vector_index_uid).
    // Paired with vector_index_ids: only meaningful when this segment has vector indexes.
    if (segment_vector_index_uid >= 0) {
        segment_meta->set_segment_vector_index_uid(segment_vector_index_uid);
    }
}

} // namespace starrocks
