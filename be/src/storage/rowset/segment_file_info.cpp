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

void SegmentFileInfo::write_sort_key_fields_to(SegmentMetadataPB* segment_meta) const {
    sort_key_min.to_proto(segment_meta->mutable_sort_key_min());
    sort_key_max.to_proto(segment_meta->mutable_sort_key_max());
    for (const auto& sample : sort_key_samples) {
        sample.to_proto(segment_meta->add_sort_key_samples());
    }
    if (!sort_key_samples.empty() && sort_key_sample_row_interval > 0) {
        segment_meta->set_sort_key_sample_row_interval(sort_key_sample_row_interval);
    }
}

} // namespace starrocks
