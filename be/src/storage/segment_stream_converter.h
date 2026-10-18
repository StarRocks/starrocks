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

#pragma once

#include <unordered_map>
#include <unordered_set>

#include "storage/file_stream_converter.h"

namespace starrocks {

class SegmentStreamConverter : public FileStreamConverter {
public:
    explicit SegmentStreamConverter(std::string_view input_file_name, uint64_t input_file_size,
                                    std::unique_ptr<WritableFile> output_file,
                                    std::unordered_map<uint32_t, uint32_t>* column_unique_id_map,
                                    bool strict_column_uid_map = false,
                                    const std::unordered_set<uint32_t>* dropped_column_uids = nullptr);

    Status append(const void* data, size_t size) override;

    Status close() override;

private:
    std::unordered_map<uint32_t, uint32_t>* _column_unique_id_map;
    bool _strict_column_uid_map;
    // Source UIDs validated by the light-schema helper as dropped scalar value columns.
    const std::unordered_set<uint32_t>* _dropped_column_uids;
    std::string _segment_footer_buffer;
};

} // namespace starrocks
