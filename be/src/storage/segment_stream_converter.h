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

#include "storage/file_stream_converter.h"

namespace starrocks {

class SegmentStreamConverter : public FileStreamConverter {
public:
    explicit SegmentStreamConverter(std::string_view input_file_name, uint64_t input_file_size,
                                    std::unique_ptr<WritableFile> output_file,
                                    const std::unordered_map<uint32_t, uint32_t>* column_unique_id_map);

    virtual Status append(const void* data, size_t size) override;

    virtual Status close() override;

private:
    const std::unordered_map<uint32_t, uint32_t>* _column_unique_id_map;
    std::string _segment_footer_buffer;
};

} // namespace starrocks
