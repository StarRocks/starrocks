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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "base/statusor.h"

namespace starrocks {

struct FileRowRef;

inline constexpr int64_t kDefaultAIMaxInputFileBytes = 10485760;

struct AIMediaInput {
    std::string content_type;
    // Exactly one source is populated for each FILE input.
    std::optional<std::string> inline_bytes;
    std::optional<std::string> referenced_url;

    bool is_image() const;
    bool is_audio() const;
    bool is_video() const;
};

// DataQualityError denotes invalid row content. Other errors apply to the batch.
StatusOr<AIMediaInput> create_ai_media(const FileRowRef& file, size_t inline_max_bytes);
StatusOr<std::string> encode_ai_media_data_uri(const AIMediaInput& media);

} // namespace starrocks
