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
#include <string>
#include <string_view>

#include "base/statusor.h"

namespace starrocks {

inline constexpr int64_t kDefaultAIMaxInputFileBytes = 10485760;

struct AIMediaInput {
    std::string mime_type;
    std::string bytes;
};

// Inspects bounded container headers, not a full codec decode. Enforces the raw byte limit before copying.
// These helpers consume bytes only and never read files or fetch URLs.
StatusOr<AIMediaInput> prepare_inline_ai_media(std::string_view bytes, std::string_view content_type, size_t max_bytes);
StatusOr<std::string> encode_ai_media_data_uri(const AIMediaInput& media);

} // namespace starrocks
