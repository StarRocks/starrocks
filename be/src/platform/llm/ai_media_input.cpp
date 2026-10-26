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

#include "platform/llm/ai_media_input.h"

#include <algorithm>
#include <limits>
#include <new>

#include "base/url_coding.h"

namespace starrocks {
namespace {

uint32_t read_u32(std::string_view bytes, size_t offset, bool little_endian) {
    uint32_t value = 0;
    for (size_t i = 0; i < 4; ++i) {
        value = (value << 8) | static_cast<unsigned char>(bytes[offset + (little_endian ? 3 - i : i)]);
    }
    return value;
}

bool is_mp4_brand(std::string_view brand) {
    return brand == "isom" || brand == "iso2" || brand == "iso3" || brand == "iso4" || brand == "iso5" ||
           brand == "iso6" || brand == "mp41" || brand == "mp42" || brand == "avc1" || brand == "M4V ";
}

std::string_view detect_mime(std::string_view bytes) {
    if (bytes.size() >= 6 && bytes.substr(0, 3) == std::string_view("\xff\xd8\xff", 3)) {
        const auto marker = static_cast<unsigned char>(bytes[3]);
        const auto segment_size = (static_cast<unsigned char>(bytes[4]) << 8) | static_cast<unsigned char>(bytes[5]);
        if (marker >= 0xc0 && marker <= 0xfe && !(marker >= 0xd0 && marker <= 0xd9) && segment_size >= 2 &&
            segment_size <= bytes.size() - 4) {
            return "image/jpeg";
        }
    }
    if (bytes.starts_with(std::string_view("\x89PNG\r\n\x1a\n", 8))) return "image/png";
    if (bytes.size() >= 20 && bytes.substr(0, 4) == "RIFF" && bytes.substr(8, 4) == "WEBP") {
        const uint32_t container_size = read_u32(bytes, 4, true);
        const uint32_t chunk_size = read_u32(bytes, 16, true);
        const auto chunk = bytes.substr(12, 4);
        if (container_size >= 12 && container_size <= bytes.size() - 8 && chunk_size <= container_size - 12 &&
            (chunk == "VP8 " || chunk == "VP8L" || chunk == "VP8X")) {
            return "image/webp";
        }
    }
    if (bytes.size() >= 16 && bytes.substr(4, 4) == "ftyp") {
        const uint32_t box_size = read_u32(bytes, 0, false);
        // Bound the file-type box independently of the whole-file limit. Only recognize known major brands:
        // image containers such as AVIF may also list generic ISO/MP4 compatible brands.
        if (box_size < 16 || box_size > 4096 || box_size > bytes.size() || box_size % 4 != 0) return {};
        if (is_mp4_brand(bytes.substr(8, 4))) return "video/mp4";
    }
    return {};
}

bool mime_matches(std::string_view declared, std::string_view detected) {
    if (declared.empty()) return true;
    if (declared.size() != detected.size()) return false;
    for (size_t i = 0; i < declared.size(); ++i) {
        const unsigned char byte = declared[i];
        if ((byte >= 'A' && byte <= 'Z' ? byte - 'A' + 'a' : byte) != detected[i]) return false;
    }
    return true;
}

} // namespace

StatusOr<AIMediaInput> prepare_inline_ai_media(std::string_view bytes, std::string_view content_type,
                                               size_t max_bytes) {
    if (bytes.empty()) return Status::InvalidArgument("AI FILE input is empty");
    if (bytes.size() > max_bytes) return Status::InvalidArgument("AI FILE input exceeds the configured byte limit");
    const auto mime = detect_mime(bytes);
    if (mime.empty()) return Status::InvalidArgument("AI FILE input has an unsupported or invalid media header");
    if (!mime_matches(content_type, mime)) {
        return Status::InvalidArgument("AI FILE content type does not match its media header");
    }
    try {
        return AIMediaInput{std::string(mime), std::string(bytes)};
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Failed to allocate AI FILE input");
    }
}

StatusOr<std::string> encode_ai_media_data_uri(const AIMediaInput& media) {
    const auto mime = detect_mime(media.bytes);
    if (mime.empty() || media.mime_type != mime) return Status::InvalidArgument("AI media input is invalid");
    constexpr size_t kDataUriOverhead = sizeof("data:;base64,") - 1;
    const size_t prefix_size = kDataUriOverhead + mime.size();
    // Provider JSON writers use 32-bit string lengths, even on hosts with a larger std::string limit.
    const size_t max_size = std::min<size_t>(std::string{}.max_size(), std::numeric_limits<uint32_t>::max());
    if (media.bytes.size() > (max_size - prefix_size) / 4 * 3) {
        return Status::InvalidArgument("AI media input is too large to encode");
    }
    try {
        std::string encoded;
        base64_encode(media.bytes, &encoded);
        encoded.insert(0, "data:" + std::string(mime) + ";base64,");
        return encoded;
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Failed to encode AI media input");
    }
}

} // namespace starrocks
