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

#include <event2/http.h>

#include <algorithm>
#include <cerrno>
#include <limits>
#include <memory>
#include <new>
#include <string_view>
#include <utility>

#include "base/url_coding.h"
#include "gutil/strings/memutil.h"
#include "types/file_value.h"

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

std::string_view detect_content_type_by_bytes(std::string_view bytes) {
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

bool ascii_iequals(std::string_view value, std::string_view other) {
    return value.size() == other.size() && memcasecmp(value.data(), other.data(), other.size()) == 0;
}

std::string_view content_type_without_parameters(std::string_view content_type) {
    return content_type.substr(0, content_type.find_first_of("; \t"));
}

bool has_content_type_prefix(std::string_view content_type, std::string_view prefix) {
    return ascii_iequals(content_type.substr(0, prefix.size()), prefix);
}

Status validate_file_url(const std::string& url) {
    if (url.find('\0') != std::string::npos) return Status::DataQualityError("Invalid AI FILE URL");
    errno = 0;
    std::unique_ptr<evhttp_uri, decltype(&evhttp_uri_free)> uri(evhttp_uri_parse(url.c_str()), evhttp_uri_free);
    if (!uri) {
        if (errno == ENOMEM) return Status::MemoryLimitExceeded("Failed to parse AI FILE URL");
        return Status::DataQualityError("Invalid AI FILE URL");
    }
    const char* scheme = evhttp_uri_get_scheme(uri.get());
    const char* host = evhttp_uri_get_host(uri.get());
    if (!scheme || (!ascii_iequals(scheme, "https") && !ascii_iequals(scheme, "http")) || !host || !*host) {
        return Status::DataQualityError("Invalid AI FILE URL");
    }
    return Status::OK();
}

StatusOr<std::string_view> detect_content_type_by_url(const std::string& url) {
    // The caller has validated the HTTP(S) URL, so the authority follows "://".
    std::string_view host_and_path(url);
    host_and_path.remove_prefix(host_and_path.find("://") + 3);
    host_and_path = host_and_path.substr(0, host_and_path.find_first_of("?#"));
    const auto slash = host_and_path.rfind('/');
    const auto dot = host_and_path.rfind('.');
    if (slash != std::string_view::npos && dot != std::string_view::npos && dot > slash) {
        const auto extension = host_and_path.substr(dot);
        if (ascii_iequals(extension, ".jpg") || ascii_iequals(extension, ".jpeg")) return "image/jpeg";
        if (ascii_iequals(extension, ".png")) return "image/png";
        if (ascii_iequals(extension, ".webp")) return "image/webp";
        if (ascii_iequals(extension, ".mp4")) return "video/mp4";
    }
    return Status::DataQualityError("Cannot infer AI FILE content type from URL path");
}

} // namespace

bool AIMediaInput::is_image() const {
    return has_content_type_prefix(content_type, "image/");
}

bool AIMediaInput::is_audio() const {
    return has_content_type_prefix(content_type, "audio/");
}

bool AIMediaInput::is_video() const {
    return has_content_type_prefix(content_type, "video/");
}

StatusOr<AIMediaInput> create_ai_media(const FileRowRef& file, size_t inline_max_bytes) {
    try {
        std::string_view content_type = file.content_type.value_or(Slice());
        if (file.inline_bytes) {
            const std::string_view bytes = *file.inline_bytes;
            if (bytes.empty()) return Status::DataQualityError("AI FILE input is empty");
            if (bytes.size() > inline_max_bytes) {
                return Status::DataQualityError("AI FILE input exceeds the configured byte limit");
            }
            if (content_type.empty()) {
                content_type = detect_content_type_by_bytes(bytes);
                if (content_type.empty())
                    return Status::DataQualityError("AI FILE input has an unsupported or invalid media header");
            }
            return AIMediaInput{.content_type = std::string(content_type), .inline_bytes = std::string(bytes)};
        }
        if (file.uri) {
            if (file.offset.value_or(0) > 0) {
                return Status::DataQualityError("AI FILE URL does not support a positive offset");
            }
            std::string url = file.uri->to_string();
            RETURN_IF_ERROR(validate_file_url(url));
            if (content_type.empty()) {
                ASSIGN_OR_RETURN(content_type, detect_content_type_by_url(url));
            }
            return AIMediaInput{.content_type = std::string(content_type), .referenced_url = std::move(url)};
        }
        return Status::DataQualityError("AI FILE input has neither inline bytes nor a URL");
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Failed to allocate AI FILE input");
    }
}

StatusOr<std::string> encode_ai_media_data_uri(const AIMediaInput& media) {
    const auto content_type = content_type_without_parameters(media.content_type);
    if (!media.inline_bytes || media.inline_bytes->empty() || media.referenced_url || content_type.empty()) {
        return Status::InvalidArgument("AI media input is invalid");
    }
    constexpr size_t kDataUriOverhead = sizeof("data:;base64,") - 1;
    // Provider JSON writers use 32-bit string lengths, even on hosts with a larger std::string limit.
    const size_t max_size = std::min<size_t>(std::string{}.max_size(), std::numeric_limits<uint32_t>::max());
    if (content_type.size() > max_size - kDataUriOverhead ||
        media.inline_bytes->size() > (max_size - kDataUriOverhead - content_type.size()) / 4 * 3) {
        return Status::InvalidArgument("AI media input is too large to encode");
    }
    try {
        std::string encoded;
        base64_encode(*media.inline_bytes, &encoded);
        encoded.insert(0, "data:" + std::string(content_type) + ";base64,");
        return encoded;
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Failed to encode AI media input");
    }
}

} // namespace starrocks
