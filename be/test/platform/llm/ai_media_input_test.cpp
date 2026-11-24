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

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "types/file_value.h"

namespace starrocks {
namespace {

FileRowRef inline_file(std::string_view bytes, std::optional<Slice> content_type = std::nullopt) {
    return {.content_type = content_type, .inline_bytes = Slice(bytes)};
}

TEST(AIMediaInputTest, ClassifiesInlineAndReferencedMediaByContentTypePrefix) {
    struct TestCase {
        const char* content_type;
        bool image;
        bool audio;
        bool video;
    };
    const TestCase cases[] = {
            {"IMAGE/JPEG", true, false, false},
            {"image/svg+xml", true, false, false},
            {"audio/mpeg", false, true, false},
            {"AuDiO/WAV; codecs=pcm", false, true, false},
            {"video/quicktime", false, false, true},
            {"VIDEO/MP4 ; codecs=avc1", false, false, true},
            {"", false, false, false},
            {"image", false, false, false},
            {"image/", true, false, false},
            {"audio/; codecs=pcm", false, true, false},
            {"video/ \t; codecs=avc1", false, false, true},
            {"application/octet-stream", false, false, false},
            {"application/octet-stream; name=image/jpeg", false, false, false},
    };
    for (const auto& [content_type, image, audio, video] : cases) {
        SCOPED_TRACE(content_type);
        for (const auto& media :
             {AIMediaInput{.content_type = content_type, .inline_bytes = "bytes"},
              AIMediaInput{.content_type = content_type, .referenced_url = "https://example.com/media"}}) {
            SCOPED_TRACE(media.inline_bytes ? "inline" : "URL");
            EXPECT_EQ(image, media.is_image());
            EXPECT_EQ(audio, media.is_audio());
            EXPECT_EQ(video, media.is_video());
        }
    }
}

TEST(AIMediaInputTest, DetectsSupportedHeadersAndOwnsBinaryBytes) {
    const std::pair<std::string, std::string> cases[] = {
            {std::string("\xff\xd8\xff\xe0\0\x02", 6), "image/jpeg"},
            {std::string("\x89PNG\r\n\x1a\n", 8), "image/png"},
            {std::string("RIFF\x0c\0\0\0WEBPVP8 \0\0\0\0", 20), "image/webp"},
            {std::string("\0\0\0\x18"
                         "ftypisom\0\0\0\0isommp42",
                         24),
             "video/mp4"},
    };
    for (const auto& [bytes, content_type] : cases) {
        std::string source = bytes;
        auto result = create_ai_media(inline_file(source), source.size());
        ASSERT_TRUE(result.ok()) << result.status();
        source.front() = 'X';
        EXPECT_EQ(content_type, result->content_type);
        ASSERT_TRUE(result->inline_bytes.has_value());
        EXPECT_EQ(bytes, *result->inline_bytes);
        EXPECT_FALSE(result->referenced_url.has_value());
        EXPECT_TRUE(create_ai_media(inline_file(bytes, ""), bytes.size()).ok());
        EXPECT_TRUE(create_ai_media(inline_file(bytes, content_type), bytes.size()).ok());
    }
}

TEST(AIMediaInputTest, RejectsEmptyUnknownTruncatedAndOversizedInputs) {
    const std::string jpeg("\xff\xd8\xff\xe0\0\x02", 6);
    for (const auto& bytes : {std::string{}, std::string("private-payload"), std::string("\xff\xd8", 2),
                              std::string("RIFF\x04\0\0\0WEBP", 12),
                              std::string("\0\0\0\x18"
                                          "ftypisom",
                                          12),
                              std::string("\0\0\0\x10"
                                          "ftypavif\0\0\0\0",
                                          16)}) {
        auto result = create_ai_media(inline_file(bytes), 1024);
        ASSERT_TRUE(result.status().is_data_quality_error());
        EXPECT_EQ(std::string::npos, result.status().message().find("private-payload"));
    }
    EXPECT_TRUE(create_ai_media(inline_file(jpeg), jpeg.size() - 1).status().is_data_quality_error());
    EXPECT_TRUE(create_ai_media(inline_file(jpeg), 0).status().is_data_quality_error());
}

TEST(AIMediaInputTest, InlineTakesPrecedenceOverReferenceAndRange) {
    FileRowRef file{.uri = Slice("s3://bucket/image.jpg"),
                    .offset = 42,
                    .size = 1,
                    .inline_bytes = Slice("\x89PNG\r\n\x1a\n", 8)};
    auto media = create_ai_media(file, 8);
    ASSERT_TRUE(media.ok()) << media.status();
    EXPECT_EQ("image/png", media->content_type);
    ASSERT_TRUE(media->inline_bytes.has_value());
    EXPECT_EQ(std::string_view(*file.inline_bytes), *media->inline_bytes);
    EXPECT_FALSE(media->referenced_url);

    file.uri = Slice("https://example.com/image.jpg");
    file.offset = 0;
    EXPECT_TRUE(create_ai_media(file, 7).status().is_data_quality_error());
    file.inline_bytes = Slice("");
    EXPECT_TRUE(create_ai_media(file, 8).status().is_data_quality_error());
    file.inline_bytes = Slice("unknown");
    EXPECT_TRUE(create_ai_media(file, 8).status().is_data_quality_error());
}

TEST(AIMediaInputTest, OwnsFullUrlAndInfersTypeFromLastPathSegment) {
    const std::pair<std::string_view, std::string_view> cases[] = {
            {"https://bucket.example/a/photo.JPG?signature=abc%2F123&name=.mp4", "image/jpeg"},
            {"http://bucket.example/photo.jPeG", "image/jpeg"},
            {"HTTPS://bucket.example/photo.png#frame.mp4", "image/png"},
            {"https://bucket.example/photo.png#preview/thumb.jpg?name=clip.mp4", "image/png"},
            {"https://bucket.example/dir.jpg/image.WebP", "image/webp"},
            {"https://bucket.example/video.MP4", "video/mp4"},
    };
    for (auto [url, content_type] : cases) {
        std::string source(url);
        auto media = create_ai_media({.uri = Slice(source), .offset = 0, .size = 1, .content_type = Slice("")}, 0);
        ASSERT_TRUE(media.ok()) << media.status();
        source.front() = 'X';
        EXPECT_EQ(content_type, media->content_type);
        EXPECT_FALSE(media->inline_bytes);
        ASSERT_TRUE(media->referenced_url);
        EXPECT_EQ(url, *media->referenced_url);
    }
    for (auto url : {"https://host.png", "https://host.jpg?file=/image.png", "https://host/image.jpg/",
                     "https://host/dir.jpg/file?name=.jpg", "https://host/image.gif"}) {
        EXPECT_TRUE(create_ai_media({.uri = Slice(url)}, 0).status().is_data_quality_error());
    }
    FileRowRef file{.uri = Slice("https://host/object?signature=secret"), .content_type = Slice("IMAGE/PNG")};
    auto declared = create_ai_media(file, 0);
    ASSERT_TRUE(declared.ok()) << declared.status();
    EXPECT_EQ("IMAGE/PNG", declared->content_type);
    file.offset = 1;
    EXPECT_TRUE(create_ai_media(file, 0).status().is_data_quality_error());
}

TEST(AIMediaInputTest, RejectsMissingSourceAndInvalidUrlEvenWithDeclaredType) {
    EXPECT_TRUE(create_ai_media({}, 0).status().is_data_quality_error());
    for (std::string_view url :
         {"", "s3://private/image.jpg", "file:///private/image.jpg", "/private/image.jpg", "http:private.jpg",
          "https:///private.jpg", "https://host:abc/private.jpg", "https://private host/image.jpg"}) {
        auto result = create_ai_media({.uri = Slice(url), .content_type = Slice("image/png")}, 0);
        ASSERT_TRUE(result.status().is_data_quality_error());
        EXPECT_EQ(std::string::npos, result.status().message().find("private"));
    }
    constexpr char kEmbeddedNull[] = "https://example.com/a.jpg\0?private";
    EXPECT_TRUE(create_ai_media({.uri = Slice(kEmbeddedNull, sizeof(kEmbeddedNull) - 1)}, 0)
                        .status()
                        .is_data_quality_error());
}

TEST(AIMediaInputTest, DataUriUsesDeclaredTypeWithoutItsParameters) {
    const std::string jpeg("\xff\xd8\xff\xe0\0\x02", 6);
    std::string declared = R"(IMAGE/PNG; name="a,b.png")";
    auto media = create_ai_media(inline_file(jpeg, declared), 1024);
    ASSERT_TRUE(media.ok()) << media.status();
    declared.front() = 'X';
    EXPECT_EQ(R"(IMAGE/PNG; name="a,b.png")", media->content_type);
    EXPECT_TRUE(media->is_image());
    auto uri = encode_ai_media_data_uri(*media);
    ASSERT_TRUE(uri.ok()) << uri.status();
    EXPECT_EQ("data:IMAGE/PNG;base64,/9j/4AAC", *uri);

    auto opaque = create_ai_media(inline_file("abc", "application/octet-stream"), 3);
    ASSERT_TRUE(opaque.ok()) << opaque.status();
    auto encoded = encode_ai_media_data_uri(*opaque);
    ASSERT_TRUE(encoded.ok()) << encoded.status();
    EXPECT_EQ("data:application/octet-stream;base64,YWJj", *encoded);
}

TEST(AIMediaInputTest, DataUriRequiresInlineBytesAndMediaType) {
    EXPECT_FALSE(encode_ai_media_data_uri({.content_type = "image/jpeg", .inline_bytes = ""}).ok());
    EXPECT_FALSE(encode_ai_media_data_uri({.inline_bytes = "bytes"}).ok());
    EXPECT_FALSE(encode_ai_media_data_uri({.content_type = "image/png", .referenced_url = "https://host/a.png"}).ok());
}

TEST(AIMediaInputTest, RejectsTruncatedJpegSegmentsAndNonVideoIsoMedia) {
    for (const auto& bytes : {std::string("\xff\xd8\xff\xe0", 4), std::string("\xff\xd8\xff\xe0\0\x10", 6),
                              std::string("\xff\xd8\xff\xd0\0\x02", 6),
                              std::string("\0\0\0\x18"
                                          "ftypavif\0\0\0\0isommp42",
                                          24)}) {
        EXPECT_TRUE(create_ai_media(inline_file(bytes), 1024).status().is_data_quality_error());
    }
}

} // namespace
} // namespace starrocks
