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

#include <string>

namespace starrocks {
namespace {

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
    for (const auto& [bytes, mime] : cases) {
        auto result = prepare_inline_ai_media(bytes, "", bytes.size());
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(mime, result->mime_type);
        EXPECT_EQ(bytes, result->bytes);
        EXPECT_NE(bytes.data(), result->bytes.data());
        EXPECT_TRUE(prepare_inline_ai_media(bytes, mime, bytes.size()).ok());
    }
}

TEST(AIMediaInputTest, RejectsEmptyUnknownTruncatedMismatchAndOversizedInputs) {
    const std::string jpeg("\xff\xd8\xff\xe0\0\x02", 6);
    for (const auto& bytes : {std::string{}, std::string("private-payload"), std::string("\xff\xd8", 2),
                              std::string("RIFF\x04\0\0\0WEBP", 12),
                              std::string("\0\0\0\x18"
                                          "ftypisom",
                                          12),
                              std::string("\0\0\0\x10"
                                          "ftypavif\0\0\0\0",
                                          16)}) {
        auto result = prepare_inline_ai_media(bytes, "", 1024);
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(std::string::npos, result.status().message().find("private-payload"));
    }
    EXPECT_FALSE(prepare_inline_ai_media(jpeg, "image/png", 1024).ok());
    EXPECT_FALSE(prepare_inline_ai_media(jpeg, "application/octet-stream", 1024).ok());
    EXPECT_FALSE(prepare_inline_ai_media(jpeg, "image/jpeg; private-secret", 1024).ok());
    EXPECT_FALSE(prepare_inline_ai_media(jpeg, "image/jpeg", jpeg.size() - 1).ok());
    EXPECT_FALSE(prepare_inline_ai_media(jpeg, "image/jpeg", 0).ok());
}

TEST(AIMediaInputTest, CanonicalizesDeclaredMimeAndEncodesAllBinaryBytes) {
    const std::string jpeg("\xff\xd8\xff\xe0\0\x02", 6);
    auto media = prepare_inline_ai_media(jpeg, "IMAGE/JPEG", 1024);
    ASSERT_TRUE(media.ok()) << media.status();
    EXPECT_EQ("image/jpeg", media->mime_type);
    auto uri = encode_ai_media_data_uri(*media);
    ASSERT_TRUE(uri.ok()) << uri.status();
    EXPECT_EQ("data:image/jpeg;base64,/9j/4AAC", *uri);
}

TEST(AIMediaInputTest, DataUriRejectsUnvalidatedMedia) {
    EXPECT_FALSE(encode_ai_media_data_uri({"image/jpeg", ""}).ok());
    EXPECT_FALSE(encode_ai_media_data_uri({"https://private.example", "private-bytes"}).ok());
    EXPECT_FALSE(encode_ai_media_data_uri({"image/png", "private-bytes"}).ok());
}

TEST(AIMediaInputTest, RejectsTruncatedJpegSegmentsAndNonVideoIsoMedia) {
    for (const auto& bytes : {std::string("\xff\xd8\xff\xe0", 4), std::string("\xff\xd8\xff\xe0\0\x10", 6),
                              std::string("\xff\xd8\xff\xd0\0\x02", 6),
                              std::string("\0\0\0\x18"
                                          "ftypavif\0\0\0\0isommp42",
                                          24)}) {
        EXPECT_FALSE(prepare_inline_ai_media(bytes, "", 1024).ok());
    }
}

} // namespace
} // namespace starrocks
