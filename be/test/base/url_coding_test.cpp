// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "base/url_coding.h"

#include <gtest/gtest.h>

#include <string>

namespace starrocks {

TEST(UrlCodingTest, UrlDecodeBasic) {
    // Simple decode
    auto res = url_decode("abc");
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value(), "abc");
    // Encoded percent
    res = url_decode("a%20b%21c");
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value(), "a b!c");
    res = url_decode("testStreamLoad%E6%A1%8C");
    EXPECT_TRUE(res.ok());
    const char c1[32] = "testStreamLoad桌";
    int ret = memcmp(c1, res.value().c_str(), 17);
    EXPECT_EQ(ret, 0);
}

TEST(UrlCodingTest, UrlDecodeEdgeCases) {
    // Empty string
    auto res = url_decode("");
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value(), "");
    // + should not be decoded to space
    res = url_decode("a+b");
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value(), "a+b");
}

TEST(UrlCodingTest, UrlEncodeBasic) {
    auto r = url_encode("abc");
    ASSERT_TRUE(r.ok());
    EXPECT_EQ(r.value(), "abc");
    // Reserved characters get percent-encoded.
    r = url_encode("a b!c");
    ASSERT_TRUE(r.ok());
    EXPECT_EQ(r.value(), "a%20b%21c");
    r = url_encode("a&b=c?d");
    ASSERT_TRUE(r.ok());
    EXPECT_EQ(r.value(), "a%26b%3Dc%3Fd");
}

TEST(UrlCodingTest, UrlEncodeEmpty) {
    // Must not crash on empty input. libcurl returns an empty buffer
    // for empty input on the versions we ship, which encodes to "".
    auto r = url_encode("");
    ASSERT_TRUE(r.ok());
    EXPECT_EQ(r.value(), "");
}

TEST(UrlCodingTest, UrlEncodeDecodeRoundtrip) {
    const std::string cases[] = {
            "",
            "plain",
            "a b!c",
            "a&b=c?d",
            "/path/to/resource?x=1&y=2",
            "testStreamLoad\xe6\xa1\x8c", // UTF-8 "桌"
            std::string("\x00\x01\x02 binary", 10),
    };
    for (const auto& original : cases) {
        auto encoded = url_encode(original);
        ASSERT_TRUE(encoded.ok()) << "encode failed for: " << original;
        auto decoded = url_decode(encoded.value());
        ASSERT_TRUE(decoded.ok()) << "round-trip failed for: " << original;
        EXPECT_EQ(decoded.value(), original);
    }
}

TEST(Base64CodingTest, EncodeDecodeRoundtrip) {
    const std::string cases[] = {
            "",
            "f",                                                               // 1 byte  -> 2 chars + 2 pads
            "fo",                                                              // 2 bytes -> 3 chars + 1 pad
            "foo",                                                             // 3 bytes -> no pad
            "foobar", "Hello, World!", std::string("\x00\x01\x02\xff\xfe", 5), // binary
    };
    for (const auto& original : cases) {
        std::string encoded;
        base64_encode(original, &encoded);
        std::string decoded;
        ASSERT_TRUE(base64_decode(encoded, &decoded)) << "decode failed for: " << original;
        EXPECT_EQ(decoded, original);
    }
}

TEST(Base64CodingTest, DecodeInvalidInputClearsOut) {
    // Pre-fill *out so we can verify it gets cleared when decode fails — the
    // contract is that on failure the caller observes no half-written buffer.
    std::string out = "PRE-EXISTING";
    // '*' is not a valid base64 character (decoding_table[*] == -2).
    EXPECT_FALSE(base64_decode("abc*def", &out));
    EXPECT_TRUE(out.empty());
}

TEST(Base64CodingTest, DecodeReplacesExistingContent) {
    // base64_decode is replace-semantics, not append. A non-empty *out
    // must be overwritten, not appended to.
    std::string encoded;
    base64_encode("foo", &encoded);

    std::string out = "garbage that should be wiped";
    ASSERT_TRUE(base64_decode(encoded, &out));
    EXPECT_EQ(out, "foo");
}

TEST(Base64CodingTest, DecodeEmptyInput) {
    std::string out = "leftover";
    ASSERT_TRUE(base64_decode("", &out));
    EXPECT_EQ(out, "");
}

} // namespace starrocks
