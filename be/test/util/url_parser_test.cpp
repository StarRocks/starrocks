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

#include "util/url_parser.h"

#include <gtest/gtest.h>

namespace starrocks {
#define TEST_URL_PARSE(URL, PART, EXPECTED)                                           \
    {                                                                                 \
        Slice result;                                                                 \
        bool res = UrlParser::parse_url(URL, UrlParser::get_url_part(PART), &result); \
        result = res ? result : "<null>";                                             \
        EXPECT_EQ(result.to_string(), EXPECTED);                                      \
    }
TEST(UrlParserTest, normal) {
    const char* url = "http://user:pass@example.com:80/docs/books/tutorial/index.html?name=networking#DOWNLOADING";
    TEST_URL_PARSE(url, "AUTHORITY", "user:pass@example.com:80");
    TEST_URL_PARSE(url, "FILE", "/docs/books/tutorial/index.html?name=networking");
    TEST_URL_PARSE(url, "HOST", "example.com");
    TEST_URL_PARSE(url, "PATH", "/docs/books/tutorial/index.html");
    TEST_URL_PARSE(url, "PROTOCOL", "http");
    TEST_URL_PARSE(url, "QUERY", "name=networking");
    TEST_URL_PARSE(url, "REF", "DOWNLOADING");
    TEST_URL_PARSE(url, "USERINFO", "user:pass");

    TEST_URL_PARSE("http://example.com/index.html?1111%22:xxx", "HOST", "example.com");
}
TEST(UrlParserTest, reletive) {
    const char* url = "/docs/books/tutorial/index.html?name=networking#DOWNLOADING";
    TEST_URL_PARSE(url, "AUTHORITY", "<null>");
    TEST_URL_PARSE(url, "FILE", "/docs/books/tutorial/index.html?name=networking");
    TEST_URL_PARSE(url, "HOST", "<null>");
    TEST_URL_PARSE(url, "PATH", "/docs/books/tutorial/index.html");
    TEST_URL_PARSE(url, "PROTOCOL", "<null>");
    TEST_URL_PARSE(url, "QUERY", "name=networking");
    TEST_URL_PARSE(url, "REF", "DOWNLOADING");
    TEST_URL_PARSE(url, "USERINFO", "<null>");

    TEST_URL_PARSE("2025-02-02", "PATH", "2025-02-02");
    TEST_URL_PARSE("2025-02-02", "FILE", "2025-02-02");
    TEST_URL_PARSE("2025-02-02", "QUERY", "<null>");
    TEST_URL_PARSE("2025-02-02", "REF", "<null>");
    TEST_URL_PARSE("2025-02-02", "PROTOCOL", "<null>");

    TEST_URL_PARSE("2025-02-02/124", "PATH", "2025-02-02/124");
}

} // namespace starrocks
