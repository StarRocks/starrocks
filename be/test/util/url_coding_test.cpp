#include "util/url_coding.h"
#include <gtest/gtest.h>
#include <string>

namespace starrocks {

TEST(UrlCodingTest, UrlDecodeBasic) {
    std::string out;
    // Simple decode
    EXPECT_TRUE(url_decode("abc", &out));
    EXPECT_EQ(out, "abc");
    // Encoded space
    EXPECT_TRUE(url_decode("a+b+c", &out));
    EXPECT_EQ(out, "a b c");
    // Encoded percent
    EXPECT_TRUE(url_decode("a%20b%21c", &out));
    EXPECT_EQ(out, "a b!c");
    EXPECT_TRUE(url_decode("testStreamLoad%E6%A1%8C", &out));
    const char c1[32] = "testStreamLoad桌";
    int res = memcmp(c1, out.c_str(), 17);
    EXPECT_EQ(res, 0);

}

TEST(UrlCodingTest, UrlDecodeInvalid) {
    std::string out;
    // Incomplete percent encoding
    EXPECT_FALSE(url_decode("abc%", &out));
    EXPECT_FALSE(url_decode("abc%2", &out));
}

TEST(UrlCodingTest, UrlDecodeEdgeCases) {
    std::string out;
    // Empty string
    EXPECT_TRUE(url_decode("", &out));
    EXPECT_EQ(out, "");
    // Only pluses
    EXPECT_TRUE(url_decode("+++", &out));
    EXPECT_EQ(out, "   ");
    // Mix of valid and invalid
    EXPECT_FALSE(url_decode("%GG", &out));
}

} // namespace starrocks
