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

#include "util/network_util.h"

#include <gtest/gtest.h>

#include <string>

#include "testutil/parallel_test.h"

namespace starrocks {

//=============================================================================
// is_private_ip() Tests - IPv4
//=============================================================================

PARALLEL_TEST(NetworkUtilTest, IsPrivateIP_IPv4_Loopback) {
    // 127.0.0.0/8 - Loopback range
    EXPECT_TRUE(is_private_ip("127.0.0.1"));
    EXPECT_TRUE(is_private_ip("127.0.0.0"));
    EXPECT_TRUE(is_private_ip("127.255.255.255"));
    EXPECT_TRUE(is_private_ip("127.1.2.3"));
}

PARALLEL_TEST(NetworkUtilTest, IsPrivateIP_IPv4_ClassAPrivate) {
    // 10.0.0.0/8 - Class A Private
    EXPECT_TRUE(is_private_ip("10.0.0.0"));
    EXPECT_TRUE(is_private_ip("10.0.0.1"));
    EXPECT_TRUE(is_private_ip("10.255.255.255"));
    EXPECT_TRUE(is_private_ip("10.1.2.3"));
}

PARALLEL_TEST(NetworkUtilTest, IsPrivateIP_IPv4_ClassBPrivate) {
    // 172.16.0.0/12 - Class B Private (172.16.0.0 - 172.31.255.255)
    EXPECT_TRUE(is_private_ip("172.16.0.0"));
    EXPECT_TRUE(is_private_ip("172.16.0.1"));
    EXPECT_TRUE(is_private_ip("172.31.255.255"));
    EXPECT_TRUE(is_private_ip("172.20.5.10"));

    // Outside Class B Private range
    EXPECT_FALSE(is_private_ip("172.15.255.255"));
    EXPECT_FALSE(is_private_ip("172.32.0.0"));
    EXPECT_FALSE(is_private_ip("172.32.0.1"));
}

PARALLEL_TEST(NetworkUtilTest, IsPrivateIP_IPv4_ClassCPrivate) {
    // 192.168.0.0/16 - Class C Private
    EXPECT_TRUE(is_private_ip("192.168.0.0"));
    EXPECT_TRUE(is_private_ip("192.168.0.1"));
    EXPECT_TRUE(is_private_ip("192.168.255.255"));
    EXPECT_TRUE(is_private_ip("192.168.1.100"));
}

PARALLEL_TEST(NetworkUtilTest, IsPrivateIP_IPv4_LinkLocal) {
    // 169.254.0.0/16 - Link-local
    EXPECT_TRUE(is_private_ip("169.254.0.0"));
    EXPECT_TRUE(is_private_ip("169.254.0.1"));
    EXPECT_TRUE(is_private_ip("169.254.255.255"));
    EXPECT_TRUE(is_private_ip("169.254.100.50"));
}

PARALLEL_TEST(NetworkUtilTest, IsPrivateIP_IPv4_CurrentNetwork) {
    // 0.0.0.0/8 - Current network
    EXPECT_TRUE(is_private_ip("0.0.0.0"));
    EXPECT_TRUE(is_private_ip("0.0.0.1"));
    EXPECT_TRUE(is_private_ip("0.255.255.255"));
}

PARALLEL_TEST(NetworkUtilTest, IsPrivateIP_IPv4_PublicAddresses) {
    // Public IP addresses should return false
    EXPECT_FALSE(is_private_ip("8.8.8.8"));       // Google DNS
    EXPECT_FALSE(is_private_ip("1.1.1.1"));       // Cloudflare DNS
    EXPECT_FALSE(is_private_ip("203.0.113.1"));   // TEST-NET-3
    EXPECT_FALSE(is_private_ip("142.250.190.78")); // google.com
    EXPECT_FALSE(is_private_ip("157.240.1.35"));  // facebook.com
    EXPECT_FALSE(is_private_ip("52.94.236.248")); // AWS
}

//=============================================================================
// is_private_ip() Tests - IPv6
//=============================================================================

PARALLEL_TEST(NetworkUtilTest, IsPrivateIP_IPv6_Unspecified) {
    // :: - Unspecified address (all zeros) - should be blocked for SSRF prevention
    EXPECT_TRUE(is_private_ip("::"));
    EXPECT_TRUE(is_private_ip("0:0:0:0:0:0:0:0"));
}

PARALLEL_TEST(NetworkUtilTest, IsPrivateIP_IPv6_Loopback) {
    // ::1 - Loopback
    EXPECT_TRUE(is_private_ip("::1"));
    EXPECT_TRUE(is_private_ip("0:0:0:0:0:0:0:1"));
}

PARALLEL_TEST(NetworkUtilTest, IsPrivateIP_IPv6_UniqueLocal) {
    // fc00::/7 - Unique local (fc00:: to fdff::)
    EXPECT_TRUE(is_private_ip("fc00::1"));
    EXPECT_TRUE(is_private_ip("fc00:1234::5678"));
    EXPECT_TRUE(is_private_ip("fd00::1"));
    EXPECT_TRUE(is_private_ip("fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"));
}

PARALLEL_TEST(NetworkUtilTest, IsPrivateIP_IPv6_LinkLocal) {
    // fe80::/10 - Link-local
    EXPECT_TRUE(is_private_ip("fe80::1"));
    EXPECT_TRUE(is_private_ip("fe80::1234:5678:abcd:ef01"));
    EXPECT_TRUE(is_private_ip("febf::1")); // fe80::/10 range includes up to febf
}

PARALLEL_TEST(NetworkUtilTest, IsPrivateIP_IPv6_IPv4Mapped) {
    // ::ffff:0:0/96 - IPv4-mapped IPv6
    // These should be checked against IPv4 private ranges

    // IPv4-mapped private addresses
    EXPECT_TRUE(is_private_ip("::ffff:127.0.0.1"));
    EXPECT_TRUE(is_private_ip("::ffff:10.0.0.1"));
    EXPECT_TRUE(is_private_ip("::ffff:192.168.1.1"));
    EXPECT_TRUE(is_private_ip("::ffff:172.16.0.1"));

    // IPv4-mapped public addresses
    EXPECT_FALSE(is_private_ip("::ffff:8.8.8.8"));
    EXPECT_FALSE(is_private_ip("::ffff:1.1.1.1"));
}

PARALLEL_TEST(NetworkUtilTest, IsPrivateIP_IPv6_PublicAddresses) {
    // Public IPv6 addresses should return false
    EXPECT_FALSE(is_private_ip("2001:4860:4860::8888")); // Google DNS
    EXPECT_FALSE(is_private_ip("2606:4700:4700::1111")); // Cloudflare DNS
    EXPECT_FALSE(is_private_ip("2607:f8b0:4004:800::200e")); // google.com
}

//=============================================================================
// is_private_ip() Tests - Invalid Input
//=============================================================================

PARALLEL_TEST(NetworkUtilTest, IsPrivateIP_InvalidInput) {
    // Invalid IP addresses should be treated as private (fail-safe)
    EXPECT_TRUE(is_private_ip(""));
    EXPECT_TRUE(is_private_ip("not-an-ip"));
    EXPECT_TRUE(is_private_ip("256.256.256.256"));
    EXPECT_TRUE(is_private_ip("1.2.3"));
    EXPECT_TRUE(is_private_ip("1.2.3.4.5"));
    EXPECT_TRUE(is_private_ip("example.com"));
}

//=============================================================================
// extract_host_from_url() Tests
//=============================================================================

PARALLEL_TEST(NetworkUtilTest, ExtractHost_BasicUrls) {
    EXPECT_EQ(extract_host_from_url("http://example.com"), "example.com");
    EXPECT_EQ(extract_host_from_url("https://example.com"), "example.com");
    EXPECT_EQ(extract_host_from_url("http://example.com/"), "example.com");
    EXPECT_EQ(extract_host_from_url("http://example.com/path"), "example.com");
    EXPECT_EQ(extract_host_from_url("http://example.com/path/to/resource"), "example.com");
}

PARALLEL_TEST(NetworkUtilTest, ExtractHost_WithPort) {
    EXPECT_EQ(extract_host_from_url("http://example.com:8080"), "example.com");
    EXPECT_EQ(extract_host_from_url("https://example.com:443"), "example.com");
    EXPECT_EQ(extract_host_from_url("http://example.com:8080/path"), "example.com");
    EXPECT_EQ(extract_host_from_url("https://api.example.com:8080/v1/users"), "api.example.com");
}

PARALLEL_TEST(NetworkUtilTest, ExtractHost_WithCredentials) {
    EXPECT_EQ(extract_host_from_url("http://user:pass@example.com"), "example.com");
    EXPECT_EQ(extract_host_from_url("http://user:pass@example.com/path"), "example.com");
    EXPECT_EQ(extract_host_from_url("http://user:pass@example.com:8080/path"), "example.com");
    EXPECT_EQ(extract_host_from_url("http://user@example.com"), "example.com");
}

PARALLEL_TEST(NetworkUtilTest, ExtractHost_IPv4Addresses) {
    EXPECT_EQ(extract_host_from_url("http://127.0.0.1"), "127.0.0.1");
    EXPECT_EQ(extract_host_from_url("http://127.0.0.1:8080"), "127.0.0.1");
    EXPECT_EQ(extract_host_from_url("http://192.168.1.1/api"), "192.168.1.1");
    EXPECT_EQ(extract_host_from_url("http://10.0.0.1:3000/test"), "10.0.0.1");
}

PARALLEL_TEST(NetworkUtilTest, ExtractHost_IPv6Addresses) {
    // IPv6 addresses in URLs are enclosed in brackets
    EXPECT_EQ(extract_host_from_url("http://[::1]"), "::1");
    EXPECT_EQ(extract_host_from_url("http://[::1]:8080"), "::1");
    EXPECT_EQ(extract_host_from_url("http://[::1]/path"), "::1");
    EXPECT_EQ(extract_host_from_url("http://[::1]:8080/test"), "::1");
    EXPECT_EQ(extract_host_from_url("http://[2001:db8::1]"), "2001:db8::1");
    EXPECT_EQ(extract_host_from_url("http://[2001:db8::1]:8080/api"), "2001:db8::1");
}

PARALLEL_TEST(NetworkUtilTest, ExtractHost_WithQueryAndFragment) {
    EXPECT_EQ(extract_host_from_url("http://example.com?query=1"), "example.com");
    EXPECT_EQ(extract_host_from_url("http://example.com#fragment"), "example.com");
    EXPECT_EQ(extract_host_from_url("http://example.com/path?query=1#fragment"), "example.com");
    EXPECT_EQ(extract_host_from_url("http://example.com:8080?q=1"), "example.com");
}

PARALLEL_TEST(NetworkUtilTest, ExtractHost_Subdomains) {
    EXPECT_EQ(extract_host_from_url("http://api.example.com"), "api.example.com");
    EXPECT_EQ(extract_host_from_url("http://www.example.com"), "www.example.com");
    EXPECT_EQ(extract_host_from_url("http://sub.domain.example.com"), "sub.domain.example.com");
    EXPECT_EQ(extract_host_from_url("https://api.v1.example.com:8443/users"), "api.v1.example.com");
}

PARALLEL_TEST(NetworkUtilTest, ExtractHost_InvalidUrls) {
    // Invalid URLs should return empty string
    EXPECT_EQ(extract_host_from_url(""), "");
    EXPECT_EQ(extract_host_from_url("not-a-url"), "");
    EXPECT_EQ(extract_host_from_url("example.com"), "");  // Missing scheme
    EXPECT_EQ(extract_host_from_url("://example.com"), ""); // Missing scheme name
    EXPECT_EQ(extract_host_from_url("http:/example.com"), ""); // Single slash
}

//=============================================================================
// extract_port_from_url() Tests
//=============================================================================

PARALLEL_TEST(NetworkUtilTest, ExtractPort_DefaultPorts) {
    // HTTP default port
    EXPECT_EQ(extract_port_from_url("http://example.com"), 80);
    EXPECT_EQ(extract_port_from_url("http://example.com/path"), 80);

    // HTTPS default port
    EXPECT_EQ(extract_port_from_url("https://example.com"), 443);
    EXPECT_EQ(extract_port_from_url("https://example.com/path"), 443);
}

PARALLEL_TEST(NetworkUtilTest, ExtractPort_ExplicitPorts) {
    EXPECT_EQ(extract_port_from_url("http://example.com:8080"), 8080);
    EXPECT_EQ(extract_port_from_url("https://example.com:8443"), 8443);
    EXPECT_EQ(extract_port_from_url("http://example.com:3000/api"), 3000);
    EXPECT_EQ(extract_port_from_url("http://127.0.0.1:9000"), 9000);
}

PARALLEL_TEST(NetworkUtilTest, ExtractPort_CaseInsensitiveScheme) {
    // Mixed case schemes should work correctly
    EXPECT_EQ(extract_port_from_url("HTTP://example.com"), 80);
    EXPECT_EQ(extract_port_from_url("HTTPS://example.com"), 443);
    EXPECT_EQ(extract_port_from_url("Http://example.com"), 80);
    EXPECT_EQ(extract_port_from_url("Https://example.com"), 443);
    EXPECT_EQ(extract_port_from_url("hTtP://example.com"), 80);
    EXPECT_EQ(extract_port_from_url("hTtPs://example.com"), 443);
    EXPECT_EQ(extract_port_from_url("HtTpS://example.com"), 443);
}

PARALLEL_TEST(NetworkUtilTest, ExtractPort_IPv6Addresses) {
    EXPECT_EQ(extract_port_from_url("http://[::1]"), 80);
    EXPECT_EQ(extract_port_from_url("https://[::1]"), 443);
    EXPECT_EQ(extract_port_from_url("http://[::1]:8080"), 8080);
    EXPECT_EQ(extract_port_from_url("http://[2001:db8::1]:9000"), 9000);
}

PARALLEL_TEST(NetworkUtilTest, ExtractPort_InvalidUrls) {
    EXPECT_EQ(extract_port_from_url(""), 0);
    EXPECT_EQ(extract_port_from_url("not-a-url"), 0);
    EXPECT_EQ(extract_port_from_url("example.com"), 0);  // Missing scheme
}

//=============================================================================
// resolve_hostname_all_ips() Tests
//=============================================================================

PARALLEL_TEST(NetworkUtilTest, ResolveHostname_DirectIP) {
    // If input is already an IP, it should be returned directly
    auto result_v4 = resolve_hostname_all_ips("8.8.8.8");
    ASSERT_TRUE(result_v4.ok());
    EXPECT_EQ(result_v4.value().size(), 1);
    EXPECT_EQ(result_v4.value()[0], "8.8.8.8");

    auto result_v6 = resolve_hostname_all_ips("::1");
    ASSERT_TRUE(result_v6.ok());
    EXPECT_EQ(result_v6.value().size(), 1);
    EXPECT_EQ(result_v6.value()[0], "::1");
}

PARALLEL_TEST(NetworkUtilTest, ResolveHostname_Localhost) {
    // localhost should resolve (to 127.0.0.1 and/or ::1)
    auto result = resolve_hostname_all_ips("localhost");
    ASSERT_TRUE(result.ok());
    EXPECT_GE(result.value().size(), 1);

    // Check that at least one result is a loopback address
    bool has_loopback = false;
    for (const auto& ip : result.value()) {
        if (ip == "127.0.0.1" || ip == "::1") {
            has_loopback = true;
            break;
        }
    }
    EXPECT_TRUE(has_loopback);
}

PARALLEL_TEST(NetworkUtilTest, ResolveHostname_InvalidHost) {
    // Invalid hostname should fail
    auto result = resolve_hostname_all_ips("this.hostname.does.not.exist.invalid");
    EXPECT_FALSE(result.ok());
}

//=============================================================================
// is_valid_ip() Tests
//=============================================================================

PARALLEL_TEST(NetworkUtilTest, IsValidIP_IPv4) {
    EXPECT_TRUE(is_valid_ip("0.0.0.0"));
    EXPECT_TRUE(is_valid_ip("127.0.0.1"));
    EXPECT_TRUE(is_valid_ip("192.168.1.1"));
    EXPECT_TRUE(is_valid_ip("255.255.255.255"));
}

PARALLEL_TEST(NetworkUtilTest, IsValidIP_IPv6) {
    EXPECT_TRUE(is_valid_ip("::"));
    EXPECT_TRUE(is_valid_ip("::1"));
    EXPECT_TRUE(is_valid_ip("fe80::1"));
    EXPECT_TRUE(is_valid_ip("2001:db8::1"));
    EXPECT_TRUE(is_valid_ip("::ffff:192.168.1.1"));
}

PARALLEL_TEST(NetworkUtilTest, IsValidIP_Invalid) {
    EXPECT_FALSE(is_valid_ip(""));
    EXPECT_FALSE(is_valid_ip("not-an-ip"));
    EXPECT_FALSE(is_valid_ip("256.256.256.256"));
    EXPECT_FALSE(is_valid_ip("1.2.3"));
    EXPECT_FALSE(is_valid_ip("example.com"));
}

} // namespace starrocks
