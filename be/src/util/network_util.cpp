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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/network_util.cpp

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

#include "util/network_util.h"

#include <arpa/inet.h>
#include <curl/curl.h>
#include <fmt/format.h>

#include <common/logging.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <cerrno>
#include <climits>
#include <cstring>
#include <sstream>
#include <utility>

#include "gutil/strings/substitute.h"

#ifdef __APPLE__
#ifndef HOST_NAME_MAX
#define HOST_NAME_MAX MAXHOSTNAMELEN
#endif
#endif

namespace starrocks {

InetAddress::InetAddress(std::string ip, sa_family_t family, bool is_loopback)
        : _ip_addr(std::move(ip)), _family(family), _is_loopback(is_loopback) {}

bool InetAddress::is_loopback() const {
    return _is_loopback;
}

std::string InetAddress::get_host_address() const {
    return _ip_addr;
}

bool InetAddress::is_ipv6() const {
    return _family == AF_INET6;
}

Status get_hostname(std::string* hostname) {
    char name[HOST_NAME_MAX];
    int ret = gethostname(name, HOST_NAME_MAX);

    if (ret != 0) {
        std::stringstream ss;
        ss << "Could not get hostname: errno: " << errno;
        return Status::InternalError(ss.str());
    }

    *hostname = std::string(name);
    return Status::OK();
}

Status get_hosts(std::vector<InetAddress>* hosts) {
    ifaddrs* if_addrs = nullptr;
    if (getifaddrs(&if_addrs)) {
        std::stringstream ss;
        char buf[64];
        ss << "getifaddrs failed because " << strerror_r(errno, buf, sizeof(buf));
        return Status::InternalError(ss.str());
    }

    std::vector<InetAddress> hosts_v4;
    std::vector<InetAddress> hosts_v6;
    for (ifaddrs* if_addr = if_addrs; if_addr != nullptr; if_addr = if_addr->ifa_next) {
        if (!if_addr->ifa_addr) {
            continue;
        }
        auto addr = if_addr->ifa_addr;
        if (addr->sa_family == AF_INET) {
            //check legitimacy of IPv4 Addresses
            char addr_buf[INET_ADDRSTRLEN];
            auto tmp_addr = &((struct sockaddr_in*)if_addr->ifa_addr)->sin_addr;
            inet_ntop(AF_INET, tmp_addr, addr_buf, INET_ADDRSTRLEN);
            //check is loopback Addresses
            in_addr_t s_addr = ((struct sockaddr_in*)if_addr->ifa_addr)->sin_addr.s_addr;
            bool is_loopback = (ntohl(s_addr) & 0xFF000000) == 0x7F000000;
            hosts_v4.emplace_back(std::string(addr_buf), AF_INET, is_loopback);
        } else if (addr->sa_family == AF_INET6) {
            //check legitimacy of IPv6 Address
            auto tmp_addr = &((struct sockaddr_in6*)if_addr->ifa_addr)->sin6_addr;
            char addr_buf[INET6_ADDRSTRLEN];
            inet_ntop(AF_INET6, tmp_addr, addr_buf, sizeof(addr_buf));
            //check is loopback Addresses
            bool is_loopback = IN6_IS_ADDR_LOOPBACK(tmp_addr);
            std::string addr_str(addr_buf);
            boost::algorithm::to_lower(addr_str);
            // Starts with "fe80"(Link local address), not supported.
            if (addr_str.rfind("fe80", 0) == 0) {
                LOG(INFO) << "ipv6 link local address " << addr_str << " is skipped";
                continue;
            }
            hosts_v6.emplace_back(addr_str, AF_INET6, is_loopback);
        } else {
            continue;
        }
    }

    // Prefer ipv4 address by default for compatibility reason.
    hosts->insert(hosts->end(), hosts_v4.begin(), hosts_v4.end());
    hosts->insert(hosts->end(), hosts_v6.begin(), hosts_v6.end());

    if (if_addrs != nullptr) {
        freeifaddrs(if_addrs);
    }

    return Status::OK();
}

Status hostname_to_ipv4(const std::string& host, std::string& ip) {
    addrinfo hints, *res;
    in_addr addr;

    memset(&hints, 0, sizeof(addrinfo));
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_INET;
    int err = getaddrinfo(host.c_str(), nullptr, &hints, &res);
    if (err != 0) {
        std::string err_msg = strings::Substitute("failed to get ipv4 from host: $0, err: $1", host, gai_strerror(err));
        LOG(WARNING) << err_msg;
        return Status::InternalError(err_msg);
    }

    addr.s_addr = ((sockaddr_in*)(res->ai_addr))->sin_addr.s_addr;
    ip = inet_ntoa(addr);

    freeaddrinfo(res);
    return Status::OK();
}

Status hostname_to_ipv6(const std::string& host, std::string& ip) {
    char ipv6_str[128];
    struct sockaddr_in6* sockaddr_ipv6;

    struct addrinfo *answer, hint;
    bzero(&hint, sizeof(hint));
    hint.ai_family = AF_INET6;
    hint.ai_socktype = SOCK_STREAM;

    int err = getaddrinfo(host.c_str(), nullptr, &hint, &answer);
    if (err != 0) {
        std::string err_msg = strings::Substitute("failed to get ipv6 from host: $0, err: $1", host, gai_strerror(err));
        LOG(WARNING) << err_msg;
        return Status::InternalError(err_msg);
    }

    sockaddr_ipv6 = reinterpret_cast<struct sockaddr_in6*>(answer->ai_addr);
    inet_ntop(AF_INET6, &sockaddr_ipv6->sin6_addr, ipv6_str, sizeof(ipv6_str));
    ip = ipv6_str;
    fflush(nullptr);
    freeaddrinfo(answer);
    return Status::OK();
}

bool is_valid_ip(const std::string& ip) {
    unsigned char buf[sizeof(struct in6_addr)];
    return (inet_pton(AF_INET6, ip.data(), buf) > 0) || (inet_pton(AF_INET, ip.data(), buf) > 0);
}

// Check if IPv4 address (as 32-bit integer in host byte order) is in a private range
static bool is_private_ipv4(uint32_t ip) {
    // 127.0.0.0/8 - Loopback
    if ((ip & 0xFF000000) == 0x7F000000) return true;

    // 10.0.0.0/8 - Class A Private
    if ((ip & 0xFF000000) == 0x0A000000) return true;

    // 172.16.0.0/12 - Class B Private
    if ((ip & 0xFFF00000) == 0xAC100000) return true;

    // 192.168.0.0/16 - Class C Private
    if ((ip & 0xFFFF0000) == 0xC0A80000) return true;

    // 169.254.0.0/16 - Link-local(Include IMDS)
    if ((ip & 0xFFFF0000) == 0xA9FE0000) return true;

    // 0.0.0.0/8 - Current network
    if ((ip & 0xFF000000) == 0x00000000) return true;

    return false;
}

// Check if IPv6 address is in a private range
static bool is_private_ipv6(const unsigned char* ip) {
    // :: - Unspecified address (all zeros) - block to prevent SSRF bypass
    static const unsigned char unspecified[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    if (memcmp(ip, unspecified, 16) == 0) return true;

    // ::1 - Loopback
    static const unsigned char loopback[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1};
    if (memcmp(ip, loopback, 16) == 0) return true;

    // fc00::/7 - Unique local (fc00:: to fdff::)
    if ((ip[0] & 0xFE) == 0xFC) return true;

    // fe80::/10 - Link-local
    if (ip[0] == 0xFE && (ip[1] & 0xC0) == 0x80) return true;

    // ::ffff:0:0/96 - IPv4-mapped IPv6, check the embedded IPv4
    static const unsigned char v4mapped_prefix[12] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF};
    if (memcmp(ip, v4mapped_prefix, 12) == 0) {
        uint32_t v4_addr = (static_cast<uint32_t>(ip[12]) << 24) | (static_cast<uint32_t>(ip[13]) << 16) |
                           (static_cast<uint32_t>(ip[14]) << 8) | static_cast<uint32_t>(ip[15]);
        return is_private_ipv4(v4_addr);
    }

    return false;
}

bool is_private_ip(const std::string& ip) {
    // Try IPv4 first
    struct in_addr addr4;
    if (inet_pton(AF_INET, ip.c_str(), &addr4) == 1) {
        return is_private_ipv4(ntohl(addr4.s_addr));
    }

    // Try IPv6
    struct in6_addr addr6;
    if (inet_pton(AF_INET6, ip.c_str(), &addr6) == 1) {
        return is_private_ipv6(addr6.s6_addr);
    }

    // Invalid IP format - treat as private for safety
    return true;
}

bool is_link_local_ip(const std::string& ip) {
    // Try IPv4 first
    struct in_addr addr4;
    if (inet_pton(AF_INET, ip.c_str(), &addr4) == 1) {
        uint32_t ip_num = ntohl(addr4.s_addr);
        // 169.254.0.0/16 - Link-local (AWS/GCP/Azure metadata, etc.)
        return (ip_num & 0xFFFF0000) == 0xA9FE0000;
    }

    // Try IPv6
    struct in6_addr addr6;
    if (inet_pton(AF_INET6, ip.c_str(), &addr6) == 1) {
        // fe80::/10 - Link-local
        if (addr6.s6_addr[0] == 0xFE && (addr6.s6_addr[1] & 0xC0) == 0x80) {
            return true;
        }
        // ::ffff:169.254.x.x - IPv4-mapped link-local
        static const unsigned char v4mapped_prefix[12] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF};
        if (memcmp(addr6.s6_addr, v4mapped_prefix, 12) == 0) {
            uint32_t v4_addr = (static_cast<uint32_t>(addr6.s6_addr[12]) << 24) |
                               (static_cast<uint32_t>(addr6.s6_addr[13]) << 16) |
                               (static_cast<uint32_t>(addr6.s6_addr[14]) << 8) |
                               static_cast<uint32_t>(addr6.s6_addr[15]);
            return (v4_addr & 0xFFFF0000) == 0xA9FE0000;
        }
    }

    return false;
}

StatusOr<std::vector<std::string>> resolve_hostname_all_ips(const std::string& hostname) {
    std::vector<std::string> results;

    // Check if hostname is already a valid IP address
    if (is_valid_ip(hostname)) {
        results.push_back(hostname);
        return results;
    }

    struct addrinfo hints {};
    hints.ai_family = AF_UNSPEC;     // Allow IPv4 or IPv6
    hints.ai_socktype = SOCK_STREAM; // TCP
    hints.ai_flags = AI_ADDRCONFIG;  // Only return addresses the system can reach

    struct addrinfo* res = nullptr;
    int err = getaddrinfo(hostname.c_str(), nullptr, &hints, &res);
    if (err != 0) {
        return Status::InvalidArgument(
                strings::Substitute("DNS resolution failed for $0: $1", hostname, gai_strerror(err)));
    }

    for (struct addrinfo* p = res; p != nullptr; p = p->ai_next) {
        char ip_str[INET6_ADDRSTRLEN];

        if (p->ai_family == AF_INET) {
            struct sockaddr_in* addr = reinterpret_cast<struct sockaddr_in*>(p->ai_addr);
            inet_ntop(AF_INET, &addr->sin_addr, ip_str, sizeof(ip_str));
            results.emplace_back(ip_str);
        } else if (p->ai_family == AF_INET6) {
            struct sockaddr_in6* addr = reinterpret_cast<struct sockaddr_in6*>(p->ai_addr);
            inet_ntop(AF_INET6, &addr->sin6_addr, ip_str, sizeof(ip_str));
            results.emplace_back(ip_str);
        }
    }

    freeaddrinfo(res);

    if (results.empty()) {
        return Status::InvalidArgument(strings::Substitute("No IP addresses found for $0", hostname));
    }

    return results;
}

// RAII wrapper for CURLU handle to ensure proper cleanup
class CurlUrlHandle {
public:
    CurlUrlHandle() : _handle(curl_url()) {}
    ~CurlUrlHandle() {
        if (_handle) {
            curl_url_cleanup(_handle);
        }
    }
    CurlUrlHandle(const CurlUrlHandle&) = delete;
    CurlUrlHandle& operator=(const CurlUrlHandle&) = delete;

    CURLU* get() const { return _handle; }
    explicit operator bool() const { return _handle != nullptr; }

private:
    CURLU* _handle;
};

// RAII wrapper for curl_free to ensure proper cleanup of curl-allocated strings
class CurlString {
public:
    explicit CurlString(char* str) : _str(str) {}
    ~CurlString() {
        if (_str) {
            curl_free(_str);
        }
    }
    CurlString(const CurlString&) = delete;
    CurlString& operator=(const CurlString&) = delete;

    const char* get() const { return _str; }
    explicit operator bool() const { return _str != nullptr; }

private:
    char* _str;
};

StatusOr<std::string> extract_host_from_url(const std::string& url) {
    if (url.empty()) {
        return Status::InvalidArgument("URL is empty");
    }

    CurlUrlHandle curl_url;
    if (!curl_url) {
        return Status::InternalError("Failed to create CURL URL handle");
    }

    // Parse the URL using libcurl's URL parser
    // This ensures we parse URLs exactly the same way curl will when making requests
    CURLUcode rc = curl_url_set(curl_url.get(), CURLUPART_URL, url.c_str(), 0);
    if (rc != CURLUE_OK) {
        return Status::InvalidArgument(
                fmt::format("Invalid URL '{}': {}", url, curl_url_strerror(rc)));
    }

    // Extract the host component
    char* host = nullptr;
    rc = curl_url_get(curl_url.get(), CURLUPART_HOST, &host, 0);
    if (rc != CURLUE_OK || host == nullptr) {
        return Status::InvalidArgument(
                fmt::format("Failed to extract host from URL '{}': {}",
                            url, curl_url_strerror(rc)));
    }

    CurlString host_guard(host);
    std::string result(host_guard.get());

    // Strip brackets from IPv6 addresses (curl returns [::1], we want ::1)
    if (result.size() >= 2 && result.front() == '[' && result.back() == ']') {
        result = result.substr(1, result.size() - 2);
    }

    return result;
}

StatusOr<int> extract_port_from_url(const std::string& url) {
    if (url.empty()) {
        return Status::InvalidArgument("URL is empty");
    }

    CurlUrlHandle curl_url;
    if (!curl_url) {
        return Status::InternalError("Failed to create CURL URL handle");
    }

    // Parse the URL using libcurl's URL parser
    CURLUcode rc = curl_url_set(curl_url.get(), CURLUPART_URL, url.c_str(), 0);
    if (rc != CURLUE_OK) {
        return Status::InvalidArgument(
                fmt::format("Invalid URL '{}': {}", url, curl_url_strerror(rc)));
    }

    // Extract the port component
    // CURLU_DEFAULT_PORT: If no port is specified, return the default port for the scheme
    char* port_str = nullptr;
    rc = curl_url_get(curl_url.get(), CURLUPART_PORT, &port_str, CURLU_DEFAULT_PORT);
    if (rc != CURLUE_OK || port_str == nullptr) {
        return Status::InvalidArgument(
                fmt::format("Failed to extract port from URL '{}': {}",
                            url, curl_url_strerror(rc)));
    }

    CurlString port_guard(port_str);

    try {
        int port = std::stoi(port_guard.get());
        if (port <= 0 || port > 65535) {
            return Status::InvalidArgument(
                    fmt::format("Port out of range in URL '{}': {}", url, port));
        }
        return port;
    } catch (const std::exception& e) {
        return Status::InvalidArgument(
                fmt::format("Invalid port in URL '{}': {}", url, e.what()));
    }
}

// Prefer ipv4 when both ipv4 and ipv6 bound to the same host
Status hostname_to_ip(const std::string& host, std::string& ip) {
    auto start = std::chrono::high_resolution_clock::now();
    Status status = hostname_to_ipv4(host, ip);
    if (status.ok()) {
        return status;
    }
    status = hostname_to_ipv6(host, ip);

    auto current = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(current - start);
    if (duration.count() >= 500) {
        LOG(WARNING) << "hostname_to_ip cost too mush time, cost_time:" << duration.count() << "ms hostname:" << host
                     << " ip:" << ip;
    }
    return status;
}

Status hostname_to_ip(const std::string& host, std::string& ip, bool ipv6) {
    if (ipv6) {
        return hostname_to_ipv6(host, ip);
    } else {
        return hostname_to_ipv4(host, ip);
    }
}

TNetworkAddress make_network_address(const std::string& hostname, int port) {
    TNetworkAddress ret;
    ret.__set_hostname(hostname);
    ret.__set_port(port);
    return ret;
}

Status get_inet_interfaces(std::vector<std::string>* interfaces, bool include_ipv6) {
    ifaddrs* if_addrs = nullptr;
    if (getifaddrs(&if_addrs)) {
        std::stringstream ss;
        char buf[64];
        ss << "getifaddrs failed, errno:" << errno << ", message" << strerror_r(errno, buf, sizeof(buf));
        return Status::InternalError(ss.str());
    }

    for (ifaddrs* if_addr = if_addrs; if_addr != nullptr; if_addr = if_addr->ifa_next) {
        if (if_addr->ifa_addr == nullptr || if_addr->ifa_name == nullptr) {
            continue;
        }
        if (if_addr->ifa_addr->sa_family == AF_INET || (include_ipv6 && if_addr->ifa_addr->sa_family == AF_INET6)) {
            interfaces->emplace_back(if_addr->ifa_name);
        }
    }
    if (if_addrs != nullptr) {
        freeifaddrs(if_addrs);
    }
    return Status::OK();
}

std::string get_host_port(const std::string& host, int port) {
    std::stringstream ss;
    if (host.find(':') == std::string::npos) {
        ss << host << ":" << port;
    } else {
        ss << "[" << host << "]"
           << ":" << port;
    }
    return ss.str();
}

} // namespace starrocks
