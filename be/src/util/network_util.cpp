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
