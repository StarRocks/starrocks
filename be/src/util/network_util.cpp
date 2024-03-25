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
#include <errno.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <climits>
#include <sstream>

namespace starrocks {

InetAddress::InetAddress(std::string ip, sa_family_t family, bool is_loopback)
        : _ip_addr(ip), _family(family), _is_loopback(is_loopback) {}

bool InetAddress::is_loopback() const {
    return _is_loopback;
}

std::string InetAddress::get_host_address() const {
    return _ip_addr;
}

bool InetAddress::is_ipv6() const {
    return _family == AF_INET6;
}

static const std::string LOCALHOST("127.0.0.1");

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
            if (addr_str.rfind("fe80", 0) == 0) { // Starts with "fe80"(Link local address), not supported.
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

Status hostname_to_ip_addrs(const std::string& name, std::vector<std::string>* addresses) {
    addrinfo hints;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* addr_info;

    if (getaddrinfo(name.c_str(), nullptr, &hints, &addr_info) != 0) {
        std::stringstream ss;
        ss << "Could not find IP address for: " << name;
        return Status::InternalError(ss.str());
    }

    addrinfo* it = addr_info;

    while (it != nullptr) {
        char addr_buf[INET6_ADDRSTRLEN];
        if (it->ai_family == AF_INET) {
            LOG(INFO) << "this host is an IPv4 address, converting...";
            const char* result = inet_ntop(AF_INET, &((sockaddr_in*)it->ai_addr)->sin_addr, addr_buf, INET6_ADDRSTRLEN);
            if (result == nullptr) {
                std::stringstream ss;
                ss << "Could not convert IPv4 address for: " << name;
                freeaddrinfo(addr_info);
                return Status::InternalError(ss.str());
            }
        } else if (it->ai_family == AF_INET6) {
            LOG(INFO) << "this host is IPv6 address, we are about to convert it now.";
            struct sockaddr_in6* sa = reinterpret_cast<struct sockaddr_in6*>(it->ai_addr);
            const char* result = inet_ntop(AF_INET6, &sa->sin6_addr, addr_buf, INET6_ADDRSTRLEN);
            if (result == nullptr) {
                std::stringstream ss;
                ss << "Could not convert IPv6 address for: " << name;
                freeaddrinfo(addr_info);
                return Status::InternalError(ss.str());
            }
        } else {
            std::stringstream ss;
            ss << "unknown address family!";
            freeaddrinfo(addr_info);
            return Status::InternalError(ss.str());
        }

        addresses->push_back(std::string(addr_buf));
        it = it->ai_next;
    }

    freeaddrinfo(addr_info);
    return Status::OK();
}

bool is_valid_ip(const std::string& ip) {
    unsigned char buf[sizeof(struct in6_addr)];
    return (inet_pton(AF_INET6, ip.data(), buf) > 0) || (inet_pton(AF_INET, ip.data(), buf) > 0);
}

std::string hostname_to_ip(const std::string& host) {
    std::vector<std::string> addresses;
    Status status = hostname_to_ip_addrs(host, &addresses);
    if (!status.ok()) {
        LOG(WARNING) << "failed to resolve this hostname " << host << " to ip address,  err is " << status.message();
        return "";
    }
    if (addresses.size() != 1) {
        LOG(WARNING) << "the number of addresses could only be equal to 1, failed to get ip from host";
        return "";
    }
    return addresses[0];
}

bool find_first_non_localhost(const std::vector<std::string>& addresses, std::string* addr) {
    for (const auto& candidate : addresses) {
        if (candidate != LOCALHOST) {
            *addr = candidate;
            return true;
        }
    }

    return false;
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
