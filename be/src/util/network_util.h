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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/network_util.h

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

#pragma once

#include <sys/un.h>

#include <string>
#include <vector>

#include "common/status.h"
#include "common/statusor.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {

class InetAddress {
public:
    InetAddress(std::string ip, sa_family_t family, bool is_loopback);
    bool is_loopback() const;
    std::string get_host_address() const;
    bool is_ipv6() const;

private:
    std::string _ip_addr;
    sa_family_t _family;
    bool _is_loopback;
};

// Looks up all IP addresses associated with a given hostname. Returns
// an error status if any system call failed, otherwise OK. Even if OK
// is returned, addresses may still be of zero length.
Status hostname_to_ip(const std::string& host, std::string& ip);
Status hostname_to_ip(const std::string& host, std::string& ip, bool ipv6);
Status hostname_to_ipv4(const std::string& host, std::string& ip);
Status hostname_to_ipv6(const std::string& host, std::string& ip);

bool is_valid_ip(const std::string& ip);

// Check if an IP address is in a private/reserved range
// Returns true for: loopback (127.0.0.0/8, ::1), private networks (10.0.0.0/8,
// 172.16.0.0/12, 192.168.0.0/16), link-local (169.254.0.0/16, fe80::/10),
// unique local (fc00::/7), and other reserved ranges.
// Also returns true for invalid IP addresses (fail-safe behavior).
bool is_private_ip(const std::string& ip);

// Check if an IP address is in link-local range (169.254.0.0/16 for IPv4, fe80::/10 for IPv6)
// These ranges are commonly used for cloud metadata services (AWS, GCP, Azure IMDS)
// and require special security warnings.
bool is_link_local_ip(const std::string& ip);

// Resolve hostname to all IP addresses (both IPv4 and IPv6)
// Returns a vector of resolved IP addresses as strings.
// If the host is already a valid IP address, returns it directly.
StatusOr<std::vector<std::string>> resolve_hostname_all_ips(const std::string& hostname);

// Extract host from URL using libcurl's URL parser (curl_url_* API)
// This ensures consistent parsing with curl's actual connection behavior,
// preventing SSRF bypass attacks via URL parsing discrepancies.
// Examples:
//   "http://example.com/path" -> "example.com"
//   "https://api.example.com:8080/v1" -> "api.example.com"
//   "http://user:pass@example.com/" -> "example.com"
//   "http://[::1]:8080/test" -> "::1"
//   "http://x@allowed.com:y@evil.com/" -> "evil.com" (matches curl behavior)
// Returns Status error for invalid URLs.
StatusOr<std::string> extract_host_from_url(const std::string& url);

// Extract port from URL using libcurl's URL parser (curl_url_* API)
// Returns default port (80 for HTTP, 443 for HTTPS) if not specified.
// This ensures consistent parsing with curl's actual connection behavior.
// Examples:
//   "http://example.com/path" -> 80
//   "https://example.com/path" -> 443
//   "http://example.com:8080/path" -> 8080
//   "https://api.example.com:9443/v1" -> 9443
// Returns Status error for invalid URLs.
StatusOr<int> extract_port_from_url(const std::string& url);

// Sets the output argument to the system defined hostname.
// Returns OK if a hostname can be found, false otherwise.
Status get_hostname(std::string* hostname);

Status get_hosts(std::vector<InetAddress>* hosts);

// Utility method because Thrift does not supply useful constructors
TNetworkAddress make_network_address(const std::string& hostname, int port);

Status get_inet_interfaces(std::vector<std::string>* interfaces, bool include_ipv6 = false);

std::string get_host_port(const std::string& host, int port);

} // namespace starrocks
