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

#include "platform/http/resolved_http_endpoint.h"

#include <arpa/inet.h>

#include <algorithm>
#include <cctype>
#include <cstring>
#include <string_view>

#include "base/network/network_util.h"

namespace starrocks {
namespace {

constexpr std::string_view kInvalidResolvedEndpoint = "invalid resolved HTTP endpoint";

bool host_equal(std::string_view left, std::string_view right) {
    return left.size() == right.size() &&
           std::equal(left.begin(), left.end(), right.begin(),
                      [](unsigned char lhs, unsigned char rhs) { return std::tolower(lhs) == std::tolower(rhs); });
}

Status invalid_resolved_endpoint() {
    return Status::InvalidArgument(kInvalidResolvedEndpoint);
}

bool ip_address_equal(std::string_view left, std::string_view right) {
    in_addr left_v4{};
    in_addr right_v4{};
    if (inet_pton(AF_INET, std::string(left).c_str(), &left_v4) == 1 &&
        inet_pton(AF_INET, std::string(right).c_str(), &right_v4) == 1) {
        return std::memcmp(&left_v4, &right_v4, sizeof(left_v4)) == 0;
    }

    in6_addr left_v6{};
    in6_addr right_v6{};
    return inet_pton(AF_INET6, std::string(left).c_str(), &left_v6) == 1 &&
           inet_pton(AF_INET6, std::string(right).c_str(), &right_v6) == 1 &&
           std::memcmp(&left_v6, &right_v6, sizeof(left_v6)) == 0;
}

bool numeric_host_matches_addresses(const ResolvedHttpEndpoint& endpoint) {
    return !is_valid_ip(endpoint.host) ||
           (endpoint.addresses.size() == 1 && ip_address_equal(endpoint.host, endpoint.addresses.front()));
}

} // namespace

StatusOr<ResolvedHttpEndpoint> resolve_http_endpoint(const std::string& url, OutboundHttpAddressPolicy address_policy) {
    ASSIGN_OR_RETURN(std::string host, extract_host_from_url(url));
    ASSIGN_OR_RETURN(int port, extract_port_from_url(url));
    ASSIGN_OR_RETURN(std::vector<std::string> addresses, resolve_hostname_all_ips(host));
    ResolvedHttpEndpoint endpoint{
            .host = std::move(host),
            .port = port,
            .addresses = std::move(addresses),
    };
    RETURN_IF_ERROR(validate_resolved_http_endpoint(url, endpoint));
    if (address_policy != OutboundHttpAddressPolicy::ALLOW_ANY &&
        std::any_of(endpoint.addresses.begin(), endpoint.addresses.end(), is_link_local_ip)) {
        return invalid_resolved_endpoint();
    }
    return endpoint;
}

Status validate_resolved_http_endpoint(const std::string& url, const ResolvedHttpEndpoint& endpoint) {
    if (endpoint.host.empty() || endpoint.port <= 0 || endpoint.port > 65535 || endpoint.addresses.empty()) {
        return invalid_resolved_endpoint();
    }

    auto request_host = extract_host_from_url(url);
    auto request_port = extract_port_from_url(url);
    if (!request_host.ok() || !request_port.ok() || !host_equal(request_host.value(), endpoint.host) ||
        request_port.value() != endpoint.port) {
        return invalid_resolved_endpoint();
    }

    if (std::any_of(endpoint.addresses.begin(), endpoint.addresses.end(),
                    [](const std::string& address) { return !is_valid_ip(address); }) ||
        !numeric_host_matches_addresses(endpoint)) {
        return invalid_resolved_endpoint();
    }
    return Status::OK();
}

bool http_endpoint_needs_dns_pinning(const ResolvedHttpEndpoint& endpoint) {
    return !is_valid_ip(endpoint.host);
}

StatusOr<std::string> make_curl_resolve_entry(const ResolvedHttpEndpoint& endpoint) {
    if (endpoint.host.empty() || endpoint.port <= 0 || endpoint.port > 65535 || endpoint.addresses.empty() ||
        std::any_of(endpoint.addresses.begin(), endpoint.addresses.end(),
                    [](const std::string& address) { return !is_valid_ip(address); }) ||
        !numeric_host_matches_addresses(endpoint)) {
        return invalid_resolved_endpoint();
    }

    std::string entry = endpoint.host + ":" + std::to_string(endpoint.port) + ":";
    for (size_t index = 0; index < endpoint.addresses.size(); ++index) {
        if (index > 0) {
            entry.push_back(',');
        }
        const std::string& address = endpoint.addresses[index];
        if (address.find(':') != std::string::npos) {
            entry.push_back('[');
            entry.append(address);
            entry.push_back(']');
        } else {
            entry.append(address);
        }
    }
    return entry;
}

} // namespace starrocks
