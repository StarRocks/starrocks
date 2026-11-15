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

#pragma once

#include <string>
#include <vector>

#include "base/status.h"
#include "base/statusor.h"

namespace starrocks {

// An immutable DNS snapshot for one outbound HTTP endpoint. Transport clients
// can validate this snapshot against the request URL and use it with
// CURLOPT_RESOLVE, so URL validation and connection establishment cannot
// observe different DNS answers.
struct ResolvedHttpEndpoint {
    std::string host;
    int port = 0;
    std::vector<std::string> addresses;
};

enum class OutboundHttpAddressPolicy {
    ALLOW_ANY,
    BLOCK_LINK_LOCAL,
};

StatusOr<ResolvedHttpEndpoint> resolve_http_endpoint(
        const std::string& url, OutboundHttpAddressPolicy address_policy = OutboundHttpAddressPolicy::ALLOW_ANY);
Status validate_resolved_http_endpoint(const std::string& url, const ResolvedHttpEndpoint& endpoint);
bool http_endpoint_needs_dns_pinning(const ResolvedHttpEndpoint& endpoint);
StatusOr<std::string> make_curl_resolve_entry(const ResolvedHttpEndpoint& endpoint);

} // namespace starrocks
