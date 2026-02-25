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

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/URI.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/Scheme.h>

#include <string>

#include "fs/s3/poco_common.h"

namespace Aws::Http::Standard {
class StandardHttpResponse;
}

namespace starrocks::poco {

class PocoHttpClient : public Aws::Http::HttpClient {
public:
    explicit PocoHttpClient(const Aws::Client::ClientConfiguration& clientConfiguration);
    ~PocoHttpClient() override = default;

    std::shared_ptr<Aws::Http::HttpResponse> MakeRequest(
            const std::shared_ptr<Aws::Http::HttpRequest>& request,
            Aws::Utils::RateLimits::RateLimiterInterface* readLimiter,
            Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter) const override;

private:
    void MakeRequestInternal(Aws::Http::HttpRequest& request,
                             std::shared_ptr<Aws::Http::Standard::StandardHttpResponse>& response,
                             Aws::Utils::RateLimits::RateLimiterInterface* readLimiter,
                             Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter) const;

    ConnectionTimeouts timeouts;
};

} // namespace starrocks::poco
