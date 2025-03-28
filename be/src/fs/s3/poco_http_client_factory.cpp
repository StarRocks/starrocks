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

#include "fs/s3/poco_http_client_factory.h"

#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/standard/StandardHttpRequest.h>

#include "fs/s3/poco_http_client.h"

namespace starrocks::poco {

std::shared_ptr<Aws::Http::HttpClient> PocoHttpClientFactory::CreateHttpClient(
        const Aws::Client::ClientConfiguration& clientConfiguration) const {
    return std::make_shared<PocoHttpClient>(clientConfiguration);
}

std::shared_ptr<Aws::Http::HttpRequest> PocoHttpClientFactory::CreateHttpRequest(
        const Aws::String& uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory& streamFactory) const {
    return CreateHttpRequest(Aws::Http::URI(uri), method, streamFactory);
}

std::shared_ptr<Aws::Http::HttpRequest> PocoHttpClientFactory::CreateHttpRequest(
        const Aws::Http::URI& uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory& streamFactory) const {
    auto request = Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>("PocoHttpClientFactory", uri, method);
    request->SetResponseStreamFactory(streamFactory);

    return request;
}

} // namespace starrocks::poco
