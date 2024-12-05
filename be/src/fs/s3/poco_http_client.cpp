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

#include "fs/s3/poco_http_client.h"

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/StreamCopier.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/standard/StandardHttpResponse.h>
#include <aws/core/monitoring/HttpClientMetrics.h>
#include <aws/core/utils/ratelimiter/RateLimiterInterface.h>
#include <fmt/format.h>

#include <istream>
#include <utility>

#include "common/logging.h"
#include "fs/s3/poco_common.h"
#include "io/s3_zero_copy_iostream.h"

namespace starrocks::poco {

PocoHttpClient::PocoHttpClient(const Aws::Client::ClientConfiguration& clientConfiguration)
        : timeouts(ConnectionTimeouts(
                  Poco::Timespan(clientConfiguration.connectTimeoutMs * 1000),     // connection timeout.
                  Poco::Timespan(clientConfiguration.httpRequestTimeoutMs * 1000), // send timeout.
                  Poco::Timespan(clientConfiguration.httpRequestTimeoutMs * 1000)  // receive timeout.
                  )) {}

std::shared_ptr<Aws::Http::HttpResponse> PocoHttpClient::MakeRequest(
        const std::shared_ptr<Aws::Http::HttpRequest>& request,
        Aws::Utils::RateLimits::RateLimiterInterface* readLimiter,
        Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter) const {
    auto response = Aws::MakeShared<Aws::Http::Standard::StandardHttpResponse>("PocoHttpClient", request);
    MakeRequestInternal(*request, response, readLimiter, writeLimiter);
    return response;
}

void PocoHttpClient::MakeRequestInternal(Aws::Http::HttpRequest& request,
                                         std::shared_ptr<Aws::Http::Standard::StandardHttpResponse>& response,
                                         Aws::Utils::RateLimits::RateLimiterInterface*,
                                         Aws::Utils::RateLimits::RateLimiterInterface*) const {
    auto uri = request.GetUri().GetURIString();

    const int MAX_REDIRECT_ATTEMPTS = 10;
    try {
        for (int attempt = 0; attempt < MAX_REDIRECT_ATTEMPTS; ++attempt) {
            Poco::URI poco_uri(uri);

            // URI may changed, because of redirection
            auto session = makeHTTPSession(poco_uri, timeouts, false);

            Poco::Net::HTTPRequest poco_request(Poco::Net::HTTPRequest::HTTP_1_1);

            /** According to RFC-2616, Request-URI is allowed to be encoded.
            * However, there is no clear agreement on which exact symbols must be encoded.
            * Effectively, `Poco::URI` chooses smaller subset of characters to encode,
            * whereas Amazon S3 and Google Cloud Storage expects another one.
            * In order to successfully execute a request, a path must be exact representation
            * of decoded path used by `AWSAuthSigner`.
            * Therefore we shall encode some symbols "manually" to fit the signatures.
            */

            std::string path_and_query;
            const std::string& query = poco_uri.getRawQuery();
            const std::string reserved = "?#:;+@&=%"; // Poco::URI::RESERVED_QUERY_PARAM without '/' plus percent sign.
            Poco::URI::encode(poco_uri.getPath(), reserved, path_and_query);

            // `target_uri.getPath()` could return an empty string, but a proper HTTP request must
            // always contain a non-empty URI in its first line (e.g. "POST / HTTP/1.1" or "POST /?list-type=2 HTTP/1.1").
            if (path_and_query.empty()) path_and_query = "/";

            // Append the query param to URI
            if (!query.empty()) {
                path_and_query += '?';
                path_and_query += query;
            }

            poco_request.setURI(path_and_query);

            switch (request.GetMethod()) {
            case Aws::Http::HttpMethod::HTTP_GET:
                poco_request.setMethod(Poco::Net::HTTPRequest::HTTP_GET);
                break;
            case Aws::Http::HttpMethod::HTTP_POST:
                poco_request.setMethod(Poco::Net::HTTPRequest::HTTP_POST);
                break;
            case Aws::Http::HttpMethod::HTTP_DELETE:
                poco_request.setMethod(Poco::Net::HTTPRequest::HTTP_DELETE);
                break;
            case Aws::Http::HttpMethod::HTTP_PUT:
                poco_request.setMethod(Poco::Net::HTTPRequest::HTTP_PUT);
                break;
            case Aws::Http::HttpMethod::HTTP_HEAD:
                poco_request.setMethod(Poco::Net::HTTPRequest::HTTP_HEAD);
                break;
            case Aws::Http::HttpMethod::HTTP_PATCH:
                poco_request.setMethod(Poco::Net::HTTPRequest::HTTP_PATCH);
                break;
            }

            for (const auto& [header_name, header_value] : request.GetHeaders()) {
                poco_request.set(header_name, header_value);
            }

            auto& request_body_stream = session->sendRequest(poco_request);

            if (request.GetContentBody()) {
                // Rewind content body buffer.
                // NOTE: we should do that always (even if `attempt == 0`) because the same request can be retried also by AWS,
                // see retryStrategy in Aws::Client::ClientConfiguration.
                request.GetContentBody()->clear();
                request.GetContentBody()->seekg(0);

                // Todo, optimize this with zero copy
                [[maybe_unused]] auto size =
                        Poco::StreamCopier::copyStream(*request.GetContentBody(), request_body_stream);
            }

            Poco::Net::HTTPResponse poco_response;

            if (dynamic_cast<starrocks::io::S3ZeroCopyIOStream*>(&(response->GetResponseBody()))) {
                poco_response.setResponseIOStream(
                        &(response->GetResponseBody()),
                        (static_cast<Aws::Utils::Stream::PreallocatedStreamBuf*>(response->GetResponseBody().rdbuf()))
                                ->GetBuffer(),
                        (static_cast<starrocks::io::S3ZeroCopyIOStream&>(response->GetResponseBody())).getSize());
            }

            auto& response_body_stream = session->receiveResponse(poco_response);

            int status_code = static_cast<int>(poco_response.getStatus());

            if (poco_response.getStatus() == Poco::Net::HTTPResponse::HTTP_TEMPORARY_REDIRECT) {
                auto location = poco_response.get("location");
                uri = location;
                continue;
            }

            response->SetResponseCode(static_cast<Aws::Http::HttpResponseCode>(status_code));
            response->SetContentType(poco_response.getContentType());

            for (const auto& [header_name, header_value] : poco_response) {
                response->AddHeader(header_name, header_value);
            }

            // Request is successful but for some special requests we can have actual error message in body
            // TODO, there is one more copy, but in these cases that RequestCanReturn2xxAndErrorInBody, it's trivial.
            if (status_code >= ResponseCode::SUCCESS_RESPONSE_MIN &&
                status_code <= ResponseCode::SUCCESS_RESPONSE_MAX && checkRequestCanReturn2xxAndErrorInBody(request)) {
                // reading the full response
                std::string response_string((std::istreambuf_iterator<char>(response_body_stream)),
                                            std::istreambuf_iterator<char>());

                const static std::string_view needle = "<Error>";
                if (auto it = std::search(response_string.begin(), response_string.end(),
                                          std::default_searcher(needle.begin(), needle.end()));
                    it != response_string.end()) {
                    LOG(WARNING) << "Response for request contain <Error> tag in body, settings internal server error "
                                    "(500 code)";
                    response->SetResponseCode(Aws::Http::HttpResponseCode::INTERNAL_SERVER_ERROR);
                }

                if (!poco_response.isBodyFilled()) {
                    response->GetResponseBody().write(response_string.data(), response_string.size());
                }
            } else {
                if (status_code == ResponseCode::TOO_MANY_REQUEST || status_code == ResponseCode::SERVICE_UNAVALIABLE) {
                    // pass
                    // TODO, throttling
                }
                if (status_code >= ResponseCode::SUCCESS_RESPONSE_MIN &&
                    status_code <= ResponseCode::SUCCESS_RESPONSE_MAX) {
                    if (!poco_response.isBodyFilled()) {
                        Poco::StreamCopier::copyStream(response_body_stream, response->GetResponseBody());
                    }
                } else {
                    std::string error_message;
                    Poco::StreamCopier::copyToString(response_body_stream, error_message);

                    response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
                    response->SetClientErrorMessage(error_message);
                }
            }

            return;
        }
        throw std::runtime_error(
                fmt::format("Too many redirects while trying to access {}", request.GetUri().GetURIString()));
    } catch (...) {
        response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
        response->SetClientErrorMessage(getCurrentExceptionMessage());
    }
}

} // namespace starrocks::poco
