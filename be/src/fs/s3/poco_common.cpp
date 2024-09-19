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

#include "fs/s3/poco_common.h"

#include <Poco/Exception.h>
#include <fmt/format.h>

#include <memory>
#include <sstream>

namespace starrocks::poco {

bool isHTTPS(const Poco::URI& uri) {
    if (uri.getScheme() == "https")
        return true;
    else if (uri.getScheme() == "http")
        return false;
    else
        throw std::runtime_error(fmt::format("Unsupported scheme in URI '{}'", uri.toString()));
}

HTTPSessionPtr makeHTTPSessionImpl(const std::string& host, Poco::UInt16 port, bool https, bool keep_alive,
                                   bool resolve_host) {
    HTTPSessionPtr session;

    if (https) {
        session = std::make_shared<Poco::Net::HTTPSClientSession>(host, port);
    } else {
        session = std::make_shared<Poco::Net::HTTPClientSession>(host, port);
    }

    // doesn't work properly without patch
    session->setKeepAlive(keep_alive);
    return session;
}

void setTimeouts(Poco::Net::HTTPClientSession& session, const ConnectionTimeouts& timeouts) {
    session.setTimeout(timeouts.connection_timeout, timeouts.send_timeout, timeouts.receive_timeout);
    session.setKeepAliveTimeout(timeouts.http_keep_alive_timeout);
}

HTTPSessionPtr makeHTTPSession(const Poco::URI& uri, const ConnectionTimeouts& timeouts, bool resolve_host) {
    const std::string& host = uri.getHost();
    Poco::UInt16 port = uri.getPort();
    bool https = isHTTPS(uri);

    auto session = makeHTTPSessionImpl(host, port, https, false, resolve_host);
    setTimeouts(*session, timeouts);
    return session;
}

std::string getCurrentExceptionMessage() {
    std::stringstream ss;

    try {
        throw;
    } catch (const Poco::Exception& e) {
        ss << fmt::format("Poco::Exception. e.code() = {}, e.displayText() = {}, e.what() = {}", e.code(),
                          e.displayText(), e.what());
    } catch (const std::exception& e) {
        ss << fmt::format("std::exception. type: {}, e.what() = {}", typeid(e).name(), e.what());
    } catch (...) {
        ss << fmt::format("Unknown exception from poco client");
    }

    return ss.str();
}

// 1) https://aws.amazon.com/premiumsupport/knowledge-center/s3-resolve-200-internalerror/
// 2) https://github.com/aws/aws-sdk-cpp/issues/658
bool checkRequestCanReturn2xxAndErrorInBody(Aws::Http::HttpRequest& request) {
    auto query_params = request.GetQueryStringParameters();
    if (request.HasHeader("x-amz-copy-source")) {
        // CopyObject https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html
        if (query_params.empty()) return true;

        // UploadPartCopy https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html
        if (query_params.contains("partNumber") && query_params.contains("uploadId")) return true;

    } else {
        // CompleteMultipartUpload https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
        if (query_params.size() == 1 && query_params.contains("uploadId")) return true;
    }

    return false;
}

} // namespace starrocks::poco
