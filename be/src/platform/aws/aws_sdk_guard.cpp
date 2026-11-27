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

#include "platform/aws/aws_sdk_guard.h"

#include <aws/core/utils/logging/LogLevel.h>

#include <boost/algorithm/string.hpp>
#include <iostream>
#include <memory>

#include "common/config_object_storage_fwd.h"
#include "common/configbase.h"
#include "common/logging.h"
#include "platform/aws/object_storage_fast_retry.h"
#include "platform/aws/poco_common.h"
#include "platform/aws/poco_http_client_factory.h"

namespace starrocks {

namespace {

Aws::Utils::Logging::LogLevel parse_aws_sdk_log_level(const std::string& s) {
    Aws::Utils::Logging::LogLevel levels[] = {
            Aws::Utils::Logging::LogLevel::Off,   Aws::Utils::Logging::LogLevel::Fatal,
            Aws::Utils::Logging::LogLevel::Error, Aws::Utils::Logging::LogLevel::Warn,
            Aws::Utils::Logging::LogLevel::Info,  Aws::Utils::Logging::LogLevel::Debug,
            Aws::Utils::Logging::LogLevel::Trace,
    };
    std::string slevel = boost::algorithm::to_upper_copy(s);
    Aws::Utils::Logging::LogLevel level = Aws::Utils::Logging::LogLevel::Warn;
    for (auto& idx : levels) {
        auto name = Aws::Utils::Logging::GetLogLevelName(idx);
        if (name == slevel) {
            level = idx;
            break;
        }
    }
    return level;
}

} // namespace

AwsSdkGuard::AwsSdkGuard(CurlLifecycle curl_lifecycle) {
    // starrocks_main initializes libcurl before constructing this guard.
    // Standalone tools should use SDK_MANAGED.
    _options.httpOptions.initAndCleanupCurl = curl_lifecycle == CurlLifecycle::SDK_MANAGED;
    if (config::aws_sdk_logging_trace_enabled) {
        auto level = parse_aws_sdk_log_level(config::aws_sdk_logging_trace_level);
        std::cerr << "enable aws sdk logging trace. log level = " << Aws::Utils::Logging::GetLogLevelName(level)
                  << "\n";
        _options.loggingOptions.logLevel = level;
    }
    if (config::aws_sdk_enable_compliant_rfc3986_encoding) {
        _options.httpOptions.compliantRfc3986Encoding = true;
    }
    Aws::InitAPI(_options);
    if (config::enable_poco_client_for_aws_sdk) {
        Aws::Http::SetHttpClientFactory(std::make_shared<poco::PocoHttpClientFactory>());
        if (config::enable_object_storage_fast_retry) {
            LOG(WARNING) << "object storage fast retry is off: it does not apply to the Poco HTTP client, "
                            "enable_poco_client_for_aws_sdk is true";
        }
    } else if (curl_lifecycle == CurlLifecycle::EXTERNALLY_MANAGED) {
        // Installed whatever enable_object_storage_fast_retry says, so that the switch can be turned on and off at
        // runtime: while it is false, the client sends every request as the SDK's own client does.
        // Replacing the factory cleans up the static state of the previous one, which leaves libcurl alone only
        // when libcurl is set up outside the SDK, as in the BE process. Standalone tools keep the SDK's client.
        Aws::Http::SetHttpClientFactory(std::make_shared<FastRetryHttpClientFactory>());
        LOG(INFO) << "object storage fast retry client is installed, enable_object_storage_fast_retry="
                  << config::enable_object_storage_fast_retry;
    }
}

AwsSdkGuard::~AwsSdkGuard() {
    if (config::enable_poco_client_for_aws_sdk) {
        poco::HTTPSessionPools::instance().shutdown();
    }
    Aws::ShutdownAPI(_options);
}

} // namespace starrocks
